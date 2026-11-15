"""Phase 3 end-to-end check against the running stack (from the Windows host):

    .venv\\Scripts\\python scripts\\smoke_gateway.py

Logs in as three demo users through the gateway (http://localhost:8000), performs allowed, ACL-denied,
unknown-resource, PDP-blocked and post-revocation requests across the three applications, then reads the
`access-events` Kafka topic and checks that every request produced exactly the expected event (or none),
keyed by user. Also checks the PostgreSQL audit log. Each request carries its own X-Request-ID, which the
gateway copies into the event's correlation_id, so the check matches requests to events exactly.

The PDP Block used here is written by this script (the PDP service arrives in Phase 8) and removed again.
"""
import argparse
import json
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

from accesssentinel.settings import get_settings  # noqa: E402
from accesssentinel.topics import ACCESS_EVENTS, topic_name  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gateway", default="http://localhost:8000")
    ap.add_argument("--timeout", type=float, default=45.0)
    args = ap.parse_args(argv)
    s = get_settings()
    password = s.secret("demo_user_password")
    t_start = time.time()

    import redis as redis_lib
    r = redis_lib.Redis(**s.redis_kwargs(), decode_responses=True)

    # wait for readiness
    deadline = time.time() + 60
    while True:
        try:
            if httpx.get(f"{args.gateway}/readyz", timeout=3).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        if time.time() > deadline:
            print("FAIL gateway not ready at", args.gateway)
            return 1
        time.sleep(2)

    clients = {u: httpx.Client(base_url=args.gateway, timeout=10) for u in ("alice", "bob", "carol", "anon")}
    for u in ("alice", "bob", "carol"):
        resp = clients[u].post("/auth/login", json={"username": u, "password": password})
        if resp.status_code != 200:
            print(f"FAIL login {u}: HTTP {resp.status_code} {resp.text[:200]}")
            return 1

    # (label, user, method, path, body, expected HTTP status, expected event or None)
    steps = [
        ("hr_manager reads sensitive payroll", "alice", "GET", "/apps/hr/payroll", None, 200,
         dict(a="hr", r="payroll", s=1, o="view", p="read", y=1, reason="allow")),
        ("hr_manager exports payroll", "alice", "GET", "/apps/hr/payroll/export", None, 200,
         dict(a="hr", r="payroll", s=1, o="export", p="export", y=1, reason="allow")),
        ("hr_manager creates a job opening", "alice", "POST", "/apps/hr/recruiting",
         {"position": "Data Engineer", "status": "open"}, 201,
         dict(a="hr", r="recruiting", s=0, o="edit", p="write", y=1, reason="allow")),
        ("staff tries payroll (ACL deny)", "carol", "GET", "/apps/hr/payroll", None, 403,
         dict(a="hr", r="payroll", s=1, o="view", p="read", y=0, reason="acl-deny")),
        ("staff reads directory", "carol", "GET", "/apps/hr/directory", None, 200,
         dict(a="hr", r="directory", s=0, o="view", p="read", y=1, reason="allow")),
        ("same user, second application", "carol", "GET", "/apps/docs/handbook", None, 200,
         dict(a="docs", r="handbook", s=0, o="view", p="read", y=1, reason="allow")),
        ("staff probes an unmapped path", "carol", "GET", "/apps/hr/salary-dump", None, 404,
         dict(a="hr", r="unmapped:salary-dump", s=1, o="view", p="read", y=0, reason="unknown-resource")),
        ("finance analyst reads payments", "bob", "GET", "/apps/finance/payments", None, 200,
         dict(a="finance", r="payments", s=1, o="view", p="read", y=1, reason="allow")),
        ("finance analyst deletes a payment (needs admin)", "bob", "DELETE", "/apps/finance/payments/1", None, 403,
         dict(a="finance", r="payments", s=1, o="delete", p="admin", y=0, reason="acl-deny")),
        ("#block bob", None, None, None, None, None, None),
        ("cached PDP Block enforced at request time", "bob", "GET", "/apps/finance/vendor-banking", None, 403,
         dict(a="finance", r="vendor-banking", s=1, o="view", p="read", y=0, reason="pdp-block:smoke-test")),
        ("#unblock bob", None, None, None, None, None, None),
        ("allowed again after the Block is removed", "bob", "GET", "/apps/finance/vendor-banking", None, 200,
         dict(a="finance", r="vendor-banking", s=1, o="view", p="read", y=1, reason="allow")),
        ("#revoke carol", None, None, None, None, None, None),
        ("revoked session is rejected (no event)", "carol", "GET", "/apps/hr/directory", None, 401, None),
        ("anonymous request is rejected (no event)", "anon", "GET", "/apps/hr/directory", None, 401, None),
    ]

    results, expected = [], {}
    try:
        for label, user, method, path, body, status, exp in steps:
            if label == "#block bob":
                r.hset("decision:bob", mapping={"decision": "Block", "reason": "smoke-test",
                                                "decided_at": time.time(), "version": 1})
                continue
            if label == "#unblock bob":
                r.delete("decision:bob")
                continue
            if label == "#revoke carol":
                resp = httpx.post(f"{args.gateway}/internal/sessions/revoke", json={"user": "carol", "reason": "smoke"},
                                  headers={"X-AS-Internal-Token": s.secret("internal_token")}, timeout=10)
                ok = resp.status_code == 200 and resp.json().get("revoked_sessions", 0) >= 1
                results.append((ok, "internal API revokes carol's session", f"HTTP {resp.status_code} {resp.text[:80]}"))
                continue
            cid = uuid.uuid4().hex
            resp = clients[user].request(method, path, json=body, headers={"X-Request-ID": cid})
            ok = resp.status_code == status and resp.headers.get("x-request-id") == cid
            results.append((ok, f"{label}: {user} {method} {path}", f"HTTP {resp.status_code} (expected {status})"))
            expected[cid] = (label, user, exp)
    finally:
        r.delete("decision:bob")

    # ---- read the events back from Kafka
    from confluent_kafka import Consumer
    consumer = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"smoke-{uuid.uuid4().hex[:8]}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([topic_name(ACCESS_EVENTS, s.kafka_topic_prefix)])
    seen, partitions = {}, {}
    want = {cid for cid, (_, _, exp) in expected.items() if exp is not None}
    deadline = time.time() + args.timeout
    while time.time() < deadline and not want <= set(seen):
        for m in consumer.consume(500, 1.0):
            if m.error():
                continue
            ev = json.loads(m.value())
            if ev.get("correlation_id") in expected:
                seen.setdefault(ev["correlation_id"], []).append(ev)
                partitions.setdefault(ev["u"], set()).add(m.partition())
                if m.key().decode() != ev["u"]:
                    results.append((False, "message key is the user id", f"key={m.key()!r} u={ev['u']}"))
    consumer.close()

    for cid, (label, user, exp) in expected.items():
        got = seen.get(cid, [])
        if exp is None:
            results.append((not got, f"event check: {label}", "no event" if not got else f"{len(got)} unexpected event(s)"))
            continue
        if len(got) != 1:
            results.append((False, f"event check: {label}", f"{len(got)} events"))
            continue
        ev = got[0]
        actual = dict(a=ev["a"], r=ev["r"], s=ev["s"], o=ev["o"], p=ev["p"], y=ev["y"], reason=ev["meta"].get("reason"))
        ok = (actual == exp and ev["u"] == user and ev["schema_version"] == 1 and len(ev["event_id"]) == 32
              and {"ip", "device", "session"} <= set(ev["c"]))
        results.append((ok, f"event check: {label}", f"u={ev['u']} " + " ".join(f"{k}={v}" for k, v in actual.items())))
    split = {u: sorted(p) for u, p in partitions.items() if len(p) > 1}
    results.append((not split, "each user's events are in a single partition",
                    ", ".join(f"{u}->p{sorted(p)[0]}" for u, p in sorted(partitions.items()))))

    # ---- audit log
    import psycopg
    with psycopg.connect(s.postgres_dsn(), connect_timeout=5) as conn:
        rows = dict(conn.execute("SELECT action, count(*) FROM audit_log WHERE ts >= to_timestamp(%s) GROUP BY action",
                                 (t_start - 1,)).fetchall())
    results.append((rows.get("login", 0) >= 3 and rows.get("sessions_revoked", 0) >= 1, "audit log rows", str(rows)))

    for ok, what, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {what}  [{detail}]")
    failed = sum(1 for ok, _, _ in results if not ok)
    print(f"{len(results) - failed}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

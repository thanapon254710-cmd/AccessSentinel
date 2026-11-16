"""Phase 4 live checks. Run INSIDE the Docker network (reaches gateway, Kafka and HDFS by service name):

    docker compose run --rm -T tools python scripts/smoke_streaming.py traffic --n 120 > run.json
    docker compose run --rm -T tools python scripts/smoke_streaming.py verify < run.json
    docker compose run --rm -T tools python scripts/smoke_streaming.py poison

traffic : logs in as several demo users and sends a mix of allowed / denied requests through the gateway;
          prints the correlation ids and expected (u, a, r, y) of every request as JSON.
verify  : waits until the hdfs-sink and repartitioner groups have zero lag, then checks that every request
          is in HDFS EXACTLY ONCE (date/app layout, fields intact) and in the resource-keyed topic with the
          composite key, one partition per key.
poison  : publishes one malformed record to access-events and checks that both consumer groups dead-letter
          it (with error headers) and keep going (lag returns to zero).
"""
import argparse
import json
import os
import random
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from accesssentinel.adapters.hdfs_lake import make_lake  # noqa: E402
from accesssentinel.streaming.resource_windows import rstar_of, shard_of, window_of  # noqa: E402
from accesssentinel.settings import get_settings  # noqa: E402
from accesssentinel.streaming.lag import GROUP_TOPICS, group_lag  # noqa: E402
from accesssentinel.topics import ACCESS_EVENTS, ACCESS_EVENTS_BY_RESOURCE, topic_name  # noqa: E402

REQUESTS = [  # (user, path) - outcome decided by the gateway's ACL
    ("alice", "/apps/hr/payroll"), ("alice", "/apps/hr/employees/3"), ("alice", "/apps/hr/reviews"),
    ("bob", "/apps/finance/payments"), ("bob", "/apps/finance/vendor-banking"), ("bob", "/apps/hr/payroll"),
    ("carol", "/apps/hr/directory"), ("carol", "/apps/docs/handbook"), ("carol", "/apps/finance/payments"),
    ("erin", "/apps/docs/contracts"), ("erin", "/apps/docs/board-minutes"), ("grace", "/apps/finance/audit-reports"),
    ("grace", "/apps/hr/payroll/export"), ("frank", "/apps/docs/board-minutes"), ("dave", "/apps/hr/payroll"),
]


def lag_total(s) -> dict:
    out = {}
    for g, topics in GROUP_TOPICS.items():
        out[g] = group_lag(s.kafka_bootstrap, g, [topic_name(t, s.kafka_topic_prefix) for t in topics])["total"]
    return out


def wait_lag_zero(s, timeout: float) -> dict:
    deadline = time.time() + timeout
    while True:
        lags = lag_total(s)
        if all(v == 0 for v in lags.values()) or time.time() > deadline:
            return lags
        time.sleep(2)


def cmd_traffic(args) -> int:
    import httpx
    s = get_settings()
    pw = s.secret("demo_user_password")
    clients = {}
    rng = random.Random(args.seed)
    out = []
    for i in range(args.n):
        user, path = REQUESTS[rng.randrange(len(REQUESTS))]
        if user not in clients:
            c = httpx.Client(base_url=args.gateway, timeout=10)
            assert c.post("/auth/login", json={"username": user, "password": pw}).status_code == 200, user
            clients[user] = c
        cid = uuid.uuid4().hex
        r = clients[user].get(path, headers={"X-Request-ID": cid})
        parts = path.split("/")
        out.append({"cid": cid, "u": user, "a": parts[2], "r": parts[3], "status": r.status_code,
                    "y": 1 if r.status_code == 200 else 0})
    json.dump({"t0": time.time() - 3600, "requests": out}, sys.stdout)
    print(f"sent {len(out)} requests ({sum(x['y'] for x in out)} allowed)", file=sys.stderr)
    return 0


def cmd_verify(args) -> int:
    s = get_settings()
    run = json.load(sys.stdin)
    reqs = {x["cid"]: x for x in run["requests"]}
    results = []
    lags = wait_lag_zero(s, args.timeout)
    results.append((all(v == 0 for v in lags.values()), "consumer lag is zero", str(lags)))

    # ---- HDFS: exactly once, fields intact, date/app layout
    lake = make_lake(s)
    rows = [e for e in lake.read_events(run["t0"], time.time() + 3600, dedup=False) if e.correlation_id in reqs]
    counts = {}
    for e in rows:
        counts[e.correlation_id] = counts.get(e.correlation_id, 0) + 1
    missing = [c for c in reqs if c not in counts]
    dups = {c: n for c, n in counts.items() if n > 1}
    results.append((not missing, "every request is in HDFS", f"{len(counts)}/{len(reqs)} found, missing={len(missing)}"))
    results.append((not dups, "no request is in HDFS twice (raw files, before read-time dedup)",
                    f"duplicates={len(dups)}"))
    deduped = [e for e in lake.read_events(run["t0"], time.time() + 3600) if e.correlation_id in reqs]
    results.append((len(deduped) == len(reqs) == len({e.correlation_id for e in deduped}),
                    "history read (with id dedup) returns each request exactly once", f"{len(deduped)}/{len(reqs)}"))
    bad = [e for e in rows if (e.u, e.a, e.r, e.y) != (reqs[e.correlation_id]["u"], reqs[e.correlation_id]["a"],
                                                      reqs[e.correlation_id]["r"], reqs[e.correlation_id]["y"])]
    results.append((not bad, "HDFS rows match the requests (u, a, r, y)", f"mismatches={len(bad)}"))
    apps = {p["app"] for p in lake.partitions()}
    results.append(({"hr", "finance", "docs"} <= apps, "history partitioned by date and application",
                    ", ".join(f"{p['date']}/{p['app']}:{p['files']} files" for p in lake.partitions())))

    # ---- resource-keyed view
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"verify-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic_name(ACCESS_EVENTS_BY_RESOURCE, s.kafka_topic_prefix)])
    seen, key_parts, wrong_key = {}, {}, 0
    deadline = time.time() + args.timeout
    while time.time() < deadline and len(seen) < len(reqs):
        for m in c.consume(500, 1.0):
            if m.error():
                continue
            ev = json.loads(m.value())
            if ev.get("correlation_id") not in reqs:
                continue
            seen[ev["correlation_id"]] = seen.get(ev["correlation_id"], 0) + 1
            want = f"{rstar_of(ev)}|{window_of(float(ev['t']), s.res_window_seconds)}|{shard_of(ev['u'], s.res_shards)}"
            if m.key().decode() != want:
                wrong_key += 1
            key_parts.setdefault(m.key(), set()).add(m.partition())
    c.close()
    results.append((len(seen) == len(reqs), "every request is in the resource-keyed topic", f"{len(seen)}/{len(reqs)}"))
    results.append((wrong_key == 0, "resource-view key = r*|window|shard (v3 Stage 1)", f"wrong={wrong_key}"))
    results.append((all(len(p) == 1 for p in key_parts.values()), "each resource key in one partition",
                    f"{len(key_parts)} keys"))
    redelivered = sum(n - 1 for n in seen.values())
    results.append((redelivered == 0, "resource view exactly once (transactional repartitioner)", f"duplicates={redelivered}"))
    return report(results)


def cmd_poison(args) -> int:
    from confluent_kafka import Consumer, Producer
    s = get_settings()
    marker = uuid.uuid4().hex
    p = Producer({"bootstrap.servers": s.kafka_bootstrap})
    p.produce(topic_name(ACCESS_EVENTS, s.kafka_topic_prefix), key=b"poison-test",
              value=json.dumps({"poison": marker, "u": "x"}).encode())
    p.flush(10)
    results = []
    dlq = topic_name(ACCESS_EVENTS, s.kafka_topic_prefix) + ".dlq"
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"verify-{marker[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([dlq])
    groups = {}
    deadline = time.time() + args.timeout
    while time.time() < deadline and len(groups) < 2:
        for m in c.consume(100, 1.0):
            if m.error() or marker.encode() not in (m.value() or b""):
                continue
            h = {k: v.decode() for k, v in (m.headers() or [])}
            groups[h.get("as-consumer-group")] = h
    c.close()
    results.append(({"hdfs-sink", "repartitioner"} <= set(groups), "both consumer groups dead-lettered the record",
                     str(sorted(groups))))
    h = next(iter(groups.values()), {})
    results.append(("schema" in h.get("as-error", "") and h.get("as-source-offset", "").isdigit(),
                    "DLQ headers carry reason and source offset", str({k: h[k] for k in sorted(h) if k != "as-dead-lettered-at"})))
    lags = wait_lag_zero(s, args.timeout)
    results.append((all(v == 0 for v in lags.values()), "consumers moved past the poison record", str(lags)))
    return report(results)


def report(results) -> int:
    for ok, what, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {what}  [{detail}]")
    failed = sum(1 for ok, _, _ in results if not ok)
    print(f"{len(results) - failed}/{len(results)} checks passed")
    return 1 if failed else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("traffic"); t.add_argument("--n", type=int, default=120); t.add_argument("--seed", type=int, default=1)
    t.add_argument("--gateway", default=os.environ.get("AS_GATEWAY_URL", "http://gateway:8000"))
    v = sub.add_parser("verify"); v.add_argument("--timeout", type=float, default=120)
    p = sub.add_parser("poison"); p.add_argument("--timeout", type=float, default=90)
    args = ap.parse_args(argv)
    return {"traffic": cmd_traffic, "verify": cmd_verify, "poison": cmd_poison}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())

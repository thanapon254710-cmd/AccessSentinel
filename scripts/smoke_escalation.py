"""Phase 6 live checks: trigger each escalation path independently on the running stack. Run INSIDE Docker:

    docker compose run --rm -T tools python scripts/smoke_escalation.py a      # Path A: unseen synthetic attacker
    docker compose run --rm -T tools python scripts/smoke_escalation.py b      # Path B: 4 demo users via the gateway
    docker compose run --rm -T tools python scripts/smoke_escalation.py c      # Path C: sweep over HDFS history
    docker compose run --rm -T tools python scripts/smoke_escalation.py d      # Path D: needs AS_ESC_GAMMA=1 (test)

Synthetic users are renamed (syn-esc-*) with fresh event ids and tagged meta.source = "synthetic-replay".
"""
import argparse
import json
import os
import subprocess
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from accesssentinel.datasets.synth import SynthConfig, generate  # noqa: E402
from accesssentinel.settings import get_settings  # noqa: E402
from accesssentinel.topics import (ACCESS_EVENTS, ESCALATIONS_BACKGROUND, ESCALATIONS_REALTIME,  # noqa: E402
                                   topic_name)


def publish_renamed(s, events, new_u):
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True, "acks": "all"})
    for e in events:
        d = dict(e.__dict__)
        d.update(u=new_u, event_id=uuid.uuid4().hex, meta={**e.meta, "source": "synthetic-replay"})
        p.produce(topic_name(ACCESS_EVENTS, s.kafka_topic_prefix), key=new_u.encode(), value=json.dumps(d).encode())
    return p.flush(30)


def wait_escalations(s, topic, users, timeout, since=0.0, want=lambda r: True):
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"verify-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic_name(topic, s.kafka_topic_prefix)])
    got, deadline = [], time.time() + timeout
    while time.time() < deadline and not any(want(r) for r in got):
        for m in c.consume(500, 1.0):
            if not m.error():
                r = json.loads(m.value())
                if r["u"] in users and r["t"] >= since:
                    got.append(r)
    c.close()
    return got


def report(results):
    for ok, what, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {what}  [{detail}]")
    failed = sum(1 for ok, _, _ in results if not ok)
    print(f"{len(results) - failed}/{len(results)} checks passed")
    return 1 if failed else 0


def path_a(s, args):
    ds = generate(SynthConfig(seed=7))
    inst = ds.instances["compromised_account-test-0"]
    u = f"syn-esc-a-{uuid.uuid4().hex[:6]}"
    t0 = time.time()
    publish_renamed(s, [e for e in ds.events if e.u == inst["user"]], u)
    got = wait_escalations(s, ESCALATIONS_REALTIME, {u}, args.timeout, t0, lambda r: "high" in r["paths"])
    hit = [r for r in got if "high" in r["paths"]]
    return report([(bool(hit), f"Path A: realtime escalation of {u} (replayed {inst['kind']}, unseen in training)",
                    f"paths={hit[0]['paths']} s_ind={hit[0]['evidence']['high']['s_ind']} model={hit[0]['evidence']['high']['model_version']}"
                    if hit else f"{len(got)} escalations")])


def path_b(s, args):
    import httpx
    users = ["bob", "judy", "frank", "grace"]           # roles with read access to finance:audit-reports (s=1)
    pw = s.secret("demo_user_password")
    t0 = time.time()
    statuses = {}
    for u in users:
        c = httpx.Client(base_url=args.gateway, timeout=10)
        assert c.post("/auth/login", json={"username": u, "password": pw}).status_code == 200, u
        statuses[u] = c.get("/apps/finance/audit-reports").status_code
    # v3 (Phase 5): Path B comes from the FINAL event-time resource window (Delta_res = 15 min + grace), keyed by the
    # namespaced r* = tenant/app:resource, so the trigger can arrive up to ~16 min after the accesses.
    is_res = lambda r: "resource" in r["paths"] and str(r["evidence"]["resource"]["resource"]).endswith("finance:audit-reports")
    got = wait_escalations(s, ESCALATIONS_REALTIME, set(users), max(args.timeout, 1200.0), t0, is_res)
    res = [r for r in got if is_res(r)]
    parts = set(res[0]["evidence"]["resource"]["participants"]) if res else set()
    return report([(all(v == 200 for v in statuses.values()), "4 permitted users read finance/audit-reports via the gateway", str(statuses)),
                   (bool(res), "Path B: realtime escalation with resource evidence",
                    f"u={res[0]['u']} paths={res[0]['paths']} distinct_users={res[0]['evidence']['resource']['distinct_users']} kappa={res[0]['evidence']['resource']['kappa']}" if res else f"{len(got)} escalations"),
                   (set(users) <= parts, "all 4 participants listed in the evidence", str(sorted(parts)))])


def path_c(s, args):
    ds = generate(SynthConfig(seed=7))
    inst = ds.instances[args.instance]
    ts = sorted(e.t for e in ds.events if ds.labels[e.event_id].instance == args.instance)
    at = ts[-1] + 60                                   # just after the attack's last access
    out = subprocess.run([sys.executable, "-m", "accesssentinel.streaming.escalation_service", "sweep-now",
                          "--at", str(at), "--publish"], capture_output=True, text=True, timeout=600)
    line = next((l for l in out.stdout.splitlines() if l.startswith("{")), None)
    if not line:
        print(out.stdout[-2000:], out.stderr[-2000:]); return 1
    res = json.loads(line)
    mine = [e for e in res["escalations"] if e["u"] == inst["user"]]
    got = wait_escalations(s, ESCALATIONS_BACKGROUND, {inst["user"]}, args.timeout, at - 1, lambda r: "sweep" in r["paths"])
    return report([(res["sweep"] is not None and res["sweep"]["lookback_events"] > 0, "sweep read history from HDFS",
                    f"lookback events {res['sweep']['lookback_events']}, baseline events {res['sweep']['baseline_events']}, "
                    f"candidates {res['sweep']['candidates']}" if res["sweep"] else "no sweep"),
                   (bool(mine), f"Path C: {inst['user']} ({args.instance}) selected", str(mine)),
                   (any("sweep" in r["paths"] for r in got), "published to escalations.background", f"{len(got)} record(s)")])


def path_d(s, args):
    ds = generate(SynthConfig(seed=7))
    normal = [u for u, p in sorted(ds.users.items()) if not p.get("attacker")]
    src = normal[5]
    evs = [e for e in ds.events if e.u == src and e.s == 0][:60]      # non-sensitive only: Path B cannot fire
    u = f"syn-esc-d-{uuid.uuid4().hex[:6]}"
    t0 = time.time()
    publish_renamed(s, evs, u)
    got = wait_escalations(s, ESCALATIONS_BACKGROUND, {u}, args.timeout, t0, lambda r: "sample" in r["paths"])
    hit = [r for r in got if "sample" in r["paths"]]
    return report([(bool(hit), f"Path D: background sample of low-risk {u}",
                    f"paths={hit[0]['paths']} gamma={hit[0]['evidence']['sample']['gamma']} low_risk_active={hit[0]['evidence']['sample']['low_risk_active']}"
                    if hit else f"{len(got)} escalations (is AS_ESC_GAMMA=1 set for the test?)")])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", choices=["a", "b", "c", "d"])
    ap.add_argument("--timeout", type=float, default=120)
    ap.add_argument("--gateway", default=os.environ.get("AS_GATEWAY_URL", "http://gateway:8000"))
    ap.add_argument("--instance", default="low_and_slow-test-2")
    args = ap.parse_args(argv)
    s = get_settings()
    return {"a": path_a, "b": path_b, "c": path_c, "d": path_d}[args.path](s, args)


if __name__ == "__main__":
    sys.exit(main())

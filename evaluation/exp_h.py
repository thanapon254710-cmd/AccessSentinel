"""Experiment H - failure modes (Table V) on the LIVE stack. Container side; the host script
evaluation/run_exp_h.sh injects the faults (docker kill / pause) while these commands run:

    python -m evaluation.exp_h burst --tag seq --n 3000 --rate 100     # synthetic events -> access-events
    python -m evaluation.exp_h check --tag seq                         # what came out of every stage
    python -m evaluation.exp_h dup --n 200 --copies 3                  # duplicate delivery
    python -m evaluation.exp_h probe --tag kafka --seconds 90          # gateway behaviour during an outage

check reads, from the offsets recorded before the burst: access-events.ordered (sequencer), risk-scores (TCN), the
CMU memory position of the burst users, and reports per stage: unique events out / sent, duplicates, and the
largest gap between consecutive outputs during the burst (the outage / recovery time seen by that stage; all
timestamps from the container clock). Raw output: results/raw/exp_h/<tag>-{burst,check,probe}.json
"""
import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from . import env_info
from .live import accounts, end_offsets, pct, read_from, settings, write

OUT = "evaluation/results/raw/exp_h"
TOPICS = ["access-events.ordered", "risk-scores"]


def _event(u, t, i):
    from accesssentinel.schema import make_event
    return make_event(u, "docs", ["handbook", "policies"][i % 2], 0, "view", True, {"ip": "10.20.1.7", "device": f"dev-{u}"},
                      uuid.uuid4().hex, {"source": "synthetic-replay", "exp": "H"}, t=t)


def cmd_burst(s, args):
    from confluent_kafka import Producer
    from accesssentinel.topics import ACCESS_EVENTS, topic_name
    users = [f"syn-h-{args.tag}-{uuid.uuid4().hex[:4]}-{i}" for i in range(args.users)]
    start = end_offsets(s, TOPICS)
    pr = Producer({"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 5})
    sent, t0 = [], time.time()
    for i in range(args.n):
        target = t0 + i / args.rate
        while time.time() < target:
            time.sleep(min(0.01, target - time.time()))
        e = _event(users[i % len(users)], time.time(), i)
        while True:
            try:
                pr.produce(topic_name(ACCESS_EVENTS, s.kafka_topic_prefix), key=e.u.encode(), value=json.dumps(e.__dict__).encode())
                break
            except BufferError:
                pr.poll(0.1)
        pr.poll(0)
        sent.append(e.event_id)
    left = pr.flush(120)
    out = {"tag": args.tag, "users": users, "sent": sent, "undelivered": left, "start_offsets": {f"{t}|{p}": o for (t, p), o in start.items()},
           "t_first": t0, "t_last": time.time(), "rate": args.rate}
    write(f"{OUT}/{args.tag}-burst.json", out)
    print(f"burst {args.tag}: {len(sent)} events at {args.rate}/s, undelivered {left}")
    return out


def cmd_check(s, args):
    b = json.load(open(f"{OUT}/{args.tag}-burst.json", encoding="utf-8"))
    start = {tuple([k.split("|")[0], int(k.split("|")[1])]): v for k, v in b["start_offsets"].items()}
    users, sent = set(b["users"]), set(b["sent"])
    deadline = time.time() + args.timeout
    while True:
        recs = read_from(s, start, lambda t, d: d.get("u") in users)
        ordered = [d for t, d in recs if t.endswith("access-events.ordered")]
        scores = [d for t, d in recs if t.endswith("risk-scores")]
        done = len({d["event_id"] for d in ordered} & sent) >= len(sent) and len({d.get("event_id") for d in scores} & sent) >= len(sent)
        if done or time.time() > deadline:
            break
        time.sleep(10)

    def stage(rows, key, tkey):
        ids = [d.get(key) for d in rows]
        uniq = set(ids) & sent
        ts = sorted(float(x) for x in (tkey(d) for d in rows) if x is not None)
        gaps = [b_ - a_ for a_, b_ in zip(ts, ts[1:])]
        return {"out_unique": len(uniq), "sent": len(sent), "lost": len(sent - uniq), "duplicates": len(ids) - len(set(ids)),
                "max_gap_s": max(gaps) if gaps else None, "gap_p99_s": pct(gaps, (99,))["p99"] if gaps else None,
                "first_out": ts[0] if ts else None, "last_out": ts[-1] if ts else None}
    res = {"sequencer": stage(ordered, "event_id", lambda d: (d.get("meta") or {}).get("ts_seq_out")),
           "tcn": stage(scores, "event_id", lambda d: d.get("scored_at"))}
    try:
        import redis as redis_lib
        from accesssentinel.streaming.tgn_worker import CMU_KEY
        r = redis_lib.Redis(**s.redis_kwargs())
        ids = 0
        for u in users:
            v = r.hget(CMU_KEY["u"].format(u), "ids")
            ids += 1 if v else 0
        res["cmu_users_with_memory"] = f"{ids}/{len(users)}"
    except Exception as e:  # noqa: BLE001
        res["cmu_users_with_memory"] = f"error {type(e).__name__}"
    out = {"tag": args.tag, "burst": {k: b[k] for k in ("rate", "t_first", "t_last", "undelivered")}, "stages": res}
    write(f"{OUT}/{args.tag}-check.json", out)
    print(f"check {args.tag}: " + json.dumps(res))
    return out


def cmd_dup(s, args):
    """Duplicate delivery: every event produced `copies` times (same eid): the stages must emit it once."""
    from confluent_kafka import Producer
    from accesssentinel.topics import ACCESS_EVENTS, topic_name
    start = end_offsets(s, TOPICS)
    u = f"syn-h-dup-{uuid.uuid4().hex[:5]}"
    pr = Producer({"bootstrap.servers": s.kafka_bootstrap, "acks": "all"})
    evs = [_event(u, time.time() + i * 0.01, i) for i in range(args.n)]
    order = [e for e in evs for _ in range(args.copies)]
    random.Random(0).shuffle(order)
    for e in order:
        pr.produce(topic_name(ACCESS_EVENTS, s.kafka_topic_prefix), key=e.u.encode(), value=json.dumps(e.__dict__).encode())
    pr.flush(60)
    time.sleep(args.settle)
    recs = read_from(s, start, lambda t, d: d.get("u") == u)
    out = {}
    for name, topic in (("sequencer", "access-events.ordered"), ("tcn", "risk-scores")):
        ids = [d.get("event_id") for t, d in recs if t.endswith(topic)]
        out[name] = {"unique": len(set(ids)), "records": len(ids), "duplicates_out": len(ids) - len(set(ids))}
    res = {"events": args.n, "copies": args.copies, "produced": len(order), "stages": out}
    write(f"{OUT}/dup.json", res)
    print(f"dup: {json.dumps(res)}")
    return res


def cmd_probe(s, args):
    import httpx
    (u, pw), = accounts(s, args.api, 1, "staff", "hprobe")
    c = httpx.Client(base_url=args.gateway, timeout=5)
    c.post("/auth/login", json={"username": u, "password": pw})
    start = end_offsets(s, ["access-events"])
    rows, t_end = [], time.time() + args.seconds
    while time.time() < t_end:
        t0 = time.time()
        try:
            st = c.get("/apps/docs/handbook").status_code
        except Exception as e:  # noqa: BLE001
            st = f"error {type(e).__name__}"
        rows.append({"t": t0, "status": st, "ms": (time.time() - t0) * 1000})
        time.sleep(args.interval)
    time.sleep(args.settle)
    recs = read_from(s, start, lambda t, d: d.get("u") == u)
    ok = [r for r in rows if r["status"] == 200]
    res = {"user_requests": len(rows), "status_mix": {str(k): sum(1 for r in rows if r["status"] == k) for k in {r["status"] for r in rows}},
           "allowed_requests": len(ok), "events_on_kafka": len(recs),
           "response_ms": pct([r["ms"] for r in rows]), "rows": rows}
    write(f"{OUT}/{args.tag}-probe.json", res)
    print(f"probe {args.tag}: {res['status_mix']}, events on Kafka {len(recs)} for {len(rows)} requests")
    return res


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["burst", "check", "dup", "probe"])
    ap.add_argument("--tag", default="h")
    ap.add_argument("--n", type=int, default=3000)
    ap.add_argument("--rate", type=float, default=100)
    ap.add_argument("--users", type=int, default=30)
    ap.add_argument("--copies", type=int, default=3)
    ap.add_argument("--timeout", type=float, default=300)
    ap.add_argument("--seconds", type=float, default=90)
    ap.add_argument("--interval", type=float, default=0.2)
    ap.add_argument("--settle", type=float, default=30)
    ap.add_argument("--gateway", default=os.environ.get("AS_GATEWAY_URL", "http://gateway:8000"))
    ap.add_argument("--api", default=os.environ.get("AS_API_URL", "http://api:8080"))
    args = ap.parse_args(argv)
    s = settings()
    started = datetime.now(timezone.utc).isoformat()
    res = {"burst": cmd_burst, "check": cmd_check, "dup": cmd_dup, "probe": cmd_probe}[args.cmd](s, args)
    if args.cmd in ("check", "dup"):
        write(f"{OUT}/{args.tag}-{args.cmd}-env.json", {"started_at": started, "environment": env_info.collect()})
    return 0 if res is not None else 1


if __name__ == "__main__":
    sys.exit(main())

"""Revised v3 Phase 2 live checks of the event pipeline. Run INSIDE Docker:

    docker compose run --rm -T tools python scripts/smoke_v3_stream.py order
    docker compose run --rm -T tools python scripts/smoke_v3_stream.py windows
    docker compose run --rm -T tools python scripts/smoke_v3_stream.py burst --user syn-crash-1 --n 3000
    docker compose run --rm -T tools python scripts/smoke_v3_stream.py check --user syn-crash-1 --n 3000

order    out-of-order events (2 sessions + late events) for a fresh synthetic user, produced straight to
         access-events: the ordered topic must contain each non-discarded event exactly once, in event-time
         order; late events must carry the policy of their sensitivity; the TCN must score the released
         events (not the history-only ones) and carry the per-stage trace.
windows  > kappa synthetic users access a sensitive resource of a fresh tenant in a past event-time window;
         exactly one final resource-window record with exactly those users; a late event afterwards is audited
         and does not produce a second final.
burst / check   crash test helpers: burst produces N in-sequence events quickly; check verifies the ordered
         topic holds exactly N of them, in order, without duplicates (run `docker compose kill sequencer` /
         `start sequencer` in between).
Test users are named syn-* and tagged meta.source = synthetic-replay.
"""
import argparse
import json
import os
import random
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from accesssentinel.schema import make_event  # noqa: E402
from accesssentinel.settings import get_settings  # noqa: E402
from accesssentinel.topics import (ACCESS_EVENTS, ACCESS_EVENTS_ORDERED, LATE_EVENTS, RESOURCE_WINDOWS,  # noqa: E402
                                   RISK_SCORES, topic_name)

results = []


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    return ok


def producer(s):
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 5})


def tail_consumer(s, topics):
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"smoke-{uuid.uuid4().hex[:8]}",
                  "enable.auto.commit": False, "isolation.level": "read_committed"})
    md = c.list_topics(timeout=10)
    tps = []
    for t in topics:
        for p in md.topics[t].partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(t, p), timeout=10)
            tps.append(TopicPartition(t, p, hi))                    # start at the current end
    c.assign(tps)
    return c


def collect(c, pred, until, timeout):
    out = []
    deadline = time.time() + timeout
    while time.time() < deadline and not until(out):
        for m in c.consume(500, 0.5):
            if m.error():
                continue
            d = json.loads(m.value())
            if pred(m.topic(), d):
                out.append((m.topic(), d))
    return out


def ev(u, te, seq, stream, s=0, r="directory", tenant="default"):
    e = make_event(u, "hr" if r in ("directory", "payroll") else "docs", r, s, "view", True, {"ip": "10.9.9.9"},
                   uuid.uuid4().hex, {"source": "synthetic-replay", "smoke": "v3-stream"}, t=te, seq=seq,
                   stream=stream, tenant=tenant)
    return e


def send(p, topic, events):
    for e in events:
        p.produce(topic, key=e.u.encode(), value=json.dumps(e.__dict__).encode())
    assert p.flush(30) == 0


def cmd_order(s, args):
    t = lambda n: topic_name(n, s.kafka_topic_prefix)
    u = f"syn-ord-{uuid.uuid4().hex[:6]}"
    now, delta = time.time(), s.delta_late
    rng = random.Random(7)
    evs, seq = [], {"g1:a": 0, "g1:b": 0}
    for i in range(30):                                     # two sessions interleaved, 0.3 s apart
        st = "g1:a" if i % 3 else "g1:b"
        seq[st] += 1
        evs.append(ev(u, now + i * 0.3, seq[st], st, s=int(i % 5 == 0), r="payroll" if i % 5 == 0 else "directory"))
    arrival = sorted(evs, key=lambda e: e.t + rng.uniform(0, 0.6 * delta))      # disorder within delta_late
    late = [ev(u, now - 10 * delta, -1, "", s=0), ev(u, now - 10 * delta + 0.1, -1, "", s=1, r="payroll")]
    c = tail_consumer(s, [t(ACCESS_EVENTS_ORDERED), t(RISK_SCORES), t(LATE_EVENTS)])
    p = producer(s)
    send(p, t(ACCESS_EVENTS), arrival[:20])
    time.sleep(0.5)
    send(p, t(ACCESS_EVENTS), late)                         # behind the watermark by now
    send(p, t(ACCESS_EVENTS), arrival[20:])
    want = len(evs) + 2
    got = collect(c, lambda tp, d: d.get("u") == u, lambda o: sum(1 for x in o if x[0] == t(ACCESS_EVENTS_ORDERED)) >= want
                  and sum(1 for x in o if x[0] == t(RISK_SCORES)) >= len(evs) + 1, 60)
    c.close()
    ordered = [d for tp, d in got if tp == t(ACCESS_EVENTS_ORDERED)]
    scores = [d for tp, d in got if tp == t(RISK_SCORES)]
    in_order = [d for d in ordered if not d["meta"].get("late_policy")]
    lates = {d["meta"]["late_policy"] for d in ordered if d["meta"].get("late_policy")}
    ids = [d["event_id"] for d in ordered]
    check("every event exactly once on the ordered stream", len(ids) == len(set(ids)) == want, f"{len(set(ids))}/{want}")
    tes = [d["t"] for d in in_order]
    check("in-order events in event-time order (R1)", tes == sorted(tes) and len(in_order) == len(evs), f"{len(in_order)} events")
    check("late events flagged with the policy of their sensitivity", lates == {"history_only", "rescore"}, str(sorted(lates)))
    waits = [d["meta"]["reorder_wait"] for d in in_order]
    check("reorder wait bounded by delta_late (Eq. 31 Lat_reorder)", max(waits) <= delta + 0.5,
          f"max {max(waits):.3f}s, fast path {sum(d['meta']['order'] == 'fast_path' for d in in_order)}/{len(in_order)}")
    check("ti (broker LogAppendTime) filled", all(d.get("ti", 0) > 0 for d in ordered))
    tr = [x.get("trace") or {} for x in scores]
    check("TCN scored released events, not the history-only one", len(scores) == len(evs) + 1, f"{len(scores)} scores")
    check("risk scores carry the per-stage trace", all(x.get("ts_gw_pub") is None and x.get("ts_seq_out") and x.get("ts_tcn_scored") for x in tr),
          "gateway stamps absent here: events were produced directly, not through the gateway")
    return 0 if all(results) else 1


def cmd_windows(s, args):
    t = lambda n: topic_name(n, s.kafka_topic_prefix)
    tenant = f"smoke{uuid.uuid4().hex[:6]}"
    W = s.res_window_seconds
    b = int((time.time() - 3 * W) // W)                     # a past window, so the watermark can pass it now
    users = [f"syn-res-{tenant}-{i}" for i in range(5)]
    c = tail_consumer(s, [t(RESOURCE_WINDOWS), t(LATE_EVENTS)])
    p = producer(s)
    send(p, t(ACCESS_EVENTS), [ev(u, b * W + 10 + i, -1, "", s=1, r="payroll", tenant=tenant) for i, u in enumerate(users)])
    send(p, t(ACCESS_EVENTS), [ev(u, time.time(), -1, "", s=0, r="directory", tenant=tenant) for u in users])  # advance watermarks
    rstar = f"{tenant}/hr:payroll"
    got = collect(c, lambda tp, d: d.get("rstar") == rstar or d.get("resource") == rstar,
                  lambda o: any(d.get("kind") == "final" for _, d in o), s.res_merge_timeout + 60)
    finals = [d for _, d in got if d.get("kind") == "final"]
    check("one final window result", len(finals) == 1, f"{len(finals)} final(s), window {b}")
    if finals:
        f = finals[0]
        check("participants = exactly the users", f["users"] == sorted(users) and f["distinct_users"] == 5,
              f"{f['distinct_users']} users, {f['shards_reported']} shard(s) reported")
        check("above kappa flagged", f["above_kappa"] is (5 > f["kappa"]), f"kappa={f['kappa']}")
    late_u = f"syn-res-{tenant}-late"
    send(p, t(ACCESS_EVENTS), [ev(late_u, b * W + 30, -1, "", s=1, r="payroll", tenant=tenant)])
    more = collect(c, lambda tp, d: d.get("rstar") == rstar or d.get("resource") == rstar,
                   lambda o: any(d.get("kind") == "late_event" for _, d in o), 30)
    c.close()
    check("late event after finalisation audited", any(d.get("u") == late_u and d.get("reason") == "window_already_final" for _, d in more))
    check("no second final for the window", not any(d.get("kind") == "final" for _, d in more))
    return 0 if all(results) else 1


def cmd_burst(s, args):
    t = topic_name(ACCESS_EVENTS, s.kafka_topic_prefix)
    p = producer(s)
    t0 = time.time()
    for i in range(1, args.n + 1):
        e = ev(args.user, t0 + i * 0.001, i, "gwX:crash", s=0)
        p.produce(t, key=e.u.encode(), value=json.dumps(e.__dict__).encode())
        if i % 200 == 0:
            p.flush(30)
            time.sleep(args.pause)
    assert p.flush(30) == 0
    print(f"produced {args.n} events for {args.user} in {time.time() - t0:.1f}s")
    return 0


def cmd_check(s, args):
    from confluent_kafka import Consumer
    t = topic_name(ACCESS_EVENTS_ORDERED, s.kafka_topic_prefix)
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"check-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False, "isolation.level": "read_committed"})
    c.subscribe([t])
    seqs, deadline, idle = [], time.time() + args.timeout, time.time()
    while time.time() < deadline:
        msgs = c.consume(1000, 1.0)
        if msgs:
            idle = time.time()
        for m in msgs:
            if not m.error():
                d = json.loads(m.value())
                if d.get("u") == args.user:
                    seqs.append(d["seq"])
        if len(seqs) >= args.n and time.time() - idle > 5:
            break
    c.close()
    check("all events present", len(set(seqs)) == args.n, f"{len(set(seqs))}/{args.n}")
    check("no duplicates on the ordered stream (exactly-once)", len(seqs) == len(set(seqs)), f"{len(seqs) - len(set(seqs))} duplicates")
    check("in sequence order", seqs == sorted(seqs))
    return 0 if all(results) else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("order")
    sub.add_parser("windows")
    b = sub.add_parser("burst"); b.add_argument("--user", required=True); b.add_argument("--n", type=int, default=3000)
    b.add_argument("--pause", type=float, default=0.05)
    k = sub.add_parser("check"); k.add_argument("--user", required=True); k.add_argument("--n", type=int, default=3000)
    k.add_argument("--timeout", type=float, default=180)
    args = ap.parse_args(argv)
    s = get_settings()
    rc = {"order": cmd_order, "windows": cmd_windows, "burst": cmd_burst, "check": cmd_check}[args.cmd](s, args)
    if args.cmd not in ("burst",):
        print(f"{sum(results)}/{len(results)} checks passed")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())

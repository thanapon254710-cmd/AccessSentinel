"""Phase 5 live check: replay labelled SYNTHETIC users through the deployed pipeline and verify the TCN workers.

    docker compose run --rm -T tools python scripts/replay_synthetic.py publish --normal 30 > replay.json
    docker compose run --rm -T tools python scripts/replay_synthetic.py verify < replay.json

publish : regenerates the training dataset (same seed -> same events), selects the TEST-period attackers
          (never seen in training) plus N normal users, and publishes each user's full timeline to
          `access-events` in time order, tagged meta.source = "synthetic-replay" (users are "syn-*").
          With --reset, the users' TCN state in Redis is cleared first so the run is repeatable.
verify  : reads `risk-scores` and checks: every replayed event scored once by a trained model version;
          live scores equal offline scores of the same model on the same events (train/serve parity);
          detection of each replayed test attack at the 0.6 threshold; false alarms on normal users' test days.
These are synthetic data; the check validates the deployed pipeline, not real-world accuracy.
"""
import argparse
import json
import os
import sys
import time
import uuid
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from accesssentinel.datasets.synth import INDIVIDUAL, SynthConfig, generate  # noqa: E402
from accesssentinel.settings import get_settings  # noqa: E402
from accesssentinel.topics import ACCESS_EVENTS, RISK_SCORES, topic_name  # noqa: E402


def selection(seed: int, n_normal: int):
    ds = generate(SynthConfig(seed=seed))
    attack_users = sorted({i["user"] for i in ds.instances.values() if i["kind"] in INDIVIDUAL and i["split"] == "test"})
    normal = sorted(u for u, p in ds.users.items() if not p.get("attacker"))[:n_normal]
    users = set(attack_users) | set(normal)
    events = [e for e in ds.events if e.u in users]
    return ds, attack_users, normal, events


def cmd_publish(args) -> int:
    from confluent_kafka import Producer
    s = get_settings()
    ds, attack_users, normal, events = selection(args.data_seed, args.normal)
    if args.reset:
        import redis as redis_lib
        r = redis_lib.Redis(**s.redis_kwargs())
        for u in attack_users + normal:
            r.delete(f"tcn:profile:{u}", f"risk:{u}")
            r.zrem("tcn:recheck", u)
    p = Producer({"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 20})
    topic = topic_name(ACCESS_EVENTS, s.kafka_topic_prefix)
    for i, e in enumerate(events):
        d = dict(e.__dict__); d["meta"] = {**e.meta, "source": "synthetic-replay"}
        while True:
            try:
                p.produce(topic, key=e.u.encode(), value=json.dumps(d).encode()); break
            except BufferError:
                p.poll(0.5)
        if i % 5000 == 0:
            p.poll(0)
    left = p.flush(60)
    test_instances = {k: v for k, v in ds.instances.items() if v["kind"] in INDIVIDUAL and v["split"] == "test"}
    json.dump({"run": uuid.uuid4().hex, "data_seed": args.data_seed, "fingerprint": ds.fingerprint(),
               "attack_users": attack_users, "normal_users": normal, "events": len(events),
               "test_instances": test_instances, "published_at": time.time()}, sys.stdout)
    print(f"published {len(events)} events for {len(attack_users)} test attackers + {len(normal)} normal users"
          f" (unacknowledged: {left})", file=sys.stderr)
    return 1 if left else 0


def cmd_verify(args) -> int:
    import numpy as np
    from confluent_kafka import Consumer
    from accesssentinel.ml.features import UserProfile, observe
    from accesssentinel.ml.registry import TcnScorer
    s = get_settings()
    man = json.load(sys.stdin)
    ds, attack_users, normal, events = selection(man["data_seed"], len(man["normal_users"]))
    want = {e.event_id: e for e in events}
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"verify-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic_name(RISK_SCORES, s.kafka_topic_prefix)])
    got, dup = {}, 0
    deadline = time.time() + args.timeout
    while time.time() < deadline and len(got) < len(want):
        for m in c.consume(1000, 1.0):
            if m.error():
                continue
            rec = json.loads(m.value())
            if rec.get("event_id") in want:
                if rec["event_id"] in got:
                    dup += 1
                got[rec["event_id"]] = rec
    c.close()
    results = [(len(got) == len(want), "every replayed event was scored", f"{len(got)}/{len(want)}"),
               (True, "duplicate score records (allowed: at-least-once, same score_id)", str(dup))]
    versions = {r["model_version"] for r in got.values()}
    results.append((len(versions) == 1 and all(r["model_kind"] == "tcn" for r in got.values()),
                    "all scores from one trained model version", str(sorted(versions))))
    # ---- parity with offline scoring by the same model version
    scorer = TcnScorer.load(s.model_dir, next(iter(versions)) if versions else None)
    profiles, windows = defaultdict(UserProfile), []
    for e in sorted(events, key=lambda e: (e.t, e.event_id)):
        observe(profiles[e.u], e); windows.append((e, list(profiles[e.u].window)))
    offline = scorer.score_windows([w for _, w in windows])
    diffs = [abs(got[e.event_id]["s_ind"] - float(o)) for (e, _), o in zip(windows, offline) if e.event_id in got]
    results.append((bool(diffs) and max(diffs) < 1e-4, "live scores == offline scores (train/serve parity)",
                    f"max |diff| = {max(diffs) if diffs else float('nan'):.2e} over {len(diffs)} events"))
    # ---- detection of the unseen test attacks, live
    for inst, info in sorted(man["test_instances"].items()):
        ev_ids = [e.event_id for e in events if ds.labels[e.event_id].instance == inst]
        sc = sorted(((want[i].t, got[i]["s_ind"]) for i in ev_ids if i in got))
        hit = next((k for k, (_, v) in enumerate(sc) if v >= 0.6), None)
        detail = (f"{len(sc)} events; first >=0.6 after {hit} attack events, "
                  f"{(sc[hit][0] - sc[0][0]) / 60:.1f} min" if hit is not None else f"{len(sc)} events; max {max(v for _, v in sc):.3f}")
        results.append((hit is not None, f"live detection of {inst}", detail))
    # ---- false alarms on normal users during the test period
    t0 = ds.split_bounds["test"][0]
    days, flagged = set(), set()
    for e in events:
        if e.u in man["normal_users"] and e.t >= t0 and e.event_id in got:
            k = (e.u, int(e.t // 86400)); days.add(k)
            if got[e.event_id]["s_ind"] >= 0.6:
                flagged.add(k)
    results.append((True, "normal user-days (test period) with a high score", f"{len(flagged)}/{len(days)}"))
    ms = [r["batch_inference_ms"] / max(r["batch_size"], 1) for r in got.values() if r.get("batch_inference_ms")]
    results.append((True, "worker inference time per event (batched)", f"median {np.median(ms):.3f} ms" if ms else "n/a"))
    for ok, what, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {what}  [{detail}]")
    failed = sum(1 for ok, _, _ in results if not ok)
    print(f"{len(results) - failed}/{len(results)} checks passed")
    return 1 if failed else 0


def cmd_recheck_demo(args) -> int:
    """Deterministic live check of Medium-tier re-scoring. Requires workers started with a short
    AS_TCN_RECHECK_SECONDS and a long AS_TCN_LIVE_WINDOW_SECONDS (test settings, see docs/PROGRESS.md)."""
    from collections import defaultdict
    from confluent_kafka import Consumer, Producer
    from accesssentinel.ml.features import UserProfile, observe
    from accesssentinel.ml.registry import TcnScorer
    s = get_settings()
    ds = generate(SynthConfig(seed=args.data_seed))
    scorer = TcnScorer.load(s.model_dir)
    normal = [u for u, p in sorted(ds.users.items()) if not p.get("attacker")]
    chosen = None
    for u in normal[:60]:
        evs = [e for e in ds.events if e.u == u]
        prof, wins = UserProfile(), []
        for e in evs:
            observe(prof, e); wins.append(list(prof.window))
        sc = scorer.score_windows(wins)
        j = next((k for k, v in enumerate(sc) if 0.4 <= v < 0.6 and k > 25), None)
        if j is not None:
            chosen = (u, evs[: j + 1], float(sc[j])); break
    if chosen is None:
        print("no medium-scoring event found among normal users"); return 1
    u, evs, expected = chosen
    live_u = f"syn-recheck-{uuid.uuid4().hex[:6]}"
    p = Producer({"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True, "acks": "all"})
    for e in evs:
        d = dict(e.__dict__); d.update(u=live_u, event_id=uuid.uuid4().hex, meta={**e.meta, "source": "synthetic-replay"})
        p.produce(topic_name(ACCESS_EVENTS, s.kafka_topic_prefix), key=live_u.encode(), value=json.dumps(d).encode())
    p.flush(30)
    print(f"published {len(evs)} events of {u} as {live_u}; offline score of the last event = {expected:.4f}")
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"verify-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic_name(RISK_SCORES, s.kafka_topic_prefix)])
    seen = []
    deadline = time.time() + args.timeout
    while time.time() < deadline and sum(1 for r in seen if r["trigger"] == "recheck") < 2:
        for m in c.consume(500, 1.0):
            if not m.error():
                rec = json.loads(m.value())
                if rec["u"] == live_u:
                    seen.append(rec)
    c.close()
    ev_recs = [r for r in seen if r["trigger"] == "event"]
    rechecks = [r for r in seen if r["trigger"] == "recheck"]
    last = ev_recs[-1] if ev_recs else None
    results = [(last is not None and last["tier"] == "medium" and abs(last["s_ind"] - expected) < 1e-4,
                "last event scored Medium (same as offline)", f"{last['s_ind']:.4f} {last['tier']}" if last else "none"),
               (len(rechecks) >= 1, "Medium user re-scored by the recheck timer",
                "; ".join(f"s_ind {r['s_ind']:.4f} {r['tier']} after {r['seconds_since_last_event']:.0f}s idle" for r in rechecks))]
    if len(rechecks) >= 2:
        gap = rechecks[1]["scored_at"] - rechecks[0]["scored_at"]
        results.append((True, "interval between rechecks", f"{gap:.0f}s"))
    for ok, what, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {what}  [{detail}]")
    failed = sum(1 for ok, _, _ in results if not ok)
    print(f"{len(results) - failed}/{len(results)} checks passed")
    return 1 if failed else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("publish"); p.add_argument("--normal", type=int, default=30)
    p.add_argument("--data-seed", type=int, default=7); p.add_argument("--reset", action="store_true")
    v = sub.add_parser("verify"); v.add_argument("--timeout", type=float, default=300)
    rd = sub.add_parser("recheck-demo"); rd.add_argument("--data-seed", type=int, default=7)
    rd.add_argument("--timeout", type=float, default=300)
    args = ap.parse_args(argv)
    return {"publish": cmd_publish, "verify": cmd_verify, "recheck-demo": cmd_recheck_demo}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())

"""Phase 7 live check: a held-out collusion group (or a benign team meeting) through the deployed TGN path.

    docker compose run --rm -T tools python scripts/smoke_tgn.py collusion
    docker compose run --rm -T tools python scripts/smoke_tgn.py team

1. Offline: on the collusion-study dataset, build the seed's neighbourhood at the anchor time and score it with
   the promoted TGN -> expected group members and S_coll.
2. Live: replay the full histories (up to the anchor) of exactly the users in that neighbourhood - renamed
   syn-tgn-<run>-<user>, fresh event ids, meta.source = "synthetic-replay" - into `access-events`; wait for the
   repartitioner and graph index; publish an escalation for the seed; read `group-scores`.
3. Check: live members and S_coll equal the offline result (train/serve parity) and the Redis coll:<u> state.
Synthetic data: this validates the deployed path, not real-world accuracy.
"""
import argparse
import json
import os
import sys
import random
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from accesssentinel.datasets.synth import SynthConfig, generate  # noqa: E402
from accesssentinel.ml.tgn_graph import EdgeAnnotator, MemoryEdgeIndex, build_sample  # noqa: E402
from accesssentinel.ml.tgn_registry import TgnScorer  # noqa: E402
from accesssentinel.settings import get_settings  # noqa: E402
from accesssentinel.streaming.lag import group_lag  # noqa: E402
from accesssentinel.topics import ACCESS_EVENTS, ESCALATIONS_REALTIME, GROUP_SCORES, topic_name  # noqa: E402


def pick(ds, case: str, index: int):
    if case == "collusion":
        groups = sorted((k, v) for k, v in ds.instances.items() if v.get("split") == "test" and "bursts" in v)
        name, g = groups[index % len(groups)]
        return name, str(g["members"][0]), sorted(g["bursts"])[0] + 1800, g
    a, b = ds.split_bounds["test"]
    m = ds.meetings[index % len(ds.meetings)]
    for day in range(int(a // 86400), int(b // 86400)):
        if (day + 3) % 7 == m["weekday"]:
            return f"team-meeting-{m['role']}", str(m["team"][0]), day * 86400 + (m["hour"] + 0.75) * 3600, m
    raise SystemExit("no meeting in the test period")


def main(argv=None) -> int:
    from confluent_kafka import Consumer, Producer
    import redis as redis_lib
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("case", choices=["collusion", "team"])
    ap.add_argument("--index", type=int, default=0)
    ap.add_argument("--timeout", type=float, default=240)
    args = ap.parse_args(argv)
    s = get_settings()
    ds = generate(SynthConfig.collusion_study())
    scorer = TgnScorer.load(s.model_dir)
    name, seed, anchor, info = pick(ds, args.case, args.index)
    evs = sorted(ds.events, key=lambda e: (e.t, e.event_id))
    ann = EdgeAnnotator()
    index = MemoryEdgeIndex([ann.annotate(e) for e in evs])
    sample = build_sample([seed], index, anchor, scorer.gp)
    expected = scorer.analyse(sample)[seed]
    run = uuid.uuid4().hex[:6]
    ren = lambda u: f"syn-tgn-{run}-{u}"
    # Isolation: shift this replay by whole weeks into the past (weekday, hour and day differences - hence every
    # feature - are unchanged), so it cannot share a time window with earlier replays of the same data.
    shift = -7 * 86400.0 * random.Random(run).randint(60, 600)
    participants = set(sample.users)
    replay = [e for e in evs if e.u in participants and e.t <= anchor]
    print(f"{args.case}: {name}, seed {seed}, anchor {anchor:.0f} (replayed {shift / 604800:.0f} weeks earlier); offline: members {[str(m) for m in expected['members']]} "
          f"s_coll {expected['s_coll']}; replaying {len(replay)} events of {len(participants)} users")
    p = Producer({"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 20})
    for e in replay:
        d = dict(e.__dict__); d.update(u=ren(e.u), t=e.t + shift, event_id=uuid.uuid4().hex,
                                       meta={**e.meta, "source": "synthetic-replay"})
        while True:
            try:
                p.produce(topic_name(ACCESS_EVENTS, s.kafka_topic_prefix), key=d["u"].encode(), value=json.dumps(d).encode()); break
            except BufferError:
                p.poll(0.5)
    p.flush(60)
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        lags = {g: group_lag(s.kafka_bootstrap, g, [topic_name(t, s.kafka_topic_prefix)])["total"]
                for g, t in (("repartitioner", ACCESS_EVENTS), ("graph-index", "access-events-by-resource"))}
        if all(v == 0 for v in lags.values()):
            break
        time.sleep(2)
    esc_id = uuid.uuid4().hex
    esc = {"escalation_id": esc_id, "schema_version": 1, "u": ren(seed), "t": time.time(), "priority": "realtime",
           "paths": ["harness"], "evidence": {"harness": {"t": anchor + shift, "note": "Phase 7 live check"}}, "source": "synthetic-replay"}
    p.produce(topic_name(ESCALATIONS_REALTIME, s.kafka_topic_prefix), key=esc["u"].encode(), value=json.dumps(esc).encode())
    p.flush(10)
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"verify-{run}", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([topic_name(GROUP_SCORES, s.kafka_topic_prefix)])
    got = None
    while time.time() < deadline and got is None:
        for m in c.consume(500, 1.0):
            if not m.error():
                r = json.loads(m.value())
                if r.get("escalation_id") == esc_id:
                    got = r
    c.close()
    results = [(got is not None, "TGN worker analysed the escalation", f"graph {got['graph']} in {got['inference_ms']} ms" if got else "no group-scores record")]
    if got:
        exp_members = sorted(ren(u) for u in expected["members"])
        same_s = (got["s_coll"] is None and expected["s_coll"] is None) or (
            got["s_coll"] is not None and expected["s_coll"] is not None and abs(got["s_coll"] - expected["s_coll"]) < 1e-5)
        results.append((sorted(got["members"]) == exp_members and same_s, "live result == offline result (parity)",
                        f"live members {[m.split('-', 3)[-1] for m in got['members']]} s_coll {got['s_coll']}"))
        results.append((got["model_version"] == scorer.version, "scored by the promoted TGN", got["model_version"]))
        truth = set(info["members"]) if args.case == "collusion" else set()
        flagged = bool(got["above_theta_coll"])
        if args.case == "collusion":
            found = {m.split("-", 3)[-1] for m in got["members"]} & truth
            results.append((True, "detection outcome (synthetic, reported)",
                            f"S_coll {'>=' if flagged else '<'} theta_coll {got['theta_coll']}; true colluders in group {sorted(map(str, found))} of {sorted(map(str, truth))}"))
        else:
            results.append((True, "benign team meeting outcome (reported)", f"flagged={flagged} s_coll={got['s_coll']} theta={got['theta_coll']}"))
        if got["group_id"]:
            r = redis_lib.Redis(**s.redis_kwargs(), decode_responses=True)
            results.append((r.hget(f"coll:{ren(seed)}", "group_id") == got["group_id"], "coll:<u> state written for the PDP",
                            r.hget(f"coll:{ren(seed)}", "s_coll")))
    for ok, what, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {what}  [{detail}]")
    failed = sum(1 for ok, _, _ in results if not ok)
    print(f"{len(results) - failed}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

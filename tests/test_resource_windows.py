"""Revised v3 Phase 2: resource-keyed view with two-stage shard-and-merge (exact participant sets, exactly one
final result per (r*, window), late shards -> corrections, late events -> audit), over in-memory Kafka."""
import json
import random

from accesssentinel.schema import make_event
from accesssentinel.streaming.resource_windows import MergeStage, RepartitionProcessor, ShardStage, shard_of
from accesssentinel.streaming.transactional import TransactionalStage

from tests import fake_txn as fk

W, J, DL = 60.0, 4, 2.0
IN, BYRES, PART, WIN, LATE = "access-events", "by-resource", "partials", "windows", "late"


class Clock:
    def __init__(self, t): self.t = t
    def __call__(self): return self.t


def ev(u, t, r="payroll", s=1):
    e = make_event(u, "hr", r, s, "view", True, {}, "c" * 32, t=t, tenant="acme")
    return json.dumps(e.__dict__).encode()


def pipeline(c, clk, kappa=3):
    rp = TransactionalStage(fk.Consumer(c, "rp"), fk.Producer(c), None, IN, RepartitionProcessor(BYRES, W, J),
                            clock=clk, tick_every=0.0, sleep=lambda s: None)
    sh = ShardStage(PART, LATE, W, DL, grace=1.0, clock=clk, source_partitions=c.n)
    s1 = TransactionalStage(fk.Consumer(c, "s1"), fk.Producer(c), lambda: fk.RestoreConsumer(c), BYRES, sh, "s1.cl",
                            clock=clk, tick_every=0.0, sleep=lambda s: None)
    mg = MergeStage(WIN, J, merge_timeout=5.0, kappa_fn=lambda: kappa, clock=clk)
    s2 = TransactionalStage(fk.Consumer(c, "s2"), fk.Producer(c), lambda: fk.RestoreConsumer(c), PART, mg, "s2.cl",
                            clock=clk, tick_every=0.0, sleep=lambda s: None)
    for st in (rp, s1, s2):
        st.start()
    return rp, s1, s2, sh, mg


def run(stages, n=3):
    for _ in range(n):
        for st in stages:
            st.run(max_iterations=1)


def windows(c):
    return [json.loads(v) for _, _, v in c.records(WIN)]


def test_exact_distinct_users_once_per_window_across_shards():
    rng = random.Random(1)
    c, clk = fk.Cluster(partitions=4), Clock(10_000.0)
    truth, evs = {}, []
    for i in range(400):                                   # a hot resource: 40 users, three windows
        u, t = f"u{rng.randrange(40)}", 1000.0 + rng.uniform(0, 2 * W - 1)
        truth.setdefault(int(t // W), set()).add(u)
        evs.append((t + rng.uniform(0, 0.9 * DL), u, t))      # arrival order: event time + jitter < delta_late
    for _, u, t in sorted(evs):
        c.append(IN, u.encode(), ev(u, t))
    c.append(IN, b"x", ev("x", 1000.0 + 5 * W, r="directory", s=0))  # advances watermarks; s=0 not aggregated
    for p in range(4):
        c.append(IN, f"p{p}".encode(), ev(f"w{p}", 1000.0 + 6 * W, r="other"))
    stages = pipeline(c, clk)
    run(stages[:3])
    clk.t += 10.0                                          # merge timeout for shards that never saw traffic
    run(stages[:3])
    finals = [w for w in windows(c) if w["kind"] == "final" and w["rstar"] == "acme/hr:payroll"]
    assert sorted(w["window"] for w in finals) == sorted(truth)          # exactly one final per window
    for w in finals:
        assert set(w["users"]) == truth[w["window"]] and w["distinct_users"] == len(truth[w["window"]])
        assert w["above_kappa"] is True and w["s"] == 1
    assert {int(k.decode().rsplit("|", 1)[1]) for _, k, _ in c.records(BYRES) if k and b"payroll" in k} == set(range(J))
    assert not any(w["rstar"].endswith(":directory") for w in windows(c))


def test_merge_stage_crash_does_not_emit_a_second_final():
    c, clk = fk.Cluster(partitions=2), Clock(10_000.0)
    for u in ("a", "b", "c", "d", "e"):
        c.append(IN, u.encode(), ev(u, 1010.0))
    for p in range(2):
        c.append(IN, f"z{p}".encode(), ev(f"z{p}", 1000.0 + 5 * W, r="other"))
    rp, s1, s2, sh, mg = pipeline(c, clk)
    run([rp, s1])
    clk.t += 11.0                                          # an empty source partition is idle after source_idle
    run([s1])
    clk.t += 10.0
    run([s2], n=2)
    assert len([w for w in windows(c) if w["kind"] == "final"]) >= 1
    n_final = len([w for w in windows(c) if w["kind"] == "final"])
    # crash Stage 2: a new instance restores "already emitted" from the changelog and re-reads nothing new
    mg2 = MergeStage(WIN, J, merge_timeout=5.0, kappa_fn=lambda: 3, clock=clk)
    s2b = TransactionalStage(fk.Consumer(c, "s2"), fk.Producer(c), lambda: fk.RestoreConsumer(c), PART, mg2, "s2.cl",
                             clock=clk, tick_every=0.0, sleep=lambda s: None)
    s2b.start()
    clk.t += 10.0
    run([s2b], n=2)
    finals = [(w["rstar"], w["window"]) for w in windows(c) if w["kind"] == "final"]
    assert len(finals) == len(set(finals)) >= n_final       # still exactly one final per (r*, window)
    assert ("acme/hr:payroll", int(1010.0 // W)) in finals
    assert s2b.stats["restored_keys"] >= 1


def test_late_event_after_window_final_is_audited_not_counted():
    c, clk = fk.Cluster(partitions=1), Clock(10_000.0)
    for u in ("a", "b"):
        c.append(IN, u.encode(), ev(u, 1010.0))
    c.append(IN, b"z", ev("z", 1000.0 + 5 * W, r="other"))
    rp, s1, s2, sh, mg = pipeline(c, clk)
    run([rp, s1])
    c.append(IN, b"late", ev("late", 1020.0))             # same window, after it was finalised
    run([rp, s1])
    audits = [json.loads(v) for _, _, v in c.records(LATE)]
    assert [a["u"] for a in audits] == ["late"] and audits[0]["reason"] == "window_already_final"
    assert sh.stats["late"] == 1


def test_shard_function_spreads_users():
    assert len({shard_of(f"user{i}", J) for i in range(100)}) == J

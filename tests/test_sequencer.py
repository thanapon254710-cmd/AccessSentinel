"""Revised v3 Phase 2: sequencer stage = event-time ordering + late-event policies, exactly-once through Kafka
transactions with a changelog (R1, R3). Uses the in-memory transactional Kafka in tests/fake_txn.py."""
import json

from accesssentinel.schema import make_event
from accesssentinel.streaming.event_time import EventTimeOrderer
from accesssentinel.streaming.sequencer import SequencerProcessor
from accesssentinel.streaming.transactional import TransactionalStage

from tests import fake_txn as fk

IN, OUT, LATE, CL = "access-events", "access-events.ordered", "late-events", "sequencer.changelog"


class Clock:
    def __init__(self, t=1000.0): self.t = t
    def __call__(self): return self.t


def ev(u, t, seq=-1, stream="", s=0, r="directory"):
    e = make_event(u, "hr", r, s, "view", True, {}, "c" * 32, t=t, seq=seq, stream=stream)
    return json.dumps(e.__dict__).encode()


def stage(cluster, clock, fail_commits=0, delta=2.0, **pol):
    proc = SequencerProcessor(EventTimeOrderer(delta, **pol), OUT, LATE, clock=clock)
    st = TransactionalStage(fk.Consumer(cluster, "sequencer"), fk.Producer(cluster, fail_commits),
                            lambda: fk.RestoreConsumer(cluster), IN, proc, CL, clock=clock, tick_every=0.0,
                            sleep=lambda s: None)
    st.start()
    return st, proc


def out_by_user(cluster):
    res = {}
    for _, k, v in cluster.records(OUT):
        d = json.loads(v)
        res.setdefault(d["u"], []).append(d)
    return res


def test_out_of_order_input_is_emitted_in_event_time_order_with_trace_fields():
    c, clk = fk.Cluster(), Clock()
    for t in (5.0, 3.5, 4.0, 10.0):                       # no sequence numbers -> watermark ordering
        c.append(IN, b"u1", ev("u1", 100 + t))
    st, proc = stage(c, clk)
    st.run(max_iterations=2)
    clk.t += 3.0                                           # idle bound releases the last event
    st.run(max_iterations=2)
    got = out_by_user(c)["u1"]
    assert [d["t"] for d in got] == [103.5, 104.0, 105.0, 110.0]
    m = got[0]["meta"]
    assert m["order"] == "watermark" and m["ts_seq_out"] >= m["ts_seq_in"] and got[0]["ti"] > 0
    assert got[-1]["meta"]["order"] == "idle" and got[-1]["meta"]["reorder_wait"] <= 3.0 + 1e-9


def test_late_events_follow_the_sensitivity_policy():
    c, clk = fk.Cluster(), Clock()
    c.append(IN, b"u1", ev("u1", 200.0, seq=1, stream="g:a"))
    c.append(IN, b"u1", ev("u1", 150.0, s=1, r="payroll"))             # s=1, behind the watermark
    c.append(IN, b"u1", ev("u1", 151.0, s=0))                          # s=0, behind the watermark
    st, _ = stage(c, clk, policy_s0="discard", policy_s1="retro_alert")
    st.run(max_iterations=2)
    got = out_by_user(c)["u1"]
    assert [d["t"] for d in got] == [200.0, 150.0]                     # discarded s=0 event is not passed on
    assert got[1]["meta"]["late_policy"] == "retro_alert"
    audits = [json.loads(v) for _, _, v in c.records(LATE)]
    assert sorted(a["policy"] for a in audits) == ["discard", "retro_alert"]


def test_aborted_transaction_is_retried_without_duplicates_or_loss():
    c, clk = fk.Cluster(), Clock()
    for i in range(1, 21):
        c.append(IN, b"u1", ev("u1", 100.0 + i, seq=i, stream="g:a"))
    st, _ = stage(c, clk, fail_commits=1)                  # first commit fails -> abort, rewind, retry
    st.run(max_iterations=4)
    got = out_by_user(c)["u1"]
    assert [d["seq"] for d in got] == list(range(1, 21))
    assert st.stats["aborted"] == 1 and st.p.aborts == 1


def test_crash_restart_restores_buffered_state_from_the_changelog_exactly_once():
    c, clk = fk.Cluster(), Clock()
    c.append(IN, b"u1", ev("u1", 100.0, seq=1, stream="g:a"))
    c.append(IN, b"u1", ev("u1", 105.0, seq=5, stream="g:a"))          # gap -> buffered at crash time
    c.append(IN, b"u2", ev("u2", 50.0, seq=1, stream="g:b"))
    st, _ = stage(c, clk)
    st.run(max_iterations=2)
    assert [d["t"] for d in out_by_user(c)["u1"]] == [100.0]           # 105 still buffered (and checkpointed)
    # crash: the process dies; a NEW stage + processor resumes from committed offsets and the changelog
    c.append(IN, b"u1", ev("u1", 103.0, seq=3, stream="g:a"))
    st2, proc2 = stage(c, clk)
    st2.run(max_iterations=2)
    assert st2.stats["restored_keys"] >= 2
    clk.t += 3.0
    st2.run(max_iterations=2)
    got = out_by_user(c)
    assert [d["t"] for d in got["u1"]] == [100.0, 103.0, 105.0]         # buffered event survived, order kept
    assert [d["t"] for d in got["u2"]] == [50.0]                         # no duplicates of earlier output
    ids = [json.loads(v)["event_id"] for _, _, v in c.records(OUT)]
    assert len(ids) == len(set(ids))

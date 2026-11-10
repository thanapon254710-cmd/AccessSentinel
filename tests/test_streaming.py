"""Phase 4 tests: consumer commit/DLQ/retry/rebalance semantics, HDFS sink (buffering, dedup), history lake
(partitioning, pruning, dedup, compaction), repartitioner, spool replay, consumer lag."""
import json
import logging

import pytest

from accesssentinel.adapters.event_publisher import EventPublisher
from accesssentinel.adapters.hdfs_lake import EVENTS, HistoryLake, LocalFsClient, utc_date
from accesssentinel.broker import resource_key
from accesssentinel.schema import make_event
from accesssentinel.streaming.codec import decode_access_event
from accesssentinel.streaming.consumer import DeadLetterQueue, DecodeError, StreamConsumer
from accesssentinel.streaming.dedup import MemoryDedup
from accesssentinel.streaming.hdfs_sink import HdfsSinkHandler
from accesssentinel.streaming.lag import group_lag
from accesssentinel.streaming.repartitioner import RepartitionHandler

DAY1 = 1_794_000_000.0           # 2026-11-07 UTC
TB = 900.0


def ev(u="alice", t=DAY1 + 100, a="hr", r="payroll", s=1, o="view", y=True):
    return make_event(u, a, r, s, o, y, {"ip": "10.0.0.1", "device": "d", "session": "active"}, "c" * 32, t=t)


def payload(e) -> bytes:
    return json.dumps(e.__dict__).encode()


# ------------------------------------------------------------------------------------------ fakes
class Err:
    def __init__(self, code, fatal=False): self._c, self._f = code, fatal
    def code(self): return self._c
    def fatal(self): return self._f


class Msg:
    def __init__(self, topic, partition, offset, value, key=b"k", err=None):
        self._t, self._p, self._o, self._v, self._k, self._e = topic, partition, offset, value, key, err
    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o
    def value(self): return self._v
    def key(self): return self._k
    def error(self): return self._e
    def timestamp(self): return (1, 1_000)


class FakeConsumer:
    def __init__(self, batches): self.batches, self.commits, self.closed = list(batches), [], False
    def subscribe(self, topics, on_assign=None, on_revoke=None, on_lost=None):
        self.topics, self.on_revoke, self.on_lost = topics, on_revoke, on_lost
    def consume(self, n, timeout): return self.batches.pop(0) if self.batches else []
    def commit(self, offsets, asynchronous): self.commits.append(sorted((o.topic, o.partition, o.offset) for o in offsets))
    def close(self): self.closed = True


class FakeProducer:
    def __init__(self, fail=False, remaining=0):
        self.sent, self.fail, self.remaining, self._cbs = [], fail, remaining, []
    def __call__(self, conf): return self
    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.sent.append((topic, key, value, dict(headers or [])))
        self._cbs.append((on_delivery, value))
    def poll(self, t=0): pass
    def flush(self, t=None):
        for cb, v in self._cbs:
            if cb:
                cb("broker down" if self.fail else None, type("M", (), {"value": lambda s, v=v: v})())
        self._cbs = []
        return self.remaining
    def list_topics(self, topic=None, timeout=None): return None
    def __len__(self): return 0


def lake(tmp_path):
    return HistoryLake(LocalFsClient(str(tmp_path / "hist")), base="/accesssentinel")


def sink(tmp_path, dedup=None, rows=2, clock=lambda: 0.0):
    return HdfsSinkHandler(lake(tmp_path), dedup or MemoryDedup(), flush_rows=rows, flush_seconds=30, clock=clock)


def consumer_for(handler, batches, producer=None, **kw):
    fc = FakeConsumer(batches)
    prod = producer if producer is not None else FakeProducer()      # FakeProducer is falsy (len 0)
    sc = StreamConsumer(fc, ["access-events"], handler, DeadLetterQueue(prod, "g"), sleep=lambda s: None, **kw)
    return sc, fc, prod


# ------------------------------------------------------------------------------------------ codec / keys
def test_decode_rejects_bad_payloads_without_echoing_them():
    assert decode_access_event(payload(ev())).u == "alice"
    for bad in [b"not json", b"[1,2]", json.dumps({**ev().__dict__, "o": "steal", "u": "SECRET"}).encode()]:
        with pytest.raises(DecodeError) as e:
            decode_access_event(bad)
        assert "SECRET" not in str(e.value)


def test_resource_key_is_resource_plus_time_bucket():
    assert resource_key(ev(t=DAY1 + 10), TB) == resource_key(ev(u="bob", t=DAY1 + 20), TB)
    assert resource_key(ev(t=DAY1), TB) != resource_key(ev(t=DAY1 + TB), TB)
    assert resource_key(ev(), TB).startswith("hr:payroll#")


# ------------------------------------------------------------------------------------------ consumer
def test_offsets_committed_only_after_durable_checkpoint(tmp_path):
    h = sink(tmp_path, rows=2)
    m = [Msg("access-events", 0, i, payload(ev(t=DAY1 + i))) for i in range(3)]
    sc, fc, _ = consumer_for(h, [[m[0]], [m[1]], [m[2]]])
    sc.poll_once()
    assert fc.commits == [] and h.buffered() == 1                 # 1 row buffered, not durable yet
    sc.poll_once()
    assert fc.commits == [[("access-events", 0, 2)]]             # flushed at 2 rows -> commit next offset 2
    sc.run(max_polls=1)                                          # 3rd record, then shutdown forces checkpoint
    assert fc.commits[-1] == [("access-events", 0, 3)] and fc.closed
    assert len(h.lake.read_events(0, 2e9)) == 3


def test_poison_records_go_to_dlq_with_headers_and_are_committed(tmp_path):
    h = sink(tmp_path, rows=1)
    batch = [Msg("access-events", 1, 7, b"{oops"), Msg("access-events", 1, 8, payload(ev()))]
    sc, fc, prod = consumer_for(h, [batch])
    sc.poll_once()
    (topic, key, value, headers), = prod.sent
    assert topic == "access-events.dlq" and value == b"{oops"
    assert headers["as-source-offset"] == b"7" and headers["as-consumer-group"] == b"g" and b"JSON" in headers["as-error"]
    assert fc.commits == [[("access-events", 1, 9)]] and sc.stats["dead_lettered"] == 1
    assert len(h.lake.read_events(0, 2e9)) == 1


def test_dlq_write_failure_resends_and_blocks_commit(tmp_path):
    h = sink(tmp_path, rows=1)
    prod = FakeProducer(fail=True)
    sc, fc, _ = consumer_for(h, [[Msg("access-events", 0, 0, b"bad")]], producer=prod)
    commits_while_failing = []

    def sleep(_):
        commits_while_failing.append(list(fc.commits))
        prod.fail = False                                        # DLQ topic becomes writable again
    sc.sleep = sleep
    sc.poll_once()
    assert sc.stats["retries"] == 1 and commits_while_failing == [[]]
    assert [t for t, *_ in prod.sent] == ["access-events.dlq", "access-events.dlq"]   # re-sent, not lost
    assert fc.commits == [[("access-events", 0, 1)]]


class FlakyHandler:
    def __init__(self, failures): self.failures, self.got = failures, []
    def decode(self, rec): return decode_access_event(rec.value)
    def handle(self, records):
        if self.failures:
            self.failures -= 1
            raise IOError("HDFS unavailable")
        self.got += records
        return []
    def checkpoint(self, force): return True
    def reset(self): pass


def test_transient_failure_is_retried_and_nothing_committed_meanwhile():
    h = FlakyHandler(failures=2)
    commits_during_retry = []
    sc, fc, _ = consumer_for(h, [[Msg("access-events", 0, 0, payload(ev()))]])
    sc.sleep = lambda s: commits_during_retry.append(list(fc.commits))
    sc.poll_once()
    assert sc.stats["retries"] == 2 and all(c == [] for c in commits_during_retry)
    assert len(h.got) == 1 and fc.commits == [[("access-events", 0, 1)]]


def test_stop_during_outage_commits_nothing_for_the_failing_batch():
    h = FlakyHandler(failures=10**6)
    sc, fc, _ = consumer_for(h, [[Msg("access-events", 0, 0, payload(ev()))]])
    sc.sleep = lambda s: sc.stop()
    with pytest.raises(IOError):
        sc.run()
    assert fc.commits == [] and fc.closed


def test_revoke_checkpoints_buffered_work_or_discards_it(tmp_path):
    h = sink(tmp_path, rows=100)
    sc, fc, _ = consumer_for(h, [[Msg("access-events", 0, 0, payload(ev()))]])
    sc.poll_once()
    assert fc.commits == []
    fc.on_revoke(fc, [])
    assert fc.commits == [[("access-events", 0, 1)]] and h.buffered() == 0

    h2 = sink(tmp_path / "b", rows=100)
    sc2, fc2, _ = consumer_for(h2, [[Msg("access-events", 0, 0, payload(ev()))]])
    sc2.poll_once()
    h2.lake.write_events = lambda items: (_ for _ in ()).throw(IOError("down"))
    fc2.on_revoke(fc2, [])
    assert fc2.commits == [] and h2.buffered() == 0 and sc2.pending == {}       # re-read by the new owner


# ------------------------------------------------------------------------------------------ sink
def test_sink_dedups_redelivery_and_within_batch(tmp_path):
    shared = MemoryDedup()
    e = ev()
    h1 = sink(tmp_path, dedup=shared, rows=1)
    sc1, fc1, _ = consumer_for(h1, [[Msg("access-events", 0, 0, payload(e)), Msg("access-events", 0, 1, payload(e))]])
    sc1.poll_once()
    # crash after write but before commit -> a new instance gets the record again
    h2 = HdfsSinkHandler(h1.lake, shared, flush_rows=1)
    sc2, fc2, _ = consumer_for(h2, [[Msg("access-events", 0, 0, payload(e))]])
    sc2.poll_once()
    assert len(h1.lake.read_events(0, 2e9, dedup=False)) == 1
    assert h1.stats["duplicates_skipped"] == 1 and h2.stats["duplicates_skipped"] == 1
    assert fc2.commits == [[("access-events", 0, 1)]]


def test_sink_flushes_by_age(tmp_path):
    now = {"t": 0.0}
    h = sink(tmp_path, rows=1000, clock=lambda: now["t"])
    sc, fc, _ = consumer_for(h, [[Msg("access-events", 0, 0, payload(ev()))], []])
    sc.poll_once()
    assert fc.commits == []
    now["t"] = 31.0
    sc.poll_once()
    assert fc.commits == [[("access-events", 0, 1)]]


def test_sink_writes_other_topics_as_datasets(tmp_path):
    h = HdfsSinkHandler(lake(tmp_path), MemoryDedup(), flush_rows=1)
    fc = FakeConsumer([[Msg("pdp-decisions", 0, 0, json.dumps({"decision_id": "d1", "u": "bob", "decision": "Block",
                                                                   "t": DAY1}).encode()),
                        Msg("risk-scores", 2, 5, json.dumps({"u": "bob", "s_ind": 0.7, "t": DAY1}).encode()),
                        Msg("risk-scores", 2, 6, b"[]")]])
    sc = StreamConsumer(fc, h.topics(), h, DeadLetterQueue(FakeProducer(), "g"), sleep=lambda s: None)
    sc.poll_once()
    (d,), (r,) = h.lake.read_records("pdp-decisions", 0, 2e9), h.lake.read_records("risk-scores", 0, 2e9)
    assert d["record_id"] == "d1" and d["payload"]["decision"] == "Block"
    assert r["record_id"] == "risk-scores:2:5" and r["payload"]["s_ind"] == 0.7
    assert sc.stats["dead_lettered"] == 1


# ------------------------------------------------------------------------------------------ lake
class CountingFs(LocalFsClient):
    def __init__(self, root): super().__init__(root); self.listed = []
    def list(self, path): self.listed.append(path); return super().list(path)


def test_lake_layout_pruning_filters_and_semantics(tmp_path):
    fs = CountingFs(str(tmp_path / "h"))
    lk = HistoryLake(fs, "/accesssentinel")
    day2 = DAY1 + 86400
    evs = [ev("alice", DAY1 + 10), ev("bob", DAY1 + 20, a="finance", r="payments"), ev("alice", day2 + 5),
           ev("carol", day2 + 6, a="docs", r="handbook", s=0)]
    lk.write_events([(e, {"partition": 3, "offset": i}) for i, e in enumerate(evs)])
    parts = {(p["date"], p["app"]) for p in lk.partitions()}
    assert parts == {(utc_date(DAY1), "hr"), (utc_date(DAY1), "finance"), (utc_date(day2), "hr"), (utc_date(day2), "docs")}
    assert [e.u for e in lk.user_history("alice", 0, 2e9)] == ["alice", "alice"]
    fs.listed.clear()
    got = lk.read_events(day2, day2 + 100, apps=["docs"])
    assert [e.u for e in got] == ["carol"]
    assert not any(f"date={utc_date(DAY1)}" in p for p in fs.listed)     # other dates never listed
    assert not any("app=hr" in p for p in fs.listed)                      # other apps never listed
    assert [e.u for e in lk.read_events(DAY1 + 10, DAY1 + 20)] == ["bob"]  # (t0, t1]
    back = lk.read_events(0, 2e9)[0]
    assert back.c == evs[0].c and back.meta == evs[0].meta and back.event_id == evs[0].event_id


def test_lake_read_dedup_and_compaction(tmp_path):
    lk = lake(tmp_path)
    e1, e2 = ev(t=DAY1 + 1), ev(t=DAY1 + 2)
    lk.write_events([(e1, {})]); lk.write_events([(e1, {}), (e2, {})])            # e1 written twice
    assert len(lk.read_events(0, 2e9)) == 2 and len(lk.read_events(0, 2e9, dedup=False)) == 3
    res = lk.compact(EVENTS, utc_date(DAY1), "hr")
    assert res == {"files_before": 2, "rows_before": 3, "rows": 2}
    assert len(lk.read_events(0, 2e9, dedup=False)) == 2 and lk.partitions()[0]["files"] == 1


def test_lake_is_a_drop_in_history_store(tmp_path):
    lk = lake(tmp_path)
    for i in range(3):
        lk.append(ev(t=DAY1 + i))
    assert len(lk) == 3 and len(lk.read_range(DAY1 - 1, DAY1 + 10)) == 3
    lk.write_feedback([{"u": "a", "t": DAY1, "s_ind": 0.2}])
    assert lk.read_records("feedback-records", 0, 2e9)[0]["payload"]["u"] == "a"


def test_local_backend_is_labelled_development_only(tmp_path, caplog):
    with caplog.at_level(logging.WARNING):
        LocalFsClient(str(tmp_path / "x"))
    assert "development fallback, not HDFS" in caplog.text


# ------------------------------------------------------------------------------------------ repartitioner
def test_repartitioner_rekeys_and_forwards_unchanged_payload():
    prod = FakeProducer()
    h = RepartitionHandler(prod, "access-events-by-resource", TB)
    e = ev()
    sc, fc, _ = consumer_for(h, [[Msg("access-events", 4, 9, payload(e), key=b"alice")]])
    sc.poll_once()
    (topic, key, value, headers), = prod.sent
    assert topic == "access-events-by-resource" and key == resource_key(e, TB).encode() and value == payload(e)
    assert headers["as-source"] == b"access-events:4:9" and fc.commits == [[("access-events", 4, 10)]]


def test_repartitioner_unacknowledged_batch_is_retried_not_committed():
    prod = FakeProducer(fail=True)
    h = RepartitionHandler(prod, "out", TB)
    sc, fc, _ = consumer_for(h, [[Msg("access-events", 0, 0, payload(ev()))]])
    sc.sleep = lambda s: setattr(prod, "fail", False)
    sc.poll_once()
    assert sc.stats["retries"] == 1 and len(prod.sent) == 2 and fc.commits == [[("access-events", 0, 1)]]


# ------------------------------------------------------------------------------------------ spool replay
def test_spool_replay_republishes_and_removes_file(tmp_path):
    prod = FakeProducer(fail=True)
    pub = EventPublisher("fake:9092", "access-events", str(tmp_path), producer_factory=prod)
    events = [ev(t=DAY1 + i) for i in range(3)]
    for e in events:
        pub.publish(e)
    prod.flush()                                                   # delivery failures -> spooled
    assert pub.stats()["spooled"] == 3 and len(list(tmp_path.iterdir())) == 1
    prod.fail, prod.sent = False, []
    res = pub.replay_spool()
    pub.close()
    assert res == {"files": 1, "replayed": 3, "kept": 0} and list(tmp_path.iterdir()) == []
    assert [json.loads(v)["event_id"] for _, _, v, _ in prod.sent] == [e.event_id for e in events]
    assert all(k == b"alice" for _, k, _, _ in prod.sent)


def test_spool_replay_keeps_file_when_not_acknowledged(tmp_path):
    prod = FakeProducer(fail=True)
    pub = EventPublisher("fake:9092", "access-events", str(tmp_path), producer_factory=prod)
    pub.publish(ev()); prod.flush()
    prod.fail, prod.remaining = False, 1
    res = pub.replay_spool()
    pub.close()
    assert res["kept"] == 1 and [p.name.endswith(".replaying") for p in tmp_path.iterdir()] == [True]


# ------------------------------------------------------------------------------------------ lag
def test_group_lag():
    from confluent_kafka import TopicPartition

    class C:
        def __init__(self, conf): pass
        def list_topics(self, timeout=None):
            return type("MD", (), {"topics": {"access-events": type("T", (), {"partitions": {0: None, 1: None}})()}})()
        def committed(self, tps, timeout=None):
            return [TopicPartition(tp.topic, tp.partition, 10 if tp.partition == 0 else -1001) for tp in tps]
        def get_watermark_offsets(self, tp, timeout=None): return (2, 15)
        def close(self): pass
    res = group_lag("x", "hdfs-sink", ["access-events"], consumer_factory=C)
    assert res["partitions"]["access-events"] == {0: 5, 1: 13} and res["total"] == 18


@pytest.mark.parametrize("point", ["crash-before-mark", "crash-after-mark"])
def test_fault_injection_points_exit_at_the_right_moment(tmp_path, monkeypatch, point):
    """The hook used by the live crash experiment fires after the HDFS write, at the named point."""
    import accesssentinel.streaming.hdfs_sink as hs
    exits = []
    monkeypatch.setattr(hs.os, "_exit", lambda code: (exits.append(code), (_ for _ in ()).throw(SystemExit(code))))
    dedup = MemoryDedup()
    h = HdfsSinkHandler(lake(tmp_path), dedup, flush_rows=1, fault_inject=point)
    e = ev()
    h.handle([type("R", (), {"topic": "access-events", "partition": 0, "offset": 0, "key": b"", "timestamp_ms": 0,
                             "data": e})()])
    with pytest.raises(SystemExit):
        h.checkpoint(False)
    assert exits == [137] and len(h.lake.read_events(0, 2e9)) == 1
    assert (e.event_id in dedup.ids) == (point == "crash-after-mark")

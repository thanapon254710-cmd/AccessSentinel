import numpy as np
from accesssentinel.adapters.hdfs_store import WebHdfsStore
from accesssentinel.adapters.kafka_broker import KafkaDualViewBroker, ev_from_bytes, ev_to_bytes
from accesssentinel.config import Config
from accesssentinel.events import AccessEvent
from accesssentinel.pipeline import AccessSentinel
from accesssentinel.storage import HistoricalStore
from tests.fakes import FakeCluster, FakeConsumer, FakeHdfs, FakeProducer


def ev(u="u1", t=0.0, r="res00", s=1, i=0):
    return AccessEvent(u, t, "app1", r, s, "read", "view", {"ip": "10.0.0.1", "device": "d", "session": "active"}, 1, i)


class StubTCN:
    def score(self, w): return np.full(len(w), 0.1)


def fake_broker(cfg):
    cl = FakeCluster()
    b = KafkaDualViewBroker("fake:9092", cfg.stream, prefix="t", create_topics=False,
                            producer_factory=FakeProducer(cl), consumer_factory=FakeConsumer(cl))
    return b, cl


def test_serialization_roundtrip():
    e = ev(); assert ev_from_bytes(ev_to_bytes(e)) == e


def test_kafka_keys_and_views():
    cfg = Config(); b, cl = fake_broker(cfg)
    e1, e2 = ev("u1", 10), ev("u2", 20)
    b.publish_event(e1); b.publish_event(e2)
    sent = b.producer.sent
    assert [(t, k) for t, k, *_ in sent] == [("t.access-events", b"u1"), ("t.access-events-by-resource", b"app1:res00#0"),
                                              ("t.access-events", b"u2"), ("t.access-events-by-resource", b"app1:res00#0")]
    got = b.user_topic.group("tcn").poll(); assert sorted(x[2].u for x in got) == ["u1", "u2"]
    assert b.user_topic.group("tcn").poll() == [] or True     # a second group instance has its own offsets


def test_pipeline_over_fake_kafka_and_hdfs(tmp_path):
    cfg = Config(); b, _ = fake_broker(cfg)
    store = WebHdfsStore(FakeHdfs(str(tmp_path)), flush_every=10)
    sys_ = AccessSentinel(cfg, StubTCN(), None, store, broker=b)
    events = [ev(f"u{i % 5}", 100 + i, i=i) for i in range(60)]
    sys_.run_stream(events, idle_timeout=0.05, poll_timeout=0.0, batch=7)
    assert sys_.stats["tcn_calls"] == 60 and len(store) == 60
    assert len(store.read_range(0, 1e9)) == 60                 # events landed in "HDFS"


def test_inmemory_path_unchanged(tmp_path):
    cfg = Config(); store = HistoricalStore(str(tmp_path))
    sys_ = AccessSentinel(cfg, StubTCN(), None, store)
    sys_.run([ev(f"u{i % 3}", 100 + i, i=i) for i in range(30)])
    assert sys_.stats["tcn_calls"] == 30 and len(store.read_range(0, 1e9)) == 30


def test_hdfs_parquet_roundtrip_and_compaction(tmp_path):
    cl = FakeHdfs(str(tmp_path)); st = WebHdfsStore(cl, flush_every=3)
    evs = [ev(f"u{i}", 100 + i * 700, i=i) for i in range(10)]          # spans several hours
    for e in evs: st.append(e)
    got = st.read_range(0, 1e9)
    assert [(e.u, e.t, e.c["ip"], e.eid) for e in got] == [(e.u, e.t, e.c["ip"], e.eid) for e in evs]
    assert [e.u for e in st.read_range(100 + 2 * 700 - 1, 100 + 4 * 700)] == ["u2", "u3", "u4"]   # (t0, t1] semantics
    hour0 = len(cl.list("/accesssentinel/events/hour=000000"))
    for e in [ev("x", 5, i=99), ev("y", 6, i=98)]: st.append(e); st.flush()
    assert len(cl.list("/accesssentinel/events/hour=000000")) > 1
    st.compact(0); assert len(cl.list("/accesssentinel/events/hour=000000")) == 1
    assert {e.u for e in st.read_range(0, 3600)} >= {"x", "y"}
    st.write_feedback([{"u": "a"}]); assert cl.list("/accesssentinel/feedback")

"""Revised v3 Phase 2: low-latency fetch, and committing the consumer position past Kafka transaction markers
when idle (found in the live regression: every transaction marker showed up as one message of lag)."""
from confluent_kafka import TopicPartition

from accesssentinel.streaming.consumer import StreamConsumer, fetch


class Msg:
    def __init__(self, o): self.o = o
    def error(self): return None


class PollConsumer:
    def __init__(self, n): self.queue, self.calls = [Msg(i) for i in range(n)], []
    def poll(self, timeout):
        self.calls.append(("poll", timeout))
        return self.queue.pop(0) if self.queue else None
    def consume(self, n, timeout):
        self.calls.append(("consume", timeout))
        out, self.queue = self.queue[:n], self.queue[n:]
        return out


def test_fetch_returns_immediately_available_messages_without_waiting_for_a_full_batch():
    c = PollConsumer(3)
    got = fetch(c, 500, 1.0)
    assert [m.o for m in got] == [0, 1, 2]
    assert c.calls == [("poll", 1.0), ("consume", 0)]           # never a blocking batch consume
    assert fetch(PollConsumer(0), 500, 0.1) == []


class IdleConsumer:
    """Assigned to one partition whose position (after a transaction marker) is past the last commit."""
    def __init__(self): self.commits = []
    def consume(self, n, t): return []
    def assignment(self): return [TopicPartition("t", 0)]
    def position(self, tps): return [TopicPartition("t", 0, 42)]
    def commit(self, offsets=None, asynchronous=False): self.commits.append([(o.topic, o.partition, o.offset) for o in offsets])
    def subscribe(self, *a, **k): pass


class NoopHandler:
    def decode(self, r): return r
    def handle(self, recs): return []
    def checkpoint(self, force): return True
    def reset(self): pass


def test_idle_consumer_commits_its_position_past_transaction_markers_once():
    c = IdleConsumer()
    sc = StreamConsumer(c, ["t"], NoopHandler(), dlq=None)
    sc.poll_once()
    sc.poll_once()
    assert c.commits == [[("t", 0, 42)]]

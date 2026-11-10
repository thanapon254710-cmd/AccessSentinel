"""In-process stand-in for Apache Kafka: partitioned append-only topics, consumer groups,
user-keyed view and a derived resource-keyed view with a composite (resource + time-bucket) key."""
import zlib
from typing import Any, List, Tuple
from .config import StreamConfig
from .events import AccessEvent


def partition_for(key: str, n: int) -> int:
    return zlib.crc32(key.encode()) % n       # Kafka uses murmur2; any stable hash preserves the property


class Topic:
    def __init__(self, name: str, n_partitions: int):
        self.name, self.n = name, n_partitions
        self.partitions: List[List[Any]] = [[] for _ in range(n_partitions)]

    def publish(self, key: str, value: Any) -> Tuple[int, int]:
        p = partition_for(key, self.n)
        self.partitions[p].append(value)
        return p, len(self.partitions[p]) - 1

    def group(self, name: str) -> "ConsumerGroup":
        return ConsumerGroup(self, name)


class ConsumerGroup:
    def __init__(self, topic: Topic, name: str):
        self.topic, self.name = topic, name
        self.offsets = [0] * topic.n

    def poll(self, partitions=None, timeout: float = 0.0):
        out = []
        for p in (range(self.topic.n) if partitions is None else partitions):
            log = self.topic.partitions[p]
            while self.offsets[p] < len(log):
                out.append((p, self.offsets[p], log[self.offsets[p]]))
                self.offsets[p] += 1
        return out


class DualViewBroker:
    """access-events topic keyed by user; resource topic re-partitioned on (resource, time bucket)."""
    def __init__(self, cfg: StreamConfig):
        self.cfg = cfg
        self.user_topic = Topic("access-events", cfg.n_partitions)
        self.resource_topic = Topic("access-events-by-resource", cfg.n_partitions)

    def resource_key(self, ev: AccessEvent) -> str:
        return f"{ev.rkey}#{int(ev.t // self.cfg.time_bucket)}"

    def publish_event(self, ev: AccessEvent):
        self.user_topic.publish(ev.u, ev)
        self.resource_topic.publish(self.resource_key(ev), ev)

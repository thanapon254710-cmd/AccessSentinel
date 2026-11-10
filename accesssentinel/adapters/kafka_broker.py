"""Real Apache Kafka backend (confluent-kafka). Drop-in for broker.DualViewBroker:
same attributes (user_topic / resource_topic with .group(name).poll()), same resource_key()."""
import json
from typing import Callable, List, Optional
from ..config import StreamConfig
from ..events import AccessEvent
from ..topics import ACCESS_EVENTS, ACCESS_EVENTS_BY_RESOURCE, topic_name


def ev_to_bytes(ev: AccessEvent) -> bytes:
    return json.dumps(ev.__dict__, separators=(",", ":")).encode()


def ev_from_bytes(b: bytes) -> AccessEvent:
    return AccessEvent(**json.loads(b))


class KafkaConsumerGroup:
    """poll(timeout) -> [(partition, offset, AccessEvent)]. Offsets auto-commit (at-least-once)."""
    def __init__(self, topic: str, group_id: str, bootstrap: str, consumer_factory: Optional[Callable] = None,
                 max_records: int = 500):
        if consumer_factory is None:
            from confluent_kafka import Consumer
            consumer_factory = Consumer
        self.c = consumer_factory({"bootstrap.servers": bootstrap, "group.id": group_id,
                                   "auto.offset.reset": "earliest", "enable.auto.commit": True})
        self.c.subscribe([topic])
        self.max_records = max_records

    def poll(self, partitions=None, timeout: float = 0.0):
        out = []
        for m in self.c.consume(self.max_records, timeout):
            if m.error():
                if getattr(m.error(), "fatal", lambda: False)():
                    raise RuntimeError(str(m.error()))
                continue                                   # transient / partition-EOF
            out.append((m.partition(), m.offset(), ev_from_bytes(m.value())))
        return out

    def close(self):
        self.c.close()


class KafkaTopic:
    def __init__(self, name: str, bootstrap: str, producer, group_prefix: str, consumer_factory=None):
        self.name, self.bootstrap, self._p = name, bootstrap, producer
        self._prefix, self._cf = group_prefix, consumer_factory

    def publish(self, key: str, value: AccessEvent):
        self._p.produce(self.name, key=key.encode(), value=ev_to_bytes(value))
        self._p.poll(0)                                    # serve delivery callbacks

    def group(self, name: str) -> KafkaConsumerGroup:
        gid = f"{self._prefix}-{name}" if self._prefix else name
        return KafkaConsumerGroup(self.name, gid, self.bootstrap, self._cf)


class KafkaDualViewBroker:
    """access-events: key = user id. access-events-by-resource: key = 'app:res#time-bucket' (composite salt).
    Kafka's default partitioner (murmur2 over the key) keeps one user -> one partition (per-user order)."""
    def __init__(self, bootstrap: str, cfg: StreamConfig, prefix: str = "as", create_topics: bool = True,
                 producer_factory: Optional[Callable] = None, consumer_factory: Optional[Callable] = None):
        self.cfg, self.bootstrap, self.prefix = cfg, bootstrap, prefix
        if producer_factory is None:
            from confluent_kafka import Producer
            producer_factory = Producer
        self.producer = producer_factory({"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 5})
        if create_topics:
            self.ensure_topics()
        self.user_topic = KafkaTopic(topic_name(ACCESS_EVENTS, prefix), bootstrap, self.producer, prefix,
                                     consumer_factory)
        self.resource_topic = KafkaTopic(topic_name(ACCESS_EVENTS_BY_RESOURCE, prefix), bootstrap, self.producer,
                                         prefix, consumer_factory)

    def ensure_topics(self):
        from ..provision import ensure_topics
        ensure_topics(self.bootstrap, [topic_name(ACCESS_EVENTS, self.prefix),
                                       topic_name(ACCESS_EVENTS_BY_RESOURCE, self.prefix)], self.cfg.n_partitions, 1)

    def resource_key(self, ev: AccessEvent) -> str:
        return f"{ev.rkey}#{int(ev.t // self.cfg.time_bucket)}"

    def publish_event(self, ev: AccessEvent):
        self.user_topic.publish(ev.u, ev)
        self.resource_topic.publish(self.resource_key(ev), ev)

    def flush(self, timeout: float = 10.0):
        self.producer.flush(timeout)

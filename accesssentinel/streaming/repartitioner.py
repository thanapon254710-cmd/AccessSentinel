"""Resource-keyed view (D3 Step 2): re-partitions `access-events` (key = user) into
`access-events-by-resource` (key = application:resource#time-bucket) so that every access to a resource
within one bucket lands in one partition, where the graph stage (Phases 6-7) can build co-access edges
locally. The payload bytes are forwarded unchanged.

    python -m accesssentinel.streaming.repartitioner

At-least-once: a batch is committed only after Kafka acknowledged every re-keyed record; a retried batch may
re-publish some records, which downstream consumers drop by event_id. Ordering: records of one source
partition are produced in offset order; no order across source partitions is assumed (consumers use t).
Scale out by running more replicas: the consumer group splits the source partitions between them.
"""
import logging
from typing import List

from ..broker import resource_key
from ..topics import ACCESS_EVENTS, ACCESS_EVENTS_BY_RESOURCE, topic_name
from .codec import decode_access_event
from .consumer import Record

log = logging.getLogger("accesssentinel.repartitioner")


class RepartitionHandler:
    def __init__(self, producer, out_topic: str, time_bucket: float, flush_timeout: float = 30.0):
        self.p, self.out_topic, self.bucket, self.flush_timeout = producer, out_topic, time_bucket, flush_timeout
        self._errors: List[str] = []
        self.stats = {"forwarded": 0}

    def decode(self, rec: Record):
        return decode_access_event(rec.value)

    def _cb(self, err, msg) -> None:
        if err is not None:
            self._errors.append(str(err))

    def handle(self, records: List[Record]):
        self._errors = []
        for rec in records:
            key = resource_key(rec.data, self.bucket).encode()
            headers = [("as-source", f"{rec.topic}:{rec.partition}:{rec.offset}".encode())]
            while True:
                try:
                    self.p.produce(self.out_topic, key=key, value=rec.value, headers=headers, on_delivery=self._cb)
                    break
                except BufferError:
                    self.p.poll(0.5)                 # local queue full: serve callbacks, then retry
        remaining = self.p.flush(self.flush_timeout)
        if remaining or self._errors:
            raise RuntimeError(f"re-keyed batch not fully acknowledged: remaining={remaining} "
                               f"errors={self._errors[:3]}")
        self.stats["forwarded"] += len(records)
        return []

    def checkpoint(self, force: bool) -> bool:
        return True                                  # handle() returns only after Kafka acknowledged the batch

    def reset(self) -> None:
        pass


def main() -> int:
    """Revised v3: the repartitioner is now the exactly-once stage in resource_windows.py (key r*|b|shard).
    RepartitionHandler above is kept for the in-process D3 pipeline and its tests."""
    from .resource_windows import main as rw_main
    return rw_main(["repartition"])


if __name__ == "__main__":
    raise SystemExit(main())

"""Reliable Kafka consumer loop shared by every AccessSentinel stream processor.

Delivery semantics: at-least-once, made effectively-once by idempotent handlers.
  * Auto-commit is OFF. Offsets are committed only after the handler's `checkpoint()` reports that every
    record handed to it so far is durable (e.g. written to HDFS, or acknowledged by Kafka).
  * A record that cannot be decoded/validated, or that the handler rejects as poison, is written to
    `<topic>.dlq` with error headers; its offset is committed only after the DLQ write is acknowledged.
    One bad message therefore never blocks a partition and is never silently dropped.
  * Any other handler failure is treated as transient (e.g. HDFS or Kafka briefly unavailable): the same
    batch is retried with exponential backoff and nothing is committed meanwhile (backpressure).
  * On partition revocation (rebalance, scale-out) the handler is checkpointed and offsets committed; if
    that fails, buffered work is discarded and the new owner re-reads it from the last committed offset.
Ordering is preserved within a partition (records are handled in offset order). Nothing assumes ordering
across partitions.
"""
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple

log = logging.getLogger("accesssentinel.streaming")

PARTITION_EOF = -191          # confluent_kafka.KafkaError._PARTITION_EOF


class DecodeError(ValueError):
    """Record payload is malformed or violates the schema -> dead-letter queue."""


@dataclass
class Record:
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: bytes
    timestamp_ms: int
    data: Any = None          # decoded payload (set by Handler.decode)


class Handler(Protocol):
    def decode(self, rec: Record) -> Any: ...                              # raise DecodeError -> DLQ
    def handle(self, records: List[Record]) -> List[Tuple[Record, str]]: ...  # returns poison records; raise = transient
    def checkpoint(self, force: bool) -> bool: ...                          # True = everything handled is durable
    def reset(self) -> None: ...                                            # drop un-checkpointed work


class DeadLetterQueue:
    def __init__(self, producer, group_id: str):
        self.p, self.group = producer, group_id
        self._failed: List[str] = []
        self._unflushed: List[Tuple[Record, str]] = []     # sent since the last fully acknowledged flush
        self.sent = 0

    def send(self, rec: Record, reason: str) -> None:
        self._unflushed.append((rec, reason))
        self._produce(rec, reason)
        self.sent += 1
        log.warning("dead-lettered %s[%d]@%d: %s", rec.topic, rec.partition, rec.offset, reason[:200])

    def _produce(self, rec: Record, reason: str) -> None:
        headers = [("as-error", reason[:500].encode()), ("as-source-topic", rec.topic.encode()),
                   ("as-source-partition", str(rec.partition).encode()),
                   ("as-source-offset", str(rec.offset).encode()), ("as-consumer-group", self.group.encode()),
                   ("as-dead-lettered-at", str(time.time()).encode())]
        self.p.produce(f"{rec.topic}.dlq", key=rec.key, value=rec.value, headers=headers, on_delivery=self._cb)
        self.p.poll(0)

    def _cb(self, err, msg) -> None:
        if err is not None:
            self._failed.append(str(err))

    def flush(self, timeout: float = 30.0) -> None:
        """Block until every DLQ record is acknowledged; raise if any was not (offsets must not be committed).
        On failure the records are produced again before raising, so the caller's retry re-delivers them
        (a dead-letter record may then appear twice, but is never lost)."""
        remaining = self.p.flush(timeout)
        if remaining or self._failed:
            errs, self._failed = self._failed, []
            for rec, reason in self._unflushed:
                self._produce(rec, reason)
            raise RuntimeError(f"DLQ delivery incomplete: remaining={remaining} errors={errs[:3]}")
        self._unflushed.clear()


class StreamConsumer:
    def __init__(self, consumer, topics: List[str], handler: Handler, dlq: DeadLetterQueue, *,
                 batch_size: int = 500, poll_timeout: float = 1.0, max_backoff: float = 30.0,
                 heartbeat: Optional[Callable[[], None]] = None, sleep: Callable[[float], None] = time.sleep,
                 tick: Optional[Callable[[], None]] = None):
        self.c, self.h, self.dlq = consumer, handler, dlq
        self.tick = tick or (lambda: None)                # periodic work between polls (e.g. TCN rechecks)
        self.batch_size, self.poll_timeout, self.max_backoff = batch_size, poll_timeout, max_backoff
        self.heartbeat = heartbeat or (lambda: None)
        self.sleep = sleep
        self.pending: Dict[Tuple[str, int], int] = {}          # (topic, partition) -> next offset to commit
        self.stats = {"consumed": 0, "handled": 0, "dead_lettered": 0, "commits": 0, "retries": 0}
        self._running = True
        consumer.subscribe(topics, on_assign=self._on_assign, on_revoke=self._on_revoke, on_lost=self._on_lost)

    # ------------------------------------------------------------------ lifecycle
    def stop(self) -> None:
        self._running = False

    def run(self, max_polls: Optional[int] = None) -> None:
        polls = 0
        try:
            while self._running and (max_polls is None or polls < max_polls):
                self.poll_once()
                polls += 1
        finally:
            self._shutdown()

    def poll_once(self) -> None:
        msgs = self.c.consume(self.batch_size, self.poll_timeout)
        self.heartbeat()
        batch: List[Record] = []
        advanced: Dict[Tuple[str, int], int] = {}      # applied to self.pending only after handle() succeeds
        for m in msgs:
            err = m.error()
            if err is not None:
                if err.code() == PARTITION_EOF:
                    continue
                if err.fatal():
                    raise RuntimeError(f"fatal consumer error: {err}")
                log.warning("consumer error: %s", err)
                continue
            ts = m.timestamp()
            rec = Record(m.topic(), m.partition(), m.offset(), m.key(), m.value(), ts[1] if ts else 0)
            self.stats["consumed"] += 1
            try:
                rec.data = self.h.decode(rec)
                batch.append(rec)
            except DecodeError as e:
                self._dead_letter(rec, f"decode: {e}")
            advanced[(rec.topic, rec.partition)] = rec.offset + 1
        if batch:
            for rec, reason in self._retry("handle", lambda: self.h.handle(batch)) or []:
                self._dead_letter(rec, reason)
            self.stats["handled"] += len(batch)
        self.pending.update(advanced)
        self._commit_if_durable(force=False)
        self._retry("tick", self.tick)

    # ------------------------------------------------------------------ internals
    def _dead_letter(self, rec: Record, reason: str) -> None:
        self.dlq.send(rec, reason)
        self.stats["dead_lettered"] += 1

    def _retry(self, what: str, fn):
        delay = 1.0
        while True:
            try:
                return fn()
            except Exception as e:  # noqa: BLE001 - transient by contract; poison is returned, not raised
                if not self._running:
                    raise
                self.stats["retries"] += 1
                log.warning("%s failed (%s: %s); retrying in %.0fs, offsets not committed", what,
                            type(e).__name__, str(e)[:200], delay)
                self._sleep(delay)
                delay = min(delay * 2, self.max_backoff)

    def _sleep(self, seconds: float) -> None:
        remaining = seconds
        while self._running and remaining > 0:          # 1 s steps: stays responsive to stop() and heartbeats
            self.heartbeat()
            step = min(1.0, remaining)
            self.sleep(step)
            remaining -= step

    def _commit_if_durable(self, force: bool) -> bool:
        if not self.pending:
            return True
        if not self._retry("checkpoint", lambda: self.h.checkpoint(force)):
            return False
        self._retry("dlq flush", self.dlq.flush)
        self._commit()
        return True

    def _commit(self) -> None:
        from confluent_kafka import TopicPartition
        offsets = [TopicPartition(t, p, o) for (t, p), o in sorted(self.pending.items())]
        self.c.commit(offsets=offsets, asynchronous=False)
        self.pending.clear()
        self.stats["commits"] += 1

    def _on_assign(self, consumer, partitions) -> None:
        log.info("assigned %s", [(p.topic, p.partition) for p in partitions])

    def _on_revoke(self, consumer, partitions) -> None:
        log.info("revoked %s", [(p.topic, p.partition) for p in partitions])
        try:
            if self.pending and self.h.checkpoint(True):
                self.dlq.flush()
                self._commit()
        except Exception as e:  # noqa: BLE001
            log.warning("checkpoint on revoke failed (%s); discarding uncommitted work", type(e).__name__)
            self.h.reset()
            self.pending.clear()

    def _on_lost(self, consumer, partitions) -> None:
        log.warning("partitions lost %s; discarding uncommitted work", [(p.topic, p.partition) for p in partitions])
        self.h.reset()
        self.pending.clear()

    def _shutdown(self) -> None:
        try:
            if self.pending and self.h.checkpoint(True):
                self.dlq.flush()
                self._commit()
        except Exception as e:  # noqa: BLE001
            log.warning("final checkpoint failed (%s); uncommitted records will be re-read", type(e).__name__)
        finally:
            self.c.close()


def consumer_config(bootstrap: str, group_id: str, client_id: str) -> dict:
    return {"bootstrap.servers": bootstrap, "group.id": group_id, "client.id": client_id,
            "enable.auto.commit": False, "enable.auto.offset.store": False, "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 600_000, "session.timeout.ms": 45_000}


def producer_config(bootstrap: str, client_id: str) -> dict:
    return {"bootstrap.servers": bootstrap, "client.id": client_id, "enable.idempotence": True, "acks": "all",
            "linger.ms": 10, "message.timeout.ms": 60_000}

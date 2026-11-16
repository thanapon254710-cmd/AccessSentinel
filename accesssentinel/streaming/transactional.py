"""Exactly-once Kafka-to-Kafka stages with checkpointed keyed state (Revised v3: R3, Table II, Table V
"state loss on rebalance"; decision D1 in docs/V3_AUDIT.md).

Each loop iteration is ONE Kafka transaction containing:
  * the output records,
  * the changelog records of every keyed-state entry changed in this iteration
    (compacted topic, key = state key, partition = the input partition that owns the key),
  * the consumer offsets of the processed input (sent with the consumer-group metadata, KIP-447 fencing).
Either all three become visible or none does. After a crash, a restart or a rebalance, the new owner of an
input partition rebuilds that partition's state from the changelog and resumes at the committed offset, so
its state equals the state after the last committed record. This is the mechanism Kafka Streams calls
`exactly_once_v2`; downstream consumers read with isolation.level=read_committed (librdkafka's default).

Scope of the guarantee: Kafka outputs + changelog + offsets. Side effects outside Kafka (Redis, HDFS) are not
part of the transaction; stages that have them stay at-least-once with idempotent writes.

A processor implements:
    decode(rec) -> data               raise DecodeError for poison records (sent to <topic>.dlq in the txn)
    process(rec, emit)                update state, emit(topic, key, value, headers=None)
    on_tick(now, emit)                timers (e.g. idle release, window finalisation)
    take_dirty() -> {(partition, key_bytes): value_bytes | None}   changed state since the last call
    restore(partition, key_bytes, value_bytes)
    drop(partitions)                  forget state of partitions no longer owned
    partitions_of_key: not needed: the processor records the partition when it touches a key.
"""
import logging
import time
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from .consumer import DecodeError, Record, fetch

log = logging.getLogger("accesssentinel.transactional")


class Emitter:
    def __init__(self):
        self.out: List[Tuple[str, Optional[bytes], bytes, Optional[list]]] = []

    def __call__(self, topic: str, key: Optional[bytes], value: bytes, headers: Optional[list] = None) -> None:
        self.out.append((topic, key, value, headers))

    def drain(self):
        out, self.out = self.out, []
        return out


class TransactionalStage:
    def __init__(self, consumer, producer, restore_consumer_factory: Optional[Callable[[], object]],
                 input_topic: str, processor, changelog_topic: Optional[str] = None, *, batch: int = 500,
                 poll_timeout: float = 0.2, tick_every: float = 0.2, clock: Callable[[], float] = time.time,
                 max_backoff: float = 10.0, sleep: Callable[[float], None] = time.sleep,
                 on_iteration: Optional[Callable[[], None]] = None, stats_fn: Optional[Callable[[], dict]] = None,
                 log_every: float = 60.0, group_id: str = "transactional"):
        self.c, self.p, self.restore_factory = consumer, producer, restore_consumer_factory
        self.topic, self.proc, self.changelog = input_topic, processor, changelog_topic
        self.batch, self.poll_timeout, self.tick_every = batch, poll_timeout, tick_every
        self.clock, self.sleep, self.max_backoff = clock, sleep, max_backoff
        self.stats = {"transactions": 0, "aborted": 0, "records": 0, "emitted": 0, "dlq": 0, "restored_keys": 0,
                      "rebalances": 0}
        self._running = False
        self._next_tick = 0.0
        self._owned: set = set()
        self.group_id = group_id
        self.on_iteration, self.stats_fn, self.log_every, self._next_log = on_iteration, stats_fn, log_every, 0.0

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        self.p.init_transactions(30)
        self.c.subscribe([self.topic], on_assign=self._on_assign, on_revoke=self._on_revoke, on_lost=self._on_revoke)
        self._running = True

    def stop(self) -> None:
        self._running = False

    def run(self, max_iterations: Optional[int] = None) -> None:
        if not self._running:
            self.start()
        n, backoff = 0, 0.5
        while self._running and (max_iterations is None or n < max_iterations):
            n += 1
            if self.on_iteration:
                self.on_iteration()
            if self.stats_fn and self.clock() >= self._next_log:
                self._next_log = self.clock() + self.log_every
                log.info("stats %s", {**self.stats, **self.stats_fn()})
            try:
                self.step()
                backoff = 0.5
            except Exception as e:  # noqa: BLE001 - abort, rewind, retry with backoff
                log.warning("transaction failed (%s: %s); aborting and rewinding", type(e).__name__, str(e)[:200])
                self._abort_and_rewind()
                self.sleep(backoff)
                backoff = min(self.max_backoff, backoff * 2)
        try:
            self.c.close()
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------ one transaction
    def step(self) -> bool:
        msgs = fetch(self.c, self.batch, self.poll_timeout)
        now = self.clock()
        msgs = [m for m in msgs or [] if not m.error()]
        due = now >= self._next_tick
        if not msgs and not due:
            return False
        emit, dlq = Emitter(), []
        for m in msgs:                         # processing happens before the transaction opens; if the
            rec = Record(m.topic(), m.partition(), m.offset(), m.key(), m.value(), m.timestamp()[1])
            try:                               # transaction then fails, state is rebuilt from the changelog
                rec.data = self.proc.decode(rec)
            except DecodeError as e:
                dlq.append((rec, str(e)))
                continue
            self.proc.process(rec, emit)
        if due:
            self.proc.on_tick(now, emit)
            self._next_tick = now + self.tick_every
        out = emit.drain()
        dirty = self.proc.take_dirty() if self.changelog else {}
        if not msgs and not out and not dirty:
            return False                       # idle timer tick with nothing to write: no empty transaction
        self.p.begin_transaction()
        for rec, err in dlq:                   # same headers as streaming.consumer.DeadLetterQueue
            self.p.produce(f"{rec.topic}.dlq", key=rec.key, value=rec.value, headers=[
                ("as-error", f"decode: {err}"[:500].encode()), ("as-source-topic", rec.topic.encode()),
                ("as-source-partition", str(rec.partition).encode()), ("as-source-offset", str(rec.offset).encode()),
                ("as-consumer-group", str(self.group_id).encode()), ("as-dead-lettered-at", str(now).encode())])
            self.stats["dlq"] += 1
        for topic, key, value, headers in out:
            self._produce(topic, key, value, headers)
        for (part, key), value in dirty.items():
            self._produce(self.changelog, key, value, None, partition=part)
        positions = self.c.position(self.c.assignment())
        if positions:
            self.p.send_offsets_to_transaction(positions, self.c.consumer_group_metadata(), 30)
        self.p.commit_transaction(60)
        self.stats["transactions"] += 1
        self.stats["records"] += len(msgs)
        self.stats["emitted"] += len(out)
        return True

    def _produce(self, topic, key, value, headers, partition=None):
        kw = {"key": key, "value": value}
        if headers:
            kw["headers"] = headers
        if partition is not None:
            kw["partition"] = partition
        while True:
            try:
                self.p.produce(topic, **kw)
                return
            except BufferError:
                self.p.poll(0.1)

    # ------------------------------------------------------------------ failure handling
    def _abort_and_rewind(self) -> None:
        self.stats["aborted"] += 1
        try:
            self.p.abort_transaction(30)
        except Exception as e:  # noqa: BLE001
            log.warning("abort failed (%s)", type(e).__name__)
        parts = list(self._owned)
        if not parts:
            return
        try:
            committed = self.c.committed(self._tps(parts), 30)
            for tp in committed:
                self.c.seek(tp if tp.offset >= 0 else self._tp(tp.partition, -2))     # -2 = OFFSET_BEGINNING
        except Exception as e:  # noqa: BLE001
            log.warning("rewind failed (%s); state will be restored on the next rebalance", type(e).__name__)
        self.proc.drop(parts)
        self._restore(parts)

    # ------------------------------------------------------------------ rebalance
    def _on_assign(self, consumer, partitions) -> None:
        mine = [tp.partition for tp in partitions if tp.topic == self.topic]
        self.stats["rebalances"] += 1
        self._owned |= set(mine)
        self._restore(mine)

    def _on_revoke(self, consumer, partitions) -> None:
        gone = [tp.partition for tp in partitions if tp.topic == self.topic]
        self._owned -= set(gone)
        self.proc.drop(gone)

    def _restore(self, parts: Iterable[int]) -> None:
        parts = list(parts)
        if not self.changelog or not parts or self.restore_factory is None:
            return
        rc = self.restore_factory()
        try:
            n = restore_partitions(rc, self.changelog, parts, self.proc.restore)
            self.stats["restored_keys"] += n
            log.info("restored %d state records for partitions %s", n, sorted(parts))
        finally:
            try:
                rc.close()
            except Exception:  # noqa: BLE001
                pass

    def _tps(self, parts):
        return [self._tp(p, -1001) for p in parts]

    def _tp(self, p, offset):
        from confluent_kafka import TopicPartition
        return TopicPartition(self.topic, p, offset)


def restore_partitions(rc, changelog: str, parts: List[int], apply: Callable, idle_timeout: float = 5.0) -> int:
    """Read changelog partitions from the beginning up to their current end; apply(partition, key, value)."""
    from confluent_kafka import TopicPartition, OFFSET_BEGINNING
    ends = {}
    for p in parts:
        lo, hi = rc.get_watermark_offsets(TopicPartition(changelog, p), timeout=10)
        if hi > lo:
            ends[p] = hi
    if not ends:
        return 0
    rc.assign([TopicPartition(changelog, p, OFFSET_BEGINNING) for p in ends])
    n, last = 0, time.time()
    while ends:
        msgs = rc.consume(1000, 0.5)
        if not msgs and time.time() - last > idle_timeout:
            log.warning("changelog restore stopped early (no data for %.0f s); remaining %s", idle_timeout, ends)
            break
        for m in msgs:
            if m.error():
                continue
            last = time.time()
            apply(m.partition(), m.key(), m.value())
            n += 1
        for p in list(ends):
            pos = rc.position([TopicPartition(changelog, p)])[0].offset
            if pos >= ends[p]:
                del ends[p]
    return n


def transactional_producer_config(bootstrap: str, transactional_id: str) -> dict:
    return {"bootstrap.servers": bootstrap, "transactional.id": transactional_id, "enable.idempotence": True,
            "acks": "all", "linger.ms": 5, "transaction.timeout.ms": 60000, "client.id": transactional_id}


def transactional_consumer_config(bootstrap: str, group_id: str, client_id: str) -> dict:
    return {"bootstrap.servers": bootstrap, "group.id": group_id, "client.id": client_id,
            "enable.auto.commit": False, "auto.offset.reset": "earliest", "isolation.level": "read_committed",
            "partition.assignment.strategy": "range",
            # static membership (KIP-345): a restarted instance (same container, same client id) takes its
            # partitions back at once instead of waiting for the dead member's session to time out. Measured
            # in Phase 2: ~38 s of no processing after a hard kill with dynamic membership and 45 s sessions.
            "group.instance.id": client_id, "session.timeout.ms": 10000,
            "max.poll.interval.ms": 300000}


def restore_consumer_config(bootstrap: str, client_id: str) -> dict:
    return {"bootstrap.servers": bootstrap, "group.id": f"{client_id}-restore", "client.id": client_id,
            "enable.auto.commit": False, "isolation.level": "read_committed", "enable.partition.eof": False}

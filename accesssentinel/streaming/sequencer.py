"""Sequencer: per-user event-time ordering stage (Revised v3 Step 2, Eq. (2), R1), exactly-once.

    access-events (append order, key u)  ->  access-events.ordered (event-time order per u, key u)
                                         ->  late-events (late-event alerts / discard audit records)

    python -m accesssentinel.streaming.sequencer

Each event is annotated (in `meta`, so model features are unaffected):
  ts_seq_in / ts_seq_out   stage entry / release (trace stamps for the latency study, Exp. C)
  reorder_wait             seconds spent waiting for order (<= delta_late)
  order                    fast_path | watermark | idle
  late_policy              set only for late events: history_only | rescore | retro_alert | discard
and `ti` (Kafka ingestion time) is filled from the broker's LogAppendTime of the input record.
The ordering state is checkpointed in `sequencer.changelog` inside the same Kafka transaction as the output
and the input offsets (accesssentinel/streaming/transactional.py).
"""
import json
import logging
import os
import time
from typing import Dict, Optional, Set, Tuple

from .codec import decode_access_dict
from .consumer import DecodeError, Record
from .event_time import EventTimeOrderer, Item

log = logging.getLogger("accesssentinel.sequencer")


class SequencerProcessor:
    def __init__(self, orderer: EventTimeOrderer, out_topic: str, late_topic: str, clock=time.time,
                 evict_every: float = 60.0):
        self.o, self.out_topic, self.late_topic, self.clock = orderer, out_topic, late_topic, clock
        self.part_of: Dict[str, int] = {}                 # user -> input partition owning its state
        self._dirty: Set[str] = set()
        self._next_evict = 0.0
        self.evict_every = evict_every
        self.stats = {"events": 0, "released": 0, "late": 0, "discarded": 0, "alerts": 0}

    # ---------------------------------------------------------------- stage interface
    def decode(self, rec: Record):
        d = decode_access_dict(rec.value)
        d["ti"] = rec.timestamp_ms / 1000.0 if rec.timestamp_ms and rec.timestamp_ms > 0 else d.get("ti", 0.0)
        return d

    def process(self, rec: Record, emit) -> None:
        d, now = rec.data, self.clock()
        d.setdefault("meta", {})["ts_seq_in"] = now
        u = d["u"]
        self.part_of[u] = rec.partition
        self.stats["events"] += 1
        item = Item(te=float(d["t"]), seq=int(d.get("seq", -1)), stream=d.get("stream", ""), eid=d["event_id"],
                    s=int(d["s"]), arrived=now, payload=d)
        for r in self.o.add(u, item):
            self._emit(u, r, emit, now)
        self._dirty.add(u)

    def on_tick(self, now: float, emit) -> None:
        for u, r in self.o.tick(now):
            self._emit(u, r, emit, now)
            self._dirty.add(u)
        if now >= self._next_evict:
            self._next_evict = now + self.evict_every
            self._dirty |= set(self.o.evict(now))

    def take_dirty(self) -> Dict[Tuple[int, bytes], Optional[bytes]]:
        out = {}
        for u in self._dirty:
            p = self.part_of.get(u)
            if p is None:
                continue
            st = self.o.user_state(u)
            out[(p, u.encode())] = None if st is None else json.dumps(st, separators=(",", ":")).encode()
            if st is None:
                self.part_of.pop(u, None)
        self._dirty.clear()
        return out

    def restore(self, partition: int, key: bytes, value: Optional[bytes]) -> None:
        u = key.decode()
        self.o.restore_user(u, None if value is None else json.loads(value))
        if value is None:
            self.part_of.pop(u, None)
        else:
            self.part_of[u] = partition

    def drop(self, partitions) -> None:
        parts = set(partitions)
        for u in [u for u, p in self.part_of.items() if p in parts]:
            self.o.restore_user(u, None)
            del self.part_of[u]
            self._dirty.discard(u)

    # ---------------------------------------------------------------- output
    def _emit(self, u: str, r, emit, now: float) -> None:
        d = r.item.payload
        m = d.setdefault("meta", {})
        m["ts_seq_out"] = now
        m["reorder_wait"] = round(r.wait, 6)
        m["order"] = r.reason
        key = u.encode()
        if r.late is None:
            self.stats["released"] += 1
            emit(self.out_topic, key, json.dumps(d).encode())
            return
        self.stats["late"] += 1
        m["late_policy"] = r.late
        audit = {"kind": "late_event", "policy": r.late, "reason": r.reason, "u": u, "event_id": d["event_id"],
                 "te": d["t"], "ti": d.get("ti"), "s": d["s"], "resource": f"{d.get('tenant', 'default')}/{d['a']}:{d['r']}",
                 "watermark": self.o.users[u].watermark(self.o.delta) if u in self.o.users else None,
                 "t": now, "record_id": f"late:{d['event_id']}"}
        if r.late == "discard":
            self.stats["discarded"] += 1
            emit(self.late_topic, key, json.dumps(audit).encode())
            return
        if r.late == "retro_alert":
            self.stats["alerts"] += 1
            emit(self.late_topic, key, json.dumps(audit).encode())
        emit(self.out_topic, key, json.dumps(d).encode())     # history_only / rescore / retro_alert: passed on, flagged


def main() -> int:
    from confluent_kafka import Consumer, Producer
    from ..settings import get_settings
    from ..topics import ACCESS_EVENTS, ACCESS_EVENTS_ORDERED, LATE_EVENTS, SEQUENCER_CHANGELOG, topic_name
    from .runtime import Heartbeat, install_stop, setup
    from .transactional import (TransactionalStage, restore_consumer_config, transactional_consumer_config,
                                transactional_producer_config)
    s = get_settings()
    setup(s.log_level)
    t = lambda n: topic_name(n, s.kafka_topic_prefix)
    inst = os.environ.get("HOSTNAME", "local")
    orderer = EventTimeOrderer(s.delta_late, s.late_policy_s0, s.late_policy_s1, s.sequencer_idle_state_seconds,
                               multi_stream_window=s.sequencer_concurrency_window)
    proc = SequencerProcessor(orderer, t(ACCESS_EVENTS_ORDERED), t(LATE_EVENTS))
    stage = TransactionalStage(
        Consumer(transactional_consumer_config(s.kafka_bootstrap, "sequencer", f"sequencer-{inst}")),
        Producer(transactional_producer_config(s.kafka_bootstrap, f"sequencer-{inst}")),
        lambda: Consumer(restore_consumer_config(s.kafka_bootstrap, f"sequencer-{inst}")),
        t(ACCESS_EVENTS), proc, t(SEQUENCER_CHANGELOG), batch=s.txn_batch, on_iteration=Heartbeat(),
        stats_fn=lambda: {**proc.stats, **orderer.stats, "users": len(orderer.users)}, group_id="sequencer")
    install_stop(stage.stop)
    log.info("sequencer: delta_late=%.2fs policies s0=%s s1=%s", s.delta_late, s.late_policy_s0, s.late_policy_s1)
    stage.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

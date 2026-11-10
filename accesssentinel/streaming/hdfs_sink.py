"""HDFS sink (D3 Step 3): an independent consumer group that writes every record of the storage topics to
the history lake. It shares nothing with the real-time path except the Kafka topics it reads.

    python -m accesssentinel.streaming.hdfs_sink

Topics -> datasets:  access-events -> events (date/app partitions);  risk-scores, group-scores,
pdp-decisions, analyst-feedback -> dataset of the same name (date partitions).
Buffers rows and writes one Parquet file per partition when `sink_flush_rows` rows are buffered or the
oldest row is `sink_flush_seconds` old; offsets are committed only after that write (StreamConsumer).
"""
import logging
import os
import time
from typing import Dict, List, Optional, Tuple

from ..events import AccessEvent
from ..topics import (ACCESS_EVENTS, ANALYST_FEEDBACK, GROUP_SCORES, PDP_DECISIONS, RISK_SCORES, topic_name)
from .codec import decode_access_event, decode_json_object, record_id
from .consumer import Record

log = logging.getLogger("accesssentinel.hdfs_sink")

SINK_TOPICS = (ACCESS_EVENTS, RISK_SCORES, GROUP_SCORES, PDP_DECISIONS, ANALYST_FEEDBACK)


class HdfsSinkHandler:
    def __init__(self, lake, dedup, prefix: str = "", flush_rows: int = 2000, flush_seconds: float = 30.0,
                 compact_max_files: int = 24, clock=time.monotonic, fault_inject: str = ""):
        self.lake, self.dedup, self.clock, self.fault_inject = lake, dedup, clock, fault_inject
        self.flush_rows, self.flush_seconds, self.compact_max_files = flush_rows, flush_seconds, compact_max_files
        self.access_topic = topic_name(ACCESS_EVENTS, prefix)
        self.dataset_of = {topic_name(t, prefix): t for t in SINK_TOPICS}
        self._events: List[Tuple[AccessEvent, dict]] = []
        self._records: Dict[str, List[dict]] = {}
        self._ids: List[str] = []
        self._ids_set = set()
        self._oldest: Optional[float] = None
        self._touched = set()
        self.stats = {"written_events": 0, "written_records": 0, "duplicates_skipped": 0, "files": 0,
                      "compactions": 0}

    def topics(self) -> List[str]:
        return sorted(self.dataset_of)

    def decode(self, rec: Record):
        if rec.topic == self.access_topic:
            return decode_access_event(rec.value)
        return decode_json_object(rec.value)

    def handle(self, records: List[Record]):
        # dedup keys are namespaced by dataset: an access event and the risk score of that event are
        # different records even when they share an event_id
        ids = [f"{self.dataset_of[r.topic]}:{record_id(r)}" for r in records]
        already = self.dedup.seen([i for i in ids if i not in self._ids_set])
        now = time.time()
        for rec, rid in zip(records, ids):
            if rid in already or rid in self._ids_set:
                self.stats["duplicates_skipped"] += 1
                continue
            self._ids_set.add(rid)
            self._ids.append(rid)
            ingest = {"partition": rec.partition, "offset": rec.offset, "ingested_at": now}
            if rec.topic == self.access_topic:
                self._events.append((rec.data, ingest))
            else:
                d = rec.data
                ts = d.get("t") or d.get("ts") or d.get("decided_at") or (rec.timestamp_ms / 1000.0) or now
                self._records.setdefault(self.dataset_of[rec.topic], []).append(
                    {"record_id": rid.split(":", 1)[1], "ts": float(ts), "key": (rec.key or b"").decode(errors="replace"),
                     "payload": d, "_kafka_partition": rec.partition, "_kafka_offset": rec.offset,
                     "_ingested_at": now})
            if self._oldest is None:
                self._oldest = self.clock()
        return []

    def buffered(self) -> int:
        return len(self._events) + sum(len(v) for v in self._records.values())

    def checkpoint(self, force: bool) -> bool:
        n = self.buffered()
        if n == 0:
            return True
        due = force or n >= self.flush_rows or (self.clock() - self._oldest) >= self.flush_seconds
        if not due:
            return False
        if self._events:
            paths = self.lake.write_events(self._events)
            self.stats["files"] += len(paths)
            self.stats["written_events"] += len(self._events)
            for ev, _ in self._events:
                self._touched.add(("events", _date(ev.t), ev.a))
        for ds, rows in self._records.items():
            paths = self.lake.write_records(ds, rows)
            self.stats["files"] += len(paths)
            self.stats["written_records"] += len(rows)
            for r in rows:
                self._touched.add((ds, _date(r["ts"]), None))
        self._fault("crash-before-mark")
        self.dedup.mark(self._ids)                  # only after the files exist
        self._fault("crash-after-mark")             # i.e. written + marked, offsets not yet committed
        log.info("flushed %d rows (%d events) in %d file(s) total so far", n, len(self._events), self.stats["files"])
        self.reset()
        self._compact_if_needed()
        return True

    def reset(self) -> None:
        self._events, self._records, self._ids, self._ids_set, self._oldest = [], {}, [], set(), None

    def _fault(self, point: str) -> None:
        """Test hook for crash-consistency experiments (AS_FAULT_INJECT=<point>, never honoured in prod):
        terminates the process at an exact point of the write/commit protocol, like a SIGKILL would."""
        if self.fault_inject == point:
            log.error("FAULT INJECTION: terminating at %s", point)
            os._exit(137)

    def _compact_if_needed(self) -> None:
        """Small-file control: merge a partition we just wrote to once it has too many part files."""
        for ds, d, app in sorted(self._touched, key=str):
            try:
                directory = self.lake._part_dir(ds, d, app)
                n = len([f for f in self.lake._ls(directory) if f.endswith(".parquet")])
                if n > self.compact_max_files:
                    res = self.lake.compact(ds, d, app)
                    self.stats["compactions"] += 1
                    log.info("compacted %s %s %s: %s", ds, d, app, res)
            except Exception as e:  # noqa: BLE001 - compaction is best-effort; data is already durable
                log.warning("compaction of %s %s %s failed: %s", ds, d, app, type(e).__name__)
        self._touched.clear()


def _date(t: float) -> str:
    from ..adapters.hdfs_lake import utc_date
    return utc_date(float(t))


def main() -> int:
    import redis as redis_lib
    from confluent_kafka import Consumer, Producer

    from ..adapters.hdfs_lake import make_lake
    from ..settings import get_settings
    from .consumer import DeadLetterQueue, StreamConsumer, consumer_config, producer_config
    from .dedup import RedisDedup
    from .runtime import Heartbeat, install_stop, setup

    s = get_settings()
    setup(s.log_level)
    group = "hdfs-sink"
    handler = HdfsSinkHandler(make_lake(s), RedisDedup(redis_lib.Redis(**s.redis_kwargs()), group, s.dedup_ttl_seconds),
                              s.kafka_topic_prefix, s.sink_flush_rows, s.sink_flush_seconds, s.sink_compact_max_files,
                              fault_inject="" if s.environment == "prod" else os.environ.get("AS_FAULT_INJECT", ""))
    consumer = Consumer(consumer_config(s.kafka_bootstrap, group, "hdfs-sink"))
    dlq = DeadLetterQueue(Producer(producer_config(s.kafka_bootstrap, "hdfs-sink-dlq")), group)
    sc = StreamConsumer(consumer, handler.topics(), handler, dlq, batch_size=s.consumer_batch, heartbeat=Heartbeat())
    install_stop(sc.stop)
    log.info("hdfs-sink consuming %s (history backend=%s)", handler.topics(), s.history_backend)
    sc.run()
    log.info("hdfs-sink stopped: %s %s", sc.stats, handler.stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

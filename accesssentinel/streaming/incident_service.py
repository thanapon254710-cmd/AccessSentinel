"""Incident service: pdp-decisions + group-scores -> PostgreSQL incidents for analyst review.

    python -m accesssentinel.streaming.incident_service
"""
import logging
import socket
from typing import List

from ..incidents import IncidentStore
from ..topics import GROUP_SCORES, PDP_DECISIONS, topic_name
from .codec import decode_json_object
from .consumer import DecodeError, Record

log = logging.getLogger("accesssentinel.incidents")


class IncidentHandler:
    def __init__(self, store: IncidentStore, decisions_topic: str, group_topic: str):
        self.store, self.decisions_topic, self.group_topic = store, decisions_topic, group_topic
        self.stats = {"decisions": 0, "group_scores": 0, "incidents_touched": 0}

    def decode(self, rec: Record):
        d = decode_json_object(rec.value)
        if not isinstance(d.get("u"), str):
            raise DecodeError("record without u")
        return d

    def handle(self, records: List[Record]):
        for rec in records:
            if rec.topic == self.group_topic:
                iid = self.store.on_group_score(rec.data); self.stats["group_scores"] += 1
            else:
                iid = self.store.on_decision(rec.data); self.stats["decisions"] += 1
            if iid:
                self.stats["incidents_touched"] += 1
        return []

    def checkpoint(self, force: bool) -> bool:
        return True                       # each record is committed to PostgreSQL inside handle()

    def reset(self) -> None:
        pass


def main() -> int:
    from confluent_kafka import Consumer, Producer
    from ..db.engine import make_engine
    from ..settings import get_settings
    from .consumer import DeadLetterQueue, StreamConsumer, consumer_config, producer_config
    from .runtime import Heartbeat, install_stop, setup

    s = get_settings()
    setup(s.log_level)
    client = f"incidents-{socket.gethostname()}"
    dec, grp = topic_name(PDP_DECISIONS, s.kafka_topic_prefix), topic_name(GROUP_SCORES, s.kafka_topic_prefix)
    h = IncidentHandler(IncidentStore(make_engine(s)), dec, grp)
    group = "incidents"
    sc = StreamConsumer(Consumer(consumer_config(s.kafka_bootstrap, group, client)), [dec, grp], h,
                        DeadLetterQueue(Producer(producer_config(s.kafka_bootstrap, f"{client}-dlq")), group),
                        batch_size=s.consumer_batch, heartbeat=Heartbeat())
    install_stop(sc.stop)
    log.info("incident service consuming %s, %s", dec, grp)
    sc.run()
    log.info("incident service stopped: %s %s", sc.stats, h.stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

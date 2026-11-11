"""Consumer-group lag = log end offset - committed offset, per partition (feeds System Monitoring, Phase 9).

    python -m accesssentinel.streaming.lag hdfs-sink repartitioner
"""
import json
import sys
from typing import Dict, List

from ..topics import TOPICS, topic_name

GROUP_TOPICS = {
    "hdfs-sink": ["access-events", "risk-scores", "escalations.realtime", "escalations.background", "group-scores",
                  "pdp-decisions", "analyst-feedback"],
    "repartitioner": ["access-events"],
    "tcn": ["access-events"],
    "escalation": ["risk-scores", "access-events-by-resource"],
}


def group_lag(bootstrap: str, group: str, topics: List[str], timeout: float = 10.0,
              consumer_factory=None) -> Dict:
    if consumer_factory is None:
        from confluent_kafka import Consumer
        consumer_factory = Consumer
    from confluent_kafka import TopicPartition
    c = consumer_factory({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
    try:
        md = c.list_topics(timeout=timeout)
        tps = [TopicPartition(t, p) for t in topics if t in md.topics for p in md.topics[t].partitions]
        committed = c.committed(tps, timeout=timeout)
        out, total = {}, 0
        for tp in committed:
            lo, hi = c.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=timeout)
            done = tp.offset if tp.offset >= 0 else lo          # no commit yet: lag counts from the log start
            lag = max(hi - done, 0)
            out.setdefault(tp.topic, {})[tp.partition] = lag
            total += lag
        return {"group": group, "total": total, "partitions": out}
    finally:
        c.close()


def main(argv=None) -> int:
    from ..settings import get_settings
    s = get_settings()
    groups = (argv if argv is not None else sys.argv[1:]) or sorted(GROUP_TOPICS)
    for g in groups:
        topics = [topic_name(t, s.kafka_topic_prefix) for t in GROUP_TOPICS.get(g, [t.name for t in TOPICS])]
        res = group_lag(s.kafka_bootstrap, g, topics)
        print(json.dumps({"group": g, "total_lag": res["total"],
                          "by_topic": {t: sum(v.values()) for t, v in res["partitions"].items()}}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

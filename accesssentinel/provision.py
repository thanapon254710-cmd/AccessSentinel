"""Idempotent Kafka topic provisioning: `python -m accesssentinel.provision`.
Run once by the `kafka-init` compose service; safe to re-run."""
import argparse
import sys
import time

from .settings import get_settings
from .topics import all_topic_names


def wait_for_kafka(admin, timeout: float) -> None:
    deadline = time.time() + timeout
    while True:
        try:
            admin.list_topics(timeout=5)
            return
        except Exception as e:  # noqa: BLE001 - KafkaException subclasses vary by version
            if time.time() > deadline:
                raise RuntimeError(f"Kafka not reachable: {e}") from e
            time.sleep(2)


def ensure_topics(bootstrap: str, names, partitions: int, replication: int, admin_factory=None) -> dict:
    """Create missing topics. Returns {topic: 'created' | 'exists'}; raises on any other error."""
    if admin_factory is None:
        from confluent_kafka.admin import AdminClient
        admin_factory = AdminClient
    from confluent_kafka.admin import NewTopic
    admin = admin_factory({"bootstrap.servers": bootstrap})
    wait_for_kafka(admin, 120)
    existing = set(admin.list_topics(timeout=10).topics)
    result = {n: "exists" for n in names if n in existing}
    missing = [n for n in names if n not in existing]
    if missing:
        futs = admin.create_topics([NewTopic(n, partitions, replication) for n in missing])
        for n, f in futs.items():
            try:
                f.result()
                result[n] = "created"
            except Exception as e:  # noqa: BLE001
                if "TOPIC_ALREADY_EXISTS" in str(e):
                    result[n] = "exists"
                else:
                    raise
    return result


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--prefix", default=None, help="topic prefix (default: AS_KAFKA_TOPIC_PREFIX)")
    args = ap.parse_args(argv)
    s = get_settings()
    prefix = s.kafka_topic_prefix if args.prefix is None else args.prefix
    res = ensure_topics(s.kafka_bootstrap, all_topic_names(prefix), s.kafka_partitions, s.kafka_replication)
    for n in sorted(res):
        print(f"{res[n]:8s} {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

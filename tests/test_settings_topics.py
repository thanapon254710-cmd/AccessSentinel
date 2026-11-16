"""Phase 2 unit tests: environment settings, topic registry, idempotent provisioning, empty Kafka prefix."""
import pytest

from accesssentinel.adapters.kafka_broker import KafkaDualViewBroker
from accesssentinel.config import Config
from accesssentinel.provision import ensure_topics
from accesssentinel.settings import Settings
from accesssentinel.topics import TOPICS, all_topic_names, topic_name
from tests.fakes import FakeCluster, FakeConsumer, FakeProducer


def settings(**env):
    return Settings(_env_file=None, **env)       # never read the developer's real .env in unit tests


def test_defaults_are_host_view_and_have_no_secrets(monkeypatch):
    for k in ("AS_POSTGRES_PASSWORD", "AS_REDIS_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    s = settings()
    assert s.kafka_bootstrap == "localhost:9092" and s.kafka_topic_prefix == ""
    assert s.postgres_password is None and s.redis_password is None
    with pytest.raises(RuntimeError, match="AS_POSTGRES_PASSWORD"):
        s.postgres_dsn()
    with pytest.raises(RuntimeError, match="AS_REDIS_PASSWORD"):
        s.redis_kwargs()


def test_env_vars_override_and_secrets_are_masked(monkeypatch):
    monkeypatch.setenv("AS_KAFKA_BOOTSTRAP", "kafka:29092")
    monkeypatch.setenv("AS_POSTGRES_PASSWORD", "p@ss/word")
    monkeypatch.setenv("AS_REDIS_PASSWORD", "r-secret")
    s = settings()
    assert s.kafka_bootstrap == "kafka:29092"
    assert "p%40ss%2Fword" in s.postgres_dsn()                 # URL-encoded, not raw
    assert s.redis_kwargs()["password"] == "r-secret"
    red = s.redacted()
    assert red["postgres_password"] == "***" and red["redis_password"] == "***"
    assert "p@ss/word" not in repr(s) and "r-secret" not in repr(s)


@pytest.mark.parametrize("field,value", [("environment", "staging"), ("kafka_partitions", 0)])
def test_invalid_values_rejected(field, value):
    with pytest.raises(ValueError):
        settings(**{field: value})


def test_topic_registry():
    names = all_topic_names()
    assert len(names) == len(set(names)) == len(TOPICS) + sum(t.dlq for t in TOPICS)
    from accesssentinel.topics import topic_configs
    cfg = topic_configs()
    assert cfg["access-events"]["message.timestamp.type"] == "LogAppendTime"            # ti = broker time
    assert all(cfg[n]["cleanup.policy"] == "compact" for n in names if n.endswith(".changelog"))
    assert "access-events.ordered.dlq" in names and "sequencer.changelog.dlq" not in names
    assert "access-events" in names and "access-events.dlq" in names and "access-events-by-resource" in names
    assert all(n.startswith("x.") for n in all_topic_names("x"))
    assert topic_name("risk-scores") == "risk-scores" and topic_name("risk-scores", "p") == "p.risk-scores"


class _Future:
    def __init__(self, err=None): self.err = err
    def result(self):
        if self.err: raise Exception(self.err)


class FakeAdmin:
    def __init__(self, existing): self.existing, self.created = set(existing), []
    def __call__(self, conf): return self
    def list_topics(self, timeout=None):
        return type("MD", (), {"topics": {t: None for t in self.existing}})()
    def create_topics(self, new):
        out = {}
        for t in new:
            self.created.append((t.topic, t.num_partitions))
            out[t.topic] = _Future("TOPIC_ALREADY_EXISTS" if t.topic in self.existing else None)
            self.existing.add(t.topic)
        return out


def test_provisioning_is_idempotent():
    admin = FakeAdmin({"access-events"})
    res = ensure_topics("fake:9092", ["access-events", "risk-scores"], 8, 1, admin_factory=admin)
    assert res == {"access-events": "exists", "risk-scores": "created"}
    assert admin.created == [("risk-scores", 8)]
    res2 = ensure_topics("fake:9092", ["access-events", "risk-scores"], 8, 1, admin_factory=admin)
    assert set(res2.values()) == {"exists"} and admin.created == [("risk-scores", 8)]


def test_kafka_adapter_with_empty_prefix_uses_deployment_names():
    cl = FakeCluster()
    b = KafkaDualViewBroker("fake:9092", Config().stream, prefix="", create_topics=False,
                            producer_factory=FakeProducer(cl), consumer_factory=FakeConsumer(cl))
    assert b.user_topic.name == "access-events" and b.resource_topic.name == "access-events-by-resource"
    assert b.user_topic.group("tcn").c.group == "tcn"

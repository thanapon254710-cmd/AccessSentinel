"""Shared helpers for the live-stack experiments (C, D, G, H). Run inside the tools container:

    docker compose run --rm -T tools python -m evaluation.exp_c ...

Accounts are fresh synthetic users (syn-*) created through the admin API; their generated passwords are used in
memory only and never printed or written."""
import json
import os
import time
import uuid
from typing import Callable, Dict, List, Optional

SUSPENDED = "suspended by the continuous-monitoring risk policy"


def settings():
    from accesssentinel.settings import get_settings
    return get_settings()


def admin_client(s, api: str):
    import httpx
    c = httpx.Client(base_url=api, headers={"X-Requested-With": "accesssentinel"}, timeout=30)
    if c.post("/api/auth/login", json={"username": "secadmin", "password": s.secret("analyst_password")}).status_code != 200:
        raise SystemExit("cannot sign in as secadmin (AS_ANALYST_PASSWORD)")
    return c


def accounts(s, api: str, n: int, role: str, prefix: str) -> List[tuple]:
    adm = admin_client(s, api)
    tag, out = uuid.uuid4().hex[:5], []
    for i in range(n):
        u = f"syn-{prefix}-{tag}-{i}"
        r = adm.post("/api/accounts", json={"id": u, "name": f"Exp {u}", "department": "Test", "role": role})
        if r.status_code != 200:
            raise SystemExit(f"account {u}: HTTP {r.status_code}")
        out.append((u, r.json()["initial_password"]))
    return out


def _watermark_high(c, tp, tries: int = 10) -> int:
    """High watermark with retries: under load the single broker briefly answered NOT_LEADER_FOR_PARTITION to the
    test tool's own reader (Exp. C, 2026-11-15) - a transient metadata error, not a failure of the system tested."""
    from confluent_kafka import KafkaException
    for i in range(tries):
        try:
            return c.get_watermark_offsets(tp, timeout=10)[1]
        except KafkaException:
            if i == tries - 1:
                raise
            time.sleep(1.0 + i)


def end_offsets(s, topics: List[str]) -> Dict[tuple, int]:
    from confluent_kafka import Consumer, TopicPartition
    from accesssentinel.topics import topic_name
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"exp-{uuid.uuid4().hex[:6]}", "enable.auto.commit": False})
    out = {}
    for t in topics:
        name = topic_name(t, s.kafka_topic_prefix)
        for p in c.list_topics(name, timeout=10).topics[name].partitions:
            out[(name, p)] = _watermark_high(c, TopicPartition(name, p))
    c.close()
    return out


def read_from(s, start: Dict[tuple, int], keep: Callable[[str, dict], bool], idle: float = 3.0) -> List[tuple]:
    """All records from the given start offsets to the current end, as (topic, record) for which keep() holds."""
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"exp-{uuid.uuid4().hex[:6]}",
                  "enable.auto.commit": False, "isolation.level": "read_committed"})
    ends = {k: _watermark_high(c, TopicPartition(*k)) for k in start}
    todo = {k for k in start if ends[k] > start[k]}
    c.assign([TopicPartition(t, p, start[(t, p)]) for (t, p) in todo])
    out, last = [], time.time()
    while todo and time.time() - last < idle:
        for m in c.consume(2000, 1.0):
            if m.error():
                continue
            last = time.time()
            k = (m.topic(), m.partition())
            try:
                d = json.loads(m.value())
            except ValueError:
                d = None
            if isinstance(d, dict) and keep(m.topic(), d):
                out.append((m.topic(), d))
            if m.offset() + 1 >= ends[k]:
                todo.discard(k)
    c.close()
    return out


_PRODUCERS: dict = {}


def _producer(s):
    """One warmed-up producer per process: a new idempotent producer per call put its connection set-up and
    InitProducerId round trip into every measured propagation delay (found in Phase 7: a constant ~0.5 s)."""
    from confluent_kafka import Producer
    if s.kafka_bootstrap not in _PRODUCERS:
        pr = Producer({"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 0})
        pr.list_topics(timeout=10)
        time.sleep(1.0)                                                      # InitProducerId completes in the background
        _PRODUCERS[s.kafka_bootstrap] = pr
    return _PRODUCERS[s.kafka_bootstrap]


def produce(s, topic: str, rec: dict) -> float:
    from accesssentinel.topics import topic_name
    pr = _producer(s)
    t0 = time.time()
    pr.produce(topic_name(topic, s.kafka_topic_prefix), key=rec["u"].encode(), value=json.dumps(rec).encode())
    assert pr.flush(10) == 0
    return t0


def lag_total(s) -> Dict[str, int]:
    from accesssentinel.streaming.lag import GROUP_TOPICS, group_lag
    from accesssentinel.topics import topic_name
    out = {}
    for g, ts in GROUP_TOPICS.items():
        try:
            out[g] = group_lag(s.kafka_bootstrap, g, [topic_name(t, s.kafka_topic_prefix) for t in ts])["total"]
        except Exception:  # noqa: BLE001 - e.g. NOT_COORDINATOR during a rebalance: recorded as missing
            out[g] = None
    return out


def write(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1, default=str)


def pct(xs, qs=(50, 95, 99)) -> Optional[dict]:
    import numpy as np
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    return {**{f"p{q}": float(np.percentile(xs, q)) for q in qs}, "mean": float(np.mean(xs)), "n": len(xs),
            "max": float(np.max(xs))}

"""Production wiring for the dashboard API.
    uvicorn --factory accesssentinel.services.api.main:build --host 0.0.0.0 --port 8080
"""
import json
import logging
import os

import redis as redis_lib
import redis.asyncio as aioredis
from fastapi import FastAPI

from ...adapters.hdfs_lake import make_lake
from ...db.engine import make_engine
from ...settings import get_settings
from ...topics import ANALYST_FEEDBACK, topic_name
from .app import create_app
from .live import LiveFeed
from .system import SystemProbe


def build() -> FastAPI:
    s = get_settings()
    logging.basicConfig(level=s.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    engine = make_engine(s)
    sync = redis_lib.Redis(**s.redis_kwargs())
    aredis = aioredis.Redis(**s.redis_kwargs(), decode_responses=True)
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True, "acks": "all"})
    fb_topic = topic_name(ANALYST_FEEDBACK, s.kafka_topic_prefix)

    def publish_feedback(payload: dict) -> None:
        errors = []
        producer.produce(fb_topic, key=payload["id"].encode(), value=json.dumps(payload).encode(),
                         on_delivery=lambda err, msg: errors.append(err) if err else None)
        if producer.flush(10) or errors:
            raise RuntimeError("analyst feedback not acknowledged by Kafka")

    def pdp_factory():
        from ...streaming.pdp_service import build_pdp
        return build_pdp(s, sync)

    from ...streaming.pdp_service import GatewayRevoker
    revoker = GatewayRevoker(s.gateway_internal_url, s.secret("internal_token")) if s.internal_token else None
    lake_factory = lambda: make_lake(s)
    static = os.environ.get("AS_DASHBOARD_DIR", "/app/dashboard_dist")
    return create_app(s, engine, aredis, sync, LiveFeed(s.kafka_bootstrap, s.kafka_topic_prefix),
                      SystemProbe(s, sync, engine, lake_factory), lake_factory, publish_feedback, s.model_dir,
                      pdp_factory, static_dir=static, cleanup=[aredis.aclose],
                      revoke_sessions=revoker.revoke if revoker else None)

"""Production wiring for the gateway.
    uvicorn --factory accesssentinel.services.gateway.main:build --host 0.0.0.0 --port 8000
"""
import logging

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI

from ...adapters.event_publisher import EventPublisher
from ...db.engine import make_engine
from ...settings import get_settings
from ...topics import ACCESS_EVENTS, topic_name
from .app import create_app
from .store import SqlGatewayStore


def build() -> FastAPI:
    s = get_settings()
    logging.basicConfig(level=s.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    redis = aioredis.Redis(**s.redis_kwargs(), decode_responses=True, socket_timeout=2.0,
                           socket_connect_timeout=2.0)
    publisher = EventPublisher(s.kafka_bootstrap, topic_name(ACCESS_EVENTS, s.kafka_topic_prefix), s.spool_dir)
    upstream = httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=3.0), follow_redirects=False)

    async def close_publisher():
        publisher.close()

    return create_app(s, SqlGatewayStore(make_engine(s)), redis, publisher, upstream,
                      cleanup=[close_publisher, upstream.aclose, redis.aclose])

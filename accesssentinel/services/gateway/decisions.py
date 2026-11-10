"""Read side of the PDP decision cache (D3 Step 8, request-time enforcement).

The PDP (Phase 8) writes one Redis hash per user; the gateway only reads it, so requests never wait for
Kafka, TCN or TGN. Format (all values strings):

    decision:<u>   decision    "Allow" | "Block"
                   reason      e.g. "individual", "collective", "policy", "analyst"
                   decided_at  epoch seconds
                   expires_at  epoch seconds, or absent = until released by the PDP / an analyst
                   version     monotonically increasing integer

A missing hash means "no risk decision yet": the request is decided by the first-line ACL alone.
A Block stays in force until the PDP rewrites or deletes it, or its explicit expires_at passes; absence
of a fresh ML score never releases it. If Redis is unreachable the gateway fails closed (HTTP 503),
because sessions live in Redis too.
"""
import time
from typing import Optional


async def active_block(redis, user: str, now: Optional[float] = None) -> Optional[dict]:
    d = await redis.hgetall(f"decision:{user}")
    if not d or d.get("decision") != "Block":
        return None
    exp = d.get("expires_at")
    if exp and float(exp) <= (time.time() if now is None else now):
        return None
    return d

"""Server-side sessions in Redis, shared by every gateway replica.

The browser holds an opaque random token. Redis stores only sha256(token) as the key, so a Redis dump
does not reveal usable session tokens. Revoking a session deletes it; the next request is rejected,
which is how session-time enforcement terminates an active session (D3 Step 8).

Keys:  sess:<ref>      hash {u, role, created, last_seen, ip, device, requests}   TTL = idle timeout
       usess:<user>    set of <ref> for that user (for user-wide revocation)
       loginfail:<u>   failed-login counter                                         TTL = lock window
"""
import time
from typing import Optional

from ...security import new_session_token, one_way_ref


class SessionStore:
    def __init__(self, redis, idle_seconds: int, max_seconds: int):
        self.r, self.idle, self.max = redis, idle_seconds, max_seconds

    @staticmethod
    def ref(token: str) -> str:
        return one_way_ref(token, 32)

    async def create(self, user: str, role: str, ip: str, device: str) -> str:
        token = new_session_token()
        ref, now = self.ref(token), time.time()
        key = f"sess:{ref}"
        pipe = self.r.pipeline()
        pipe.hset(key, mapping={"u": user, "role": role, "created": now, "last_seen": now, "ip": ip,
                                "device": device, "requests": 0})
        pipe.expire(key, self.idle)
        pipe.sadd(f"usess:{user}", ref)
        pipe.expire(f"usess:{user}", self.max)
        await pipe.execute()
        return token

    async def get(self, token: Optional[str]) -> Optional[dict]:
        if not token or len(token) > 200:
            return None
        ref = self.ref(token)
        data = await self.r.hgetall(f"sess:{ref}")
        if not data:
            return None
        if time.time() - float(data["created"]) > self.max:          # absolute lifetime
            await self.revoke(ref, data.get("u"))
            return None
        data["ref"] = ref
        return data

    async def touch(self, ref: str) -> int:
        """Count a request on the session and extend the idle timeout. Returns the request number (1 = first)."""
        key = f"sess:{ref}"
        pipe = self.r.pipeline()
        pipe.hincrby(key, "requests", 1)
        pipe.hset(key, "last_seen", time.time())
        pipe.expire(key, self.idle)
        n, _, _ = await pipe.execute()
        return int(n)

    async def revoke(self, ref: str, user: Optional[str] = None) -> None:
        await self.r.delete(f"sess:{ref}")
        if user:
            await self.r.srem(f"usess:{user}", ref)

    async def revoke_user(self, user: str) -> int:
        refs = await self.r.smembers(f"usess:{user}")
        n = 0
        for ref in refs:
            n += await self.r.delete(f"sess:{ref}")
        await self.r.delete(f"usess:{user}")
        return n

    # -------------------------------------------------- login throttling
    async def login_locked(self, user: str, max_failures: int) -> bool:
        v = await self.r.get(f"loginfail:{user}")
        return v is not None and int(v) >= max_failures

    async def login_failed(self, user: str, lock_seconds: int) -> int:
        key = f"loginfail:{user}"
        pipe = self.r.pipeline()
        pipe.incr(key)
        pipe.expire(key, lock_seconds)
        n, _ = await pipe.execute()
        return int(n)

    async def login_succeeded(self, user: str) -> None:
        await self.r.delete(f"loginfail:{user}")

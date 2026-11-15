"""Read side of the PDP decision cache (Revised v3 Step 8, request-time enforcement).

The PDP writes one Redis hash per user; the gateway only reads it, so requests never wait for Kafka, TCN or TGN.
Format (all values strings):

    decision:<u>   decision    Allow | Throttle | StepUp | Restrict | Terminate | Block   (Eq. 20)
                   reason      e.g. "individual", "collective", "policy", "analyst"
                   decided_at  epoch seconds
                   expires_at  epoch seconds, or absent = until released by the PDP / an analyst
                   psi         global, strictly increasing decision version; v_model / v_policy complete the triple

A missing hash means "no risk decision yet": the request is decided by the first-line ACL alone. An action stays
in force until the PDP rewrites it or its explicit expires_at passes; absence of a fresh ML score never releases it.

DecisionView adds the two v3 gateway rules:
  * No stale overwrite: a decision is applied only if psi > psi_cur(u); an older psi read later (a lagging writer,
    a restored snapshot) is rejected and the applied decision stays in force.
  * Decision TTL: if the PDP heartbeat (pdp:alive) is older than the TTL, the PDP is treated as unavailable and the
    sensitivity-based fallback applies: s = 0 Allow (fail-open), s = 1 Restrict (fail-closed); per application
    "open" (allow every resource) or "closed" (restrict every resource) overrides it. An applied Terminate / Block
    stays in force regardless.
If Redis is unreachable the gateway fails closed (HTTP 503), because sessions live in Redis too.
"""
import time
from collections import OrderedDict
from typing import Dict, Optional

DENY_ALL = ("Terminate", "Block")


def _psi(d: dict) -> int:
    try:
        return int(float(d.get("psi") or 0))
    except ValueError:
        return 0


def _active(d: Optional[dict], now: float) -> Optional[dict]:
    if not d or d.get("decision") in (None, "", "Allow"):
        return None
    exp = d.get("expires_at")
    if exp and float(exp) <= now:
        return None
    return d


def parse_fallback(spec: str) -> Dict[str, str]:
    """"finance=closed,docs=open" -> {"finance": "closed", "docs": "open"}."""
    out = {}
    for part in (spec or "").split(","):
        if "=" in part:
            app, mode = (x.strip() for x in part.split("=", 1))
            if mode not in ("open", "closed"):
                raise ValueError(f"fallback mode for {app} must be open or closed")
            out[app] = mode
    return out


class DecisionView:
    def __init__(self, ttl: float = 0.0, fallback: Optional[Dict[str, str]] = None, clock=time.time,
                 max_users: int = 100_000):
        self.ttl, self.fallback, self.clock, self.max_users = ttl, fallback or {}, clock, max_users
        self.applied: "OrderedDict[str, tuple]" = OrderedDict()      # u -> (psi_cur, decision)
        self.stats = {"stale_rejected": 0, "fallback_restricted": 0}
        self._alive = (-1e18, None)

    async def current(self, redis, user: str) -> Optional[dict]:
        """The active non-Allow decision for user (psi-ordered), or None."""
        d = await redis.hgetall(f"decision:{user}")
        if not d:
            self.applied.pop(user, None)            # no decision any more (cleared by the PDP / an operator)
            return None
        psi, cur = _psi(d), self.applied.get(user)
        if cur is not None and psi < cur[0]:
            self.stats["stale_rejected"] += 1       # psi <= psi_cur: rejected, the applied decision stays
            d = cur[1]
        elif cur is None or psi > cur[0]:
            self.applied[user] = (psi, d)
            self.applied.move_to_end(user)
            if len(self.applied) > self.max_users:
                self.applied.popitem(last=False)
        return _active(d, self.clock())

    async def pdp_available(self, redis) -> bool:
        if self.ttl <= 0:
            return True
        now = self.clock()
        if now - self._alive[0] >= 1.0:              # one heartbeat read per second per gateway process
            self._alive = (now, await redis.get("pdp:alive"))
        v = self._alive[1]
        return v is not None and now - float(v) <= self.ttl

    def fallback_denies(self, app: str, s: int) -> bool:
        mode = self.fallback.get(app)
        deny = mode == "closed" or (mode is None and int(s) == 1)
        if deny:
            self.stats["fallback_restricted"] += 1
        return deny


async def active_block(redis, user: str, now: Optional[float] = None) -> Optional[dict]:
    """An active Terminate / Block (every request and new logins refused), read directly (no psi state)."""
    d = _active(await redis.hgetall(f"decision:{user}"), time.time() if now is None else now)
    return d if d and d.get("decision") in DENY_ALL else None

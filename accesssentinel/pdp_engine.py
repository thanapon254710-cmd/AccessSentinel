"""Policy Decision Point (D3 Step 8, Eq. 7): D_u = h(S_ind, S_coll, C) with an explicit Block lifecycle.

Block when S_ind >= theta_high ("individual"), when S_coll(g(u)) >= theta_coll ("collective", applied to every
member of g(u)), or when organisational policy C denies ("policy"); otherwise Allow subject to the gateway's
first-line ACL. Lifecycle ([IMPL], audit defect D3):
  * Sticky: a lower score - or no score at all (ML unavailable) - never lifts a Block.
  * A risk Block ends at its expiry (block_ttl) or by an analyst release; a release opens a grace period during
    which ML evidence cannot re-block the user (the same ongoing evidence would otherwise re-block at once).
  * A policy Block ends only when the policy is lifted (no expiry by ML, no analyst release).
  * Evidence older than max_score_age / max_group_score_age is not enforced (backlogs after a restart).
  * shadow mode: decisions are computed and audited but neither cached for the gateway nor enforced.

State (Redis):  decision:<u>   hash read by the gateway (decision, reason, decided_at, expires_at, version, ...)
                override:<u>   analyst grace period end      pdp:blocked  zset u -> expiry (inf = none)
                pdp:outbox     list of decision records written in the SAME transaction as the state change;
                               the service drains it to Kafka, the PostgreSQL audit table and session revocation.
"""
import json
import time
import uuid
from typing import Callable, Dict, Iterable, List, Optional

from redis.exceptions import WatchError

from .config import PDPConfig, RiskConfig

K_DEC = "decision:{}"
K_SHADOW = "shadow:decision:{}"
K_OVR = "override:{}"
K_BLOCKED = "pdp:blocked"
K_OUTBOX = "pdp:outbox"
K_STATS = "pdp:stats"
INF = float("inf")


def _s(x):
    return x.decode() if isinstance(x, bytes) else x


class PolicyLocked(Exception):
    """The user is blocked by organisational policy; only lifting the policy releases the block."""


class PolicyDecisionPoint:
    def __init__(self, redis, cfg: PDPConfig = None, risk_cfg: RiskConfig = None, mode: str = "enforce",
                 theta_coll: Optional[float] = None, clock: Callable[[], float] = time.time):
        self.r, self.cfg, self.rc, self.mode, self.clock = redis, cfg or PDPConfig(), risk_cfg or RiskConfig(), mode, clock
        self.theta_override = theta_coll          # None: use the TGN model's calibrated theta_coll from each record
        if mode not in ("enforce", "shadow"):
            raise ValueError("mode must be enforce or shadow")

    # ------------------------------------------------------------------ inputs from the detectors
    def on_risk_score(self, rec: dict) -> List[dict]:
        now = self.clock()
        when = float(rec.get("scored_at") or rec.get("t") or 0.0)
        if when < now - self.cfg.max_score_age:
            self._stat("stale_ignored")
            return []
        s = float(rec["s_ind"])
        if s < self.rc.theta_high:
            return []                                   # Allow path: no change (a Block stays)
        if self._overridden(rec["u"], now):
            self._stat("override_suppressed")
            return []
        basis = {"source": "tcn", "score_id": rec.get("score_id"), "s_ind": s, "trigger": rec.get("trigger"),
                 "model_version": rec.get("model_version"), "theta_high": self.rc.theta_high}
        return self._block(rec["u"], "individual", basis, ttl=self.cfg.block_ttl, s_ind=s)

    def on_group_score(self, rec: dict) -> List[dict]:
        now = self.clock()
        theta = self.theta_override if self.theta_override is not None else float(rec.get("theta_coll") or self.cfg.theta_coll)
        if rec.get("s_coll") is None or float(rec["s_coll"]) < theta:
            return []
        if float(rec.get("t") or 0.0) < now - self.cfg.max_group_score_age:
            self._stat("stale_ignored")
            return []
        basis = {"source": "tgn", "score_id": rec.get("score_id"), "s_coll": float(rec["s_coll"]),
                 "group_id": rec.get("group_id"), "members": rec.get("members"), "seed": rec["u"],
                 "theta_coll": theta, "model_version": rec.get("model_version")}
        out = []
        for m in rec.get("members") or [rec["u"]]:          # D3: the decision applies to every member of g(u)
            if self._overridden(m, now):
                self._stat("override_suppressed")
                continue
            out += self._block(m, "collective", basis, ttl=self.cfg.block_ttl, s_coll=float(rec["s_coll"]),
                               group_id=rec.get("group_id"))
        return out

    def sync_policy(self, denied: Dict[str, dict]) -> List[dict]:
        """denied: user -> {"reason", "by", "expires_at" (epoch or None)} from organisational policy C."""
        now, out = self.clock(), []
        for u, p in sorted(denied.items()):
            exp = p.get("expires_at")
            if exp is not None and exp <= now:
                continue
            out += self._block(u, "policy", {"source": "policy", "policy_reason": p.get("reason"), "by": p.get("by")},
                               ttl=None if exp is None else exp - now, actor=p.get("by") or "policy")
        for u in self.blocked_users():
            cur = self._current(u)
            if cur.get("reason") == "policy" and (u not in denied or (denied[u].get("expires_at") or INF) <= now):
                out += self._allow(u, "policy_lifted", "policy", actor="policy")
        return out

    # ------------------------------------------------------------------ analyst / operator actions
    def release(self, u: str, actor: str, note: str = "", grace: Optional[float] = None) -> List[dict]:
        cur = self._current(u)
        if cur.get("decision") == "Block" and cur.get("reason") == "policy":
            raise PolicyLocked(f"{u} is blocked by organisational policy; lift the policy instead")
        g = self.cfg.release_grace if grace is None else grace
        until = self.clock() + g
        self.r.set(K_OVR.format(u), until, ex=int(g) + 3600)
        return self._allow(u, "release", "analyst_release", actor=actor, note=note, extra={"grace_until": until})

    def block_manual(self, u: str, actor: str, note: str = "", ttl: Optional[float] = None) -> List[dict]:
        self.r.delete(K_OVR.format(u))
        return self._block(u, "analyst", {"source": "analyst", "note": note}, ttl=ttl, actor=actor, force=True)

    def expire_due(self) -> List[dict]:
        now, out = self.clock(), []
        for u in [_s(x) for x in self.r.zrangebyscore(self._blocked_key(), "-inf", now)]:
            cur = self._current(u)
            exp = cur.get("expires_at")
            if cur.get("decision") == "Block" and exp and float(exp) <= now:
                out += self._allow(u, "expire", "expired", actor="pdp")
            else:
                self.r.zrem(self._blocked_key(), u)
        return out

    # ------------------------------------------------------------------ queries
    def status(self, u: str) -> dict:
        cur = self._current(u)
        ovr = self.r.get(K_OVR.format(u))
        active = self._is_active_block(cur, self.clock())
        return {"user": u, "mode": self.mode, "blocked": active, **cur,
                "override_until": float(_s(ovr)) if ovr else None}

    def blocked_users(self) -> List[str]:
        return sorted(_s(x) for x in self.r.zrange(self._blocked_key(), 0, -1))

    # ------------------------------------------------------------------ internals
    def _key(self, u):
        return (K_DEC if self.mode == "enforce" else K_SHADOW).format(u)

    def _blocked_key(self):
        return K_BLOCKED if self.mode == "enforce" else "shadow:" + K_BLOCKED

    def _current(self, u) -> dict:
        return {_s(k): _s(v) for k, v in self.r.hgetall(self._key(u)).items()}

    @staticmethod
    def _is_active_block(cur: dict, now: float) -> bool:
        if cur.get("decision") != "Block":
            return False
        exp = cur.get("expires_at")
        return not exp or float(exp) > now

    def _overridden(self, u, now) -> bool:
        v = self.r.get(K_OVR.format(u))
        return v is not None and float(_s(v)) > now

    def _stat(self, k, n=1):
        self.r.hincrby(K_STATS, k, n)

    def _record(self, u, decision, transition, reason, actor, now, version, basis=None, expires_at=None,
                s_ind=None, s_coll=None, group_id=None, note="", extra=None) -> dict:
        return {"decision_id": uuid.uuid4().hex, "schema_version": 1, "u": u, "t": now, "decision": decision,
                "transition": transition, "reason": reason, "actor": actor, "note": note[:500], "mode": self.mode,
                "enforced": self.mode == "enforce", "version": version, "expires_at": expires_at, "s_ind": s_ind,
                "s_coll": s_coll, "group_id": group_id, "basis": basis or {}, **(extra or {})}

    def _transition(self, u, build) -> List[dict]:
        """Optimistic transaction on decision:<u>: build(cur, now) -> (record, mapping) or None."""
        key = self._key(u)
        with self.r.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(key)
                    cur = {_s(k): _s(v) for k, v in pipe.hgetall(key).items()}
                    now = self.clock()
                    res = build(cur, now)
                    if res is None:
                        pipe.unwatch()
                        return []
                    rec, mapping = res
                    pipe.multi()
                    pipe.hset(key, mapping={k: ("" if v is None else v) for k, v in mapping.items()})
                    if rec["decision"] == "Block":
                        pipe.zadd(self._blocked_key(), {u: rec["expires_at"] or INF})
                    else:
                        pipe.zrem(self._blocked_key(), u)
                    pipe.rpush(K_OUTBOX, json.dumps(rec))
                    pipe.hincrby(K_STATS, f"{rec['transition']}:{rec['reason']}", 1)
                    pipe.execute()
                    return [rec]
                except WatchError:
                    continue

    def _block(self, u, reason, basis, ttl, actor="pdp", s_ind=None, s_coll=None, group_id=None, force=False):
        def build(cur, now):
            if self._is_active_block(cur, now) and not force:
                if reason == "policy" and cur.get("reason") != "policy":
                    pass                                   # policy dominates: upgrade the block below
                else:
                    also = json.loads(cur.get("also") or "[]")
                    if reason == cur.get("reason") or reason in also:
                        self.r.hincrby(K_STATS, "already_blocked", 1)
                        return None
                    # New KIND of evidence for a user who is already blocked: keep the block (and its expiry)
                    # unchanged but record the evidence, so the audit shows every reason (e.g. collective).
                    version = int(cur.get("version") or 0) + 1
                    exp = float(cur["expires_at"]) if cur.get("expires_at") else None
                    rec = self._record(u, "Block", "evidence", reason, actor, now, version, basis, exp, s_ind, s_coll,
                                       group_id, extra={"block_reason": cur.get("reason")})
                    return rec, {"version": version, "also": json.dumps(also + [reason]),
                                 "group_id": group_id or cur.get("group_id")}
            version = int(cur.get("version") or 0) + 1
            exp = None if ttl is None else now + ttl
            rec = self._record(u, "Block", "block", reason, actor, now, version, basis, exp, s_ind, s_coll, group_id,
                               note=basis.get("note", "") if isinstance(basis, dict) else "")
            mapping = {"decision": "Block", "reason": reason, "decided_at": now, "expires_at": exp, "version": version,
                       "decision_id": rec["decision_id"], "actor": actor, "group_id": group_id,
                       "basis": json.dumps(basis), "also": "[]"}
            return rec, mapping
        return self._transition(u, build)

    def _allow(self, u, transition, reason, actor, note="", extra=None):
        def build(cur, now):
            if cur.get("decision") != "Block":
                return None
            version = int(cur.get("version") or 0) + 1
            rec = self._record(u, "Allow", transition, reason, actor, now, version, note=note,
                               extra={"previous": {"reason": cur.get("reason"), "decided_at": cur.get("decided_at"),
                                                   "decision_id": cur.get("decision_id")}, **(extra or {})})
            mapping = {"decision": "Allow", "reason": reason, "decided_at": now, "expires_at": None, "version": version,
                       "decision_id": rec["decision_id"], "actor": actor, "group_id": None, "basis": "{}", "also": "[]"}
            return rec, mapping
        return self._transition(u, build)

    # ------------------------------------------------------------------ outbox
    def outbox_peek(self, n: int = 200) -> List[dict]:
        return [json.loads(x) for x in self.r.lrange(K_OUTBOX, 0, n - 1)]

    def outbox_ack(self, n: int) -> None:
        self.r.ltrim(K_OUTBOX, n, -1)

"""Policy Decision Point (Revised v3 Step 8, Eqs. 20-21): D_u = h(S_ind, S_coll_u, A_u, C) with a sticky lifecycle.

Action set (Eq. 20), and the illustrative mapping of Eq. 21, evaluated top-down:
    Block       C(u,r,t) = deny                    organisational policy (also: an analyst's manual block)
    Terminate   S_ind >= theta_B                   end every session; requests and new logins refused
    Restrict    S_coll_u >= theta_C and A_u >= theta_A    sensitive (s = 1) resources refused
    StepUp      S_coll_u >= theta_C and A_u <  theta_A    re-authentication required (peripheral group member)
    Throttle    theta_high <= S_ind < theta_B      request rate limited
    Allow       otherwise                           first-line ACL only
theta_high / theta_B come with each v3 TCN score and theta_C / theta_A with each v3 group score (quantiles of
nominal validation, Eq. 8); S_coll_u and A_u are per member (Eqs. 18-19). D3 models carry no theta_B / A_u: a D3
score >= theta_high terminates (the D3 Block) and a D3 group score >= theta_coll restricts every member.

Because the PDP keeps the most severe active action, "evaluated top-down" is the action order itself: a more
severe action replaces a milder one at once; a milder one never downgrades an active decision ([IMPL], audit defect
D3 - sticky). Lifecycle:
  * A lower score - or no score at all (ML unavailable) - never lifts an action; it ends at its expiry (per-action
    TTL) or by an analyst release; a release opens a grace period during which ML evidence cannot re-apply it.
  * A policy Block ends only when the policy is lifted (no expiry by ML, no analyst release).
  * Evidence older than max_score_age / max_group_score_age is not enforced (backlogs after a restart).
  * shadow mode: decisions are computed and audited but neither cached for the gateway nor enforced.
Every decision carries the version triple (v_model, v_policy, psi): psi is a global, strictly increasing counter
(seeded from the clock in ms, so a flushed store cannot re-issue an old psi); the gateway applies a decision only
if psi > psi_cur (no stale overwrite).

State (Redis):  decision:<u>   hash read by the gateway (decision = action, reason, decided_at, expires_at, psi, ...)
                override:<u>   analyst grace period end      pdp:blocked  zset u -> expiry of any non-Allow action
                pdp:psi        the psi counter               pdp:alive    PDP heartbeat (gateway TTL fallback)
                pdp:outbox     list of decision records written in the SAME transaction as the state change;
                               the service drains it to Kafka, the PostgreSQL audit table and session revocation.
"""
import hashlib
import json
import time
import uuid
from typing import Callable, Dict, List, Optional

try:
    from redis.exceptions import WatchError
except ImportError:
    # Pure threshold/policy helpers and in-process Escalator evaluation do not
    # require the optional Redis client. The real service installs redis.
    class WatchError(Exception):
        pass

from .config import PDPConfig, RiskConfig

K_DEC = "decision:{}"
K_SHADOW = "shadow:decision:{}"
K_OVR = "override:{}"
K_BLOCKED = "pdp:blocked"
K_OUTBOX = "pdp:outbox"
K_STATS = "pdp:stats"
K_PSI = "pdp:psi"
K_ALIVE = "pdp:alive"
INF = float("inf")

ACTIONS = ("Allow", "Throttle", "StepUp", "Restrict", "Terminate", "Block")    # Eq. 20, ascending severity
RANK = {a: i for i, a in enumerate(ACTIONS)}                                 # Eq. 21 top-down = descending rank
DENY_ALL = ("Terminate", "Block")                                            # every request and new logins refused


def _s(x):
    return x.decode() if isinstance(x, bytes) else x


class PolicyLocked(Exception):
    """The user is blocked by organisational policy; only lifting the policy releases the block."""


def theta_high_for(rec: dict, rc: RiskConfig, override: Optional[float]) -> float:
    """theta_high for one score (Revised v3 Eq. 8): an explicit operator setting wins; otherwise the threshold the
    TCN model was calibrated with (carried in every v3 score); otherwise the configured default (D3 models)."""
    if override is not None:
        return float(override)
    v = rec.get("theta_high")
    return float(v) if v is not None else rc.theta_high


def individual_action(s_ind: float, theta_high: float, theta_b: Optional[float]) -> Optional[str]:
    """Eq. 21 rows Terminate / Throttle. theta_b None (D3 model): theta_high terminates (the D3 Block)."""
    if theta_b is None:
        return "Terminate" if s_ind >= theta_high else None
    theta_b = max(theta_b, theta_high)                  # an operator theta_high above theta_B leaves no Throttle band
    if s_ind >= theta_b:
        return "Terminate"
    return "Throttle" if s_ind >= theta_high else None


def collective_action(s_coll: Optional[float], a_u: Optional[float], theta_c: float,
                      theta_a: Optional[float]) -> Optional[str]:
    """Eq. 21 rows Restrict / StepUp for one member. No attribution (D3 group score): Restrict."""
    if s_coll is None or s_coll < theta_c:
        return None
    if a_u is None or theta_a is None:
        return "Restrict"
    return "Restrict" if a_u >= theta_a else "StepUp"


def decide(policy_deny: bool, s_ind: Optional[float], theta_high: float, theta_b: Optional[float],
           s_coll: Optional[float] = None, a_u: Optional[float] = None, theta_c: float = 1.0,
           theta_a: Optional[float] = None) -> str:
    """Eq. 21 as one pure function (used by the tests and Experiment G to check the live PDP)."""
    if policy_deny:
        return "Block"
    ind = individual_action(s_ind, theta_high, theta_b) if s_ind is not None else None
    coll = collective_action(s_coll, a_u, theta_c, theta_a)
    return max([ind, coll, "Allow"], key=lambda a: RANK[a] if a else -1)


class PolicyDecisionPoint:
    def __init__(self, redis, cfg: PDPConfig = None, risk_cfg: RiskConfig = None, mode: str = "enforce",
                 theta_coll: Optional[float] = None, clock: Callable[[], float] = time.time):
        self.r, self.cfg, self.rc, self.mode, self.clock = redis, cfg or PDPConfig(), risk_cfg or RiskConfig(), mode, clock
        self.theta_override = theta_coll          # None: use the TGN model's calibrated theta_coll from each record
        self.theta_high_override = None           # None: use the TCN model's calibrated theta_high from each score (v3)
        self.v_policy = ""                        # hash of the last policy-C snapshot (decision version triple)
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
        th = theta_high_for(rec, self.rc, self.theta_high_override)
        tb = float(rec["theta_B"]) if rec.get("theta_B") is not None else None
        act = individual_action(s, th, tb)
        if act is None:
            return []                                   # Allow path: no change (an active action stays)
        if self._overridden(rec["u"], now):
            self._stat("override_suppressed")
            return []
        basis = {"source": "tcn", "score_id": rec.get("score_id"), "s_ind": s, "trigger": rec.get("trigger"),
                 "model_version": rec.get("model_version"), "theta_high": th, "theta_B": tb, "action": act,
                 "event_t": rec.get("t")}                  # event time of the scored access (incident window)
        ttl = self.cfg.block_ttl if act == "Terminate" else self.cfg.throttle_ttl
        return self._apply(rec["u"], act, "individual", basis, ttl=ttl, s_ind=s, v_model=rec.get("model_version"))

    def on_group_score(self, rec: dict) -> List[dict]:
        now = self.clock()
        theta = self.theta_override if self.theta_override is not None else float(rec.get("theta_coll") or self.cfg.theta_coll)
        if rec.get("s_coll") is None:
            return []
        if float(rec.get("t") or 0.0) < now - self.cfg.max_group_score_age:
            self._stat("stale_ignored")
            return []
        v3 = rec.get("model_kind") == "tgn-v3"
        theta_a = rec.get("theta_A") if v3 else None
        ms = rec.get("member_scores") if isinstance(rec.get("member_scores"), dict) else {}
        out = []
        for m in rec.get("members") or [rec["u"]]:
            x = ms.get(m) if v3 else None                 # v3: per-member S_coll_u and A_u (Eqs. 18-19)
            s_m = float(x["s_coll"]) if isinstance(x, dict) and x.get("s_coll") is not None else float(rec["s_coll"])
            a_m = float(x["A_u"]) if isinstance(x, dict) and x.get("A_u") not in (None, "") else None
            act = collective_action(s_m, a_m, theta, theta_a)
            if act is None:
                continue
            if self._overridden(m, now):
                self._stat("override_suppressed")
                continue
            basis = {"source": "tgn", "score_id": rec.get("score_id"), "s_coll": s_m, "A_u": a_m,
                     "group_s_coll": float(rec["s_coll"]), "group_id": rec.get("group_id"), "members": rec.get("members"),
                     "seed": rec["u"], "theta_coll": theta, "theta_A": theta_a, "action": act,
                     "model_version": rec.get("model_version"),
                     "event_window": rec.get("window")}   # TGN neighbourhood window, event time
            ttl = self.cfg.restrict_ttl if act == "Restrict" else self.cfg.stepup_ttl
            out += self._apply(m, act, "collective", basis, ttl=ttl, s_coll=s_m, group_id=rec.get("group_id"),
                               v_model=rec.get("model_version"))
        return out

    def sync_policy(self, denied: Dict[str, dict]) -> List[dict]:
        """denied: user -> {"reason", "by", "expires_at" (epoch or None)} from organisational policy C."""
        now, out = self.clock(), []
        self.v_policy = hashlib.sha256(json.dumps(denied, sort_keys=True, default=str).encode()).hexdigest()[:12]
        for u, p in sorted(denied.items()):
            exp = p.get("expires_at")
            if exp is not None and exp <= now:
                continue
            out += self._apply(u, "Block", "policy", {"source": "policy", "policy_reason": p.get("reason"), "by": p.get("by")},
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
        return self._apply(u, "Block", "analyst", {"source": "analyst", "note": note}, ttl=ttl, actor=actor, force=True)

    def expire_due(self) -> List[dict]:
        now, out = self.clock(), []
        for u in [_s(x) for x in self.r.zrangebyscore(self._blocked_key(), "-inf", now)]:
            cur = self._current(u)
            exp = cur.get("expires_at")
            if cur.get("decision") in RANK and cur.get("decision") != "Allow" and exp and float(exp) <= now:
                out += self._allow(u, "expire", "expired", actor="pdp")
            else:
                self.r.zrem(self._blocked_key(), u)
        return out

    def heartbeat(self) -> None:
        """Liveness for the gateway's decision TTL (sensitivity-based fallback when the PDP is unavailable)."""
        self.r.set(K_ALIVE, self.clock())

    # ------------------------------------------------------------------ queries
    def status(self, u: str) -> dict:
        cur = self._current(u)
        ovr = self.r.get(K_OVR.format(u))
        now = self.clock()
        act = cur.get("decision") if self._is_active(cur, now) else "Allow"
        return {"user": u, "mode": self.mode, "blocked": act in DENY_ALL, "action": act, **cur,
                "override_until": float(_s(ovr)) if ovr else None}

    def blocked_users(self) -> List[str]:
        """Users with an active non-Allow action (any of Throttle ... Block)."""
        return sorted(_s(x) for x in self.r.zrange(self._blocked_key(), 0, -1))

    # ------------------------------------------------------------------ internals
    def _key(self, u):
        return (K_DEC if self.mode == "enforce" else K_SHADOW).format(u)

    def _blocked_key(self):
        return K_BLOCKED if self.mode == "enforce" else "shadow:" + K_BLOCKED

    def _current(self, u) -> dict:
        return {_s(k): _s(v) for k, v in self.r.hgetall(self._key(u)).items()}

    @staticmethod
    def _is_active(cur: dict, now: float) -> bool:
        if cur.get("decision") in (None, "", "Allow"):
            return False
        exp = cur.get("expires_at")
        return not exp or float(exp) > now

    def _overridden(self, u, now) -> bool:
        v = self.r.get(K_OVR.format(u))
        return v is not None and float(_s(v)) > now

    def _stat(self, k, n=1):
        self.r.hincrby(K_STATS, k, n)

    def _next_psi(self) -> int:
        if not self.r.exists(K_PSI):
            self.r.set(K_PSI, int(self.clock() * 1000), nx=True)
        return int(self.r.incr(K_PSI))

    def _record(self, u, decision, transition, reason, actor, now, version, psi, basis=None, expires_at=None,
                s_ind=None, s_coll=None, group_id=None, note="", extra=None, v_model=None) -> dict:
        return {"decision_id": uuid.uuid4().hex, "schema_version": 2, "u": u, "t": now, "decision": decision,
                "transition": transition, "reason": reason, "actor": actor, "note": note[:500], "mode": self.mode,
                "enforced": self.mode == "enforce", "version": version, "psi": psi, "v_model": v_model,
                "v_policy": self.v_policy, "expires_at": expires_at, "s_ind": s_ind, "s_coll": s_coll,
                "group_id": group_id, "basis": basis or {}, **(extra or {})}

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
                    if rec["decision"] != "Allow":
                        pipe.zadd(self._blocked_key(), {u: rec["expires_at"] or INF})
                    else:
                        pipe.zrem(self._blocked_key(), u)
                    pipe.rpush(K_OUTBOX, json.dumps(rec))
                    pipe.hincrby(K_STATS, f"{rec['transition']}:{rec['reason']}", 1)
                    pipe.execute()
                    return [rec]
                except WatchError:
                    continue

    def _apply(self, u, action, reason, basis, ttl, actor="pdp", s_ind=None, s_coll=None, group_id=None, force=False,
               v_model=None):
        def build(cur, now):
            active = self._is_active(cur, now)
            cur_act = cur.get("decision") if active else "Allow"
            also = json.loads(cur.get("also") or "[]") if active else []
            if active and not force:
                policy_upgrade = reason == "policy" and cur.get("reason") != "policy"
                if not policy_upgrade and RANK[action] <= RANK.get(cur_act, 0):
                    if reason == cur.get("reason") or reason in also:
                        self.r.hincrby(K_STATS, "already_applied", 1)
                        return None
                    # A new KIND of evidence that does not warrant a more severe action: keep the decision (and its
                    # expiry) unchanged but record the evidence, so the audit shows every reason (e.g. collective).
                    version = int(cur.get("version") or 0) + 1
                    psi = self._next_psi()
                    exp = float(cur["expires_at"]) if cur.get("expires_at") else None
                    rec = self._record(u, cur_act, "evidence", reason, actor, now, version, psi, basis, exp, s_ind,
                                       s_coll, group_id, extra={"block_reason": cur.get("reason"), "evidence_action": action},
                                       v_model=v_model)
                    return rec, {"version": version, "psi": psi, "also": json.dumps(also + [reason]),
                                 "group_id": group_id or cur.get("group_id")}
                if cur.get("reason") and cur.get("reason") != reason and cur.get("reason") not in also:
                    also = also + [cur["reason"]]          # escalation keeps the earlier evidence kinds
            version = int(cur.get("version") or 0) + 1
            psi = self._next_psi()
            exp = None if ttl is None else now + ttl
            rec = self._record(u, action, "block", reason, actor, now, version, psi, basis, exp, s_ind, s_coll, group_id,
                               note=basis.get("note", "") if isinstance(basis, dict) else "",
                               extra={"previous_action": cur_act} if cur_act != "Allow" else None, v_model=v_model)
            mapping = {"decision": action, "reason": reason, "decided_at": now, "expires_at": exp, "version": version,
                       "psi": psi, "v_model": v_model, "v_policy": self.v_policy, "decision_id": rec["decision_id"],
                       "actor": actor, "group_id": group_id, "basis": json.dumps(basis), "also": json.dumps(also)}
            return rec, mapping
        return self._transition(u, build)

    def _allow(self, u, transition, reason, actor, note="", extra=None):
        def build(cur, now):
            if cur.get("decision") in (None, "", "Allow"):
                return None
            version = int(cur.get("version") or 0) + 1
            psi = self._next_psi()
            rec = self._record(u, "Allow", transition, reason, actor, now, version, psi, note=note,
                               extra={"previous": {"action": cur.get("decision"), "reason": cur.get("reason"),
                                                   "decided_at": cur.get("decided_at"),
                                                   "decision_id": cur.get("decision_id")}, **(extra or {})})
            mapping = {"decision": "Allow", "reason": reason, "decided_at": now, "expires_at": None, "version": version,
                       "psi": psi, "v_policy": self.v_policy, "decision_id": rec["decision_id"], "actor": actor,
                       "group_id": None, "basis": "{}", "also": "[]"}
            return rec, mapping
        return self._transition(u, build)

    # ------------------------------------------------------------------ outbox
    def outbox_peek(self, n: int = 200) -> List[dict]:
        return [json.loads(x) for x in self.r.lrange(K_OUTBOX, 0, n - 1)]

    def outbox_ack(self, n: int) -> None:
        self.r.ltrim(K_OUTBOX, n, -1)

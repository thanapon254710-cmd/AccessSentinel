"""Four-path escalation policy (D3 Step 6, Eq. 4): U_t = U_high ∪ U_res ∪ U_sweep ∪ U_samp.

  Path A  high      S_ind >= theta_high (any TCN score, event or recheck)              -> real time
  Path B  resource  more than kappa distinct users access one sensitive (s=1) resource
                    within delta_res; every participant is escalated                   -> real time
  Path C  sweep     every T_sweep, rules over HDFS history (`sweep_candidates`)        -> background
  Path D  sample    each active Low-risk user independently with probability gamma    -> background

Union: candidates are merged per user (all paths + evidence kept) and a user is escalated at most once per
cooldown, so each escalated user is analysed once per window (D3 Step 10). Real-time candidates (A, B) are
unioned for `realtime_window` seconds, ordered by urgency and capped at `max_realtime_per_window` (overflow
is DEFERRED to the next window, never dropped). Background candidates (C, D) are emitted once per analysis
window W and capped at `max_background_per_window` (overflow is SHED and counted) - audit D13.

All state is in Redis (works across replicas and restarts; candidates are staged before Kafka offsets are
committed). Path B windows use event time; cooldowns, activity and windows use the injected clock (wall
clock live, simulated time in the offline evaluation), so the same code runs in both.
"""
import json
import uuid
from collections import defaultdict
from typing import Callable, Dict, Iterable, List, Optional

from .config import EscalationConfig, RiskConfig
from .events import AccessEvent

PATHS = ("high", "resource", "sweep", "sample")
REALTIME_PATHS = {"high", "resource"}
SCHEMA_VERSION = 1

K_PENDING = "esc:pending"                # hash u -> json {paths: {path: evidence}, priority, since}
K_SWEEP = "esc:sweep_pending"            # hash u -> json evidence from the last sweep
K_ACTIVE = "esc:active"                  # zset u -> last activity time
K_TIER = "esc:tier"                      # hash u -> latest tier
K_STATS = "esc:stats"                    # hash counters
K_WINDOWS = "esc:windows"                # list of per-window JSON summaries (most recent first)
K_RES = "esc:res:{}"                     # zset per sensitive resource: u -> last access (event time)
K_COOL_RT = "esc:cool:rt:{}"
K_COOL_BG = "esc:cool:bg:{}"
K_SWEEP_LAST = "esc:sweep:last"
K_SWEEP_LOCK = "esc:sweep:lock"
K_BG_NEXT = "esc:bg:next"


def _s(x):
    return x.decode() if isinstance(x, bytes) else x


class Escalator:
    def __init__(self, redis, cfg: EscalationConfig = None, risk_cfg: RiskConfig = None, rng=None,
                 clock: Callable[[], float] = None, on_trigger: Callable[[float, str, str], None] = None):
        import random
        import time
        self.on_trigger = on_trigger or (lambda t, u, path: None)    # (time, user, path) for every path trigger
        self.r = redis
        self.cfg = cfg or EscalationConfig()
        self.rc = risk_cfg or RiskConfig()
        self.rng = rng or random.Random()
        self.clock = clock or time.time

    def config_snapshot(self) -> dict:
        c = self.cfg
        return {"theta_high": self.rc.theta_high, "kappa": c.kappa, "delta_res": c.delta_res, "t_sweep": c.t_sweep,
                "gamma": c.gamma, "analysis_window": c.analysis_window, "realtime_window": c.realtime_window}

    # ------------------------------------------------------------------ inputs
    def on_score(self, rec: dict) -> None:
        """A TCN risk score (risk-scores topic): activity + tier for Path D, Path A trigger."""
        u, now = rec["u"], self.clock()
        pipe = self.r.pipeline(transaction=False)
        pipe.zadd(K_ACTIVE, {u: now}, gt=True)
        pipe.hset(K_TIER, u, rec.get("tier", "low"))
        pipe.execute()
        if float(rec["s_ind"]) >= self.rc.theta_high:
            self._pend(u, "high", {"s_ind": round(float(rec["s_ind"]), 4), "score_id": rec.get("score_id"),
                                   "trigger": rec.get("trigger"), "t": rec.get("t"),
                                   "model_version": rec.get("model_version")}, urgency=1.0 + float(rec["s_ind"]))

    def on_event(self, ev: AccessEvent) -> int:
        """An access event from the resource-keyed view: Path B sliding window per sensitive resource.
        Returns the number of distinct users in the window (0 for non-sensitive resources)."""
        if int(ev.s) != 1:
            return 0
        key = K_RES.format(ev.rkey)
        pipe = self.r.pipeline(transaction=False)
        pipe.zadd(key, {ev.u: ev.t}, gt=True)
        pipe.zremrangebyscore(key, "-inf", ev.t - self.cfg.delta_res)
        pipe.zrangebyscore(key, ev.t - self.cfg.delta_res, "+inf")
        pipe.expire(key, int(2 * self.cfg.delta_res) + 60)
        _, _, members, _ = pipe.execute()
        users = sorted(_s(m) for m in members)
        n = len(users)
        if n > self.cfg.kappa:
            ev_info = {"resource": ev.rkey, "distinct_users": n, "kappa": self.cfg.kappa,
                       "window_s": self.cfg.delta_res, "participants": users[:50], "t": ev.t}
            for m in users:
                self._pend(m, "resource", ev_info, urgency=1.0 + n / 100.0)
        return n

    def _pend(self, u: str, path: str, evidence: dict, urgency: float) -> None:
        raw = self.r.hget(K_PENDING, u)
        cur = json.loads(raw) if raw else {"paths": {}, "urgency": 0.0, "since": self.clock()}
        cur["paths"][path] = evidence
        cur["urgency"] = max(cur["urgency"], urgency)
        self.r.hset(K_PENDING, u, json.dumps(cur))
        self.r.hincrby(K_STATS, f"candidates:{path}", 1)
        self.on_trigger(self.clock(), u, path)

    # ------------------------------------------------------------------ cooldowns (on the injected clock)
    def _cooling(self, key: str, now: float) -> bool:
        v = self.r.get(key)
        return v is not None and float(v) > now

    def _claim(self, key: str, seconds: float, now: float) -> bool:
        """Start a cooldown unless one is running. The key stores its end time on the injected clock (Redis
        TTL only garbage-collects), so simulated time and wall-clock deployments behave identically."""
        until, ttl = now + seconds, int(seconds) + 3600
        if self.r.set(key, until, nx=True, ex=ttl):
            return True
        if self._cooling(key, now):
            return False
        old = self.r.getset(key, until)
        self.r.expire(key, ttl)
        return old is None or float(old) <= now

    # ------------------------------------------------------------------ real-time emission (A, B)
    def flush_realtime(self, force: bool = False) -> List[dict]:
        now = self.clock()
        pend = {_s(k): json.loads(v) for k, v in self.r.hgetall(K_PENDING).items()}
        if not pend:
            return []
        if not force and min(p["since"] for p in pend.values()) > now - self.cfg.realtime_window:
            return []
        order = sorted(pend, key=lambda u: (-pend[u]["urgency"], pend[u]["since"], u))
        cooling = [u for u in order if self._cooling(K_COOL_RT.format(u), now)]     # analysed recently: drop
        eligible = [u for u in order if u not in set(cooling)]
        out, done = [], list(cooling)
        for u in eligible[: self.cfg.max_realtime_per_window]:                    # the rest is deferred
            done.append(u)
            if not self._claim(K_COOL_RT.format(u), self.cfg.realtime_cooldown, now):
                cooling.append(u)                                                 # lost a race to a replica
                continue
            self.r.set(K_COOL_BG.format(u), now + self.cfg.background_cooldown, ex=int(self.cfg.background_cooldown) + 3600)
            out.append(self._record(u, "realtime", pend[u]["paths"], now))
        suppressed = len(cooling)
        deferred = max(0, len(eligible) - self.cfg.max_realtime_per_window)
        if done:
            self.r.hdel(K_PENDING, *done)
        self._account("realtime", out, now, suppressed=suppressed, deferred=deferred)
        return out

    # ------------------------------------------------------------------ background emission (C, D)
    def background_due(self) -> bool:
        nxt = self.r.get(K_BG_NEXT)
        return nxt is None or float(nxt) <= self.clock()

    def flush_background(self, force: bool = False) -> List[dict]:
        now = self.clock()
        if not force and not self.background_due():
            return []
        self.r.set(K_BG_NEXT, now + self.cfg.analysis_window)
        cands: Dict[str, Dict[str, dict]] = defaultdict(dict)
        sweep = {_s(k): json.loads(v) for k, v in self.r.hgetall(K_SWEEP).items()}
        for u, evid in sweep.items():
            cands[u]["sweep"] = evid
            self.on_trigger(now, u, "sweep")
        if sweep:
            self.r.delete(K_SWEEP)
        active = [_s(u) for u in self.r.zrangebyscore(K_ACTIVE, now - self.cfg.analysis_window, "+inf")]
        tiers = dict(zip(active, self.r.hmget(K_TIER, active))) if active else {}
        low = sorted(u for u in active if _s(tiers.get(u)) == "low")
        for u in low:
            if self.rng.random() < self.cfg.gamma:
                cands[u]["sample"] = {"gamma": self.cfg.gamma, "low_risk_active": len(low)}
                self.on_trigger(now, u, "sample")
        self.r.zremrangebyscore(K_ACTIVE, "-inf", now - 7 * 86400)
        order = sorted(cands, key=lambda u: (-int("sweep" in cands[u]), -float(cands[u].get("sweep", {}).get("strength", 0)), u))
        eligible = [u for u in order if not self._cooling(K_COOL_RT.format(u), now)
                    and not self._cooling(K_COOL_BG.format(u), now)]
        suppressed = len(order) - len(eligible)
        out = []
        for u in eligible[: self.cfg.max_background_per_window]:
            if self._claim(K_COOL_BG.format(u), self.cfg.background_cooldown, now):
                out.append(self._record(u, "background", cands[u], now))
            else:
                suppressed += 1
        shed = max(0, len(eligible) - self.cfg.max_background_per_window)          # genuine budget overflow only
        self._account("background", out, now, suppressed=suppressed, shed=shed, low_risk_active=len(low))
        return out

    # ------------------------------------------------------------------ Path C
    def sweep_due(self) -> bool:
        last = self.r.get(K_SWEEP_LAST)
        return last is None or float(last) + self.cfg.t_sweep <= self.clock()

    def run_sweep(self, load: Callable[[float], dict], lock_seconds: int = 1800, force: bool = False) -> Optional[dict]:
        """`load(now)` returns {"lookback_events", "baseline_events", "lookback_scores"} (see sweep_candidates).
        One replica runs it at a time (Redis lock). Candidates are staged for the next background window."""
        now = self.clock()
        if (not force and not self.sweep_due()) or not self.r.set(K_SWEEP_LOCK, 1, nx=True, ex=lock_seconds):
            return None
        try:
            data = load(now)
            cands = sweep_candidates(data["lookback_events"], data["baseline_events"], data["lookback_scores"],
                                     self.cfg, now)
            ranked = sorted(cands, key=lambda u: (-cands[u]["strength"], u))[: self.cfg.sweep_batch]
            if ranked:
                self.r.hset(K_SWEEP, mapping={u: json.dumps(cands[u]) for u in ranked})
            self.r.set(K_SWEEP_LAST, now)
            info = {"t": now, "candidates": len(cands), "staged": len(ranked),
                    "lookback_events": len(data["lookback_events"]), "baseline_events": len(data["baseline_events"])}
            self.r.hincrby(K_STATS, "sweeps", 1)
            return info
        finally:
            self.r.delete(K_SWEEP_LOCK)

    def drain_sweep(self) -> List[dict]:
        """Emit the staged Path C candidates directly (operator- or experiment-triggered sweeps, possibly at a
        historical reference time): live cooldowns and Path D sampling do not apply, because they describe the
        live clock, not the swept period."""
        now = self.clock()
        sweep = {_s(k): json.loads(v) for k, v in self.r.hgetall(K_SWEEP).items()}
        if sweep:
            self.r.delete(K_SWEEP)
        order = sorted(sweep, key=lambda u: (-float(sweep[u].get("strength", 0)), u))
        out = [self._record(u, "background", {"sweep": sweep[u]}, now) for u in order]
        self._account("background", out, now)
        return out

    def restage(self, records: List[dict]) -> None:
        """Undo the emission of records whose publication failed: candidates return to their queues and the
        cooldowns they set are cleared, so the next attempt emits them again (no escalation is lost)."""
        pipe = self.r.pipeline(transaction=False)
        for rec in records:
            u = rec["u"]
            if rec["priority"] == "realtime":
                pipe.delete(K_COOL_RT.format(u), K_COOL_BG.format(u))
                pipe.hset(K_PENDING, u, json.dumps({"paths": rec["evidence"], "urgency": 9.0, "since": 0.0}))
            else:
                pipe.delete(K_COOL_BG.format(u))
                if "sweep" in rec["evidence"]:
                    pipe.hset(K_SWEEP, u, json.dumps(rec["evidence"]["sweep"]))
        pipe.hincrby(K_STATS, "restaged", len(records))
        pipe.execute()

    # ------------------------------------------------------------------ helpers
    def _record(self, u: str, priority: str, paths: Dict[str, dict], now: float) -> dict:
        return {"escalation_id": uuid.uuid4().hex, "schema_version": SCHEMA_VERSION, "u": u, "t": now,
                "priority": priority, "paths": sorted(paths), "evidence": paths, "config": self.config_snapshot()}

    def _account(self, kind: str, out: List[dict], now: float, **extra) -> None:
        pipe = self.r.pipeline(transaction=False)
        for rec in out:
            pipe.hincrby(K_STATS, f"escalated:{kind}", 1)
            for p in rec["paths"]:
                pipe.hincrby(K_STATS, f"escalated_by:{p}", 1)
        for k, v in extra.items():
            if v and k != "low_risk_active":
                pipe.hincrby(K_STATS, f"{k}:{kind}", int(v))
        if out or any(extra.values()):
            summary = {"t": now, "kind": kind, "escalated": len(out),
                       **{p: sum(1 for rec in out if p in rec["paths"]) for p in PATHS}, **extra}
            pipe.lpush(K_WINDOWS, json.dumps(summary))
            pipe.ltrim(K_WINDOWS, 0, 999)
        pipe.execute()


# ---------------------------------------------------------------------------------------------- Path C rules
def baseline_range(lookback_start: float, cfg: EscalationConfig):
    """[start, end) of the history that defines a user's usual behaviour for Path C: sweep_baseline_days long,
    ending sweep_baseline_gap_days before the lookback. Without the gap, a low-and-slow attack becomes part of
    its own baseline after one day and C1 stops seeing it (found in the Phase 6 live test)."""
    end = lookback_start - cfg.sweep_baseline_gap_days * 86400.0
    return end - cfg.sweep_baseline_days * 86400.0, end


def sweep_candidates(lookback_events: Iterable[AccessEvent], baseline_events: Iterable[AccessEvent],
                     lookback_scores: Iterable[dict], cfg: EscalationConfig, now: float) -> Dict[str, dict]:
    """Batch rules over history ([IMPL]; D3 only says the sweep surfaces low-and-slow and threshold evasion):
      C1 rare_sensitive     >= sweep_min_rare accesses in the lookback to sensitive resources the user used
                            <= sweep_rare_max times in the baseline (users with enough baseline history only)
      C2 threshold_evasion  >= sweep_min_medium Medium-tier scores in the lookback and no High
      C3 uncommon_coaccess  within the last T_sweep, a sensitive resource accessed by >= sweep_min_users users
                            for whom it is rare: all those users
    Returns {user: {"rules": {...}, "strength": float}}."""
    base_counts, base_n = defaultdict(int), defaultdict(int)
    for e in baseline_events:
        base_counts[(e.u, e.rkey)] += 1
        base_n[e.u] += 1
    look = list(lookback_events)
    out: Dict[str, dict] = defaultdict(lambda: {"rules": {}, "strength": 0.0})
    rare_hits = defaultdict(list)
    for e in look:
        if int(e.s) == 1 and base_n[e.u] >= cfg.sweep_min_baseline_events and base_counts[(e.u, e.rkey)] <= cfg.sweep_rare_max:
            rare_hits[e.u].append(e.rkey)
    for u, hits in rare_hits.items():
        if len(hits) >= cfg.sweep_min_rare:
            out[u]["rules"]["rare_sensitive"] = {"count": len(hits), "resources": sorted(set(hits))[:10]}
            out[u]["strength"] += len(hits)
    med, high = defaultdict(int), defaultdict(int)
    for s in lookback_scores:
        if s.get("tier") == "medium":
            med[s["u"]] += 1
        elif s.get("tier") == "high":
            high[s["u"]] += 1
    for u, n in med.items():
        if n >= cfg.sweep_min_medium and high[u] == 0:
            out[u]["rules"]["threshold_evasion"] = {"medium_scores": n}
            out[u]["strength"] += n
    co = defaultdict(set)
    for e in look:
        if e.t > now - cfg.t_sweep and int(e.s) == 1 and base_counts[(e.u, e.rkey)] <= cfg.sweep_rare_max:
            co[e.rkey].add(e.u)
    for rkey, us in co.items():
        if len(us) >= cfg.sweep_min_users:
            for u in us:
                out[u]["rules"].setdefault("uncommon_coaccess", {"resources": []})["resources"].append(
                    {"resource": rkey, "users": len(us)})
                out[u]["strength"] += len(us)
    return dict(out)

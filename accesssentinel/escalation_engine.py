"""Four-path escalation policy (D3 Step 6, Eq. 4): U_t = U_high ∪ U_res ∪ U_sweep ∪ U_samp.

  Path A  high      S_ind >= theta_high (any TCN score, event or recheck)              -> real time
  Path B  resource  more than kappa distinct users access one sensitive (s=1) resource
                    within delta_res; every participant is escalated                   -> real time
  Path C  sweep     every T_sweep, rules over HDFS history (`sweep_candidates`)        -> background
  Path D  sample    each active Low-risk user independently with probability gamma    -> background

Union: candidates are merged per user (all paths + evidence kept) and a user is escalated at most once per
cooldown, so each escalated user is analysed once per window (D3 Step 10).

Revised v3 compute admission (Phase 5, Algorithm 1, Eqs. 26-30): every candidate gets a cost
C_u = c0 + c1 |E(N_h(u))| + c2 |V(N_h(u))| (estimated from the live edge index) and the TGN work per analysis
window W never exceeds B_W = floor(rho' N mu_graph W). `priority_admit` admits in the order resource >
high > sweep > sample while used + C_u <= B_W. Overflow: sample -> dropped, sweep -> deferred to the next
window, resource -> queued until `res_deadline`, high -> not analysed by the TGN (the PDP acts on S_ind alone;
it already consumes every risk score). Real-time candidates (A, B) are admitted as they flush; background
candidates (C, D) at the end of the window from what is left. Path B comes from the finalised event-time
resource windows (`resource-windows`, Phase 2) when `use_resource_windows` is set (Algorithm 1, line 2).

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
K_BUDGET = "esc:budget:{}"                # used work units in analysis window <id>

PRIORITY = ("resource", "high", "sweep", "sample")         # Algorithm 1, line 6


def top_path(paths) -> str:
    return next(p for p in PRIORITY if p in paths)


def priority_admit(cands: Dict[str, Iterable[str]], costs: Dict[str, float], budget: float, used: float = 0.0):
    """Algorithm 1 (lines 6-13). cands: user -> its escalation paths (the union U_W, each user once).
    Returns (admitted users in order, used, overflow by action). Invariant: used <= budget (Table IV)."""
    order = sorted(cands, key=lambda u: PRIORITY.index(top_path(cands[u])))     # stable within a class
    admitted, over = [], {"sample_dropped": [], "sweep_deferred": [], "resource_queued": [], "high_pdp_alone": []}
    for u in order:
        c = float(costs[u])
        if used + c <= budget:
            admitted.append(u); used += c
        else:
            over[{"sample": "sample_dropped", "sweep": "sweep_deferred", "resource": "resource_queued",
                  "high": "high_pdp_alone"}[top_path(cands[u])]].append(u)
    return admitted, used, over


def graph_budget(cfg) -> float:
    """Eq. 29: B_W = floor(rho' N mu_graph W)."""
    return float(int(cfg.rho_graph * cfg.tgn_workers * cfg.mu_graph * cfg.analysis_window))


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
        self.cost_fn: Callable[[str, float], tuple] = lambda u, now: (0, 1)      # (|E|, |V|) of N_h(u); set by the service
        self.theta_high_override = None           # None: the TCN model's calibrated theta_high carried in the score
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
        from .pdp_engine import theta_high_for
        if float(rec["s_ind"]) >= theta_high_for(rec, self.rc, self.theta_high_override):
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

    def on_resource_window(self, rec: dict) -> int:
        """Path B from a FINALISED resource window (resource-windows, kind final): more than kappa distinct users
        on a sensitive resource in event-time window b -> every participant (exactly once per (r*, b))."""
        if rec.get("kind") != "final" or int(rec.get("s", 1)) != 1:
            return 0
        users = rec.get("users") or []
        if len(users) <= self.cfg.kappa:
            return 0
        info = {"resource": rec.get("rstar"), "window": rec.get("window"), "distinct_users": len(users),
                "kappa": self.cfg.kappa, "window_start": rec.get("window_start"), "window_end": rec.get("window_end"),
                "participants": users[:50], "t": rec.get("window_end")}
        for m in users:
            self._pend(m, "resource", info, urgency=1.0 + len(users) / 100.0)
        return len(users)

    # ------------------------------------------------------------------ compute admission (Algorithm 1)
    def cost(self, u: str, now: float) -> float:
        e, v = self.cost_fn(u, now)
        return self.cfg.cost_c0 + self.cfg.cost_c1 * e + self.cfg.cost_c2 * v           # Eq. 26

    def _window_id(self, now: float) -> int:
        return int(now // self.cfg.analysis_window)

    def _budget_left(self, now: float):
        wid = self._window_id(now)
        used = float(self.r.get(K_BUDGET.format(wid)) or 0.0)
        return wid, used, graph_budget(self.cfg)

    def _spend(self, wid: int, used: float) -> None:
        self.r.set(K_BUDGET.format(wid), used, ex=int(4 * self.cfg.analysis_window) + 60)

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
        wid, used, budget = self._budget_left(now)
        costs = {u: self.cost(u, now) for u in eligible}
        admitted, used2, over = priority_admit({u: pend[u]["paths"] for u in eligible}, costs, budget, used)
        for u in admitted:
            done.append(u)
            if not self._claim(K_COOL_RT.format(u), self.cfg.realtime_cooldown, now):
                cooling.append(u)                                                 # lost a race to a replica
                used2 -= costs[u]
                continue
            self.r.set(K_COOL_BG.format(u), now + self.cfg.background_cooldown, ex=int(self.cfg.background_cooldown) + 3600)
            rec = self._record(u, "realtime", pend[u]["paths"], now)
            rec["cost"] = round(costs[u], 3)
            out.append(rec)
        self._spend(wid, used2)
        done += over["high_pdp_alone"]                     # high-risk overflow: the PDP acts on S_ind alone
        expired = []
        for u in over["resource_queued"]:                  # resource overflow: wait for budget until the deadline
            first = float(pend[u].get("queued_at", now))
            if now - first > self.cfg.res_deadline:
                expired.append(u); done.append(u)
            else:
                pend[u]["queued_at"] = first
                self.r.hset(K_PENDING, u, json.dumps(pend[u]))
        suppressed = len(cooling)
        if done:
            self.r.hdel(K_PENDING, *done)
        self._account("realtime", out, now, suppressed=suppressed, deferred=len(over["resource_queued"]) - len(expired),
                      high_pdp_alone=len(over["high_pdp_alone"]), resource_expired=len(expired),
                      demand=round(sum(costs.values()), 3), budget=budget, used=round(used2, 3))
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
        wid, used, budget = self._budget_left(now)
        costs = {u: self.cost(u, now) for u in eligible}
        admitted, used2, over = priority_admit({u: list(cands[u]) for u in eligible}, costs, budget, used)
        for u in admitted:
            if self._claim(K_COOL_BG.format(u), self.cfg.background_cooldown, now):
                rec = self._record(u, "background", cands[u], now)
                rec["cost"] = round(costs[u], 3)
                out.append(rec)
            else:
                suppressed += 1; used2 -= costs[u]
        self._spend(wid, used2)
        if over["sweep_deferred"]:                         # sweep overflow: deferred to the next window
            self.r.hset(K_SWEEP, mapping={u: json.dumps(cands[u]["sweep"]) for u in over["sweep_deferred"]})
        self._account("background", out, now, suppressed=suppressed, sample_dropped=len(over["sample_dropped"]),
                      sweep_deferred=len(over["sweep_deferred"]), low_risk_active=len(low),
                      demand=round(sum(costs.values()), 3), budget=budget, used=round(used2, 3))
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

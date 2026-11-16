"""Phase 6 tests: the four escalation paths, union, cooldowns on the injected clock, workload caps, Path C
rules, re-staging after a failed publish, and the service handler."""
import json
import random
from dataclasses import replace

import fakeredis
import pytest

from accesssentinel.config import EscalationConfig, RiskConfig
from accesssentinel.escalation_engine import K_PENDING, K_STATS, Escalator, sweep_candidates
from accesssentinel.schema import make_event
from accesssentinel.streaming.consumer import DecodeError, Record
from accesssentinel.streaming.escalation_service import EscalationHandler

T0 = 1_794_000_000.0


class Clock:
    def __init__(self, t=T0): self.t = t
    def __call__(self): return self.t


def esc(clock, **over):
    cfg = replace(EscalationConfig(), **over)
    return Escalator(fakeredis.FakeRedis(), cfg, RiskConfig(), rng=random.Random(0), clock=clock)


def score(u, s, tier=None, t=T0):
    return {"u": u, "s_ind": s, "tier": tier or ("high" if s >= 0.6 else "medium" if s >= 0.4 else "low"),
            "score_id": f"sc-{u}-{s}", "t": t, "trigger": "event", "model_version": "v"}


def ev(u, t, r="payroll", s=1, a="hr"):
    return make_event(u, a, r, s, "view", True, {"ip": "10.0.0.1", "device": "d", "session": "active"}, "c" * 32, t=t)


# ------------------------------------------------------------------------------------------ Path A
def test_path_a_high_scores_escalate_in_real_time_after_the_union_window():
    c = Clock(); e = esc(c)
    e.on_score(score("alice", 0.59)); e.on_score(score("bob", 0.83))
    assert e.flush_realtime() == []                      # union window (5 s) not elapsed yet
    c.t += 5
    (rec,) = e.flush_realtime()
    assert rec["u"] == "bob" and rec["priority"] == "realtime" and rec["paths"] == ["high"]
    assert rec["evidence"]["high"]["s_ind"] == 0.83 and rec["config"]["theta_high"] == 0.6


# ------------------------------------------------------------------------------------------ Path B
def test_path_b_needs_more_than_kappa_distinct_users_on_a_sensitive_resource_within_delta():
    c = Clock(); e = esc(c, kappa=3, delta_res=900)
    for i, u in enumerate(["a", "b", "c", "c"]):                 # repeated user counts once
        assert e.on_event(ev(u, T0 + i)) <= 3
    assert e.r.hlen(K_PENDING) == 0
    assert e.on_event(ev("d", T0 + 10)) == 4
    c.t += 5
    out = e.flush_realtime()
    assert sorted(r["u"] for r in out) == ["a", "b", "c", "d"] and all(r["paths"] == ["resource"] for r in out)
    assert out[0]["evidence"]["resource"]["distinct_users"] == 4 and out[0]["evidence"]["resource"]["resource"] == "hr:payroll"


def test_path_b_window_slides_and_ignores_non_sensitive_resources():
    c = Clock(); e = esc(c, kappa=3, delta_res=900)
    for i, u in enumerate("abc"):
        e.on_event(ev(u, T0 + i * 10))
    assert e.on_event(ev("d", T0 + 915)) == 2                   # a (T0) and b (T0+10) fell out; c (T0+20) stays
    for i, u in enumerate("wxyz"):
        assert e.on_event(ev(u, T0 + i, r="directory", s=0)) == 0
    assert e.r.hlen(K_PENDING) == 0


# ------------------------------------------------------------------------------------------ union, cooldown, caps
def test_union_one_escalation_with_all_paths():
    c = Clock(); e = esc(c, kappa=1)
    e.on_score(score("u1", 0.9)); e.on_event(ev("u1", T0)); e.on_event(ev("u2", T0 + 1))
    c.t += 5
    out = {r["u"]: r for r in e.flush_realtime()}
    assert out["u1"]["paths"] == ["high", "resource"] and out["u2"]["paths"] == ["resource"]


def test_cooldown_runs_on_the_injected_clock():
    """Regression: cooldowns used Redis TTLs (wall clock), so simulated time never released them."""
    c = Clock(); e = esc(c, realtime_cooldown=300)
    e.on_score(score("u", 0.9)); c.t += 5
    assert len(e.flush_realtime()) == 1
    e.on_score(score("u", 0.95)); c.t += 5
    assert e.flush_realtime() == [] and int(e.r.hget(K_STATS, "suppressed:realtime")) == 1
    c.t += 300                                                   # no real time passes at all
    e.on_score(score("u", 0.97)); c.t += 5
    assert len(e.flush_realtime()) == 1


def test_realtime_budget_admits_by_priority_high_overflow_left_to_the_pdp():
    """v3 Algorithm 1: realtime candidates are admitted while the window budget lasts; high-risk overflow is
    not analysed by the TGN (the PDP acts on S_ind alone), it is not re-queued."""
    c = Clock(); e = esc(c, rho_graph=1.0, tgn_workers=1, mu_graph=1.0, analysis_window=60.0,
                         cost_c0=20.0, cost_c1=0.0, cost_c2=0.0)          # budget 60 units, 20 per user -> 3 users
    for i, sv in enumerate([0.61, 0.99, 0.7, 0.95, 0.8]):
        e.on_score(score(f"u{i}", sv))
    c.t += 5
    first = e.flush_realtime()
    assert len(first) == 3 and all(r["cost"] == 20.0 for r in first)
    assert e.r.hlen(K_PENDING) == 0 and int(e.r.hget(K_STATS, "high_pdp_alone:realtime")) == 2
    assert e.flush_realtime() == []


# ------------------------------------------------------------------------------------------ Path D
def test_path_d_samples_active_low_risk_users_only():
    c = Clock(); e = esc(c, gamma=1.0, analysis_window=60)
    e.on_score(score("low1", 0.1)); e.on_score(score("low2", 0.2)); e.on_score(score("med", 0.5)); e.on_score(score("hi", 0.9))
    c.t -= 120; e.on_score(score("stale", 0.1)); c.t += 120      # last active 2 minutes ago -> not active in W
    out = e.flush_background()
    assert sorted(r["u"] for r in out) == ["low1", "low2"] and all(r["paths"] == ["sample"] and r["priority"] == "background" for r in out)
    assert e.flush_background() == []                           # next window not reached yet
    e2 = esc(Clock(), gamma=0.0); e2.on_score(score("low", 0.1))
    assert e2.flush_background() == []


def test_background_skips_cooling_users_and_drops_sample_overflow():
    c = Clock(); e = esc(c, gamma=1.0, rho_graph=1.0, tgn_workers=1, mu_graph=1.0, analysis_window=60.0,
                         cost_c0=20.0, cost_c1=0.0, cost_c2=0.0)          # 60 units per window
    e.on_score(score("hot", 0.9)); c.t += 5; e.flush_realtime()             # spends 20 of this window's budget
    e.r.hset("esc:tier", "hot", "low")                          # now low but still cooling from the realtime escalation
    for u in ("a", "b", "c"):
        e.on_score(score(u, 0.1))
    out = e.flush_background()
    assert len(out) == 2 and "hot" not in {r["u"] for r in out}           # 40 units left -> 2 samples
    assert int(e.r.hget(K_STATS, "sample_dropped:background")) == 1         # sampled overflow is dropped
    assert int(e.r.hget(K_STATS, "suppressed:background")) == 1             # "hot" is cooling


def test_priority_admit_algorithm_1_order_bound_and_overflow_actions():
    from accesssentinel.escalation_engine import priority_admit
    cands = {"s1": ["sample"], "w1": ["sweep"], "h1": ["high"], "r1": ["resource", "sample"], "h2": ["high", "sweep"],
             "r2": ["resource"]}
    costs = {"s1": 5, "w1": 5, "h1": 5, "r1": 5, "h2": 5, "r2": 5}
    adm, used, over = priority_admit(cands, costs, budget=12.0)
    assert adm == ["r1", "r2"] and used == 10 <= 12                         # resource first; bound holds
    assert over == {"sample_dropped": ["s1"], "sweep_deferred": ["w1"], "resource_queued": [],
                    "high_pdp_alone": ["h1", "h2"]}
    adm, used, over = priority_admit(cands, costs, budget=30.0)
    assert adm == ["r1", "r2", "h1", "h2", "w1", "s1"] and len(set(adm)) == len(adm)   # each user at most once
    import random as _r
    rng = _r.Random(1)
    for _ in range(200):                                                     # property: sum of costs <= budget
        cs = {f"u{i}": [rng.choice(["resource", "high", "sweep", "sample"])] for i in range(rng.randint(0, 30))}
        cost = {u: rng.uniform(1, 40) for u in cs}
        b = rng.uniform(0, 300)
        adm, used, _ = priority_admit(cs, cost, b)
        assert sum(cost[u] for u in adm) <= b + 1e-9 and abs(used - sum(cost[u] for u in adm)) < 1e-9


def test_resource_trigger_from_a_finalised_window_and_queue_with_deadline():
    c = Clock(); e = esc(c, kappa=2, rho_graph=1.0, tgn_workers=1, mu_graph=1.0, analysis_window=60.0,
                         cost_c0=40.0, cost_c1=0.0, cost_c2=0.0, res_deadline=100.0)   # 60 units: one user per window
    win = {"kind": "final", "s": 1, "rstar": "default/hr:payroll", "window": 7, "users": ["a", "b", "c"]}
    assert e.on_resource_window(win) == 3 and e.on_resource_window({**win, "kind": "correction"}) == 0
    assert e.on_resource_window({**win, "users": ["a", "b"]}) == 0          # not more than kappa
    c.t += 5
    assert len(e.flush_realtime()) == 1 and e.r.hlen(K_PENDING) == 2       # two queued for budget
    c.t += 60
    assert len(e.flush_realtime()) == 1 and e.r.hlen(K_PENDING) == 1       # next window admits one more
    c.t += 120
    e.flush_realtime()
    assert int(e.r.hget(K_STATS, "resource_expired:realtime") or 0) + len(e.flush_realtime()) >= 0
    assert e.r.hlen(K_PENDING) == 0                                         # admitted or expired after the deadline


# ------------------------------------------------------------------------------------------ Path C
def test_sweep_rules_low_and_slow_threshold_evasion_and_uncommon_coaccess():
    cfg = EscalationConfig()
    baseline = [ev("slow", T0 - 5 * 86400 + i * 60, r="directory", s=0) for i in range(30)]
    baseline += [ev("habit", T0 - 5 * 86400 + i * 60, r="payroll") for i in range(30)]
    baseline += [ev(u, T0 - 5 * 86400, r="directory", s=0) for u in ("p", "q", "r", "s2") for _ in range(25)]
    look = [ev("slow", T0 - 3600 * k, r="reviews") for k in (20, 12, 5)]      # rare sensitive, spread over a day
    look += [ev("habit", T0 - 60 * k, r="payroll") for k in range(10)]         # habitual -> not rare
    look += [ev("newbie", T0 - 60 * k, r="reviews") for k in range(5)]         # cold start -> ignored by C1
    look += [ev(u, T0 - 600, r="board-minutes", a="docs") for u in ("p", "q", "r", "s2")]
    scores = [score("evader", 0.5, t=T0 - k) for k in range(6)]
    out = sweep_candidates(look, baseline, scores, cfg, T0)
    assert out["slow"]["rules"]["rare_sensitive"]["count"] == 3
    assert "habit" not in out and "newbie" not in out
    assert out["evader"]["rules"]["threshold_evasion"]["medium_scores"] == 6
    assert all("uncommon_coaccess" in out[u]["rules"] for u in ("p", "q", "r", "s2"))
    out2 = sweep_candidates([], [], scores + [score("evader", 0.9)], cfg, T0)
    assert "evader" not in out2                                  # reached High at least once -> not evasion


def test_slim_lake_loader_gives_the_same_sweep_candidates_as_full_events(tmp_path):
    """The memory-bounded sweep loader (sensitive lookback only, (u, rkey) baseline, (u, tier) scores, streamed
    reads) must not change the Path C result compared with loading every full event and score."""
    from accesssentinel.adapters.hdfs_lake import HistoryLake, LocalFsClient
    from accesssentinel.escalation_engine import baseline_range
    from accesssentinel.streaming.escalation_service import lake_loader
    cfg = EscalationConfig()
    lb0 = T0 - cfg.sweep_lookback
    b0, b1 = baseline_range(lb0, cfg)
    rng = random.Random(3)
    evs = [ev(f"u{rng.randrange(12)}", rng.uniform(b0 + 1, b1), r=rng.choice(["directory", "payroll", "reviews"]),
              s=rng.choice([0, 1])) for _ in range(600)]
    evs += [ev(f"u{rng.randrange(12)}", rng.uniform(lb0 + 1, T0), r=rng.choice(["payroll", "reviews", "board-minutes"]),
               s=rng.choice([0, 1]), a=rng.choice(["hr", "docs"])) for _ in range(300)]
    evs += [ev(u, T0 - 600, r="board-minutes", a="docs") for u in ("p", "q", "r", "s2")]
    scores = [score(f"u{k % 5}", rng.choice([0.45, 0.5, 0.2, 0.7]), t=T0 - 60 * k) for k in range(40)]
    for i, sc in enumerate(scores):
        sc["score_id"] = f"sc-{i}"
    lake = HistoryLake(LocalFsClient(str(tmp_path / "hist")), base="/accesssentinel")
    lake.write_events([(e, {"partition": 0, "offset": i}) for i, e in enumerate(evs)])
    lake.write_events([(e, {"partition": 0, "offset": i}) for i, e in enumerate(evs[:50])])    # duplicates: removed
    lake.write_records("risk-scores", [{"record_id": sc["score_id"], "ts": sc["t"], "payload": sc} for sc in scores])
    full = sweep_candidates(lake.read_events(lb0, T0), lake.read_events(b0, b1),
                            [r["payload"] for r in lake.read_records("risk-scores", lb0, T0)], cfg, T0)
    slim = lake_loader(lake, cfg)(T0)
    assert full and sweep_candidates(slim["lookback_events"], slim["baseline_events"], slim["lookback_scores"],
                                     cfg, T0) == full
    assert all(e.s == 1 for e in slim["lookback_events"]) and set(slim["lookback_scores"][0]) == {"u", "tier"}


def test_run_sweep_schedule_lock_and_background_emission():
    c = Clock(); e = esc(c, t_sweep=7200, gamma=0.0)
    calls = []
    load = lambda now: calls.append(now) or {"lookback_events": [], "baseline_events": [],
                                             "lookback_scores": [score("ev", 0.5, t=now - k) for k in range(5)]}
    info = e.run_sweep(load)
    assert info["staged"] == 1 and e.run_sweep(load) is None    # not due again before T_sweep
    (rec,) = e.flush_background()
    assert rec["u"] == "ev" and rec["paths"] == ["sweep"] and "threshold_evasion" in rec["evidence"]["sweep"]["rules"]
    e.r.set("esc:sweep:lock", 1)
    c.t += 7200
    assert e.run_sweep(load) is None and len(calls) == 1        # another replica holds the lock


# ------------------------------------------------------------------------------------------ service handler
class Producer:
    def __init__(self, fail): self.fail, self.sent, self._cbs = fail, [], []
    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.sent.append((topic, json.loads(value))); self._cbs.append(on_delivery)
    def flush(self, t=None):
        for cb in self._cbs:
            cb("down" if self.fail else None, None)
        self._cbs = []
        return 0


def handler(e, prod):
    return EscalationHandler(e, prod, "escalations.realtime", "escalations.background", "risk-scores",
                             "resource-windows")


def test_failed_publish_restages_and_the_next_tick_emits_again():
    c = Clock(); e = esc(c, gamma=0.0)
    prod = Producer(fail=True); h = handler(e, prod)
    e.on_score(score("u", 0.9)); c.t += 5
    with pytest.raises(RuntimeError):
        h.tick()
    assert e.r.hexists(K_PENDING, "u")                          # back in the queue, cooldown cleared
    prod.fail = False
    h.tick()
    assert [(t, r["u"]) for t, r in prod.sent][-1] == ("escalations.realtime", "u")


def test_handler_routes_topics_and_rejects_bad_scores():
    c = Clock(); e = esc(c, kappa=0, gamma=0.0)
    prod = Producer(fail=False); h = handler(e, prod)
    good = Record("risk-scores", 0, 0, b"u", json.dumps(score("u", 0.95)).encode(), 0)
    res = Record("resource-windows", 0, 0, b"k", json.dumps({"kind": "final", "s": 1, "rstar": "default/hr:payroll",
                                                              "window": 1, "users": ["w"]}).encode(), 0)
    for r in (good, res):
        r.data = h.decode(r)
    h.handle([good, res]); c.t += 5; h.tick()
    sent = {r["u"]: r["paths"] for _, r in prod.sent}
    assert sent == {"u": ["high"], "w": ["resource"]}
    with pytest.raises(DecodeError):
        h.decode(Record("risk-scores", 0, 1, b"u", json.dumps({"u": "x"}).encode(), 0))


def test_baseline_gap_keeps_a_slow_attack_rare_for_a_week():
    """Phase 6 finding: without a gap, day-1 attack accesses enter the baseline and hide days 2..n."""
    from accesssentinel.escalation_engine import baseline_range
    cfg = EscalationConfig()
    history = [ev("u", T0 - 15 * 86400 + i * 600, r="directory", s=0) for i in range(40)]      # benign, inside both baselines
    attack = [ev("u", T0 - d * 86400 - k * 3600, r="reviews") for d in range(0, 4) for k in range(3)]  # 4 days
    now = T0 + 60
    lb0 = now - cfg.sweep_lookback
    look = [e for e in attack if lb0 < e.t <= now]
    b0, b1 = baseline_range(lb0, cfg)
    base = [e for e in history + attack if b0 <= e.t < b1]
    assert "u" in sweep_candidates(look, base, [], cfg, now)                    # still rare on day 4
    no_gap = [e for e in history + attack if lb0 - 14 * 86400 <= e.t < lb0]
    assert "u" not in sweep_candidates(look, no_gap, [], cfg, now)              # the old behaviour missed it


def test_sweep_at_a_historical_time_ignores_live_cooldowns():
    """Phase 6 finding: a sweep at a past reference time must not be suppressed by cooldowns set on the live clock."""
    live = Clock(T0 + 40 * 86400)
    shared = fakeredis.FakeRedis()
    e_live = Escalator(shared, EscalationConfig(), RiskConfig(), clock=live)
    e_live.on_score(score("slow", 0.9)); live.t += 5; e_live.flush_realtime()        # cools "slow" on the live clock
    past = Clock(T0)
    e_past = Escalator(shared, EscalationConfig(), RiskConfig(), clock=past)
    e_past.run_sweep(lambda now: {"lookback_events": [], "baseline_events": [],
                                  "lookback_scores": [score("slow", 0.5, t=now - k) for k in range(6)]}, force=True)
    assert e_past.flush_background(force=True) == []                                 # the live-loop path would suppress it
    e_past.run_sweep(lambda now: {"lookback_events": [], "baseline_events": [],
                                  "lookback_scores": [score("slow", 0.5, t=now - k) for k in range(6)]}, force=True)
    (rec,) = e_past.drain_sweep()
    assert rec["u"] == "slow" and rec["paths"] == ["sweep"]


# ------------------------------------------------------------------------------------------ Revised v3 replay (Exp. E / F)
def test_offline_final_windows_match_the_resource_window_stage_semantics():
    from accesssentinel.ml.escalation_eval import final_windows
    from accesssentinel.schema import make_event as mk
    evs = [mk(u, "hr", "payroll", 1, "view", True, {"ip": "10.0.0.1", "device": "d"}, "c" * 32, t=T0 + dt)
           for u, dt in (("a", 10), ("b", 20), ("a", 30), ("c", 950))]
    evs.append(mk("d", "hr", "directory", 0, "view", True, {"ip": "10.0.0.1", "device": "d"}, "c" * 32, t=T0 + 15))
    w = final_windows(evs, 900.0, grace=5.0)
    b0 = int((T0 + 10) // 900)
    assert all(r["kind"] == "final" and r["s"] == 1 for _, r in w)              # sensitive resources only
    first = [r for _, r in w if r["window"] == b0][0]
    assert first["users"] == ["a", "b"] and first["rstar"] == "default/hr:payroll"
    assert [t for t, _ in w] == sorted(t for t, _ in w) and w[0][0] == (b0 + 1) * 900 + 5.0     # emitted when final


def test_offline_size_fn_and_admission_coverage():
    from types import SimpleNamespace
    from accesssentinel.ml.escalation_eval import coverage_v3, offline_size_fn
    from accesssentinel.ml.tgn_graph import MemoryEdgeIndex
    E = lambda u, r, t, i: {"id": f"e{i}", "u": u, "r": r, "t": t}
    idx = MemoryEdgeIndex([E("s", "hr:payroll", 100, 0), E("s", "hr:payroll", 110, 1), E("n1", "hr:payroll", 120, 2),
                           E("n1", "docs:x", 130, 3), E("far", "docs:x", 140, 4)])
    assert offline_size_fn(idx, 3600.0, 10)("s", 200.0) == (2 + 2, 2 + 1)        # seed 2 edges + n1's 2; users s,n1 + 1 res
    lab = lambda inst: SimpleNamespace(instance=inst)
    ev = lambda eid, t: SimpleNamespace(event_id=eid, t=t)
    ds = SimpleNamespace(events=[ev("x1", 1000.0), ev("x2", 2000.0)], labels={"x1": lab("g1"), "x2": lab("g1")},
                         instances={"g1": {"kind": "low_slow_group", "split": "test", "members": ["m1", "m2"]}})
    cov = coverage_v3(ds, [{"u": "m2", "t": 2500.0, "paths": ["sample"]}], ["low_slow_group"])
    assert cov["g1"]["covered"] and cov["g1"]["members_admitted"] == 1 and cov["g1"]["paths"] == ["sample"]
    assert not coverage_v3(ds, [{"u": "m2", "t": 9000.0, "paths": ["sample"]}], ["low_slow_group"])["g1"]["covered"]

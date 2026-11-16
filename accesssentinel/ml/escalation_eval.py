"""Offline evaluation of the four-path escalation policy on SYNTHETIC data (D3 Steps 6 and 10).

    python -m accesssentinel.ml.escalation_eval --model-dir models

Replays the synthetic timeline in event time through the same `Escalator` used by the service (state in an
in-process fakeredis), with risk scores from the promoted TCN. Path C reads in-memory history exactly as the
service reads HDFS. Reported:
  * per attack instance / collusion group: which paths TRIGGERED for its users (before union and cooldown)
    and whether/when the users were ESCALATED (after union) - i.e. does Path B/C/D reach what Path A misses
  * benign load: escalations of non-attackers per path
  * TGN workload per analysis window W: measured unique escalations vs the Eq. (11) estimate
"""
import argparse
import bisect
import json
import os
import random
import time
from collections import Counter, defaultdict
from dataclasses import asdict, replace
from datetime import datetime, timezone

import numpy as np

from ..config import EscalationConfig, RiskConfig
from ..datasets.synth import COLLUSION, INDIVIDUAL, SynthConfig, generate
from ..escalation_engine import PATHS, Escalator, baseline_range
from .dataset import build
from .registry import TcnScorer


class RecordingEscalator(Escalator):
    def __init__(self, *a, **kw):
        self.triggers = []                    # (t, user, path) for all four paths, before union/cooldown
        super().__init__(*a, on_trigger=lambda t, u, p: self.triggers.append((t, u, p)), **kw)


def score_all(ds, scorer, log=print):
    idx, _, _ = build(ds.events, ds.labels, ds.split_of, pre=scorer.pre)
    out = np.zeros(len(idx))
    for i in range(0, len(idx), 4096):
        b = np.arange(i, min(i + 4096, len(idx)))
        out[b] = scorer.score_X(idx.gather(b))
    return sorted(ds.events, key=lambda e: (e.t, e.event_id)), out


def prepare(model_dir: str, seed: int = 7, log=print):
    ds = generate(SynthConfig(seed=seed))
    scorer = TcnScorer.load(model_dir)
    log(f"scoring {len(ds.events)} synthetic events with {scorer.version} ...")
    events, s = score_all(ds, scorer)
    return ds, scorer.version, events, s


def run(model_dir: str, results_dir: str, seed: int = 7, cfg: EscalationConfig = None, sim_seed: int = 0, log=print,
        prepared=None) -> dict:
    import fakeredis
    t_start = time.time()
    cfg = cfg or EscalationConfig()
    rc = RiskConfig()
    ds, version, events, s = prepared or prepare(model_dir, seed, log)
    tier = lambda x: "high" if x >= rc.theta_high else ("medium" if x >= rc.theta_low else "low")
    times = np.array([e.t for e in events])
    score_recs = [{"u": e.u, "t": e.t, "s_ind": float(v), "tier": tier(v), "score_id": e.event_id, "trigger": "event"}
                  for e, v in zip(events, s)]

    def load(now):
        lb0 = now - cfg.sweep_lookback
        a, b = bisect.bisect_right(times, lb0), bisect.bisect_right(times, now)
        b0, b1 = baseline_range(lb0, cfg)
        c, d = bisect.bisect_right(times, b0), bisect.bisect_right(times, b1)
        return {"lookback_events": events[a:b], "baseline_events": events[c:d], "lookback_scores": score_recs[a:b]}

    clock = [events[0].t]
    esc = RecordingEscalator(fakeredis.FakeRedis(), cfg, rc, rng=random.Random(sim_seed), clock=lambda: clock[0])
    escalations, sweeps = [], []
    log("replaying through the escalation engine (event time) ...")
    for e, rec in zip(events, score_recs):
        clock[0] = e.t
        esc.on_event(e)
        esc.on_score(rec)
        escalations += esc.flush_realtime()
        if esc.background_due():
            if esc.sweep_due():
                info = esc.run_sweep(load)
                if info:
                    sweeps.append(info)
            escalations += esc.flush_background()
    clock[0] = events[-1].t + cfg.realtime_window + 1
    escalations += esc.flush_realtime(force=True)
    log(f"{len(escalations)} escalations, {len(esc.triggers)} path triggers, {len(sweeps)} sweeps")
    return analyse(ds, events, s, escalations, esc.triggers, sweeps, cfg, rc, version, seed, sim_seed,
                   results_dir, time.time() - t_start, log)


def analyse(ds, events, s, escalations, triggers, sweeps, cfg, rc, version, seed, sim_seed, results_dir, wall, log):
    attackers = {u for u, p in ds.users.items() if p.get("attacker")}
    t_test0, t_test1 = ds.split_bounds["test"]
    by_user_esc, by_user_trig = defaultdict(list), defaultdict(list)
    for x in escalations:
        by_user_esc[x["u"]].append(x)
    for t, u, p in triggers:
        by_user_trig[u].append((t, p))
    slack = cfg.t_sweep + cfg.analysis_window + cfg.realtime_window

    def coverage(users, t0, t1):
        trig = Counter(); esc_paths = Counter(); first = None; reached = set()
        for u in users:
            pts = {p for t, p in by_user_trig[u] if t0 <= t <= t1 + slack}
            trig.update(pts)
            for x in by_user_esc[u]:
                if t0 <= x["t"] <= t1 + slack:
                    reached.add(u); esc_paths.update(x["paths"])
                    first = x["t"] if first is None else min(first, x["t"])
        return {"paths_triggered": sorted(trig), "users_escalated": f"{len(reached)}/{len(users)}",
                "escalated_paths": dict(esc_paths), "minutes_to_first_escalation": None if first is None else round((first - t0) / 60, 1)}

    inst_t = defaultdict(list)
    for e in events:
        lab = ds.labels[e.event_id]
        if lab.instance:
            inst_t[lab.instance].append(e.t)
    instances = {}
    for k, info in sorted(ds.instances.items()):
        users = [info["user"]] if "user" in info else info["members"]
        ts = inst_t.get(k, [])
        if not ts:
            continue
        cov = coverage(users, min(ts), max(ts))
        t0, t1 = min(ts), max(ts) + slack
        member_paths = {u: {p for t, p in by_user_trig[u] if t0 <= t <= t1} for u in users}
        member_esc = {u: any(t0 <= x["t"] <= t1 for x in by_user_esc[u]) for u in users}
        cov["members_without_own_high"] = sum(1 for u in users if "high" not in member_paths[u])
        cov["members_reached_without_own_high"] = sum(1 for u in users if "high" not in member_paths[u] and member_esc[u])
        tcn_high = sum(1 for e, v in zip(events, s) if ds.labels[e.event_id].instance == k and v >= rc.theta_high)
        instances[k] = {"kind": info["kind"], "split": info["split"], "users": len(users), "events": len(ts),
                        "events_scored_high_by_tcn": tcn_high, **cov}

    def summarise(kinds, split=None):
        rows = [v for v in instances.values() if v["kind"] in kinds and (split is None or v["split"] == split)]
        out = {"instances": len(rows)}
        for p in PATHS:
            out[f"triggered_by_{p}"] = sum(1 for v in rows if p in v["paths_triggered"])
        out["escalated"] = sum(1 for v in rows if not v["users_escalated"].startswith("0/"))
        out["reached_only_without_path_A"] = sum(1 for v in rows if v["paths_triggered"] and "high" not in v["paths_triggered"])
        out["members"] = sum(v["users"] for v in rows)
        out["members_without_own_high"] = sum(v["members_without_own_high"] for v in rows)
        out["members_reached_without_own_high"] = sum(v["members_reached_without_own_high"] for v in rows)
        return out

    # benign load and workload, test period
    test_esc = [x for x in escalations if t_test0 <= x["t"] < t_test1]
    benign = [x for x in test_esc if x["u"] not in attackers]
    days = (t_test1 - t_test0) / 86400.0
    W = cfg.analysis_window
    buckets = Counter(int(x["t"] // W) for x in test_esc)
    active_buckets = sorted(buckets)
    n_buckets = int((t_test1 - t_test0) // W)
    per_w = np.array([buckets.get(b, 0) for b in range(int(t_test0 // W), int(t_test0 // W) + n_buckets)])
    trig_test = [(t, u, p) for t, u, p in triggers if t_test0 <= t < t_test1]
    uniq = lambda p: len({(int(t // W), u) for t, u, q in trig_test if q == p}) / max(n_buckets, 1)
    test_ev = [(e, v) for e, v in zip(events, s) if t_test0 <= e.t < t_test1]
    low_active = defaultdict(set)
    for e, v in test_ev:
        if v < rc.theta_low:
            low_active[int(e.t // W)].add(e.u)
    n_low = sum(len(x) for x in low_active.values()) / max(n_buckets, 1)
    eq11 = {"U_high": uniq("high"), "n_res": uniq("resource"), "B_sweep_over_T_sweep_x_W": cfg.sweep_batch / cfg.t_sweep * W,
            "gamma_x_N_low": cfg.gamma * n_low}
    eq11["estimate"] = sum(eq11.values())
    result = {
        "data": {"source": "SYNTHETIC (accesssentinel.datasets.synth v2)", "fingerprint": ds.fingerprint(), "seed": seed,
                 "events": len(events), "test_period": [datetime.fromtimestamp(t_test0, tz=timezone.utc).isoformat(),
                                                        datetime.fromtimestamp(t_test1, tz=timezone.utc).isoformat()]},
        "model_version": version, "config": asdict(cfg), "theta_high": rc.theta_high, "sim_seed": sim_seed,
        "totals": {"escalations": len(escalations), "by_priority": dict(Counter(x["priority"] for x in escalations)),
                   "triggers_by_path": dict(Counter(p for _, _, p in triggers)), "sweeps": len(sweeps)},
        "coverage": {"individual_test": summarise(INDIVIDUAL, "test"), "individual_all": summarise(INDIVIDUAL),
                     "collusion_all": summarise(COLLUSION)},
        "instances": instances,
        "benign_test": {"escalations": len(benign), "per_day": round(len(benign) / days, 1),
                        "by_path": dict(Counter(p for x in benign for p in x["paths"])),
                        "distinct_users": len({x["u"] for x in benign}),
                        "normal_users": sum(1 for u in ds.users if u not in attackers)},
        "workload_test": {"analysis_window_s": W, "windows": n_buckets, "windows_with_escalations": len(active_buckets),
                          "mean_per_window": float(per_w.mean()), "p95_per_window": float(np.percentile(per_w, 95)),
                          "max_per_window": int(per_w.max(initial=0)), "eq11_terms_per_window": eq11},
        "wall_seconds": round(wall, 1),
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    os.makedirs(results_dir, exist_ok=True)
    base = os.path.join(results_dir, f"escalation-{stamp}-kappa{cfg.kappa}")
    with open(base + ".json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    with open(base + ".md", "w", encoding="utf-8") as f:
        f.write(markdown(result))
    result["files"] = [base + ".json", base + ".md"]
    log(f"wrote {base}.md")
    return result


def markdown(r: dict) -> str:
    c = r["config"]
    L = [f"# Escalation policy evaluation (synthetic) — model {r['model_version']}", "",
         f"**Data: {r['data']['source']}**, fingerprint `{r['data']['fingerprint']}`, {r['data']['events']:,} events. "
         "Synthetic results only.", "",
         f"Configuration: theta_high {r['theta_high']}, kappa {c['kappa']}, delta_res {c['delta_res']:.0f} s, "
         f"T_sweep {c['t_sweep']:.0f} s, gamma {c['gamma']}, W {c['analysis_window']:.0f} s, real-time window "
         f"{c['realtime_window']:.0f} s, cooldowns {c['realtime_cooldown']:.0f}/{c['background_cooldown']:.0f} s, "
         f"caps {c['max_realtime_per_window']}/{c['max_background_per_window']}.", "",
         "## Which paths reach which attacks", "",
         "| Set | instances | Path A high | Path B resource | Path C sweep | Path D sample | escalated | users never scored High | of those, escalated via B/C/D |",
         "|---|---|---|---|---|---|---|---|---|"]
    for k, v in r["coverage"].items():
        L.append(f"| {k} | {v['instances']} | {v['triggered_by_high']} | {v['triggered_by_resource']} | "
                 f"{v['triggered_by_sweep']} | {v['triggered_by_sample']} | {v['escalated']} | "
                 f"{v['members_without_own_high']}/{v['members']} | {v['members_reached_without_own_high']} |")
    L += ["", "Paths are counted when they triggered for the instance's users during the attack (plus T_sweep + W); "
          "'users never scored High' = users for whom Path A never fired during that interval."]
    L += ["", "## Per instance", "", "| Instance | split | users | events | TCN high events | paths triggered | users escalated | minutes to first escalation |",
          "|---|---|---|---|---|---|---|---|"]
    for k, v in r["instances"].items():
        L.append(f"| {k} | {v['split']} | {v['users']} | {v['events']} | {v['events_scored_high_by_tcn']} | "
                 f"{', '.join(v['paths_triggered']) or '-'} | {v['users_escalated']} | {v['minutes_to_first_escalation']} |")
    b, w = r["benign_test"], r["workload_test"]
    eq = w["eq11_terms_per_window"]
    L += ["", "## Load (test period)", "",
          f"- Escalations of non-attackers: {b['escalations']} ({b['per_day']}/day) over {b['distinct_users']} of "
          f"{b['normal_users']} normal users; by path {b['by_path']}.",
          f"- Unique escalations per W = {w['analysis_window_s']:.0f} s: mean {w['mean_per_window']:.3f}, "
          f"p95 {w['p95_per_window']:.0f}, max {w['max_per_window']} ({w['windows_with_escalations']} of {w['windows']} windows non-empty).",
          f"- Eq. (11) terms per window: |U_high| {eq['U_high']:.3f} + n_res {eq['n_res']:.3f} + B_sweep/T_sweep*W "
          f"{eq['B_sweep_over_T_sweep_x_W']:.3f} + gamma*N_low {eq['gamma_x_N_low']:.3f} = {eq['estimate']:.3f} "
          f"(upper bound; measured {w['mean_per_window']:.3f} after union and cooldowns).",
          f"- Totals: {r['totals']}", f"- Wall time {r['wall_seconds']} s.", ""]
    return "\n".join(L)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model-dir", default=os.environ.get("AS_MODEL_DIR", "models"))
    ap.add_argument("--results-dir", default="experiments/results")
    ap.add_argument("--data-seed", type=int, default=7)
    ap.add_argument("--sim-seed", type=int, default=0)
    ap.add_argument("--gamma", type=float, default=None)
    ap.add_argument("--kappas", default="3", help="comma-separated kappa values (sensitivity study)")
    args = ap.parse_args(argv)
    cfg = EscalationConfig()
    if args.gamma is not None:
        cfg = replace(cfg, gamma=args.gamma)
    prepared = prepare(args.model_dir, args.data_seed)
    rows = []
    for k in [int(x) for x in args.kappas.split(",")]:
        r = run(args.model_dir, args.results_dir, args.data_seed, replace(cfg, kappa=k), args.sim_seed, prepared=prepared)
        rows.append(r)
    if len(rows) > 1:
        write_sensitivity(rows, args.results_dir)
    return 0


def write_sensitivity(rows, results_dir: str) -> str:
    L = ["# Escalation: kappa sensitivity (synthetic data, test period)", "",
         "| kappa | Path B trigger user-windows / W | unique escalations / W (mean) | max / W | benign escalations / day | "
         "individual attacks escalated (test) | collusion groups with >=1 member escalated | colluders never High, escalated via B/C/D | Eq.(11) estimate / W |",
         "|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        w, b, c = r["workload_test"], r["benign_test"], r["coverage"]
        L.append(f"| {r['config']['kappa']} | {w['eq11_terms_per_window']['n_res']:.3f} | {w['mean_per_window']:.4f} | "
                 f"{w['max_per_window']} | {b['per_day']} | {c['individual_test']['escalated']}/{c['individual_test']['instances']} | "
                 f"{c['collusion_all']['escalated']}/{c['collusion_all']['instances']} | "
                 f"{c['collusion_all']['members_reached_without_own_high']}/{c['collusion_all']['members_without_own_high']} | "
                 f"{w['eq11_terms_per_window']['estimate']:.3f} |")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(results_dir, f"escalation-kappa-sensitivity-{stamp}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    print("\n".join(L))
    return path


if __name__ == "__main__":
    raise SystemExit(main())


# ============================================================================================================
# Revised v3 (Experiments E and F): replay with final resource windows, Eq. 26 costs and Algorithm 1
# ============================================================================================================
def offline_size_fn(index, window: float = 3600.0, k: int = 10, cache: dict = None):
    """Offline twin of streaming.escalation_service.neighbourhood_size_fn (same neighbourhood as the SGE):
    the seed's window edges + up to k most recent users per touched resource and all of their window edges."""
    cache = {} if cache is None else cache

    def size(u: str, now: float):
        key = (u, int(now // 60))
        if key in cache:
            return cache[key]
        seed = index.user_edges(u, now - window, now)
        res = sorted({e["r"] for e in seed})
        users = {u}
        for rk in res:
            recent = []
            for e in reversed(index.resource_edges(rk, now - window, now)):
                if e["u"] not in recent:
                    recent.append(e["u"])
                if len(recent) >= k:
                    break
            users |= set(recent)
        n_other = sum(len(index.user_edges(v, now - window, now)) for v in users - {u})
        cache[key] = (len(seed) + n_other, len(users) + len(res))
        return cache[key]
    return size


def final_windows(events, delta_res: float, grace: float = 5.0, tenant: str = "default"):
    """Event-time resource windows b = floor(t / delta_res) of sensitive resources, emitted when final
    (window end + grace), as the resource-windows stage does: [(emit_t, record), ...] sorted by emit time."""
    win = defaultdict(set)
    for e in events:
        if int(e.s) == 1:
            win[(f"{tenant}/{e.a}:{e.r}", int(e.t // delta_res))].add(e.u)
    out = [((b + 1) * delta_res + grace, {"kind": "final", "s": 1, "rstar": r, "window": b, "users": sorted(us),
                                          "window_start": b * delta_res, "window_end": (b + 1) * delta_res})
           for (r, b), us in win.items()]
    return sorted(out, key=lambda x: x[0])


def replay_v3(ds, events, scores, thresholds: dict, cfg: EscalationConfig, size_fn, paths=PATHS, t_from=None,
              sim_seed: int = 0, log=print) -> dict:
    """Event-time replay of the v3 escalation engine. paths: the enabled paths (Experiment F ablation).
    Returns the emitted (admitted) escalations with their costs and the engine's overflow counters."""
    import fakeredis
    from ..escalation_engine import K_STATS
    rc = replace(RiskConfig(), theta_low=thresholds["theta_low"], theta_high=thresholds["theta_high"])
    tier = lambda x: "high" if x >= rc.theta_high else ("medium" if x >= rc.theta_low else "low")
    times = np.array([e.t for e in events])
    t_from = events[0].t if t_from is None else t_from
    start = int(np.searchsorted(times, t_from))
    if "sample" not in paths:
        cfg = replace(cfg, gamma=0.0)
    recs = [{"u": e.u, "t": e.t, "s_ind": float(v), "tier": tier(v), "score_id": e.event_id, "trigger": "event",
             "theta_high": rc.theta_high, "theta_low": rc.theta_low} for e, v in zip(events, scores)]

    def load(now):
        lb0 = now - cfg.sweep_lookback
        a, b = bisect.bisect_right(times, lb0), bisect.bisect_right(times, now)
        b0, b1 = baseline_range(lb0, cfg)
        c, d = bisect.bisect_right(times, b0), bisect.bisect_right(times, b1)
        return {"lookback_events": events[a:b], "baseline_events": events[c:d], "lookback_scores": recs[a:b]}

    clock = [events[start].t]
    esc = Escalator(fakeredis.FakeRedis(), cfg, rc, rng=random.Random(sim_seed), clock=lambda: clock[0])
    esc.cost_fn = size_fn
    if "high" not in paths:
        esc.theta_high_override = 2.0                       # Path A never fires
    wins = final_windows(events[start:], cfg.delta_res) if "resource" in paths else []
    wi, out, sweeps = 0, [], 0
    for i in range(start, len(events)):
        e = events[i]
        clock[0] = e.t
        while wi < len(wins) and wins[wi][0] <= e.t:
            esc.on_resource_window(wins[wi][1]); wi += 1
        esc.on_score(recs[i])
        out += esc.flush_realtime()
        if esc.background_due():
            if "sweep" in paths and esc.sweep_due():
                sweeps += bool(esc.run_sweep(load))
            out += esc.flush_background()
    clock[0] = events[-1].t + cfg.realtime_window + 1
    out += esc.flush_realtime(force=True)
    stats = {k.decode(): int(v) for k, v in esc.r.hgetall(K_STATS).items()}
    return {"escalations": out, "stats": stats, "sweeps": sweeps}


def coverage_v3(ds, escalations, kinds, split: str = "test", slack: float = 3600.0) -> dict:
    """Admission coverage: an instance is covered if >= 1 of its users was ADMITTED to graph analysis between its
    first event and its last event + slack. (A necessary condition for graph detection, not detection itself.)"""
    span = {}
    for e in ds.events:
        inst = ds.labels[e.event_id].instance
        if inst:
            a, b = span.get(inst, (e.t, e.t)); span[inst] = (min(a, e.t), max(b, e.t))
    by_u = defaultdict(list)
    for x in escalations:
        by_u[x["u"]].append(x)
    rows = {}
    for k, info in ds.instances.items():
        if info.get("split") != split or info.get("kind") not in kinds or k not in span:
            continue
        users = [info["user"]] if "user" in info else info["members"]
        a, b = span[k]
        hit = [x for u in users for x in by_u[u] if a <= x["t"] <= b + slack]
        rows[k] = {"kind": info["kind"], "size": len(users), "covered": bool(hit),
                   "members_admitted": len({x["u"] for x in hit}),
                   "minutes_to_admission": (min(x["t"] for x in hit) - a) / 60.0 if hit else None,
                   "paths": sorted({p for x in hit for p in x.get("paths", [])})}
    return rows

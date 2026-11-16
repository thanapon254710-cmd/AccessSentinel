"""Experiments E (compute budget) and F (path ablation) - Deliverable 4 Sections V-E / V-F. SYNTHETIC data.

    .venv-gpu/Scripts/python -m evaluation.exp_e

Data: SynthConfig.collusion_v3_eval(seed = 11) (= Experiment B seed 0); risk scores from the promoted v3 TCN at its
calibrated thresholds. The escalation engine (the deployed code) is replayed in event time over the test period
(+ 1 day warm-up; sweeps read the full history): Path B from FINAL resource windows, Path A at theta_high,
sweeps, sampling at gamma; every candidate priced by Eq. 26 with the MEASURED coefficients over the same
neighbourhood the SGE embeds, and admitted by Algorithm 1 against B_W = floor(rho' N mu W) (Eq. 29).

Strategies (E): all     every active user analysed every window W (estimated on 300 sampled windows)
                high    Path A only, no budget
                union   four paths, no budget (Algorithm 1 with an unbounded B_W)
                admit-x four paths + PriorityAdmit at budget x * B_W(deployed); x = 1, 0.1, 0.01, 0.001
Ablation (F):   union without one path at a time.
Outcome: ADMISSION coverage of the test groups (Experiment B kinds) and individual attack instances (>= 1 user
admitted between the instance's first event and its last + 1 h) - a necessary condition for graph detection, not
detection - against the admitted work (ms of SGE time) per window.

Raw output: results/raw/exp_e/<config>-sim<k>.json and results/raw/exp_e/all-estimate.json
"""
import argparse
import json
import os
import random
import sys
import time
from dataclasses import asdict, replace
from datetime import datetime, timezone

import numpy as np
import torch

from accesssentinel.config import EscalationConfig
from accesssentinel.datasets.synth import COLLUSION_V3, INDIVIDUAL_V3, SynthConfig, generate
from accesssentinel.escalation_engine import PATHS, graph_budget
from accesssentinel.ml import registry
from accesssentinel.ml.dataset import build_v3
from accesssentinel.ml.escalation_eval import coverage_v3, offline_size_fn, replay_v3
from accesssentinel.ml.features import SimulatedDevPosture, SimulatedThreatFeed
from accesssentinel.ml.tgn_graph import EdgeAnnotator, MemoryEdgeIndex
from accesssentinel.ml.train_tcn import scores_v3

from . import env_info
from .exp_a import _dirty, _git


def prepare(device, log):
    ds = generate(SynthConfig.collusion_v3_eval(seed=11))
    events = sorted(ds.events, key=lambda e: (e.t, e.event_id))
    sc = registry.TcnScorer.load("models")
    assert getattr(sc, "arch", "") == "tcn-v3", "the promoted TCN must be the v3 model"
    idx, _, _ = build_v3(ds.events, ds.labels, ds.split_of, SimulatedThreatFeed(), SimulatedDevPosture.from_dataset(ds),
                         sc.L, enc=sc.enc)
    assert list(idx.event_ids) == [e.event_id for e in events]
    s = scores_v3(sc.model.to(device), idx, np.arange(len(idx)), device)
    log(f"scored {len(events)} events with {sc.version}")
    index = MemoryEdgeIndex([EdgeAnnotator().annotate(e) for e in events])
    return ds, events, s, sc, index


def summarise(ds, run, cfg, t0, t1, budget):
    esc = [x for x in run["escalations"] if t0 <= x["t"] < t1]
    W = cfg.analysis_window
    per_w = {}
    for x in esc:
        per_w[int(x["t"] // W)] = per_w.get(int(x["t"] // W), 0.0) + float(x.get("cost") or 0.0)
    n_w = int((t1 - t0) // W)
    work = np.array([per_w.get(b, 0.0) for b in range(int(t0 // W), int(t0 // W) + n_w)])
    groups = coverage_v3(ds, esc, COLLUSION_V3)
    indiv = coverage_v3(ds, esc, INDIVIDUAL_V3)
    attackers = {u for u, p in ds.users.items() if p.get("attacker")}
    return {"admitted": len(esc), "admitted_benign": sum(1 for x in esc if x["u"] not in attackers),
            "work_ms_total": float(work.sum()), "work_ms_per_window_mean": float(work.mean()),
            "work_ms_per_window_p99": float(np.percentile(work, 99)), "work_ms_per_window_max": float(work.max(initial=0)),
            "budget_per_window": budget, "windows_over_budget": int((work > budget + 1e-6).sum()), "windows": n_w,
            "group_coverage": sum(v["covered"] for v in groups.values()) / max(len(groups), 1), "groups": groups,
            "individual_coverage": sum(v["covered"] for v in indiv.values()) / max(len(indiv), 1), "individual": indiv,
            "engine_stats": run["stats"], "sweeps": run["sweeps"],
            "by_priority": {p: sum(1 for x in esc if x.get("priority") == p) for p in ("realtime", "background")}}


def estimate_all(ds, events, size_fn, cfg, t0, t1, n=300, seed=0):
    """Strategy 'all': analyse every active user each window; work estimated on n sampled windows."""
    W, rng = cfg.analysis_window, random.Random(seed)
    times = np.array([e.t for e in events])
    work = []
    for _ in range(n):
        w0 = int(rng.uniform(t0, t1 - W) // W) * W
        a, b = np.searchsorted(times, [w0, w0 + W])
        users = {events[i].u for i in range(a, b)}
        work.append(sum(cfg.cost_c0 + cfg.cost_c1 * e + cfg.cost_c2 * v for e, v in (size_fn(u, w0 + W) for u in users)))
    return {"windows_sampled": n, "work_ms_per_window_mean": float(np.mean(work)),
            "work_ms_per_window_p99": float(np.percentile(work, 99)), "group_coverage": 1.0, "individual_coverage": 1.0,
            "note": "every active user is analysed, so coverage is 1 by construction (upper bound)"}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="evaluation/results/raw/exp_e")
    ap.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
    ap.add_argument("--sims", type=int, nargs="+", default=[0, 1, 2])
    ap.add_argument("--only", nargs="*", default=None, help="run only these configs")
    args = ap.parse_args(argv)
    log = lambda *a: print(*a, flush=True)
    os.makedirs(args.out, exist_ok=True)
    started = datetime.now(timezone.utc).isoformat()
    ds, events, s, sc, index = prepare(args.device, log)
    th = {k: float(sc.thresholds[k]) for k in ("theta_low", "theta_high")}
    t0, t1 = ds.split_bounds["test"]
    base = EscalationConfig()
    b_dep = graph_budget(base)
    cache = {}
    size_fn = offline_size_fn(index, 3600.0, 10, cache)
    configs = {"high": (("high",), None), "union": (PATHS, 1e9)}
    for x in (1.0, 0.1, 0.01, 0.001):
        configs[f"admit-{x:g}"] = (PATHS, x)
    for p in PATHS:
        configs[f"union-no-{p}"] = (tuple(q for q in PATHS if q != p), 1e9)
    meta = {"data": {"fingerprint": ds.fingerprint(), "events": len(events), "synth": "collusion_v3_eval(seed=11)"},
            "tcn": sc.version, "thresholds": th, "escalation_config": asdict(base), "deployed_budget_per_window": b_dep,
            "git_commit": _git(), "git_dirty": _dirty(), "environment": env_info.collect(), "started_at": started}
    for name, (paths, scale) in configs.items():
        if args.only and name not in args.only:
            continue
        sims = args.sims if (name in ("union", "admit-1") and "sample" in paths) else args.sims[:1]
        for k in sims:
            tic = time.time()
            cfg = base if scale is None else replace(base, rho_graph=base.rho_graph * scale)
            budget = graph_budget(cfg)
            run = replay_v3(ds, events, s, th, cfg, size_fn, paths, t_from=t0 - 86400.0, sim_seed=k, log=log)
            out = {**meta, "config": name, "paths": list(paths), "budget_scale": scale, "sim_seed": k,
                   "result": summarise(ds, run, cfg, t0, t1, budget), "wall_seconds": time.time() - tic}
            json.dump(out, open(os.path.join(args.out, f"{name}-sim{k}.json"), "w", encoding="utf-8"), indent=1, default=str)
            r = out["result"]
            log(f"{name:18s} sim {k}: admitted {r['admitted']:6d}, work/window mean {r['work_ms_per_window_mean']:9.1f} ms "
                f"(budget {budget:.0f}, over {r['windows_over_budget']}), group coverage {r['group_coverage']:.3f}, "
                f"individual {r['individual_coverage']:.3f} ({out['wall_seconds']:.0f} s)")
    if not args.only or "all" in args.only:
        est = estimate_all(ds, events, size_fn, base, t0, t1)
        json.dump({**meta, "config": "all", "result": est}, open(os.path.join(args.out, "all-estimate.json"), "w",
                                                                  encoding="utf-8"), indent=1, default=str)
        log(f"all (estimate): work/window mean {est['work_ms_per_window_mean']:.1f} ms")
    return 0


if __name__ == "__main__":
    sys.exit(main())

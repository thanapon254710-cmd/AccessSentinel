"""Experiment A - individual detection (Deliverable 4 Section V-A). SYNTHETIC data only.

    .venv-gpu/Scripts/python -m evaluation.exp_a --seeds 0 1 2 3 4
    .venv-gpu/Scripts/python -m evaluation.exp_a --seeds 0 --quick          # pipeline check only, never reported

Seed s: data SynthConfig.tcn_v3_eval(seed = 23 + s) (ThreatFeed / DevPosture simulated), model seed s.
Every model is scored on the SAME evaluable test windows (chronological split with the Eq. 7 gap) and calibrated
by the SAME rule (Eq. 8: quantiles of ITS OWN scores on nominal validation windows, alpha 0.05 / 0.01 / 0.001), so
no model gets the fixed 0.4 / 0.6 of D3 and none gets a threshold tuned on the test period.

Models: tcn-v3 (Revised v3), tcn-sup (generic supervised TCN, no auxiliary heads, no z_u), lstm, iforest
(Isolation Forest on window summaries, unsupervised), rule (the D3 hand-written heuristic), tcn-d3 (the
Deliverable 3 TCN, L = 20, d = 50, retrained on this data); ablations for Experiment F: tcn-noz, tcn-noaux.

Raw output (inputs of evaluation/figures.py; nothing is typed by hand):
    results/raw/exp_a/seed<s>.json          metrics per model, thresholds, latency, training history, environment
    results/raw/exp_a/seed<s>-scores.npz    test labels / scenario / instance / user / time + every model's scores
"""
import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, replace
from datetime import datetime, timezone

import numpy as np
import torch

from accesssentinel.config import TCNConfig, TCNv3Config
from accesssentinel.datasets.synth import SynthConfig, generate
from accesssentinel.ml import evaluation as ev
from accesssentinel.ml.dataset import build, build_v3
from accesssentinel.ml.features import SimulatedDevPosture, SimulatedThreatFeed
from accesssentinel.ml.train_tcn import (VARIANTS, _logits, calibrate_v3, fit_iforest, latency_v3, scores_v3,
                                         train_once, train_variant, window_aggregates)

from . import env_info

MAIN = ["tcn-v3", "tcn-sup", "lstm", "iforest", "rule", "tcn-d3"]
ABLATIONS = ["tcn-noz", "tcn-noaux"]


def _git():
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, timeout=5).stdout.strip()
    except Exception:  # noqa: BLE001
        return None


def _dirty():
    try:
        return bool(subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, timeout=10).stdout.strip())
    except Exception:  # noqa: BLE001
        return None


def _latency_fn(fn, X1, repeats=200):
    for _ in range(5):
        fn(X1)
    ts = []
    for _ in range(repeats):
        t0 = time.perf_counter(); fn(X1); ts.append((time.perf_counter() - t0) * 1000)
    return {"batch_1_ms_median": float(np.median(ts)), "batch_1_ms_p95": float(np.percentile(ts, 95)), "device": "cpu"}


def run_seed(seed: int, models, out_dir: str, device: str, quick: bool, log=print) -> dict:
    t_start, started = time.time(), datetime.now(timezone.utc).isoformat()
    cfg = TCNv3Config()
    synth = SynthConfig.tcn_v3_eval(seed=23 + seed)
    if quick:
        synth = replace(synth, n_users=220, days=40, split_days=(22, 8, 10), attack_margin_days=(0, 6, 6),
                        attacks={k: (1, 1, 1) for k in synth.attacks})
        cfg = replace(cfg, pretrain_epochs=1, finetune_epochs=1)
    torch.manual_seed(seed)
    log(f"[seed {seed}] generating {synth.n_users} users x {synth.days} days (data seed {synth.seed}) ...")
    ds = generate(synth)
    threat, posture = SimulatedThreatFeed(), SimulatedDevPosture.from_dataset(ds)
    idx, enc, _ = build_v3(ds.events, ds.labels, ds.split_of, threat, posture, cfg.window_len)
    tr, va, te = idx.mask("train"), idx.mask("val"), idx.mask("test")
    attackers = {u for u, p in ds.users.items() if p.get("attacker")}
    not_att = lambda ids: ~np.isin(idx.users[ids], list(attackers))
    nom_val = va[(idx.y[va] == 0) & not_att(va)]
    nom_tr = tr[(idx.y[tr] == 0) & not_att(tr)]
    log(f"[seed {seed}] {len(ds.events)} events; windows train {len(tr)} val {len(va)} test {len(te)} "
        f"(+{int(idx.y[te].sum())} attack); nominal val {len(nom_val)}")
    res, scores = {}, {}
    d3 = {}

    def finish(name, s_nom, s_te, extra):
        th = calibrate_v3(np.asarray(s_nom, dtype=float), cfg)
        rep = ev.report_at(name, idx, te, np.asarray(s_te, dtype=float), attackers,
                           {k: th[k] for k in ("theta_low", "theta_high", "theta_B")})
        rep["calibration"] = th
        rep.update(extra)
        res[name] = rep
        scores[name] = np.asarray(s_te, dtype=np.float32)
        e = rep["event_level"]
        log(f"[seed {seed}] {name:10s} ROC-AUC {e['roc_auc']:.4f} PR-AUC {e['pr_auc']:.4f} | at theta_high "
            f"P {e['at_theta_high']['precision']:.3f} R {e['at_theta_high']['recall']:.3f} FPR {e['at_theta_high']['fpr']:.4f}")

    def d3_index():
        if not d3:
            c3 = TCNConfig()
            idx3, _, recs3 = build(ds.events, ds.labels, ds.split_of, c3.window_len)
            row = {e: i for i, e in enumerate(idx3.event_ids)}
            d3.update(idx=idx3, recs=recs3, cfg=c3, te=np.array([row[e] for e in idx.event_ids[te]]),
                      nom=np.array([row[e] for e in idx.event_ids[nom_val]]))
        return d3

    for m in models:
        t0 = time.time()
        if m in VARIANTS:
            model, hist, best_ap = train_variant(idx, enc, cfg, m, tr, va, device, seed, log=lambda *a: log(f"[seed {seed}] {m}", *a))
            train_s = time.time() - t0
            lat = {"cpu": latency_v3(model, idx, te, "cpu")}
            if torch.cuda.is_available():
                lat["cuda"] = latency_v3(model, idx, te, "cuda")
            finish(m, scores_v3(model, idx, nom_val, device), scores_v3(model, idx, te, device),
                   {"desc": VARIANTS[m]["desc"], "train_seconds": train_s, "history": hist, "best_val_pr_auc": best_ap,
                    "latency": lat, "parameters": int(sum(p.numel() for p in model.parameters()))})
            del model
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        elif m == "iforest":
            rng = np.random.default_rng(seed)
            sub = nom_tr[rng.choice(len(nom_tr), min(60000, len(nom_tr)), replace=False)]
            f = fit_iforest(window_aggregates(idx, sub), seed)
            train_s = time.time() - t0
            sc = lambda ids: -f.score_samples(window_aggregates(idx, ids))
            lat = {"cpu": _latency_fn(lambda ids: sc(ids), te[:1], 100)}
            finish(m, sc(nom_val), sc(te), {"desc": "Isolation Forest (200 trees) on window summaries, fitted on "
                                                    "nominal training windows (unsupervised)",
                                            "train_seconds": train_s, "latency": lat})
        elif m == "rule":
            d = d3_index()
            sc = lambda rows: np.array([ev.rule_score(d["recs"][i]) for i in rows])
            finish(m, sc(d["nom"]), sc(d["te"]), {"desc": "D3 hand-written heuristic over the newest event (untrained)",
                                                  "train_seconds": 0.0})
        elif m == "tcn-d3":
            d = d3_index()
            idx3 = d["idx"]
            model, info = train_once(idx3, idx3.mask("train"), idx3.mask("val"), d["cfg"], 2 if quick else 25,
                                     1 if quick else 5, seed, log=lambda *a: None, device=device)
            train_s = time.time() - t0
            sig = lambda rows: 1.0 / (1.0 + np.exp(-_logits(model, idx3, rows)))
            finish(m, sig(d["nom"]), sig(d["te"]), {"desc": "Deliverable 3 TCN (L = 20, d = 50, 10 blocks), retrained "
                                                            "on this data, focal loss, early stop on val PR-AUC",
                                                    "train_seconds": train_s, "best_epoch": info["best_epoch"],
                                                    "best_val_pr_auc": info["best_val_pr_auc"],
                                                    "parameters": int(sum(p.numel() for p in model.parameters()))})
            del model
        else:
            raise ValueError(m)
    out = {"experiment": "A", "seed": seed, "quick": quick, "started_at": started,
           "finished_at": datetime.now(timezone.utc).isoformat(), "wall_seconds": time.time() - t_start,
           "data": {"source": "synthetic", "fingerprint": ds.fingerprint(), "events": len(ds.events), "users": len(ds.users),
                    "synth_config": asdict(synth), "timer_context": "simulated",
                    "windows": {"train": int(len(tr)), "val": int(len(va)), "test": int(len(te))},
                    "test_attack_events": int(idx.y[te].sum()), "nominal_val_windows": int(len(nom_val))},
           "tcn_v3_config": cfg.to_dict(), "device": device, "models": res,
           "git_commit": _git(), "git_dirty": _dirty(), "environment": env_info.collect()}
    os.makedirs(out_dir, exist_ok=True)
    tag = f"seed{seed}" + ("-quick" if quick else "")
    with open(os.path.join(out_dir, f"{tag}.json"), "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=1, default=str)
    np.savez_compressed(os.path.join(out_dir, f"{tag}-scores.npz"), y=idx.y[te], scenario=idx.scenario[te],
                        instance=idx.instance[te], users=idx.users[te], t=idx.t[te],
                        attacker=np.isin(idx.users[te], list(attackers)), **{f"s_{k}": v for k, v in scores.items()})
    log(f"[seed {seed}] done in {time.time() - t_start:.0f} s -> {out_dir}/{tag}.json")
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2, 3, 4])
    ap.add_argument("--models", nargs="+", default=MAIN, choices=MAIN + ABLATIONS)
    ap.add_argument("--out", default="evaluation/results/raw/exp_a")
    ap.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args(argv)
    log = lambda *a: print(*a, flush=True)
    for s in args.seeds:
        run_seed(s, args.models, args.out, args.device, args.quick, log)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Reproducible TCN training + evaluation (D3 Step 4) on leak-free synthetic data.

    python -m accesssentinel.ml.train_tcn --model-dir models --promote
    python -m accesssentinel.ml.train_tcn --model-dir models --cv-folds 0 --epochs 3     # quick

Protocol (deviation from D3's random 10 % test + 5-fold CV is deliberate, see audit D1):
  * splits by TIME: train days -> validation days -> test days; attacker users disjoint between splits
  * vocabulary/scaler fitted on the training period only
  * TCN architecture and optimiser exactly as D3 (TCNConfig): 10 layers, dilations (1,2,4,...,18),
    filters [512..12], spatial dropout 0.3, dense dropout 0.5, Focal Loss, Adam 1e-3, batch 512
  * early stopping on validation PR-AUC (patience), Platt calibration on validation logits, so that
    D3's fixed thresholds 0.4 / 0.6 apply to probability-like scores
  * 5-fold GroupKFold by USER over the training period (fixed epoch budget = best epoch) gives the CV spread
  * the test period is scored once, together with a rule baseline and a logistic-regression baseline
Outputs: a registry version (model + preprocessor + manifest) and experiments/results/<version>.{json,md}.
"""
import argparse
import copy
import json
import os
import platform
import subprocess
import time
from dataclasses import asdict
from datetime import datetime, timezone

import numpy as np
import torch
import torch.nn.functional as F
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import GroupKFold

from ..config import TCNConfig
from ..datasets.synth import SynthConfig, generate
from ..tcn import TCN, fit_platt, focal_loss, platt_sigmoid
from . import evaluation as ev
from . import registry
from .dataset import build


def _logits(model, idx, ids, batch=4096):
    model.eval()
    out = []
    with torch.no_grad():
        for i in range(0, len(ids), batch):
            out.append(model(torch.from_numpy(idx.gather(ids[i:i + batch]))).numpy())
    return np.concatenate(out) if out else np.zeros(0)


def train_once(idx, train_ids, val_ids, cfg: TCNConfig, epochs: int, patience: int, seed: int, log=print,
               fixed_epochs: bool = False):
    torch.manual_seed(seed)
    rng = np.random.default_rng(seed)
    model = TCN(cfg)
    opt = torch.optim.Adam(model.parameters(), lr=cfg.lr)
    yt = torch.from_numpy(idx.y.astype(np.float32))
    best, best_ap, best_ep, bad, history = None, -1.0, -1, 0, []
    for epoch in range(epochs):
        t0 = time.time()
        model.train()
        perm = rng.permutation(train_ids)
        tot = 0.0
        for i in range(0, len(perm), cfg.batch_size):
            b = perm[i:i + cfg.batch_size]
            loss = focal_loss(model(torch.from_numpy(idx.gather(b))), yt[b], cfg.focal_gamma, cfg.focal_alpha)
            opt.zero_grad(); loss.backward(); opt.step()
            tot += loss.item() * len(b)
        row = {"epoch": epoch, "train_loss": tot / len(perm), "seconds": round(time.time() - t0, 1)}
        if val_ids is not None and len(val_ids):
            z = _logits(model, idx, val_ids)
            row["val_loss"] = float(focal_loss(torch.from_numpy(z), yt[val_ids], cfg.focal_gamma, cfg.focal_alpha))
            row["val_pr_auc"] = float(average_precision_score(idx.y[val_ids], z))
            row["val_roc_auc"] = float(roc_auc_score(idx.y[val_ids], z))
            if row["val_pr_auc"] > best_ap:
                best, best_ap, best_ep, bad = copy.deepcopy(model.state_dict()), row["val_pr_auc"], epoch, 0
            else:
                bad += 1
        history.append(row)
        log("  epoch %(epoch)2d  train_loss %(train_loss).4f  %(seconds)5.1fs" % row
            + ("  val_pr_auc %.3f  val_roc_auc %.3f" % (row["val_pr_auc"], row["val_roc_auc"]) if "val_pr_auc" in row else ""))
        if not fixed_epochs and val_ids is not None and bad >= patience:
            break
    if best is not None and not fixed_epochs:
        model.load_state_dict(best)
    return model.eval(), {"best_epoch": best_ep, "best_val_pr_auc": best_ap, "history": history}


def cross_validate(idx, train_ids, cfg, folds, epochs, seed, log=print):
    out = []
    gkf = GroupKFold(n_splits=folds)
    for k, (tr, ho) in enumerate(gkf.split(train_ids, groups=idx.users[train_ids])):
        a, b = train_ids[tr], train_ids[ho]
        if idx.y[b].sum() == 0:
            log(f"  fold {k}: no positives among held-out users, skipped"); continue
        log(f"  fold {k}: train {len(a)} windows / held-out users {len(set(idx.users[b]))} ({len(b)} windows)")
        m, _ = train_once(idx, a, None, cfg, epochs, 0, seed + k, log=lambda *_: None, fixed_epochs=True)
        z = _logits(m, idx, b)
        out.append({"fold": k, "roc_auc": float(roc_auc_score(idx.y[b], z)), "pr_auc": float(average_precision_score(idx.y[b], z)),
                    "positives": int(idx.y[b].sum())})
        log(f"           roc_auc {out[-1]['roc_auc']:.3f}  pr_auc {out[-1]['pr_auc']:.3f}")
    summ = {}
    for k in ("roc_auc", "pr_auc"):
        v = [f[k] for f in out]
        summ[k] = {"mean": float(np.mean(v)), "std": float(np.std(v))} if v else None
    return {"folds": out, "summary": summ, "epochs_per_fold": epochs}


def _git_commit():
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, timeout=5).stdout.strip() or None
    except Exception:  # noqa: BLE001
        return None


def run(model_dir: str, results_dir: str, synth: SynthConfig, cfg: TCNConfig, epochs: int, patience: int,
        cv_folds: int, seed: int, promote: bool, log=print) -> dict:
    torch.set_num_threads(max(1, os.cpu_count() or 1))
    t_start = time.time()
    log(f"generating synthetic data (seed {synth.seed}) ...")
    ds = generate(synth)
    idx, pre, recs = build(ds.events, ds.labels, ds.split_of, cfg.window_len)
    tr, va, te = idx.mask("train"), idx.mask("val"), idx.mask("test")
    data = {"source": "SYNTHETIC (accesssentinel.datasets.synth v2)", "fingerprint": ds.fingerprint(),
            "events": len(ds.events), "users": len(ds.users), "synth_config": asdict(synth),
            "splits": {s: {"windows": int(len(m)), "positives": int(idx.y[m].sum()),
                           "from": datetime.fromtimestamp(ds.split_bounds[s][0], tz=timezone.utc).date().isoformat(),
                           "to_exclusive": datetime.fromtimestamp(ds.split_bounds[s][1], tz=timezone.utc).date().isoformat()}
                       for s, m in (("train", tr), ("val", va), ("test", te))}}
    log(f"windows: train {len(tr)} ({data['splits']['train']['positives']} pos)  val {len(va)}  test {len(te)}")

    log("training TCN (early stopping on validation PR-AUC) ...")
    model, info = train_once(idx, tr, va, cfg, epochs, patience, seed, log)
    platt = fit_platt(_logits(model, idx, va), idx.y[va])
    log(f"best epoch {info['best_epoch']}  val PR-AUC {info['best_val_pr_auc']:.3f}  platt {platt}")

    cv = None
    if cv_folds and cv_folds > 1:
        log(f"{cv_folds}-fold user-grouped CV over the training period ({info['best_epoch'] + 1} epochs each) ...")
        cv = cross_validate(idx, tr, cfg, cv_folds, info["best_epoch"] + 1, seed, log)

    attackers = {u for u, p in ds.users.items() if p.get("attacker")}
    version = f"tcn-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{ds.fingerprint()[:6]}"
    scorer = registry.TcnScorer(model, platt, pre, version, {})
    s_tcn = platt_sigmoid(_logits(model, idx, te), *platt)
    s_rule = np.array([ev.rule_score(recs[i]) for i in te])
    lr = ev.fit_logreg(idx.last_step(tr), idx.y[tr], seed)
    s_lr = lr.predict_proba(idx.last_step(te))[:, 1]
    results = {
        "version": version, "data": data,
        "protocol": "time-split train/val/test; attacker users disjoint across splits; preprocessing fitted on train only",
        "tcn_config": asdict(cfg), "training": {**info, "epochs_max": epochs, "patience": patience, "seed": seed,
                                                 "platt": list(platt), "early_stopping_metric": "val PR-AUC"},
        "cross_validation": cv,
        "test": {"tcn": ev.report("tcn", idx, te, s_tcn, attackers),
                 "baseline_rule": ev.report("rule", idx, te, s_rule, attackers),
                 "baseline_logreg_last_event": ev.report("logreg", idx, te, s_lr, attackers)},
        "latency_cpu": ev.latency(scorer, idx.gather(te[:256])),
        "environment": {"python": platform.python_version(), "torch": torch.__version__, "machine": platform.machine(),
                        "cpu_count": os.cpu_count(), "git_commit": _git_commit()},
        "wall_seconds": round(time.time() - t_start, 1),
    }
    registry.save(model_dir, version, model, platt, pre, cfg,
                  {"data": data, "training": {k: v for k, v in results["training"].items() if k != "history"},
                   "test_summary": {k: v["event_level"] for k, v in results["test"].items()},
                   "git_commit": results["environment"]["git_commit"]})
    if promote:
        registry.promote(model_dir, version)
    os.makedirs(results_dir, exist_ok=True)
    with open(os.path.join(results_dir, f"{version}.json"), "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    with open(os.path.join(results_dir, f"{version}.md"), "w", encoding="utf-8") as f:
        f.write(markdown_report(results))
    log(f"saved {version} ({'promoted to CURRENT' if promote else 'not promoted'}) in {results['wall_seconds']}s")
    return results


def markdown_report(r: dict) -> str:
    def f(x):
        return "n/a" if x is None else f"{x:.3f}"
    lines = [f"# TCN evaluation — {r['version']}", "",
             f"**Data: {r['data']['source']}.** These numbers describe behaviour on synthetic data only, "
             "not on real access logs.", "",
             f"- Events {r['data']['events']:,}, users {r['data']['users']}, data fingerprint `{r['data']['fingerprint']}`",
             f"- Protocol: {r['protocol']}"]
    for s, v in r["data"]["splits"].items():
        lines.append(f"- {s}: {v['from']} to {v['to_exclusive']} (exclusive), {v['windows']:,} windows, {v['positives']} attack windows")
    tr = r["training"]
    lines += ["", f"Training: best epoch {tr['best_epoch']} (max {tr['epochs_max']}, patience {tr['patience']}), "
              f"validation PR-AUC {tr['best_val_pr_auc']:.3f}, seed {tr['seed']}, wall time {r['wall_seconds']} s.", ""]
    cv = r.get("cross_validation")
    if cv and cv["summary"]["roc_auc"]:
        lines += [f"5-fold user-grouped CV on the training period ({cv['epochs_per_fold']} epochs/fold): "
                  f"ROC-AUC {cv['summary']['roc_auc']['mean']:.3f} ± {cv['summary']['roc_auc']['std']:.3f}, "
                  f"PR-AUC {cv['summary']['pr_auc']['mean']:.3f} ± {cv['summary']['pr_auc']['std']:.3f}.", ""]
    lines += ["## Test period (unseen attackers, later dates)", "",
              "| Model | ROC-AUC | PR-AUC | P@0.6 | R@0.6 | F1@0.6 | FPR@0.6 | P@0.4 | R@0.4 | FPR@0.4 | attacks detected @0.6 | normal user-days flagged @0.6 |",
              "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for name, rep in r["test"].items():
        e, a, n = rep["event_level"], rep["attack_instances_at_0.6"], rep["normal_user_days_flagged_at_0.6"]
        lines.append(f"| {name} | {f(e['roc_auc'])} | {f(e['pr_auc'])} | {f(e['at_0.6']['precision'])} | {f(e['at_0.6']['recall'])} | "
                     f"{f(e['at_0.6']['f1'])} | {f(e['at_0.6']['fpr'])} | {f(e['at_0.4']['precision'])} | {f(e['at_0.4']['recall'])} | "
                     f"{f(e['at_0.4']['fpr'])} | {a['detected']}/{a['instances']} | {n['flagged']}/{n['normal_user_days']} ({n['rate']:.1%}) |")
    t = r["test"]["tcn"]
    cm = t["event_level"]["at_0.6"]
    lines += ["", f"TCN confusion matrix @0.6: TP {cm['tp']}, FP {cm['fp']}, FN {cm['fn']}, TN {cm['tn']}.", "",
              "### Per scenario @0.6 (attacks: recall; benign: share of events flagged)", "",
              "| Scenario | events | TCN | rule | logreg |", "|---|---|---|---|---|"]
    for sc, v in t["by_scenario_at_0.6"].items():
        k = "recall" if "recall" in v else "flag_rate"
        vals = [r["test"][m]["by_scenario_at_0.6"][sc][k] for m in r["test"]]
        lines.append(f"| {sc} ({k}) | {v['events']} | " + " | ".join(f"{x:.3f}" for x in vals) + " |")
    lines += ["", "### Attack instances (TCN @0.6)", "", "| Instance | events | detected | minutes to detect | attack events before detection |",
              "|---|---|---|---|---|"]
    for k, v in t["attack_instances_at_0.6"]["per_instance"].items():
        lines.append(f"| {k} | {v['events']} | {'yes' if v['detected'] else 'NO'} | {f(v['minutes_to_detect'])} | {v['events_before_detection'] if v['detected'] else '-'} |")
    lat = r["latency_cpu"]
    lines += ["", f"Inference latency (CPU, {lat['torch_threads']} threads): 1 window median {lat['batch_1_ms_median']:.2f} ms "
              f"(p95 {lat['batch_1_ms_p95']:.2f}); 64 windows median {lat['batch_64_ms_median']:.2f} ms (p95 {lat['batch_64_ms_p95']:.2f}).",
              "", f"Environment: {r['environment']}", ""]
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model-dir", default=os.environ.get("AS_MODEL_DIR", "models"))
    ap.add_argument("--results-dir", default="experiments/results")
    ap.add_argument("--epochs", type=int, default=25)
    ap.add_argument("--patience", type=int, default=5)
    ap.add_argument("--cv-folds", type=int, default=5)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--data-seed", type=int, default=7)
    ap.add_argument("--promote", action="store_true")
    args = ap.parse_args(argv)
    run(args.model_dir, args.results_dir, SynthConfig(seed=args.data_seed), TCNConfig(), args.epochs, args.patience,
        args.cv_folds, args.seed, args.promote)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

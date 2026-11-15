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


# ============================================================================================================
# Revised v3 TCN (Phase 3):  python -m accesssentinel.ml.train_tcn --arch v3 [--device cuda] [--promote]
# ============================================================================================================
"""
Protocol (Revised v3 Step 4-5):
  * data: SynthConfig.tcn_v3_study() (long val/test periods; attacks after the gap); timer context simulated
    (features.SimulatedThreatFeed / SimulatedDevPosture; v3 Limitation 4)
  * chronological split with gap g >= L events (Eq. 7, dataset.build_v3); encoder fitted on training only
  * stage 1, self-supervised pre-training: next resource + next operation + time gap (l4 = 0)
  * stage 2, fine-tuning on all four terms of Eq. 6, Focal Loss on L_sup only; early stopping on validation PR-AUC
  * thresholds theta_low / theta_high / theta_B = quantiles 1 - alpha of S_ind on held-out NOMINAL validation
    windows (Eq. 8); 0.4 / 0.6 are not used
  * optional rolling-origin validation (--rolling-origin K): K origins inside the training period, each fold
    trained on [t0, origin) and validated on the following days with the same gap rule
  * test period scored once; latency measured on CPU (the deployed path) and on the GPU if present
"""
from ..config import TCNv3Config  # noqa: E402
from ..tcn import TCNv3, multitask_loss  # noqa: E402
from .dataset import build_v3  # noqa: E402
from .features import SimulatedDevPosture, SimulatedThreatFeed  # noqa: E402


def _tensors(b: dict, device):
    t = {k: torch.from_numpy(np.ascontiguousarray(v)).to(device, non_blocking=True) for k, v in b.items()}
    t["next_mask"] = t["next_mask"].bool()
    return t


@torch.no_grad()
def scores_v3(model, idx, ids, device, batch: int = 1024) -> np.ndarray:
    model.eval()
    out = []
    for i in range(0, len(ids), batch):
        b = _tensors(idx.gather(ids[i:i + batch]), device)
        out.append(torch.sigmoid(model(b["res"], b["op"], b["net"], b["num"], b["z"])["logit"]).float().cpu().numpy())
    return np.concatenate(out) if out else np.zeros(0)


def train_v3(idx, enc, cfg: TCNv3Config, train_ids, val_ids, device, seed: int, pre_epochs: int, ft_epochs: int,
             patience: int = 3, log=print):
    torch.manual_seed(seed); rng = np.random.default_rng(seed)
    model = TCNv3(cfg, enc.n_res, enc.n_op, enc.n_net).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=cfg.lr)
    hist = []

    def epoch(supervised: bool):
        model.train()
        perm = rng.permutation(train_ids)
        tot, parts_sum, n = 0.0, {}, 0
        for i in range(0, len(perm), cfg.batch_size):
            b = _tensors(idx.gather(perm[i:i + cfg.batch_size]), device)
            out = model(b["res"], b["op"], b["net"], b["num"], b["z"])
            loss, parts = multitask_loss(out, b, cfg, supervised)
            opt.zero_grad(); loss.backward(); opt.step()
            tot += float(loss.detach()); n += 1
            for k, v in parts.items():
                parts_sum[k] = parts_sum.get(k, 0.0) + v
        return tot / max(n, 1), {k: v / max(n, 1) for k, v in parts_sum.items()}

    for ep in range(pre_epochs):
        t0 = time.time(); loss, parts = epoch(False)
        hist.append({"stage": "pretrain", "epoch": ep, "loss": loss, **parts, "seconds": time.time() - t0})
        log(f"  [pretrain] epoch {ep} loss {loss:.4f} {({k: round(v, 4) for k, v in parts.items()})} ({time.time() - t0:.0f}s)")
    best, best_ap, bad = copy.deepcopy(model.state_dict()), -1.0, 0
    for ep in range(ft_epochs):
        t0 = time.time(); loss, parts = epoch(True)
        s = scores_v3(model, idx, val_ids, device)
        yv = idx.y[val_ids]
        ap = float(average_precision_score(yv, s)) if yv.sum() else float("nan")
        hist.append({"stage": "finetune", "epoch": ep, "loss": loss, **parts, "val_pr_auc": ap, "seconds": time.time() - t0})
        log(f"  [finetune] epoch {ep} loss {loss:.4f} sup {parts.get('sup', 0):.4f} val PR-AUC {ap:.4f} ({time.time() - t0:.0f}s)")
        if ap > best_ap:
            best_ap, best, bad = ap, copy.deepcopy(model.state_dict()), 0
        else:
            bad += 1
            if bad >= patience:
                break
    model.load_state_dict(best)
    return model, hist, best_ap


def calibrate_v3(scores_nominal: np.ndarray, cfg: TCNv3Config) -> dict:
    q = lambda a: float(np.quantile(scores_nominal, 1.0 - a))
    th = {"theta_low": q(cfg.alpha_low), "theta_high": q(cfg.alpha_high), "theta_B": q(cfg.alpha_block)}
    th["theta_high"] = max(th["theta_high"], th["theta_low"]); th["theta_B"] = max(th["theta_B"], th["theta_high"])
    th.update({"alpha_low": cfg.alpha_low, "alpha_high": cfg.alpha_high, "alpha_block": cfg.alpha_block,
               "n_nominal": int(len(scores_nominal))})
    return th


def latency_v3(model, idx, ids, device_name: str, repeats: int = 100) -> dict:
    dev = torch.device(device_name)
    m = copy.deepcopy(model).to(dev).eval()
    out = {}
    for bsz in (1, 64):
        b = _tensors(idx.gather(ids[:bsz]), dev)
        with torch.no_grad():
            for _ in range(5):
                m(b["res"], b["op"], b["net"], b["num"], b["z"])
            ts = []
            for _ in range(repeats if bsz == 1 else max(20, repeats // 5)):
                if dev.type == "cuda":
                    torch.cuda.synchronize()
                t0 = time.perf_counter()
                m(b["res"], b["op"], b["net"], b["num"], b["z"])
                if dev.type == "cuda":
                    torch.cuda.synchronize()
                ts.append((time.perf_counter() - t0) * 1000)
        out[f"batch_{bsz}_ms_median"] = float(np.median(ts)); out[f"batch_{bsz}_ms_p95"] = float(np.percentile(ts, 95))
    out["device"] = device_name
    out["torch_threads"] = torch.get_num_threads()
    return out


def rolling_origin_v3(ds, threat, posture, cfg, k: int, device, seed: int, log=print, span_days: float = 12.0,
                      first_origin_day: float = 12.0) -> dict:
    """K origins inside the training period; fold j trains on [t0, o_j) and validates on [o_j, o_j + span)
    (evaluated windows fully inside the validation segment = the same gap rule). The segment must be longer than
    the gap of L - 1 events per user (~8 days here), otherwise nothing is evaluable (first run: 4-day segments
    left ~480 windows and no attacks). Folds without attack windows report PR-AUC None."""
    t0, t1 = ds.split_bounds["train"]
    span = span_days * 86400.0
    step = ((t1 - span) - (t0 + first_origin_day * 86400.0)) / max(1, k - 1)
    origins = [t0 + first_origin_day * 86400.0 + j * step for j in range(k)]
    folds = []
    for j, o in enumerate(origins):
        split_of = lambda t, o=o: "train" if t < o else ("val" if t < o + span else "ignore")
        idx, enc, _ = build_v3(ds.events, ds.labels, split_of, threat, posture, cfg.window_len)
        tr, va = idx.mask("train"), idx.mask("val")
        pos = int(idx.y[va].sum())
        model, _, ap = train_v3(idx, enc, cfg, tr, va, device, seed + j, 1, 3, patience=2, log=lambda *a: None)
        ap = ap if (pos > 0 and ap == ap and ap >= 0) else None
        folds.append({"origin_day": (o - t0) / 86400.0, "val_windows": int(len(va)), "val_positives": pos,
                      "val_pr_auc": ap})
        log(f"  [rolling-origin] fold {j}: origin day {(o - t0) / 86400:.0f}, {len(va)} windows ({pos} attack), "
            f"val PR-AUC {ap if ap is None else round(ap, 4)}")
    aps = [f["val_pr_auc"] for f in folds if f["val_pr_auc"] is not None]
    return {"folds": folds, "pr_auc_mean": float(np.mean(aps)) if aps else None,
            "pr_auc_std": float(np.std(aps)) if aps else None}


def run_v3(model_dir: str, results_dir: str, synth: SynthConfig, cfg: TCNv3Config, device: str, seed: int,
           promote: bool, rolling_origin: int = 0, log=print) -> dict:
    if cfg.finetune_epochs < 1:
        raise ValueError("refusing to train a v3 TCN without supervised fine-tuning: an untrained network must "
                         "never be saved or served")
    t_start = time.time()
    started = datetime.now(timezone.utc).isoformat()
    torch.manual_seed(seed)
    log(f"generating synthetic data ({synth.n_users} users, {synth.days} days, seed {synth.seed}) ...")
    ds = generate(synth)
    threat, posture = SimulatedThreatFeed(), SimulatedDevPosture.from_dataset(ds)
    idx, enc, _ = build_v3(ds.events, ds.labels, ds.split_of, threat, posture, cfg.window_len)
    tr, va, te = idx.mask("train"), idx.mask("val"), idx.mask("test")
    lost = {s: {"windows": int((idx.split == s).sum() - len(idx.mask(s))),
                "attack_events_in_gap": int(idx.y[(idx.split == s) & ~idx.evaluable].sum())} for s in ("val", "test")}
    log(f"windows: train {len(tr)} (+{int(idx.y[tr].sum())}), val {len(va)} (+{int(idx.y[va].sum())}), "
        f"test {len(te)} (+{int(idx.y[te].sum())}); gap removed {lost}")
    model, hist, best_ap = train_v3(idx, enc, cfg, tr, va, device, seed, cfg.pretrain_epochs, cfg.finetune_epochs, log=log)
    attackers = {u for u, p in ds.users.items() if p.get("attacker")}
    s_val = scores_v3(model, idx, va, device)
    nominal = s_val[(idx.y[va] == 0) & ~np.isin(idx.users[va], list(attackers))]
    th = calibrate_v3(nominal, cfg)
    log(f"calibrated on {len(nominal)} nominal validation windows: theta_low {th['theta_low']:.4f}, "
        f"theta_high {th['theta_high']:.4f}, theta_B {th['theta_B']:.4f}")
    s_te = scores_v3(model, idx, te, device)
    test = ev.report_at("tcn_v3", idx, te, s_te, attackers,
                        {k: th[k] for k in ("theta_low", "theta_high", "theta_B")})
    test["fixed_0.6_for_reference"] = ev.point_metrics(idx.y[te], s_te).get("at_0.6")
    lat = {"cpu": latency_v3(model, idx, te, "cpu")}
    if torch.cuda.is_available():
        lat["cuda"] = latency_v3(model, idx, te, "cuda")
    ro = rolling_origin_v3(ds, threat, posture, cfg, rolling_origin, device, seed, log) if rolling_origin else None
    version = f"tcn3-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{ds.fingerprint()[:6]}"
    manifest = {"parent_version": None, "tcn_config": cfg.to_dict(), "receptive_field": cfg.receptive_field(),
                "data": {"source": "synthetic", "synth_config": asdict(synth), "fingerprint": ds.fingerprint(),
                         "events": len(ds.events), "timer_context": "simulated (SimulatedThreatFeed, SimulatedDevPosture)"},
                "encoder": {"n_res": enc.n_res, "n_op": enc.n_op, "n_net": enc.n_net, "fingerprint": enc.fingerprint()},
                "split": {"rule": "Eq. 7 chronological, gap g = L - 1 events per user", "windows":
                          {"train": int(len(tr)), "val": int(len(va)), "test": int(len(te))}, "removed_by_gap": lost},
                "training": {"history": hist, "best_val_pr_auc": best_ap, "seed": seed, "device": device},
                "test_summary": {"pr_auc": test["event_level"]["pr_auc"], "roc_auc": test["event_level"]["roc_auc"],
                                 "at_theta_high": test["event_level"]["at_theta_high"]},
                "git_commit": _git_commit(), "torch": torch.__version__}
    registry.save_v3(model_dir, version, model.cpu(), enc, cfg, th, manifest)
    if promote:
        registry.promote(model_dir, version)
    r = {"version": version, "started_at": started, "finished_at": datetime.now(timezone.utc).isoformat(),
         "wall_seconds": time.time() - t_start, "platform": platform.platform(), "thresholds": th, "test": test,
         "latency": lat, "rolling_origin": ro, **{k: manifest[k] for k in ("tcn_config", "receptive_field", "data", "split", "training")},
         "promoted": promote}
    os.makedirs(results_dir, exist_ok=True)
    with open(os.path.join(results_dir, f"{version}.json"), "w", encoding="utf-8") as f:
        json.dump(r, f, indent=2, default=str)
    with open(os.path.join(results_dir, f"{version}.md"), "w", encoding="utf-8") as f:
        f.write(markdown_report_v3(r))
    log(f"saved {version} (promoted={promote}); report {results_dir}/{version}.md")
    return r


def markdown_report_v3(r: dict) -> str:
    t, ev_ = r["test"], r["test"]["event_level"]
    L = [f"# Revised v3 TCN {r['version']}", "",
         f"Synthetic data only (fingerprint {r['data']['fingerprint']}, {r['data']['events']} events). Timer context "
         "ThreatFeed / DevPosture is simulated. Started {0}, finished {1} ({2:.0f} s).".format(
             r["started_at"], r["finished_at"], r["wall_seconds"]), "",
         f"Architecture: L = {r['tcn_config']['window_len']}, d = 43, dilations {r['tcn_config']['dilations']}, "
         f"filters {r['tcn_config']['filters']}, two convolutions per block, receptive field {r['receptive_field']}.", "",
         f"Split (Eq. 7, gap = L - 1 events per user): windows {r['split']['windows']}; removed by the gap {r['split']['removed_by_gap']}.", "",
         "## Thresholds (Eq. 8, quantiles of S_ind on nominal validation windows)", "",
         "| threshold | alpha | value |", "|---|---|---|",
         f"| theta_low | {r['thresholds']['alpha_low']} | {r['thresholds']['theta_low']:.4f} |",
         f"| theta_high | {r['thresholds']['alpha_high']} | {r['thresholds']['theta_high']:.4f} |",
         f"| theta_B | {r['thresholds']['alpha_block']} | {r['thresholds']['theta_B']:.4f} |", "",
         f"## Test period ({ev_['n']} windows, {ev_['positives']} attack events)", "",
         f"ROC-AUC {ev_['roc_auc']:.4f}, PR-AUC {ev_['pr_auc']:.4f}", "",
         "| at | threshold | precision | recall | F1 | FPR | TP | FP | FN |", "|---|---|---|---|---|---|---|---|---|"]
    for k in ("theta_low", "theta_high", "theta_B"):
        m = ev_[f"at_{k}"]
        L.append(f"| {k} | {m['threshold']:.4f} | {m['precision']:.3f} | {m['recall']:.3f} | {m['f1']:.3f} | {m['fpr']:.4f} | {m['tp']} | {m['fp']} | {m['fn']} |")
    inst = t["attack_instances_at_theta_high"]
    L += ["", f"Attack instances detected at theta_high: {inst['detected']}/{inst['instances']} "
              f"(median minutes to detect {inst['median_minutes_to_detect']}).",
          f"Normal user-days flagged at theta_high: {t['normal_user_days_flagged_at_theta_high']['rate']:.3%}.", "",
          "| scenario | events | recall / flag rate |", "|---|---|---|"]
    for sc, v in t["by_scenario_at_theta_high"].items():
        L.append(f"| {sc} | {v['events']} | {v.get('recall', v.get('flag_rate')):.3f} |")
    L += ["", "## Inference latency (one window = L x d)", "", "| device | batch 1 median ms | batch 1 p95 | batch 64 median ms |", "|---|---|---|---|"]
    for k, v in r["latency"].items():
        L.append(f"| {k} | {v['batch_1_ms_median']:.2f} | {v['batch_1_ms_p95']:.2f} | {v['batch_64_ms_median']:.2f} |")
    if r.get("rolling_origin"):
        ro = r["rolling_origin"]
        head = (f"PR-AUC {ro['pr_auc_mean']:.4f} +- {ro['pr_auc_std']:.4f}" if ro["pr_auc_mean"] is not None else "no fold had attack windows")
        L += ["", f"## Rolling-origin validation: {head}", ""]
        for f in ro["folds"]:
            v = "n/a (no attack windows)" if f["val_pr_auc"] is None else f"{f['val_pr_auc']:.4f}"
            L.append(f"- origin day {f['origin_day']:.0f}: {f['val_windows']} windows ({f['val_positives']} attack), PR-AUC {v}")
    L += ["", "## Training", ""]
    for h in r["training"]["history"]:
        L.append(f"- {h['stage']} {h['epoch']}: loss {h['loss']:.4f}" + (f", val PR-AUC {h['val_pr_auc']:.4f}" if "val_pr_auc" in h else ""))
    return "\n".join(L) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--arch", choices=["d3", "v3"], default="d3", help="d3 = Deliverable 3 baseline; v3 = Revised v3")
    ap.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
    ap.add_argument("--pretrain-epochs", type=int, default=None)
    ap.add_argument("--finetune-epochs", type=int, default=None)
    ap.add_argument("--rolling-origin", type=int, default=0)
    ap.add_argument("--model-dir", default=os.environ.get("AS_MODEL_DIR", "models"))
    ap.add_argument("--results-dir", default="experiments/results")
    ap.add_argument("--epochs", type=int, default=25)
    ap.add_argument("--patience", type=int, default=5)
    ap.add_argument("--cv-folds", type=int, default=5)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--data-seed", type=int, default=7)
    ap.add_argument("--promote", action="store_true")
    args = ap.parse_args(argv)
    if args.arch == "v3":
        cfg = TCNv3Config()
        if args.pretrain_epochs is not None:
            cfg.pretrain_epochs = args.pretrain_epochs
        if args.finetune_epochs is not None:
            cfg.finetune_epochs = args.finetune_epochs
        run_v3(args.model_dir, args.results_dir, SynthConfig.tcn_v3_study(seed=23 if args.data_seed == 7 else args.data_seed),
               cfg, args.device, args.seed, args.promote, args.rolling_origin)
        return 0
    run(args.model_dir, args.results_dir, SynthConfig(seed=args.data_seed), TCNConfig(), args.epochs, args.patience,
        args.cv_folds, args.seed, args.promote)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

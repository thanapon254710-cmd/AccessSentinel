"""Evaluation metrics and baselines for individual-risk models (D3 Step 4/5 thresholds 0.4 / 0.6).

Reported per model: ROC-AUC, PR-AUC; at each threshold precision, recall, F1, false-positive rate and the
confusion matrix; recall per attack scenario and false-positive rate per benign hard-negative scenario;
attack-instance detection rate and time to detection; false-alarm rate over normal user-days.

Baselines (so the TCN is compared with something simpler, not reported in isolation):
  rule      hand-written, untrained score over the newest event's raw features
  logreg    logistic regression on the newest event only (no sequence)
"""
import math
import time
from collections import defaultdict
from typing import Dict

import numpy as np
from sklearn.metrics import average_precision_score, confusion_matrix, roc_auc_score

THRESHOLDS = (0.4, 0.6)


def point_metrics(y, s) -> dict:
    y, s = np.asarray(y).astype(int), np.asarray(s, dtype=float)
    out = {"n": int(len(y)), "positives": int(y.sum()),
           "roc_auc": float(roc_auc_score(y, s)) if 0 < y.sum() < len(y) else None,
           "pr_auc": float(average_precision_score(y, s)) if y.sum() > 0 else None}
    for th in THRESHOLDS:
        p = (s >= th).astype(int)
        tn, fp, fn, tp = confusion_matrix(y, p, labels=[0, 1]).ravel()
        prec = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / (tp + fn) if tp + fn else 0.0
        out[f"at_{th}"] = {"tp": int(tp), "fp": int(fp), "tn": int(tn), "fn": int(fn), "precision": prec,
                           "recall": rec, "f1": 2 * prec * rec / (prec + rec) if prec + rec else 0.0,
                           "fpr": fp / (fp + tn) if fp + tn else 0.0}
    return out


def scenario_breakdown(scenario, y, s, th: float = 0.6) -> dict:
    out = {}
    for sc in sorted(set(scenario)):
        m = scenario == sc
        hits = int((s[m] >= th).sum())
        key = "recall" if y[m].max(initial=0) == 1 else "flag_rate"
        out[sc] = {"events": int(m.sum()), key: hits / int(m.sum())}
    return out


def instance_detection(instance, t, s, th: float = 0.6) -> dict:
    per = {}
    for inst in sorted(set(instance) - {""}):
        m = np.where(instance == inst)[0]
        order = m[np.argsort(t[m])]
        hit = np.where(s[order] >= th)[0]
        per[inst] = {"events": int(len(m)), "detected": bool(len(hit)),
                     "minutes_to_detect": float((t[order[hit[0]]] - t[order[0]]) / 60.0) if len(hit) else None,
                     "events_before_detection": int(hit[0]) if len(hit) else None}
    det = [v for v in per.values() if v["detected"]]
    return {"instances": len(per), "detected": len(det),
            "median_minutes_to_detect": float(np.median([v["minutes_to_detect"] for v in det])) if det else None,
            "per_instance": per}


def user_day_false_alarms(users, t, s, attackers, th: float = 0.6) -> dict:
    flagged, days = defaultdict(bool), set()
    for u, tt, ss in zip(users, t, s):
        if u in attackers:
            continue
        k = (u, int(tt // 86400))
        days.add(k)
        flagged[k] |= bool(ss >= th)
    n = sum(flagged.values())
    return {"normal_user_days": len(days), "flagged": int(n), "rate": n / len(days) if days else 0.0}


def report(name: str, idx, mask, scores, attackers) -> dict:
    y, sc, inst, t, users = idx.y[mask], idx.scenario[mask], idx.instance[mask], idx.t[mask], idx.users[mask]
    individual_inst = np.where(np.isin(sc, ["compromised_account", "insider_exfiltration", "bac_probe", "low_and_slow"]), inst, "")
    return {"model": name, "event_level": point_metrics(y, scores),
            "by_scenario_at_0.6": scenario_breakdown(sc, y, scores, 0.6),
            "attack_instances_at_0.6": instance_detection(individual_inst, t, scores, 0.6),
            "normal_user_days_flagged_at_0.6": user_day_false_alarms(users, t, scores, attackers, 0.6)}


def report_at(name: str, idx, mask, scores, attackers, thresholds: Dict[str, float]) -> dict:
    """Like report(), at CALIBRATED thresholds (Revised v3 Eq. 8) instead of the fixed D3 0.4 / 0.6."""
    y, sc, inst, t, users = idx.y[mask], idx.scenario[mask], idx.instance[mask], idx.t[mask], idx.users[mask]
    from ..datasets.synth import INDIVIDUAL_V3                 # incl. the Phase 8 scenarios
    individual_inst = np.where(np.isin(sc, list(INDIVIDUAL_V3)), inst, "")
    out = {"model": name, "thresholds": thresholds,
           "event_level": {"n": int(len(y)), "positives": int(y.sum()),
                           "roc_auc": float(roc_auc_score(y, scores)) if 0 < y.sum() < len(y) else None,
                           "pr_auc": float(average_precision_score(y, scores)) if y.sum() > 0 else None}}
    for k, th in thresholds.items():
        p = (np.asarray(scores) >= th).astype(int)
        tn, fp, fn, tp = confusion_matrix(y, p, labels=[0, 1]).ravel()
        prec = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / (tp + fn) if tp + fn else 0.0
        out["event_level"][f"at_{k}"] = {"threshold": th, "tp": int(tp), "fp": int(fp), "tn": int(tn), "fn": int(fn),
                                         "precision": prec, "recall": rec,
                                         "f1": 2 * prec * rec / (prec + rec) if prec + rec else 0.0,
                                         "fpr": fp / (fp + tn) if fp + tn else 0.0}
    th = thresholds["theta_high"]
    out["by_scenario_at_theta_high"] = scenario_breakdown(sc, y, scores, th)
    out["attack_instances_at_theta_high"] = instance_detection(individual_inst, t, scores, th)
    out["normal_user_days_flagged_at_theta_high"] = user_day_false_alarms(users, t, scores, attackers, th)
    return out


# ------------------------------------------------------------------------------------------ baselines
def rule_score(rec: dict) -> float:
    """Hand-written, untrained heuristic over the newest event (no learning, no sequence)."""
    z = (-4.0 + 2.0 * rec["new_net"] + 2.0 * rec["new_device"] + 1.5 * (1 - rec["y"]) + 2.0 * rec["unmapped"]
         + 1.5 * rec["seq_obj"] + 1.0 * (rec["denies1h"] > math.log1p(3)) + 1.0 * (rec["rate_ratio"] > math.log(3))
         + 1.0 * (rec["hour_dev"] > 0.4) + 1.0 * (rec["exports24h"] > math.log1p(10)))
    return 1.0 / (1.0 + math.exp(-z))


def fit_logreg(X_last: np.ndarray, y: np.ndarray, seed: int = 0):
    from sklearn.linear_model import LogisticRegression
    return LogisticRegression(class_weight="balanced", max_iter=2000, random_state=seed).fit(X_last, y)


# ------------------------------------------------------------------------------------------ latency
def latency(scorer, X: np.ndarray, repeats: int = 200) -> Dict[str, float]:
    import torch
    out = {}
    for b in (1, 64):
        xs = X[:b]
        scorer.score_X(xs)                                           # warm-up
        ts = []
        for _ in range(repeats if b == 1 else max(20, repeats // 10)):
            t0 = time.perf_counter(); scorer.score_X(xs); ts.append((time.perf_counter() - t0) * 1000)
        out[f"batch_{b}_ms_median"] = float(np.median(ts))
        out[f"batch_{b}_ms_p95"] = float(np.percentile(ts, 95))
    out["torch_threads"] = torch.get_num_threads()
    return out

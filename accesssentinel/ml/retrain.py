"""Retraining from analyst-verified feedback (D3 Step 9), with versioning, validation gate and promotion.

    python -m accesssentinel.ml.retrain dataset                 # what verified labels are available (HDFS)
    python -m accesssentinel.ml.retrain tcn [--promote-if-better]
    python -m accesssentinel.ml.retrain tgn [--promote-if-better]

Pipeline: load the CURRENT model -> verified feedback from HDFS (ml/feedback_data.py; analyst verdicts only) ->
split feedback into fine-tune / holdout by feedback record -> fine-tune a copy on the base training data plus
the feedback examples -> evaluate candidate and current model on (a) the fixed synthetic test set and (b) the
held-out feedback -> save the candidate as a new immutable version (never overwrites) -> promote only if
--promote-if-better and the gate passes:
  * base test PR-AUC not lower than the current model's by more than 0.01, and
  * on held-out feedback (if any): recall on confirmed-malicious not lower, flag rate on false positives not higher.
Results: experiments/results/retrain-<kind>-<version>.json/.md. Feedback ids used are recorded in the manifest.
"""
import argparse
import copy
import json
import os
import time
from datetime import datetime, timezone

import numpy as np
import torch
import torch.nn.functional as F
from sklearn.metrics import average_precision_score

from ..tcn import fit_platt, focal_loss, platt_sigmoid
from . import feedback_data as fb

GATE_TOLERANCE = 0.01


def _feedback_metrics(scores, y, th):
    y, s = np.asarray(y), np.asarray(scores)
    pos, neg = y == 1, y == 0
    return {"n": int(len(y)), "positives": int(pos.sum()), "negatives": int(neg.sum()),
            "recall_on_confirmed": float((s[pos] >= th).mean()) if pos.any() else None,
            "flag_rate_on_false_positives": float((s[neg] >= th).mean()) if neg.any() else None}


def _gate(cur, cand):
    reasons = []
    ok = cand["base_test_pr_auc"] >= cur["base_test_pr_auc"] - GATE_TOLERANCE
    reasons.append(f"base test PR-AUC {cand['base_test_pr_auc']:.3f} vs current {cur['base_test_pr_auc']:.3f} (tolerance {GATE_TOLERANCE})")
    hc, hn = cur.get("feedback_holdout") or {}, cand.get("feedback_holdout") or {}
    if hn.get("n"):
        if hn.get("recall_on_confirmed") is not None and hc.get("recall_on_confirmed") is not None:
            ok &= hn["recall_on_confirmed"] >= hc["recall_on_confirmed"]
            reasons.append(f"holdout recall {hn['recall_on_confirmed']:.3f} vs {hc['recall_on_confirmed']:.3f}")
        if hn.get("flag_rate_on_false_positives") is not None and hc.get("flag_rate_on_false_positives") is not None:
            ok &= hn["flag_rate_on_false_positives"] <= hc["flag_rate_on_false_positives"]
            reasons.append(f"holdout FP flag rate {hn['flag_rate_on_false_positives']:.3f} vs {hc['flag_rate_on_false_positives']:.3f}")
    else:
        reasons.append("no feedback holdout (fewer than 4 verified records): gate uses the base test set only")
    return bool(ok), reasons


# ---------------------------------------------------------------------------------------------- TCN
class NoFeedbackData(RuntimeError):
    """The verified feedback labels no training examples (e.g. no history of the users inside the feedback windows).
    No candidate is produced: a fine-tune on base data alone would be presented as learned from feedback."""


def retrain_tcn(model_dir, results_dir, lake, epochs=4, lr=1e-4, base_sample=30000, seed=0, promote_if_better=False, log=print):
    from ..datasets.synth import SynthConfig, generate
    from .dataset import build
    from .registry import TcnScorer, promote, save
    torch.manual_seed(seed); rng = np.random.default_rng(seed)
    cur = TcnScorer.load(model_dir)
    rows = fb.verified_feedback(lake)
    tune_rows, hold_rows = fb.holdout_split([r for r in rows if r["scope"] == "individual"], seed=seed)
    Xf, yf, fids = fb.tcn_examples(tune_rows, lake, cur.pre)
    Xh, yh, _ = fb.tcn_examples(hold_rows, lake, cur.pre)
    log(f"verified individual feedback: {len(tune_rows)} for fine-tuning ({len(yf)} windows), {len(hold_rows)} held out ({len(yh)} windows)")
    if not len(yf):
        raise NoFeedbackData("verified individual feedback labels no access windows in HDFS; no candidate trained")
    seed_data = (cur.manifest.get("data") or {}).get("synth_config", {}).get("seed", 7)
    ds = generate(SynthConfig(seed=seed_data))
    idx, _, _ = build(ds.events, ds.labels, ds.split_of, pre=cur.pre)
    tr, va, te = idx.mask("train"), idx.mask("val"), idx.mask("test")
    pos = tr[idx.y[tr] == 1]
    sub = np.concatenate([pos, rng.choice(tr[idx.y[tr] == 0], size=min(base_sample, int((idx.y[tr] == 0).sum())), replace=False)])
    model = copy.deepcopy(cur.model)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    reps = max(1, 10 if len(yf) else 0)
    best, best_ap = copy.deepcopy(model.state_dict()), -1.0

    def logits(m, X):
        m.eval(); out = []
        with torch.no_grad():
            for i in range(0, len(X), 4096):
                out.append(m(torch.from_numpy(np.ascontiguousarray(X[i:i + 4096]))).numpy())
        return np.concatenate(out) if out else np.zeros(0)
    for ep in range(epochs):
        model.train()
        perm = rng.permutation(len(sub))
        fb_order = np.tile(np.arange(len(yf)), reps) if len(yf) else np.zeros(0, int)
        steps = max(1, len(perm) // 512)
        fb_chunks = np.array_split(rng.permutation(fb_order), steps) if len(fb_order) else [np.zeros(0, int)] * steps
        for k, b in enumerate(np.array_split(perm, steps)):
            Xb = idx.gather(sub[b]); yb = idx.y[sub[b]].astype(np.float32)
            if len(fb_chunks[k]):
                Xb = np.concatenate([Xb, Xf[fb_chunks[k]]]); yb = np.concatenate([yb, yf[fb_chunks[k]].astype(np.float32)])
            loss = focal_loss(model(torch.from_numpy(Xb)), torch.from_numpy(yb))
            opt.zero_grad(); loss.backward(); opt.step()
        ap = float(average_precision_score(idx.y[va], logits(model, idx.gather(va))))
        log(f"  [fine-tune] epoch {ep} validation PR-AUC {ap:.3f}")
        if ap > best_ap:
            best_ap, best = ap, copy.deepcopy(model.state_dict())
    model.load_state_dict(best)
    platt = fit_platt(logits(model, idx.gather(va)), idx.y[va])
    th = 0.6

    def evaluate(m, pl):
        st = platt_sigmoid(logits(m, idx.gather(te)), *pl)
        out = {"base_test_pr_auc": float(average_precision_score(idx.y[te], st)),
               "base_test_recall_at_0.6": float((st[idx.y[te] == 1] >= th).mean()),
               "base_test_fpr_at_0.6": float((st[idx.y[te] == 0] >= th).mean())}
        if len(yh):
            out["feedback_holdout"] = _feedback_metrics(platt_sigmoid(logits(m, Xh), *pl), yh, th)
        if len(yf):
            out["feedback_finetune_in_sample"] = _feedback_metrics(platt_sigmoid(logits(m, Xf), *pl), yf, th)
        return out
    cur_eval, cand_eval = evaluate(cur.model, cur.platt), evaluate(model, platt)
    ok, reasons = _gate(cur_eval, cand_eval)
    version = f"tcn-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-ft"
    manifest = {"parent_version": cur.version, "data": cur.manifest.get("data"), "retraining": {
        "feedback_ids_finetune": sorted(set(fids)), "feedback_ids_holdout": [r["id"] for r in hold_rows],
        "feedback_windows": int(len(yf)), "epochs": epochs, "lr": lr, "base_sample": int(len(sub)), "seed": seed},
        "evaluation": {"current": cur_eval, "candidate": cand_eval}, "gate": {"passed": ok, "reasons": reasons}}
    from ..config import TCNConfig
    save(model_dir, version, model, platt, cur.pre, TCNConfig(), manifest)
    promoted = bool(promote_if_better and ok)
    if promoted:
        promote(model_dir, version)
    return _report(results_dir, "tcn", version, cur.version, manifest, promoted, rows, log)


# ---------------------------------------------------------------------------------------------- TGN
def retrain_tgn(model_dir, results_dir, lake, epochs=5, lr=1e-4, seed=0, promote_if_better=False, log=print):
    import random as pyrandom
    from ..datasets.synth import SynthConfig, generate
    from .tgn_registry import TgnScorer, promote, save
    from .tgn_train import Corpus, groups_for, _logits
    torch.manual_seed(seed)
    cur = TgnScorer.load(model_dir)
    rows = fb.verified_feedback(lake)
    tune_rows, hold_rows = fb.holdout_split([r for r in rows if r["scope"] == "collective"], seed=seed)
    fitems, hitems = fb.tgn_items(tune_rows, lake, cur.gp), fb.tgn_items(hold_rows, lake, cur.gp)
    log(f"verified collective feedback: {len(tune_rows)} for fine-tuning ({len(fitems)} graphs), {len(hold_rows)} held out ({len(hitems)} graphs)")
    if not fitems:
        raise NoFeedbackData("verified collective feedback yields no graph samples in HDFS; no candidate trained")
    corpus = Corpus(generate(SynthConfig.collusion_study()), cur.gp)
    base = corpus.evaluation_set("train", pyrandom.Random(seed + 1), anchors_per_burst=2, n_random=150, n_resource=150)
    test = corpus.evaluation_set("test", pyrandom.Random(seed + 3), anchors_per_burst=2, n_random=300, n_resource=300)
    model = copy.deepcopy(cur.model)
    opt = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-6)
    train = base + fitems * 5
    rng = np.random.default_rng(seed)
    pos = sum(float(it.y.sum()) for it in train); neg = sum(float((1 - it.y).sum()) for it in train)
    pw = torch.tensor(min(neg / max(pos, 1.0), 50.0))
    for ep in range(epochs):
        model.train()
        for j in rng.permutation(len(train)):
            it = train[j]
            loss = F.binary_cross_entropy_with_logits(_logits(model, it), it.y, pos_weight=pw)
            opt.zero_grad(); loss.backward(); opt.step()
        log(f"  [fine-tune] epoch {ep}")

    def seed_scores(m, items):
        m.eval(); out = []
        with torch.no_grad():
            for it in items:
                sc = torch.sigmoid(_logits(m, it)).numpy()
                out.append(groups_for(it.sample, sc, cur.tau_edge)[it.sample.seeds[0]]["s_coll"] or 0.0)
        return np.array(out)

    def evaluate(m):
        y = np.array([it.seed_label for it in test]); s = seed_scores(m, test)
        out = {"base_test_pr_auc": float(average_precision_score(y, s)),
               "base_test_recall_at_theta": float((s[y == 1] >= cur.theta_coll).mean()),
               "base_test_fpr_at_theta": float((s[y == 0] >= cur.theta_coll).mean())}
        if hitems:
            out["feedback_holdout"] = _feedback_metrics(seed_scores(m, hitems), [it.seed_label for it in hitems], cur.theta_coll)
        if fitems:
            out["feedback_finetune_in_sample"] = _feedback_metrics(seed_scores(m, fitems), [it.seed_label for it in fitems], cur.theta_coll)
        return out
    cur_eval, cand_eval = evaluate(cur.model), evaluate(model)
    ok, reasons = _gate(cur_eval, cand_eval)
    version = f"tgn-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-ft"
    from .tgn_train import tgn_config
    manifest = {"parent_version": cur.version, "retraining": {
        "feedback_ids_finetune": [r["id"] for r in tune_rows], "feedback_ids_holdout": [r["id"] for r in hold_rows],
        "feedback_graphs": len(fitems), "epochs": epochs, "lr": lr, "seed": seed},
        "evaluation": {"current": cur_eval, "candidate": cand_eval}, "gate": {"passed": ok, "reasons": reasons}}
    save(model_dir, version, model, tgn_config(cur.gp), cur.gp, cur.tau_edge, cur.theta_coll, manifest)
    promoted = bool(promote_if_better and ok)
    if promoted:
        promote(model_dir, version)
    return _report(results_dir, "tgn", version, cur.version, manifest, promoted, rows, log)


def _report(results_dir, kind, version, parent, manifest, promoted, rows, log):
    os.makedirs(results_dir, exist_ok=True)
    out = {"kind": kind, "version": version, "parent_version": parent, "promoted": promoted, **manifest,
           "feedback_available": fb.summary(rows)}
    base = os.path.join(results_dir, f"retrain-{kind}-{version}")
    with open(base + ".json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=str)
    ev = manifest["evaluation"]
    L = [f"# Retraining from analyst feedback — {kind} {version}", "",
         f"Parent model {parent}. Labels: analyst-verified feedback only. Base data: SYNTHETIC.", "",
         f"Feedback available: {out['feedback_available']['by_scope_verdict']}", "",
         "| Metric | current | candidate |", "|---|---|---|"]
    for k in sorted(set(ev["current"]) | set(ev["candidate"])):
        a, b = ev["current"].get(k), ev["candidate"].get(k)
        L.append(f"| {k} | {json.dumps(a)} | {json.dumps(b)} |")
    L += ["", f"Gate {'PASSED' if manifest['gate']['passed'] else 'FAILED'}: " + "; ".join(manifest["gate"]["reasons"]),
          f"Promoted: {promoted}.", ""]
    with open(base + ".md", "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    log(f"saved candidate {version} (promoted={promoted}); report {base}.md")
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("what", choices=["dataset", "tcn", "tgn"])
    ap.add_argument("--model-dir", default=os.environ.get("AS_MODEL_DIR", "models"))
    ap.add_argument("--results-dir", default="experiments/results")
    ap.add_argument("--epochs", type=int, default=None)
    ap.add_argument("--promote-if-better", action="store_true")
    args = ap.parse_args(argv)
    from ..adapters.hdfs_lake import make_lake
    from ..settings import get_settings
    lake = make_lake(get_settings())
    if args.what == "dataset":
        rows = fb.verified_feedback(lake)
        print(json.dumps({"source": "HDFS analyst-feedback (analyst-verified verdicts only)", **fb.summary(rows),
                          "records": [{k: r.get(k) for k in ("id", "scope", "verdict", "users", "incident_id", "analyst",
                                                              "window_t0", "window_t1", "source")} for r in rows]}, indent=2))
        return 0
    fn = retrain_tcn if args.what == "tcn" else retrain_tgn
    kw = {"epochs": args.epochs} if args.epochs else {}
    try:
        fn(args.model_dir, args.results_dir, lake, promote_if_better=args.promote_if_better, **kw)
    except NoFeedbackData as e:
        print(f"not retrained: {e}")
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

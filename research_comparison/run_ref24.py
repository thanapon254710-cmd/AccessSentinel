from __future__ import annotations
import argparse, json, os, platform, time
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import (roc_auc_score, average_precision_score, precision_score,
                             recall_score, f1_score, confusion_matrix)
from accesssentinel.datasets.synth import SynthConfig, generate
from accesssentinel.ml.dataset import build_v3
from accesssentinel.ml.features import SimulatedDevPosture, SimulatedThreatFeed
from research_comparison.methods.ref24_user_resource_embedding import UserResourceSpectralNN, Ref24Config

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(__file__).resolve().parent / "results"

def metrics(y, s, threshold):
    ok = np.isfinite(s); y = np.asarray(y)[ok]; s = np.asarray(s)[ok]
    if len(np.unique(y)) < 2: raise ValueError("Both classes are required for ROC/PR-AUC")
    pred = (s >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0,1]).ravel()
    return {"n_scored":int(len(y)),"n_unscored":int((~ok).sum()),
            "roc_auc":float(roc_auc_score(y,s)),"pr_auc":float(average_precision_score(y,s)),
            "precision":float(precision_score(y,pred,zero_division=0)),
            "recall":float(recall_score(y,pred,zero_division=0)),"f1":float(f1_score(y,pred,zero_division=0)),
            "fpr":float(fp/max(1,fp+tn)),"threshold":float(threshold)}

def run(seed=0, dim=8, quick=False):
    data_seed = 23 + seed
    cfg = SynthConfig.tcn_v3_eval(seed=data_seed)
    if quick:
        from dataclasses import replace
        cfg = replace(cfg, n_users=220, days=40, split_days=(22,8,10), attack_margin_days=(0,6,6),
                      attacks={k:(1,1,1) for k in cfg.attacks})
    ds = generate(cfg)
    threat, posture = SimulatedThreatFeed(), SimulatedDevPosture.from_dataset(ds)
    idx, _, _ = build_v3(ds.events, ds.labels, ds.split_of, threat, posture)
    tr, va, te = idx.mask("train"), idx.mask("val"), idx.mask("test")
    # Fit spectral representation using training-period interactions only.
    events_sorted = sorted(ds.events, key=lambda e:(e.t,e.event_id))
    train_events = [e for e in events_sorted if ds.split_of(e.t)=="train"]
    model = UserResourceSpectralNN(Ref24Config(embedding_dim=dim, distance="cosine"))
    t0=time.perf_counter(); model.fit(train_events); fit_ms=(time.perf_counter()-t0)*1000
    # Process validation then test chronologically; history is updated only after scoring each event.
    val_mask = {str(idx.event_ids[i]) for i in va}
    test_mask = {str(idx.event_ids[i]) for i in te}
    val_scores={}; test_scores={}; score_times=[]
    for e in events_sorted:
        eid=str(e.event_id)
        if eid in val_mask or eid in test_mask:
            t0=time.perf_counter(); score=model.score_and_update(e); score_times.append((time.perf_counter()-t0)*1000)
            (val_scores if eid in val_mask else test_scores)[eid]=score
        elif ds.split_of(e.t)=="train":
            # Train interactions seed the resource-specific prior-user history.
            model.prior_users_by_resource.setdefault(f"{e.tenant}/{e.a}:{e.r}",set()).add(str(e.u))
    yv=idx.y[va]; yt=idx.y[te]
    sv=np.array([val_scores[str(idx.event_ids[i])] for i in va],dtype=float)
    st=np.array([test_scores[str(idx.event_ids[i])] for i in te],dtype=float)
    # Match AccessSentinel's no-test-tuning rule: threshold from nominal validation scores only.
    nominal_val=sv[(yv==0)&np.isfinite(sv)]
    if len(nominal_val)==0: raise RuntimeError("No nominal validation scores available for calibration")
    threshold=float(np.quantile(nominal_val,0.95))
    ref_metrics=metrics(yt,st,threshold)
    # Existing AccessSentinel Exp.A score file is for the exact seed/data preset.
    as_file=ROOT/"evaluation"/"results"/"raw"/"exp_a"/f"seed{seed}-scores.npz"
    as_metrics=None; alignment={"available":False}
    if as_file.exists():
        with np.load(as_file,allow_pickle=False) as z:
            same=(len(z["y"])==len(yt) and np.array_equal(z["y"],yt))
            alignment={"available":True,"same_test_labels_and_order":bool(same),"file":str(as_file.relative_to(ROOT))}
            if same:
                a=z["s_tcn-v3"].astype(float)
                # Existing AccessSentinel evaluation calibrates at its own validation threshold;
                # report threshold-free metrics here to avoid reconstructing unavailable val scores.
                valid=np.isfinite(a)
                as_metrics={"n_scored":int(valid.sum()),"roc_auc":float(roc_auc_score(yt[valid],a[valid])),
                            "pr_auc":float(average_precision_score(yt[valid],a[valid])),
                            "comparison_note":"Threshold-free metrics from existing Exp.A seed file."}
    result={"reference":"[24] A Graph Embedding Approach to User Behavior Anomaly Detection",
            "method":"regularized bipartite spectral embedding + nearest previous user on resource",
            "protocol":"AccessSentinel synthetic tcn_v3_eval; chronological split; train-only graph fit; threshold calibrated on nominal validation scores",
            "seed":seed,"data_seed":data_seed,"dataset_fingerprint":ds.fingerprint(),
            "config":{"embedding_dim":dim,"distance":"cosine","regularization":"average bipartite degree"},
            "adaptations":["AccessSentinel user/application/resource event schema replaces paper's generic user-resource records.",
                           "Embedding is fitted on the training-period bipartite graph; paper permits periodic recomputation.",
                           "Embedding dimension is fixed by CLI (paper recommends scree-plot elbow).",
                           "Cosine distance is selected from the paper's permitted distance examples.",
                           "Cold-start user/resource events are left unscored because the paper does not define an out-of-sample rule."],
            "reference24_test":ref_metrics,"accesssentinel_tcn_v3_test":as_metrics,"alignment":alignment,
            "timing":{"fit_ms":fit_ms,"score_event_ms_median":float(np.median(score_times)),
                      "score_event_ms_p95":float(np.percentile(score_times,95)),"score_event_count":len(score_times)},
            "environment":{"python":platform.python_version(),"numpy":np.__version__},
            "started_utc":datetime.now(timezone.utc).isoformat()}
    OUT.joinpath("raw").mkdir(parents=True,exist_ok=True)
    OUT.joinpath("tables").mkdir(parents=True,exist_ok=True)
    OUT.joinpath("figures").mkdir(parents=True,exist_ok=True)
    raw=OUT/"raw"/f"ref24_seed{seed}.json"; raw.write_text(json.dumps(result,indent=2))
    rows=[("Reference [24]",ref_metrics["roc_auc"],ref_metrics["pr_auc"])]
    if as_metrics: rows.append(("AccessSentinel TCN v3",as_metrics["roc_auc"],as_metrics["pr_auc"]))
    with (OUT/"tables"/f"ref24_seed{seed}_metrics.csv").open("w") as f:
        f.write("method,roc_auc,pr_auc\n")
        for row in rows: f.write(",".join(map(str,row))+"\n")
    if as_metrics:
        fig,ax=plt.subplots(figsize=(8,5)); x=np.arange(len(rows)); w=.35
        ax.bar(x-w/2,[r[1] for r in rows],w,label="ROC-AUC")
        ax.bar(x+w/2,[r[2] for r in rows],w,label="PR-AUC")
        ax.set_xticks(x,[r[0] for r in rows]); ax.set_ylim(0,1); ax.set_ylabel("Score")
        ax.set_title("AccessSentinel vs Reference [24] — same test events")
        ax.legend(); fig.tight_layout(); fig.savefig(OUT/"figures"/f"ref24_seed{seed}_detection.png",dpi=300); plt.close(fig)
    print(json.dumps({"raw":str(raw),"reference24_test":ref_metrics,"accesssentinel":as_metrics,"alignment":alignment},indent=2))

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument("--seed",type=int,default=0); p.add_argument("--dim",type=int,default=8); p.add_argument("--quick",action="store_true")
    a=p.parse_args(); run(a.seed,a.dim,a.quick)

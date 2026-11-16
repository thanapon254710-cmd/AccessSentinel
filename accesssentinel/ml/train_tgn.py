"""Train and evaluate the TGN on the SYNTHETIC collusion-study dataset (D3 Step 7).

    python -m accesssentinel.ml.train_tgn --model-dir models --promote

Compares, on the held-out test period (later dates, unseen collusion groups):
  tgn            two-stage TGN (stage 1 self-supervised on benign graphs, stage 2 fine-tuned), calibrated
  tgn_stage2     ablation: same model without stage 1 (does self-supervision help?)
  static_gnn     ablation: same pipeline with T = 1 (no temporal slicing: does time help?)
  rule_shared    baseline R1: >= 2 shared sensitive resources (the pre-Phase-7 grouping rule)
  rule_uncommon  baseline R2: concurrent co-access of a sensitive resource unfamiliar to both users
"""
import argparse
import json
import os
import platform
import random
import subprocess
import time
from dataclasses import asdict, replace
from datetime import datetime, timezone

import numpy as np
import torch

from ..datasets.synth import SynthConfig, generate
from ..tgn import TGN
from . import tgn_registry
from .tgn_graph import GraphParams
from .tgn_train import (Corpus, calibrate, evaluate, group_instance_detection, groups_for, rule_shared_sensitive,
                        rule_uncommon_coaccess, stage1, stage2, tgn_config, _logits)


def build_sets(corpus: Corpus, seed: int, n_benign: int):
    sets = {}
    sets["benign_train"] = corpus.benign("train", n_benign, random.Random(seed))
    sets["train"] = corpus.evaluation_set("train", random.Random(seed + 1), anchors_per_burst=4, n_random=300, n_resource=300)
    sets["val"] = corpus.evaluation_set("val", random.Random(seed + 2), anchors_per_burst=3, n_random=150, n_resource=150)
    sets["test"] = corpus.evaluation_set("test", random.Random(seed + 3), anchors_per_burst=2, n_random=300, n_resource=300)
    return sets


def train_variant(name, sets, cfg, s1_epochs, s2_epochs, lr2, patience, seed, log):
    torch.manual_seed(seed)
    model = TGN(cfg)
    info = {"name": name}
    t0 = time.time()
    if s1_epochs:
        log(f"[{name}] stage 1: self-supervised on {len(sets['benign_train'])} benign graphs, {s1_epochs} epochs")
        info["stage1_loss"] = stage1(model, sets["benign_train"], s1_epochs, cfg, seed, log)[-1]
    log(f"[{name}] stage 2: fine-tuning on {len(sets['train'])} training graphs "
        f"({sum(it.seed_label for it in sets['train'])} with a colluding seed)")
    info["stage2"] = {k: v for k, v in stage2(model, sets["train"], sets["val"], s2_epochs, lr2, cfg, seed, patience, log).items() if k != "history"}
    info["calibration"] = calibrate(model, sets["val"])
    info["train_seconds"] = round(time.time() - t0, 1)
    log(f"[{name}] calibrated on validation: {info['calibration']}")
    return model, info


def score_items(model, items, tau):
    model.eval()
    edge, groups = [], []
    with torch.no_grad():
        for it in items:
            sc = torch.sigmoid(_logits(model, it)).numpy()
            edge.append(sc); groups.append(groups_for(it.sample, sc, tau)[it.sample.seeds[0]])
    return edge, groups


def run(model_dir, results_dir, data_seed=11, seed=0, s1_epochs=300, s2_epochs=60, lr2=3e-4, patience=10,
        n_benign=400, promote=False, log=print) -> dict:
    torch.set_num_threads(max(1, os.cpu_count() or 1))
    t_start = time.time()
    ds = generate(SynthConfig.collusion_study(seed=data_seed))
    log(f"collusion-study dataset: {len(ds.events):,} events, fingerprint {ds.fingerprint()}")
    gp = GraphParams()
    corpus = Corpus(ds, gp)
    sets = build_sets(corpus, seed, n_benign)
    corpus1 = Corpus.__new__(Corpus); corpus1.__dict__.update(corpus.__dict__); corpus1.gp = replace(gp, T=1)
    sets1 = build_sets(corpus1, seed, n_benign)                      # identical samples, featurised with T = 1
    for k, v in sets.items():
        log(f"  {k}: {len(v)} graphs, {sum(it.seed_label for it in v)} with a colluding seed")
    cfg = tgn_config(gp)
    results = {"data": {"source": "SYNTHETIC (accesssentinel.datasets.synth v2, collusion_study preset)",
                        "fingerprint": ds.fingerprint(), "events": len(ds.events), "users": len(ds.users),
                        "groups": {s: sum(1 for v in ds.instances.values() if v.get("split") == s and "bursts" in v)
                                   for s in ("train", "val", "test")},
                        "familiar_groups": sum(1 for v in ds.instances.values() if v.get("familiar")),
                        "team_meetings": len(ds.meetings),
                        "sets": {k: {"graphs": len(v), "colluding_seed": sum(it.seed_label for it in v)} for k, v in sets.items()}},
               "graph_params": asdict(gp), "tgn_config": asdict(cfg),
               "training": {"stage1_epochs": s1_epochs, "stage2_epochs_max": s2_epochs, "stage2_lr": lr2,
                            "patience": patience, "seed": seed}}
    test = sets["test"]
    models = {}
    for name, ss, c, e1 in (("tgn", sets, cfg, s1_epochs), ("tgn_stage2_only", sets, cfg, 0),
                            ("static_gnn_T1", sets1, replace(cfg, window_T=1), s1_epochs)):
        m, info = train_variant(name, ss, c, e1, s2_epochs, lr2, patience, seed, log)
        models[name] = (m, info, ss["test"])
    evals = {}
    for name, (m, info, items) in models.items():
        cal = info["calibration"]
        edge, groups = score_items(m, items, cal["tau_edge"])
        s = [g["s_coll"] or 0.0 for g in groups]
        ev = evaluate(name, items, s, cal["theta_coll"], groups, edge)
        ev["groups_detected"] = group_instance_detection(ds, items, [x >= cal["theta_coll"] for x in s])
        ev["training"] = info
        evals[name] = ev
    for name, fn in (("rule_shared_sensitive", rule_shared_sensitive), ("rule_uncommon_coaccess", rule_uncommon_coaccess)):
        s = [fn(it.sample, it.sample.seeds[0]) for it in test]
        ev = evaluate(name, test, s, 0.5)
        ev["groups_detected"] = group_instance_detection(ds, test, [x >= 0.5 for x in s])
        evals[name] = ev
    tgn_model, tgn_info, _ = models["tgn"]
    cal = tgn_info["calibration"]
    scorer = tgn_registry.TgnScorer(tgn_model, gp, cal["tau_edge"], cal["theta_coll"], "tmp")
    lat = []
    for it in test[:200]:
        t0 = time.perf_counter(); scorer.analyse(it.sample); lat.append((time.perf_counter() - t0) * 1000)
    results["latency_ms_per_graph"] = {"median": float(np.median(lat)), "p95": float(np.percentile(lat, 95)),
                                       "torch_threads": torch.get_num_threads()}
    results["test"] = evals
    version = f"tgn-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{ds.fingerprint()[:6]}"
    results["version"] = version
    try:
        commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, timeout=5).stdout.strip()
    except Exception:  # noqa: BLE001
        commit = None
    results["environment"] = {"python": platform.python_version(), "torch": torch.__version__, "cpu_count": os.cpu_count(),
                              "git_commit": commit}
    results["wall_seconds"] = round(time.time() - t_start, 1)
    tgn_registry.save(model_dir, version, tgn_model, cfg, gp, cal["tau_edge"], cal["theta_coll"],
                      {"data": results["data"], "calibration": cal, "test_summary": evals["tgn"]["seed_level"],
                       "git_commit": commit})
    if promote:
        tgn_registry.promote(model_dir, version)
    os.makedirs(results_dir, exist_ok=True)
    with open(os.path.join(results_dir, f"{version}.json"), "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    with open(os.path.join(results_dir, f"{version}.md"), "w", encoding="utf-8") as f:
        f.write(markdown(results))
    log(f"saved {version} ({'promoted' if promote else 'not promoted'}) in {results['wall_seconds']} s")
    return results


def markdown(r: dict) -> str:
    f = lambda x: "n/a" if x is None else f"{x:.3f}"
    d = r["data"]
    L = [f"# TGN collusion detection — {r['version']}", "",
         f"**Data: {d['source']}**, fingerprint `{d['fingerprint']}`, {d['events']:,} events, {d['users']} users. "
         "Synthetic results only; collusion labels stand in for analyst-verified incidents.", "",
         f"- Collusion groups: train {d['groups']['train']}, validation {d['groups']['val']}, test {d['groups']['test']} "
         f"(unseen); {d['familiar_groups']} 'familiar' groups (abusing resources their members usually use); "
         f"{d['team_meetings']} recurring benign team meetings as hard negatives.",
         f"- Graph samples: " + ", ".join(f"{k} {v['graphs']} ({v['colluding_seed']} colluding seeds)" for k, v in d["sets"].items()),
         f"- Graph: window {r['graph_params']['window']:.0f} s, T = {r['graph_params']['T']}, <= {r['graph_params']['max_resources']} resources, "
         f"<= {r['graph_params']['max_users_per_resource']} co-accessors per resource. TGN: {r['tgn_config']['gcn_layers']} GCN layers, "
         f"embedding {r['tgn_config']['emb_dim']}, GRU, attention over T, Adam {r['tgn_config']['lr']}, L2 {r['tgn_config']['weight_decay']}; "
         f"stage 1 {r['training']['stage1_epochs']} epochs, stage 2 <= {r['training']['stage2_epochs_max']} epochs (lr {r['training']['stage2_lr']}).", "",
         "## Test period: is the escalated seed part of a colluding group?", "",
         "| Model | ROC-AUC | PR-AUC | threshold | precision | recall | F1 | FPR | groups detected | member precision | member recall | edge PR-AUC |",
         "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for name, e in r["test"].items():
        s, g, m, el = e["seed_level"], e["groups_detected"], e.get("member_identification", {}), e.get("edge_level", {})
        L.append(f"| {name} | {f(s['roc_auc'])} | {f(s['pr_auc'])} | {e['threshold']} | {f(s['precision'])} | {f(s['recall'])} | "
                 f"{f(s['f1'])} | {f(s['fpr'])} | {g['detected']}/{g['groups']} | {f(m.get('precision'))} | {f(m.get('recall'))} | {f(el.get('pr_auc'))} |")
    L += ["", "## Flag rate by sample type (positives: recall; others: false-alarm rate)", "",
          "| Sample type | n | " + " | ".join(r["test"]) + " |", "|---|---|" + "---|" * len(r["test"])]
    kinds = sorted(r["test"]["tgn"]["flag_rate_by_sample_kind"])
    for k in kinds:
        n = r["test"]["tgn"]["flag_rate_by_sample_kind"][k]["n"]
        L.append(f"| {k} | {n} | " + " | ".join(f"{r['test'][m]['flag_rate_by_sample_kind'].get(k, {}).get('flagged', float('nan')):.3f}" for m in r["test"]) + " |")
    L += ["", "## Calibration (validation period)", ""]
    for name in ("tgn", "tgn_stage2_only", "static_gnn_T1"):
        t = r["test"][name]["training"]
        L.append(f"- {name}: tau_edge {t['calibration']['tau_edge']}, theta_coll {t['calibration']['theta_coll']}, "
                 f"validation F1 {t['calibration']['val_f1']:.3f}, best validation seed PR-AUC {t['stage2']['best_val_seed_pr_auc']:.3f}, "
                 f"training {t['train_seconds']} s")
    lat = r["latency_ms_per_graph"]
    L += ["", f"Latency per escalated seed (featurise + TGN + grouping, CPU {lat['torch_threads']} threads): median {lat['median']:.1f} ms, "
          f"p95 {lat['p95']:.1f} ms. Wall time {r['wall_seconds']} s. Environment: {r['environment']}", ""]
    return "\n".join(L)


# ============================================================================================================
# Revised v3 TGN:  python -m accesssentinel.ml.train_tgn --arch v3 [--device cuda] [--promote]
# ============================================================================================================
from ..config import TGNv3Config  # noqa: E402
from ..datasets.synth import COLLUSION_V3  # noqa: E402
from .tgn_train import StreamV3, anchor_replay_v3, train_tgn_v3  # noqa: E402


def _anchor_labels(rec, true_members):
    """Member label = the user has a collusion-labelled edge in the anchor window."""
    coll_users = {e["u"] for e in rec["edges"] if e["coll"] > 0}
    return coll_users


def evaluate_v3(ds, seed=0, device="cpu", cfg=None, log=print) -> dict:
    """Train the v3 TGN on ds, replay the stream with memory at escalation-like anchors, fit Eq. 19 on train,
    calibrate theta_C / theta_A on nominal validation members, evaluate on test. Shared by run_v3 and
    Experiment B (evaluation/exp_b.py)."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import average_precision_score, roc_auc_score
    cfg = cfg or TGNv3Config()
    st = StreamV3(ds)
    log(f"stream: {len(st.t)} events, {st.n_nodes} nodes ({st.U} users, {st.R} resources)")
    model, hist, best_ap = train_tgn_v3(st, cfg, device, seed, log)
    # ---- escalation-like anchors (same sampling as the Phase 7 evaluation)
    corpus = Corpus(ds, GraphParams())
    anchors = []
    for split, k in (("train", 1), ("val", 2), ("test", 3)):
        for it in corpus.evaluation_set(split, random.Random(seed + k), anchors_per_burst=2, n_random=150 if split != "test" else 300,
                                        n_resource=150 if split != "test" else 300):
            anchors.append({"t": float(it.sample.t1), "seed": it.sample.seeds[0], "kind": it.kind, "split": split,
                            "seed_label": it.seed_label, "true": it.true_members,
                            "instance": getattr(it, "instance", None)})
    log(f"anchors: {len(anchors)}; replaying the stream with memory ...")
    t_rep = time.time()
    recs = anchor_replay_v3(model, st, cfg, device, [(a["t"], a["seed"]) for a in anchors])
    replay_s = time.time() - t_rep
    by_key = {(r["t"], r["seed"]): r for r in recs}
    for a in anchors:
        a["rec"] = by_key[(a["t"], a["seed"])]
    # ---- Eq. 19 weights + calibration: logistic regression on TRAINING-period group members
    X, y = [], []
    for a in anchors:
        if a["split"] != "train":
            continue
        coll_users = _anchor_labels(a["rec"], a["true"])
        for m, f in a["rec"].get("features", {}).items():
            X.append(f); y.append(int(m in coll_users))
    if len(set(y)) < 2:
        raise RuntimeError("training anchors produced no positive or no negative group members; cannot fit Eq. 19")
    lr = LogisticRegression(class_weight="balanced", max_iter=2000).fit(np.array(X), np.array(y))
    cal = {"coef": lr.coef_[0].tolist(), "intercept": float(lr.intercept_[0]), "features": ["TM_pi(eps)", "A_u", "Nov(u,g)"],
           "n_train_members": len(y), "positives": int(sum(y))}
    s_of = lambda f: 1.0 / (1.0 + np.exp(-(np.dot(cal["coef"], f) + cal["intercept"])))
    # ---- theta_C, theta_A: quantiles over NOMINAL validation members (Eq. 8 style)
    nom_s, nom_a = [], []
    for a in anchors:
        if a["split"] == "val":
            coll_users = _anchor_labels(a["rec"], a["true"])
            for m, f in a["rec"].get("features", {}).items():
                if m not in coll_users:
                    nom_s.append(s_of(f)); nom_a.append(f[1])
    th = {"theta_C": float(np.quantile(nom_s, 1 - cfg.alpha_coll)) if nom_s else 0.5,
          "theta_A": float(np.quantile(nom_a, 1 - cfg.alpha_attr)) if nom_a else 0.0,
          "alpha_coll": cfg.alpha_coll, "alpha_attr": cfg.alpha_attr, "n_nominal_members": len(nom_s)}
    log(f"Eq. 19 weights {np.round(cal['coef'], 3).tolist()} (+{cal['intercept']:.3f}); theta_C {th['theta_C']:.4f} theta_A {th['theta_A']:.4f}")
    # ---- test evaluation (seed level: is the escalated seed in a colluding group; member attribution)
    def seed_score(a):
        f = a["rec"].get("features", {}).get(a["seed"])
        return s_of(f) if f is not None else 0.0
    test = [a for a in anchors if a["split"] == "test"]
    ys = np.array([a["seed_label"] for a in test]); ss = np.array([seed_score(a) for a in test])
    flag = ss >= th["theta_C"]
    tp, fp = int((flag & (ys == 1)).sum()), int((flag & (ys == 0)).sum())
    fn, tn = int((~flag & (ys == 1)).sum()), int((~flag & (ys == 0)).sum())
    by_kind = {}
    for a, f in zip(test, flag):
        k = a["kind"].split(":")[0] + (":" + a["kind"].split(":")[1] if a["kind"].startswith(("hard", "collusion")) else "")
        d = by_kind.setdefault(k, {"anchors": 0, "flagged": 0}); d["anchors"] += 1; d["flagged"] += int(f)
    att_tp = att_fp = att_fn = 0
    for a, f in zip(test, flag):
        if not f:
            continue
        coll_users = _anchor_labels(a["rec"], a["true"])
        flagged_members = {m for m, ft in a["rec"]["features"].items() if s_of(ft) >= th["theta_C"]}
        att_tp += len(flagged_members & coll_users); att_fp += len(flagged_members - coll_users); att_fn += len(coll_users - flagged_members)
    groups_total = {i for i, inst in ds.instances.items() if inst.get("split") == "test" and "members" in inst
                    and inst.get("kind") in COLLUSION_V3}
    groups_hit = set()
    for a, f in zip(test, flag):
        if f and a["seed_label"]:
            for i in groups_total:
                if a["seed"] in ds.instances[i]["members"]:
                    groups_hit.add(i)
    sizes = [(len(r["rec"]["edges"]), len({e["u"] for e in r["rec"]["edges"]}) + len({e["r"] for e in r["rec"]["edges"]})) for r in test]
    res = {"seed_level": {"anchors": len(test), "positives": int(ys.sum()),
                          "roc_auc": float(roc_auc_score(ys, ss)) if 0 < ys.sum() < len(ys) else None,
                          "pr_auc": float(average_precision_score(ys, ss)) if ys.sum() else None,
                          "at_theta_C": {"tp": tp, "fp": fp, "fn": fn, "tn": tn, "precision": tp / (tp + fp) if tp + fp else 0.0,
                                         "recall": tp / (tp + fn) if tp + fn else 0.0, "fpr": fp / (fp + tn) if fp + tn else 0.0}},
           "by_anchor_kind": by_kind,
           "member_attribution": {"tp": att_tp, "fp": att_fp, "fn": att_fn,
                                  "precision": att_tp / (att_tp + att_fp) if att_tp + att_fp else None,
                                  "recall": att_tp / (att_tp + att_fn) if att_tp + att_fn else None},
           "groups_detected": {"test_groups": len(groups_total), "detected": len(groups_hit)},
           "neighbourhood_size": {"edges_median": float(np.median([s[0] for s in sizes])), "nodes_median": float(np.median([s[1] for s in sizes]))},
           "replay_seconds_all_events": replay_s}
    # ---- per group (Experiment B: recall and time-to-detect vs group size and kind)
    first_ev = {}
    for e in ds.events:
        inst = ds.labels[e.event_id].instance
        if inst in groups_total and inst not in first_ev:
            first_ev[inst] = e.t
    per_group = {}
    for i in sorted(groups_total):
        g = ds.instances[i]
        hits = sorted(a["t"] for a, f in zip(test, flag) if f and a.get("instance") == i)
        n_anch = sum(1 for a in test if a.get("instance") == i)
        per_group[i] = {"kind": g["kind"], "size": len(g["members"]), "familiar": bool(g.get("familiar")),
                        "apps": g.get("apps"), "anchors": n_anch, "anchors_flagged": len(hits),
                        "detected": i in groups_hit,
                        "minutes_to_detect": (hits[0] - first_ev[i]) / 60.0 if hits and i in first_ev else None}
    res["per_group"] = per_group
    res["anchors"] = [{"t": a["t"], "seed": a["seed"], "kind": a["kind"], "instance": a.get("instance"),
                       "seed_label": int(a["seed_label"]), "s_coll": float(sc), "flagged": bool(f),
                       "A_u": (a["rec"].get("features", {}).get(a["seed"]) or [None, None])[1]}
                      for a, sc, f in zip(test, ss, flag)]
    log(f"test: seed PR-AUC {res['seed_level']['pr_auc']}, at theta_C {res['seed_level']['at_theta_C']}; groups {len(groups_hit)}/{len(groups_total)}")
    return {"model": model, "history": hist, "best_ap": best_ap, "cal": cal, "th": th, "res": res, "cfg": cfg,
            "stream_events": int(len(st.t))}


def run_v3(model_dir, results_dir, data_seed=11, seed=0, device="cpu", epochs=None, promote=False, log=print) -> dict:
    t_start = time.time(); started = datetime.now(timezone.utc).isoformat()
    cfg = TGNv3Config()
    if epochs is not None:
        cfg.epochs = epochs
    synth = SynthConfig.collusion_study(seed=data_seed)
    log(f"generating synthetic collusion-study data (seed {data_seed}) ...")
    ds = generate(synth)
    ev3 = evaluate_v3(ds, seed, device, cfg, log)
    model, hist, best_ap, cal, th, res = (ev3[k] for k in ("model", "history", "best_ap", "cal", "th", "res"))
    st_events = ev3["stream_events"]
    version = f"tgn3-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{ds.fingerprint()[:6]}"
    manifest = {"tgn_config": asdict(cfg), "data": {"source": "synthetic", "synth_config": asdict(synth), "fingerprint": ds.fingerprint(),
                "events": st_events}, "training": {"history": hist, "best_val_edge_pr_auc": best_ap, "seed": seed, "device": device},
                "test_summary": res["seed_level"], "git_commit": _git(), "torch": torch.__version__}
    res = {k: v for k, v in res.items() if k != "anchors"}               # per-anchor rows: Experiment B only
    tgn_registry.save_v3(model_dir, version, model.cpu(), cfg, cal, th, manifest)
    if promote:
        tgn_registry.promote(model_dir, version)
    r = {"version": version, "started_at": started, "finished_at": datetime.now(timezone.utc).isoformat(),
         "wall_seconds": time.time() - t_start, "platform": platform.platform(), "calibration": cal, "thresholds": th,
         "test": res, **{k: manifest[k] for k in ("tgn_config", "data", "training")}, "promoted": promote}
    os.makedirs(results_dir, exist_ok=True)
    json.dump(r, open(os.path.join(results_dir, f"{version}.json"), "w", encoding="utf-8"), indent=2, default=str)
    open(os.path.join(results_dir, f"{version}.md"), "w", encoding="utf-8").write(_md_v3(r))
    log(f"saved {version} (promoted={promote})")
    return r


def _git():
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, timeout=5).stdout.strip() or None
    except Exception:  # noqa: BLE001
        return None


def _md_v3(r):
    t, sl = r["test"], r["test"]["seed_level"]
    L = [f"# Revised v3 TGN {r['version']} (CMU + SGE)", "",
         f"Synthetic data only (fingerprint {r['data']['fingerprint']}, {r['data']['events']} events). "
         f"Started {r['started_at']}, finished {r['finished_at']} ({r['wall_seconds']:.0f} s).", "",
         f"Memory / message / time dims {r['tgn_config']['mem_dim']} / {r['tgn_config']['msg_dim']} / {r['tgn_config']['time_dim']}, "
         f"{r['tgn_config']['n_neighbors']} neighbours, {r['tgn_config']['n_hops']} hop, Adam {r['tgn_config']['lr']}, "
         f"L2 {r['tgn_config']['weight_decay']}, best validation collusion-edge PR-AUC {r['training']['best_val_edge_pr_auc']:.4f}.", "",
         f"Eq. 19: S_coll = sigmoid({r['calibration']['coef'][0]:.3f} TM + {r['calibration']['coef'][1]:.3f} A + "
         f"{r['calibration']['coef'][2]:.3f} Nov + {r['calibration']['intercept']:.3f}); theta_C {r['thresholds']['theta_C']:.4f}, "
         f"theta_A {r['thresholds']['theta_A']:.4f}.", "",
         f"## Test anchors ({sl['anchors']}, {sl['positives']} seeds in a colluding group)", "",
         f"ROC-AUC {sl['roc_auc']}, PR-AUC {sl['pr_auc']}; at theta_C: {sl['at_theta_C']}", "",
         f"Groups detected: {t['groups_detected']['detected']}/{t['groups_detected']['test_groups']}. "
         f"Member attribution: precision {t['member_attribution']['precision']}, recall {t['member_attribution']['recall']}.", "",
         "| anchor kind | anchors | flagged |", "|---|---|---|"]
    for k, v in sorted(t["by_anchor_kind"].items()):
        L.append(f"| {k} | {v['anchors']} | {v['flagged']} |")
    L += ["", f"Neighbourhood per anchor: median {t['neighbourhood_size']['edges_median']} edges, {t['neighbourhood_size']['nodes_median']} nodes.", "",
          "## Training", ""]
    for h in r["training"]["history"]:
        L.append(f"- epoch {h['epoch']} ({h['stage']}): loss {h['loss']:.4f}, val edge PR-AUC {h['val_edge_pr_auc']:.4f}")
    return "\n".join(L) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model-dir", default=os.environ.get("AS_MODEL_DIR", "models"))
    ap.add_argument("--results-dir", default="experiments/results")
    ap.add_argument("--data-seed", type=int, default=11)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--stage1-epochs", type=int, default=300)
    ap.add_argument("--stage2-epochs", type=int, default=60)
    ap.add_argument("--promote", action="store_true")
    ap.add_argument("--arch", choices=["d3", "v3"], default="d3")
    ap.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
    ap.add_argument("--epochs", type=int, default=None, help="v3: maximum epochs (default 50, early stopping)")
    args = ap.parse_args(argv)
    if args.arch == "v3":
        run_v3(args.model_dir, args.results_dir, args.data_seed, args.seed, args.device, args.epochs, args.promote)
        return 0
    run(args.model_dir, args.results_dir, args.data_seed, args.seed, args.stage1_epochs, args.stage2_epochs, promote=args.promote)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

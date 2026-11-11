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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model-dir", default=os.environ.get("AS_MODEL_DIR", "models"))
    ap.add_argument("--results-dir", default="experiments/results")
    ap.add_argument("--data-seed", type=int, default=11)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--stage1-epochs", type=int, default=300)
    ap.add_argument("--stage2-epochs", type=int, default=60)
    ap.add_argument("--promote", action="store_true")
    args = ap.parse_args(argv)
    run(args.model_dir, args.results_dir, args.data_seed, args.seed, args.stage1_epochs, args.stage2_epochs, promote=args.promote)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

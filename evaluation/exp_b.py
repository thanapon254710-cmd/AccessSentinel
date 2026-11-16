"""Experiment B - collusion detection (Deliverable 4 Section V-B). SYNTHETIC data only.

    .venv-gpu/Scripts/python -m evaluation.exp_b --seeds 0 1 2 3 4

Seed s: data SynthConfig.collusion_v3_eval(seed = 11 + s): group sizes 2 / 3 / 5 / 8, coordinated collection,
split exfiltration, cross-application and low-and-slow groups; hard negatives = recurring team meetings,
temporary project teams, flash crowds. Model seed s.

Compared on the SAME escalation-like test anchors (Corpus.evaluation_set, test period, unseen groups):
  tgn-v3          CMU + SGE + coordination graph + Eq. 19, theta_C / theta_A calibrated on nominal validation
  rule-shared     D3 baseline: >= 2 shared sensitive resources with the seed in the window
  rule-uncommon   D3 baseline: concurrent co-access of a sensitive resource unfamiliar to both users
and, per group, TCN-only: the promoted v3 TCN (trained on the TCN study data, applied unchanged to this data) at
its own calibrated theta_high - does any member's S_ind reach theta_high during the group's activity? This answers
"does the graph path find groups the individual path misses" and measures the share of below-threshold members.
The D3 TGN and the static GNN are NOT re-run here (CPU cost: 2,337 s and 17,565 s per run in D3 Phase 7); their
like-for-like comparison with the v3 TGN is on the D3 collusion dataset (docs/PROGRESS.md, v3 Phases 4-6).

Raw output: results/raw/exp_b/seed<s>.json (+ seed<s>-anchors.json with one row per test anchor)
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
from sklearn.metrics import average_precision_score, roc_auc_score

from accesssentinel.config import TGNv3Config
from accesssentinel.datasets.synth import COLLUSION_V3, SynthConfig, generate
from accesssentinel.ml import registry
from accesssentinel.ml.dataset import build_v3
from accesssentinel.ml.features import SimulatedDevPosture, SimulatedThreatFeed
from accesssentinel.ml.tgn_graph import GraphParams
from accesssentinel.ml.tgn_train import Corpus, rule_shared_sensitive, rule_uncommon_coaccess
from accesssentinel.ml.train_tcn import scores_v3
from accesssentinel.ml.train_tgn import evaluate_v3

from . import env_info
from .exp_a import _dirty, _git


def _seed_metrics(y, s, flag):
    y, s, flag = np.asarray(y), np.asarray(s, float), np.asarray(flag, bool)
    tp, fp = int((flag & (y == 1)).sum()), int((flag & (y == 0)).sum())
    fn, tn = int((~flag & (y == 1)).sum()), int((~flag & (y == 0)).sum())
    return {"anchors": int(len(y)), "positives": int(y.sum()),
            "roc_auc": float(roc_auc_score(y, s)) if 0 < y.sum() < len(y) else None,
            "pr_auc": float(average_precision_score(y, s)) if y.sum() else None,
            "tp": tp, "fp": fp, "fn": fn, "tn": tn, "precision": tp / (tp + fp) if tp + fp else 0.0,
            "recall": tp / (tp + fn) if tp + fn else 0.0, "fpr": fp / (fp + tn) if fp + tn else 0.0}


def _by_kind(kinds, flag):
    out = {}
    for k, f in zip(kinds, flag):
        d = out.setdefault(k, {"anchors": 0, "flagged": 0}); d["anchors"] += 1; d["flagged"] += int(f)
    return out


def tcn_only(ds, device, groups, log):
    """Per test group: max S_ind of each member during the group's activity (first..last group event + 1 h) under
    the promoted v3 TCN, compared with that model's calibrated theta_high."""
    sc = registry.TcnScorer.load("models")
    if getattr(sc, "arch", "") != "tcn-v3":
        return {"available": False, "reason": "promoted TCN is not v3"}
    threat, posture = SimulatedThreatFeed(), SimulatedDevPosture.from_dataset(ds)
    idx, _, _ = build_v3(ds.events, ds.labels, ds.split_of, threat, posture, sc.L, enc=sc.enc)
    te = idx.mask("test", evaluable_only=False)
    model = sc.model.to(device)
    s = scores_v3(model, idx, te, device)
    th = float(sc.thresholds["theta_high"])
    span = {}
    for e in ds.events:
        inst = ds.labels[e.event_id].instance
        if inst in groups:
            a, b = span.get(inst, (e.t, e.t)); span[inst] = (min(a, e.t), max(b, e.t))
    users, t = idx.users[te], idx.t[te]
    out = {}
    for i in groups:
        a, b = span[i]
        mem = ds.instances[i]["members"]
        mx = {}
        for m in mem:
            sel = (users == m) & (t >= a) & (t <= b + 3600)
            mx[m] = float(s[sel].max()) if sel.any() else None
        over = [m for m, v in mx.items() if v is not None and v >= th]
        first = None
        if over:
            sel = np.isin(users, over) & (t >= a) & (t <= b + 3600) & (s >= th)
            first = (float(t[sel].min()) - a) / 60.0
        out[i] = {"member_max_s_ind": mx, "members_at_or_above_theta_high": len(over),
                  "members_below_theta_high": len(mem) - len(over), "detected": bool(over),
                  "minutes_to_detect": first}
    log(f"TCN-only ({sc.version}, theta_high {th:.4f}): {sum(v['detected'] for v in out.values())}/{len(out)} groups "
        "have a member at or above theta_high")
    return {"available": True, "model": sc.version, "theta_high": th, "per_group": out}


def tcn_only_posthoc(ds, device, groups, seed, n_null=20, log=print):
    """TCN-only with a NULL comparison (added after the first runs: the 'any member >= theta_high anywhere in the
    group's multi-day span' rule fires by chance on ordinary activity, so it needs a matched baseline).
      on_group_events  some member's score on one of the group's OWN collusion events >= theta_high
      any_in_span      some member's score on any event in [first, last group event + 1 h] >= theta_high (as before)
      null_any_in_span share of n_null random non-attacker user sets of the same size, same span, that satisfy
                       any_in_span (the chance alarm rate of that rule)"""
    import random as _r
    sc = registry.TcnScorer.load("models")
    threat, posture = SimulatedThreatFeed(), SimulatedDevPosture.from_dataset(ds)
    idx, _, _ = build_v3(ds.events, ds.labels, ds.split_of, threat, posture, sc.L, enc=sc.enc)
    te = idx.mask("test", evaluable_only=False)
    s = scores_v3(sc.model.to(device), idx, te, device)
    th = float(sc.thresholds["theta_high"])
    users, t, inst = idx.users[te], idx.t[te], idx.instance[te]
    by_user = {}
    for i, u in enumerate(users):
        by_user.setdefault(u, []).append(i)
    attackers = {u for u, p in ds.users.items() if p.get("attacker")}
    benign = sorted(u for u in by_user if u not in attackers)
    rng = _r.Random(seed)

    def alarmed(members, a, b):
        for m in members:
            for i in by_user.get(m, []):
                if a <= t[i] <= b + 3600 and s[i] >= th:
                    return True
        return False
    out = {}
    for g in groups:
        sel = np.where(inst == g)[0]
        a, b = float(t[sel].min()), float(t[sel].max())
        mem = ds.instances[g]["members"]
        null = [alarmed(rng.sample(benign, len(mem)), a, b) for _ in range(n_null)]
        out[g] = {"size": len(mem), "kind": ds.instances[g]["kind"], "span_hours": (b - a) / 3600.0,
                  "on_group_events": bool((s[sel] >= th).any()), "any_in_span": alarmed(mem, a, b),
                  "null_any_in_span": float(np.mean(null))}
    days = {}
    for i, u in enumerate(users):
        if u not in attackers:
            k = (u, int(t[i] // 86400))
            days[k] = days.get(k, False) or bool(s[i] >= th)
    log(f"TCN-only post hoc: on own events {sum(v['on_group_events'] for v in out.values())}/{len(out)}, any in span "
        f"{sum(v['any_in_span'] for v in out.values())}, null rate {np.mean([v['null_any_in_span'] for v in out.values()]):.2f}")
    return {"model": sc.version, "theta_high": th, "per_group": out,
            "normal_user_days_flagged": float(np.mean(list(days.values()))) if days else None, "n_null": n_null}


def run_seed(seed, out_dir, device, epochs, log=print):
    t_start, started = time.time(), datetime.now(timezone.utc).isoformat()
    synth = SynthConfig.collusion_v3_eval(seed=11 + seed)
    log(f"[seed {seed}] generating {synth.n_users} users x {synth.days} days (data seed {synth.seed}) ...")
    ds = generate(synth)
    cfg = TGNv3Config() if epochs is None else replace(TGNv3Config(), epochs=epochs)
    ev3 = evaluate_v3(ds, seed, device, cfg, log=lambda *a: log(f"[seed {seed}]", *a))
    res, th = ev3["res"], ev3["th"]
    anchors = res.pop("anchors")
    groups = sorted(res["per_group"])
    # ---- rule baselines on the identical test anchors (same sampler, same RNG seed as evaluate_v3)
    corpus = Corpus(ds, GraphParams())
    items = corpus.evaluation_set("test", random.Random(seed + 3), anchors_per_burst=2, n_random=300, n_resource=300)
    assert [(float(it.sample.t1), it.sample.seeds[0]) for it in items] == [(a["t"], a["seed"]) for a in anchors], \
        "rule baselines must see the same anchors as the TGN"
    y = [it.seed_label for it in items]
    rules = {}
    for name, fn in (("rule-shared", rule_shared_sensitive), ("rule-uncommon", rule_uncommon_coaccess)):
        s = [fn(it.sample, it.sample.seeds[0]) for it in items]
        flag = [x >= 0.5 for x in s]
        per = {i: {"detected": any(f and getattr(it, "instance", None) == i for it, f in zip(items, flag))} for i in groups}
        rules[name] = {"seed_level": _seed_metrics(y, s, flag), "by_anchor_kind": _by_kind([it.kind for it in items], flag),
                       "per_group": per}
        for a, x in zip(anchors, s):
            a[f"s_{name}"] = float(x)
    tgn = {"seed_level": {**res["seed_level"], **res["seed_level"].pop("at_theta_C")}, "by_anchor_kind": res["by_anchor_kind"],
           "member_attribution": res["member_attribution"], "per_group": res["per_group"],
           "neighbourhood_size": res["neighbourhood_size"], "calibration": ev3["cal"], "thresholds": th,
           "best_val_edge_pr_auc": ev3["best_ap"], "history": ev3["history"]}
    tcn = tcn_only(ds, device, groups, lambda *a: log(f"[seed {seed}]", *a))
    combined = {i: {"tgn": res["per_group"][i]["detected"], "tcn_only": tcn.get("per_group", {}).get(i, {}).get("detected"),
                    "either": bool(res["per_group"][i]["detected"] or tcn.get("per_group", {}).get(i, {}).get("detected"))}
                for i in groups}
    out = {"experiment": "B", "seed": seed, "started_at": started, "finished_at": datetime.now(timezone.utc).isoformat(),
           "wall_seconds": time.time() - t_start,
           "data": {"source": "synthetic", "fingerprint": ds.fingerprint(), "events": len(ds.events), "users": len(ds.users),
                    "synth_config": asdict(synth), "test_groups": len(groups),
                    "group_kinds": sorted({ds.instances[i]["kind"] for i in groups}), "kinds_generated": list(COLLUSION_V3),
                    "team_meetings": len(ds.meetings), "projects": len(ds.projects)},
           "tgn_v3_config": asdict(ev3["cfg"]), "device": device,
           "models": {"tgn-v3": tgn, **rules}, "tcn_only": tcn, "combined_per_group": combined,
           "git_commit": _git(), "git_dirty": _dirty(), "environment": env_info.collect()}
    os.makedirs(out_dir, exist_ok=True)
    json.dump(out, open(os.path.join(out_dir, f"seed{seed}.json"), "w", encoding="utf-8"), indent=1, default=str)
    json.dump(anchors, open(os.path.join(out_dir, f"seed{seed}-anchors.json"), "w", encoding="utf-8"), indent=0, default=str)
    det = lambda k: sum(1 for v in combined.values() if v[k])
    log(f"[seed {seed}] groups detected: TGN {det('tgn')}/{len(groups)}, TCN-only {det('tcn_only')}, either {det('either')}; "
        f"{time.time() - t_start:.0f} s")
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seeds", type=int, nargs="+", default=[0, 1, 2, 3, 4])
    ap.add_argument("--out", default="evaluation/results/raw/exp_b")
    ap.add_argument("--device", default="cuda" if torch.cuda.is_available() else "cpu")
    ap.add_argument("--epochs", type=int, default=None, help="TGN max epochs (default 50, early stopping); pipeline checks only")
    ap.add_argument("--posthoc-tcn", action="store_true", help="add the TCN-only null comparison to finished seeds")
    args = ap.parse_args(argv)
    log = lambda *a: print(*a, flush=True)
    if args.posthoc_tcn:
        from accesssentinel.datasets.synth import SynthConfig as _SC
        for s in args.seeds:
            r = json.load(open(os.path.join(args.out, f"seed{s}.json"), encoding="utf-8"))
            ds = generate(_SC.collusion_v3_eval(seed=11 + s))
            assert ds.fingerprint() == r["data"]["fingerprint"]
            res = tcn_only_posthoc(ds, args.device, sorted(r["models"]["tgn-v3"]["per_group"]), s,
                                   log=lambda *a: log(f"[seed {s}]", *a))
            json.dump(res, open(os.path.join(args.out, f"seed{s}-tcnonly.json"), "w", encoding="utf-8"), indent=1)
        return 0
    for s in args.seeds:
        run_seed(s, args.out if args.epochs is None else args.out + "-quick", args.device, args.epochs, log)
    return 0


if __name__ == "__main__":
    sys.exit(main())

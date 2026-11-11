"""TGN collusion detection (D3 Step 7): two-stage training, learned grouping g(u), evaluation against baselines.

    python -m accesssentinel.ml.train_tgn --model-dir models --promote      (entry point: ml/train_tgn.py)

Model: accesssentinel.tgn.TGN exactly as D3/[14]: 2 GCN layers, 50-d embeddings, GRU temporal encoder,
attention over T = 10 time steps, edge score sigma(W_s [h_i; h_j] + b_s); Adam lr 1e-3, L2 1e-6.

Training (D3: "the edge score is first trained self-supervised on benign interaction history ... analyst-
verified collusion incidents are then used only to fine-tune the model and to calibrate tau_edge and theta_coll"):
  Stage 1  self-supervised, BENIGN graphs only (Path-D-like seeds, graphs with any labelled attack edge are
           dropped): real access edges -> 0 (normal), corrupted edges (the user paired with a resource of the
           neighbourhood it did not access) -> 1 (anomalous). 300 epochs [D3].
  Stage 2  fine-tuning of the same edge score on training-period graphs with collusion labels. In this
           prototype the labels come from the SYNTHETIC generator and stand in for analyst-verified incidents.
  Calibration on the validation period: tau_edge (suspicious edge) and theta_coll (group alert), by F1.

Group g(u) (audit D8): users linked when both have suspicious edges (score >= tau_edge) to the same resource in
the window; g(u) = connected component of u; S_coll = mean of the top-k suspicious edge scores of g(u)'s
members (k = max(3, 25 %)). |g(u)| < 2 -> S_coll undefined (no coupled group), as D3 Step 8 allows.
"""
import copy
import json
import os
import random
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from typing import Dict, List, Optional

import numpy as np
import torch
import torch.nn.functional as F
from sklearn.metrics import average_precision_score, roc_auc_score

from ..config import TGNConfig
from ..tgn import TGN
from .tgn_graph import NODE_FEATURES, EdgeAnnotator, GraphParams, GraphSample, MemoryEdgeIndex, build_sample, edge_endpoints, featurize

ATTACK_INDIVIDUAL = {"compromised_account", "insider_exfiltration", "bac_probe", "low_and_slow"}


def tgn_config(gp: GraphParams) -> TGNConfig:
    return replace(TGNConfig(), node_feat_dim=NODE_FEATURES, window_T=gp.T)


# ---------------------------------------------------------------------------------------------- grouping
def groups_for(sample: GraphSample, scores: np.ndarray, tau_edge: float, top_frac: float = 0.25) -> Dict[str, dict]:
    """For every seed: its coupled group g(u), S_coll and evidence (members, suspicious edges)."""
    susp = [k for k, sc in enumerate(scores) if sc >= tau_edge]
    by_res = defaultdict(set)
    for k in susp:
        by_res[sample.edges[k]["r"]].add(sample.edges[k]["u"])
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]; x = parent[x]
        return x
    for us in by_res.values():
        us = sorted(us)
        for v in us[1:]:
            parent[find(us[0])] = find(v)
    out = {}
    for seed in sample.seeds:
        if seed not in parent:
            out[seed] = {"members": [seed], "s_coll": None, "reason": "no suspicious co-access"}
            continue
        root = find(seed)
        members = sorted(u for u in parent if find(u) == root)
        if len(members) < 2:
            out[seed] = {"members": members, "s_coll": None, "reason": "no coupled group"}
            continue
        ks = [k for k in susp if sample.edges[k]["u"] in members]
        vals = np.sort(scores[ks])[::-1]
        k = max(3, int(np.ceil(top_frac * len(vals))))
        member_scores = {m: float(max(scores[j] for j in ks if sample.edges[j]["u"] == m)) for m in members}
        top = sorted(ks, key=lambda j: -scores[j])[:10]
        out[seed] = {"members": members, "s_coll": float(vals[:k].mean()), "member_scores": member_scores,
                     "resources": sorted({sample.edges[j]["r"] for j in ks}),
                     "top_edges": [{"u": sample.edges[j]["u"], "r": sample.edges[j]["r"], "t": sample.edges[j]["t"],
                                    "score": round(float(scores[j]), 4)} for j in top]}
    return out


def rule_shared_sensitive(sample: GraphSample, seed: str) -> float:
    """Baseline R1 (the pre-Phase-7 grouping rule): partners sharing >= 2 sensitive resources with the seed."""
    mine = {e["r"] for e in sample.edges if e["u"] == seed and e["s"] == 1}
    per = defaultdict(set)
    for e in sample.edges:
        if e["u"] != seed and e["s"] == 1 and e["r"] in mine:
            per[e["u"]].add(e["r"])
    best = max((len(v) for v in per.values()), default=0)
    return min(1.0, best / 4.0)                    # >= 2 shared -> 0.5 (flag threshold 0.5)


def rule_uncommon_coaccess(sample: GraphSample, seed: str) -> float:
    """Baseline R2: partners concurrently accessing a sensitive resource that is unfamiliar to both."""
    mine = defaultdict(list)
    for e in sample.edges:
        if e["u"] == seed and e["s"] == 1 and e["fam"] <= 1:
            mine[e["r"]].append(e["t"])
    partners = set()
    for e in sample.edges:
        if e["u"] != seed and e["s"] == 1 and e["fam"] <= 1 and any(abs(e["t"] - t) <= 900 for t in mine.get(e["r"], [])):
            partners.add(e["u"])
    return min(1.0, len(partners) / 2.0)           # >= 1 partner -> 0.5


# ---------------------------------------------------------------------------------------------- data
@dataclass
class Item:
    sample: GraphSample
    X: torch.Tensor
    A: torch.Tensor
    eu: torch.Tensor
    er: torch.Tensor
    y: torch.Tensor                 # per edge collusion label
    kind: str                       # how the sample was drawn (for per-type reporting)
    seed_label: int                 # seed has collusion edges in the window
    true_members: List[str]


class Corpus:
    def __init__(self, ds, gp: GraphParams):
        self.ds, self.gp = ds, gp
        evs = sorted(ds.events, key=lambda e: (e.t, e.event_id))
        ann = EdgeAnnotator()
        self.edges = [ann.annotate(e) for e in evs]
        self.index = MemoryEdgeIndex(self.edges)
        self.lab = ds.labels
        self.times = np.array([e["t"] for e in self.edges])

    def item(self, seeds, t1, kind) -> Optional[Item]:
        s = build_sample(seeds, self.index, t1, self.gp)
        if not s.edges or not s.seeds:
            return None
        y = np.array([self.lab[e["id"]].collusion for e in s.edges], dtype=np.float32)
        s.label = y
        X, A = featurize(s, self.gp)
        eu, er = edge_endpoints(s)
        seed = s.seeds[0]
        true = sorted({e["u"] for e, l in zip(s.edges, y) if l})
        return Item(s, torch.from_numpy(X), torch.from_numpy(A), torch.from_numpy(eu), torch.from_numpy(er),
                    torch.from_numpy(y), kind, int(seed in true), true)

    def active_users(self, t0, t1):
        a, b = np.searchsorted(self.times, [t0, t1])
        return sorted({self.edges[i]["u"] for i in range(a, b)})

    def benign(self, split: str, n: int, rng: random.Random) -> List[Item]:
        a, b = self.ds.split_bounds[split]
        out, tries = [], 0
        while len(out) < n and tries < n * 20:
            tries += 1
            day = rng.randrange(int(a // 86400), int(b // 86400))
            t1 = day * 86400 + rng.uniform(9, 18) * 3600
            us = self.active_users(t1 - self.gp.window, t1)
            if not us:
                continue
            it = self.item(rng.sample(us, min(len(us), rng.randint(1, 2))), t1, "benign_random")
            if it is None:
                continue
            labs = [self.lab[e["id"]] for e in it.sample.edges]
            if any(l.collusion or l.scenario in ATTACK_INDIVIDUAL for l in labs):
                continue                                              # stage 1 sees benign history only
            out.append(it)
        return out

    def evaluation_set(self, split: str, rng: random.Random, anchors_per_burst: int = 2, n_random: int = 200,
                       n_resource: int = 200) -> List[Item]:
        """Escalation-like samples: collusion bursts (seed = one member, as if escalated), team meetings and
        flash crowds (hard negatives), Path-B-like busy sensitive resources, Path-D-like random users."""
        a, b = self.ds.split_bounds[split]
        items = []
        for name, inst in sorted(self.ds.instances.items()):
            if inst.get("split") != split or "bursts" not in inst:
                continue
            for t0 in inst["bursts"]:
                for _ in range(anchors_per_burst):
                    it = self.item([rng.choice(inst["members"])], t0 + rng.uniform(10, 50) * 60,
                                   f"collusion:{inst['kind']}:{'familiar' if inst['familiar'] else 'unfamiliar'}")
                    if it is not None:
                        items.append(it)
        for m in getattr(self.ds, "meetings", []):
            for day in range(int(a // 86400), int(b // 86400)):
                t = day * 86400
                if (int(t // 86400) + 3) % 7 == m["weekday"]:
                    it = self.item([rng.choice(m["team"])], t + (m["hour"] + 0.75) * 3600, "hard:team_meeting")
                    if it is not None:
                        items.append(it)
        crowd = [e for e in self.edges if a <= e["t"] < b and self.lab[e["id"]].scenario == "benign_flash_crowd"]
        for e in crowd[::3]:
            it = self.item([e["u"]], e["t"] + 900, "hard:flash_crowd")
            if it is not None:
                items.append(it)
        sens_idx = [i for i, e in enumerate(self.edges) if a <= e["t"] < b and e["s"] == 1]
        for _ in range(n_resource):
            e = self.edges[rng.choice(sens_idx)]
            it = self.item([e["u"]], e["t"] + rng.uniform(0, 900), "pathB_like:sensitive_access")
            if it is not None:
                items.append(it)
        all_idx = np.where((self.times >= a) & (self.times < b))[0]
        for _ in range(n_random):
            e = self.edges[int(rng.choice(list(all_idx)))]
            it = self.item([e["u"]], e["t"] + rng.uniform(0, 600), "pathD_like:random")
            if it is not None:
                items.append(it)
        return items


# ---------------------------------------------------------------------------------------------- training
def _logits(model, it: Item, eu=None, er=None):
    h = model.embed(it.X, it.A)
    eu = it.eu if eu is None else eu
    er = it.er if er is None else er
    return model.dec(torch.cat([h[eu], h[er]], -1)).squeeze(-1)


def corrupt(it: Item, rng: np.random.Generator):
    """Negative (fake) edges: each real edge's user paired with another resource of the neighbourhood that the
    user did not access in the window."""
    U, R = it.sample.n_users, len(it.sample.resources)
    if R < 2:
        return None
    acc = set(zip(it.eu.tolist(), it.er.tolist()))
    fu, fr = [], []
    for u in it.eu.tolist():
        for _ in range(5):
            r = U + int(rng.integers(R))
            if (u, r) not in acc:
                fu.append(u); fr.append(r); break
    if not fu:
        return None
    return torch.tensor(fu), torch.tensor(fr)


def stage1(model, items: List[Item], epochs: int, cfg: TGNConfig, seed: int, log=print):
    opt = torch.optim.Adam(model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)
    rng = np.random.default_rng(seed)
    hist = []
    for ep in range(epochs):
        model.train(); tot = 0.0; t0 = time.time()
        for j in rng.permutation(len(items)):
            it = items[j]
            neg = corrupt(it, rng)
            if neg is None:
                continue
            h = model.embed(it.X, it.A)
            pos_l = model.dec(torch.cat([h[it.eu], h[it.er]], -1)).squeeze(-1)
            neg_l = model.dec(torch.cat([h[neg[0]], h[neg[1]]], -1)).squeeze(-1)
            loss = F.binary_cross_entropy_with_logits(torch.cat([pos_l, neg_l]),
                                                      torch.cat([torch.zeros_like(pos_l), torch.ones_like(neg_l)]))
            opt.zero_grad(); loss.backward(); opt.step(); tot += loss.item()
        hist.append(tot / max(len(items), 1))
        if ep % 50 == 0 or ep == epochs - 1:
            log(f"  [stage 1] epoch {ep:3d}  loss {hist[-1]:.4f}  ({time.time() - t0:.1f}s/epoch)")
    return hist


def seed_scores(model, items: List[Item], tau: float):
    model.eval()
    out = []
    with torch.no_grad():
        for it in items:
            sc = torch.sigmoid(_logits(model, it)).numpy()
            g = groups_for(it.sample, sc, tau)[it.sample.seeds[0]]
            out.append((sc, g))
    return out


def calibrate(model, items: List[Item]):
    """tau_edge and theta_coll maximising seed-level F1 on validation items."""
    model.eval()
    with torch.no_grad():
        scs = [torch.sigmoid(_logits(model, it)).numpy() for it in items]
    y = np.array([it.seed_label for it in items])
    best = (-1.0, 0.5, 0.5)
    for tau in (0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9):
        s = np.array([(groups_for(it.sample, sc, tau)[it.sample.seeds[0]]["s_coll"] or 0.0) for it, sc in zip(items, scs)])
        for th in np.arange(0.3, 0.96, 0.05):
            p = s >= th
            tp, fp, fn = int((p & (y == 1)).sum()), int((p & (y == 0)).sum()), int((~p & (y == 1)).sum())
            f1 = 2 * tp / (2 * tp + fp + fn) if tp else 0.0
            if f1 > best[0] + 1e-9:
                best = (f1, tau, float(round(th, 2)))
    return {"val_f1": best[0], "tau_edge": best[1], "theta_coll": best[2]}


def seed_pr_auc(model, items, tau=0.5):
    y = np.array([it.seed_label for it in items])
    s = np.array([(g["s_coll"] or 0.0) for _, g in seed_scores(model, items, tau)])
    return float(average_precision_score(y, s)) if 0 < y.sum() < len(y) else float("nan")


def stage2(model, train: List[Item], val: List[Item], epochs: int, lr: float, cfg: TGNConfig, seed: int, patience: int, log=print):
    opt = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=cfg.weight_decay)
    rng = np.random.default_rng(seed)
    pos = sum(float(it.y.sum()) for it in train); neg = sum(float((1 - it.y).sum()) for it in train)
    pw = torch.tensor(min(neg / max(pos, 1.0), 50.0))
    best, best_ap, bad, hist = copy.deepcopy(model.state_dict()), -1.0, 0, []
    for ep in range(epochs):
        model.train(); tot = 0.0
        for j in rng.permutation(len(train)):
            it = train[j]
            loss = F.binary_cross_entropy_with_logits(_logits(model, it), it.y, pos_weight=pw)
            opt.zero_grad(); loss.backward(); opt.step(); tot += loss.item()
        ap = seed_pr_auc(model, val)
        hist.append({"epoch": ep, "loss": tot / len(train), "val_seed_pr_auc": ap})
        if ap > best_ap + 1e-6:
            best, best_ap, bad = copy.deepcopy(model.state_dict()), ap, 0
        else:
            bad += 1
        if ep % 5 == 0 or bad >= patience:
            log(f"  [stage 2] epoch {ep:3d}  loss {hist[-1]['loss']:.4f}  val seed PR-AUC {ap:.3f}")
        if bad >= patience:
            break
    model.load_state_dict(best)
    return {"best_val_seed_pr_auc": best_ap, "history": hist, "pos_weight": float(pw)}


# ---------------------------------------------------------------------------------------------- evaluation
def evaluate(name: str, items: List[Item], seed_score: List[float], theta: float, groups=None, edge_scores=None) -> dict:
    y = np.array([it.seed_label for it in items]); s = np.array(seed_score)
    p = s >= theta
    tp, fp, fn, tn = int((p & (y == 1)).sum()), int((p & (y == 0)).sum()), int((~p & (y == 1)).sum()), int((~p & (y == 0)).sum())
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    out = {"model": name, "threshold": theta, "seed_level": {
        "n": len(y), "positives": int(y.sum()), "roc_auc": float(roc_auc_score(y, s)) if 0 < y.sum() < len(y) else None,
        "pr_auc": float(average_precision_score(y, s)) if y.sum() else None, "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": prec, "recall": rec, "f1": 2 * prec * rec / (prec + rec) if prec + rec else 0.0,
        "fpr": fp / (fp + tn) if fp + tn else 0.0}}
    kinds = defaultdict(list)
    for it, flag in zip(items, p):
        kinds[it.kind].append(bool(flag))
    out["flag_rate_by_sample_kind"] = {k: {"n": len(v), "flagged": float(np.mean(v))} for k, v in sorted(kinds.items())}
    if edge_scores is not None:
        ye = np.concatenate([it.y.numpy() for it in items]); se = np.concatenate(edge_scores)
        out["edge_level"] = {"edges": int(len(ye)), "positives": int(ye.sum()),
                             "roc_auc": float(roc_auc_score(ye, se)), "pr_auc": float(average_precision_score(ye, se))}
    if groups is not None:
        precs, recs = [], []
        for it, g, flag in zip(items, groups, p):
            if it.seed_label and flag:
                pred = set(g["members"]); true = set(it.true_members)
                precs.append(len(pred & true) / len(pred)); recs.append(len(pred & true) / len(true))
        out["member_identification"] = {"flagged_positive_samples": len(precs),
                                        "precision": float(np.mean(precs)) if precs else None,
                                        "recall": float(np.mean(recs)) if recs else None}
    return out


def group_instance_detection(ds, items, flags) -> dict:
    hit = defaultdict(bool)
    for it, f in zip(items, flags):
        if it.seed_label:
            insts = {ds.labels[e["id"]].instance for e in it.sample.edges if ds.labels[e["id"]].collusion}
            for i in insts:
                hit[i] |= bool(f)
    return {"groups": len(hit), "detected": int(sum(hit.values())), "per_group": dict(sorted(hit.items()))}

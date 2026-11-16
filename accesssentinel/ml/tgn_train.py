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
import math
import os
import random
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from typing import Dict, List, Optional, Tuple

import numpy as np
import torch
import torch.nn.functional as F
from sklearn.metrics import average_precision_score, roc_auc_score

from ..config import TGNConfig
from ..tgn import TGN
from .tgn_graph import NODE_FEATURES, EdgeAnnotator, GraphParams, GraphSample, MemoryEdgeIndex, build_sample, edge_endpoints, featurize

from ..datasets.synth import INDIVIDUAL_V3 as _IND_V3  # noqa: E402
ATTACK_INDIVIDUAL = set(_IND_V3)                         # D3 four + the Phase 8 individual scenarios


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
                                   f"collusion:{inst['kind']}:{'familiar' if inst.get('familiar') else 'unfamiliar'}")
                    if it is not None:
                        it.instance = name                                  # group instance (Experiment B)
                        items.append(it)
        for m in getattr(self.ds, "meetings", []):
            for day in range(int(a // 86400), int(b // 86400)):
                t = day * 86400
                if (int(t // 86400) + 3) % 7 == m["weekday"]:
                    it = self.item([rng.choice(m["team"])], t + (m["hour"] + 0.75) * 3600, "hard:team_meeting")
                    if it is not None:
                        items.append(it)
        for p in getattr(self.ds, "projects", []):                        # Phase 8 hard negatives
            for t0 in p.get("bursts", []):
                if a <= t0 < b:
                    it = self.item([rng.choice(p["team"])], t0 + rng.uniform(10, 50) * 60, "hard:benign_project")
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


# ============================================================================================================
# Revised v3 TGN (Phases 4 and 6): continuous memory (CMU) over the whole event stream + selective graph
# encoding (SGE) + candidate coordination graph and per-member collective score (Eqs. 11-19).
#     python -m accesssentinel.ml.train_tgn --arch v3 [--device cuda] [--promote]
# ============================================================================================================
"""
Training (TGN procedure of Rossi et al. [23], adapted):
  * the TRAINING-period event stream is replayed in time order, in batches of `batch_size` events
  * at the start of a batch the memories of the nodes touched by the PREVIOUS batch are updated by the CMU
    (gradients flow into the message function and the GRU through these updates), then the SGE embeds the
    current batch's users / resources from the updated memories and each node's K most recent neighbours
  * edge reconstruction: observed edges -> eps = 0, corrupted edges (same user, random other resource) -> 1;
    stage A (first `self_supervised_epochs`) uses benign edges only; stage B adds the collusion labels of the
    synthetic generator (stand-in for analyst-verified incidents) as eps = 1 targets
  * early stopping on validation PR-AUC of eps for collusion edges
Collective scoring at an escalation anchor (seed u, time t):
  * window edges of the seed and of the users sharing its resources in [t - W, t]; eps from h_u(t), h_r(t)
  * H_W (Eq. 15): pairs sharing >= h_min resources within Delta_c, resources with window degree > d_cap
    excluded; weights w_uv (Eq. 16); groups = connected components with |g| >= n_min, >= ns_min shared
    sensitive resources and density >= delta_min (Eq. 17)
  * A_u (Eq. 18); Nov(u, g) = share of u's accesses to the group's shared resources that are FIRST accesses
    of u to that resource [IMPL definition]; S_coll = Cal(w1 TM_pi(eps) + w2 A_u + w3 Nov) (Eq. 19), with w and
    Cal fitted together by logistic regression on training-period anchors; theta_C, theta_A = quantiles on
    nominal validation members (as Eq. 8)
"""
from ..config import TGNv3Config  # noqa: E402
from ..tgn import NODE_TYPES, TGNv3  # noqa: E402

OPS_V3 = ("view", "edit", "delete", "export")


def edge_feat_v3(e) -> List[float]:
    """f(e) for the CMU / SGE: sensitivity, denied, operation one-hot, unmapped path (7)."""
    return [float(e.s), float(1 - e.y)] + [float(e.o == o) for o in OPS_V3] + [float(e.r.startswith("unmapped:"))]


class StreamV3:
    """The event stream as arrays + node indices (users, resources r*, applications)."""

    def __init__(self, ds):
        evs = sorted(ds.events, key=lambda e: (e.t, e.event_id))
        self.users = sorted({e.u for e in evs})
        self.res = sorted({e.rstar for e in evs})
        self.apps = sorted({e.a for e in evs})
        U, R = len(self.users), len(self.res)
        self.uid = {u: i for i, u in enumerate(self.users)}
        self.rid = {r: U + i for i, r in enumerate(self.res)}
        self.aid = {a: U + R + i for i, a in enumerate(self.apps)}
        self.n_nodes = U + R + len(self.apps)
        self.U, self.R = U, R
        self.res_app = {self.rid[e.rstar]: self.aid[e.a] for e in evs}
        self.res_s = {self.rid[e.rstar]: int(e.s) for e in evs}
        self.u = np.array([self.uid[e.u] for e in evs]); self.r = np.array([self.rid[e.rstar] for e in evs])
        self.t = np.array([e.t for e in evs], dtype=np.float64)
        self.f = np.array([edge_feat_v3(e) for e in evs], dtype=np.float32)
        lab = [ds.labels[e.event_id] for e in evs]
        self.coll = np.array([l.collusion for l in lab], dtype=np.float32)
        self.indiv = np.array([l.individual for l in lab], dtype=np.float32)
        self.split = np.array([ds.split_of(e.t) for e in evs])
        self.events = evs
        ann = EdgeAnnotator()                                     # same familiarity as the live graph index
        self.fam = np.array([ann.annotate(e)["fam"] for e in evs], dtype=np.int64)
        self.ntype = np.zeros((self.n_nodes, NODE_TYPES), np.float32)
        self.ntype[:U, 0] = 1; self.ntype[U:U + R, 1] = 1; self.ntype[U + R:, 2] = 1


class MemoryStateV3:
    """Node memories, last-update times and K-most-recent neighbour buffers (offline replay; the live worker
    keeps the same state in Redis)."""

    def __init__(self, st: StreamV3, cfg: TGNv3Config, device):
        N, K = st.n_nodes, cfg.n_neighbors
        self.st, self.cfg, self.dev = st, cfg, device
        self.M = torch.zeros(N, cfg.mem_dim, device=device)
        self.last_t = np.full(N, np.nan)
        self.nbr = np.full((N, K), -1, dtype=np.int64)
        self.nbr_t = np.zeros((N, K)); self.nbr_f = np.zeros((N, K, cfg.edge_feat_dim), np.float32)
        self.ptr = np.zeros(N, dtype=np.int64)
        self.pending = None                                      # previous batch (indices) not yet in memory
        self.ntype = torch.from_numpy(st.ntype).to(device)

    def apply_pending(self, model, grad: bool):
        """CMU step for the nodes of the pending batch ('last message' aggregation per node, as in [23])."""
        if self.pending is None or len(self.pending) == 0:
            return self.M
        st, idx = self.st, self.pending
        last = {}
        for i in idx:                                            # last event per node within the batch
            last[st.u[i]] = (i, st.r[i]); last[st.r[i]] = (i, st.u[i])
        nodes = np.array(list(last.keys())); ev = np.array([last[n][0] for n in nodes]); other = np.array([last[n][1] for n in nodes])
        lt = self.last_t[nodes]
        dt = np.where(np.isnan(lt), 0.0, st.t[ev] - np.nan_to_num(lt))
        d = self.dev
        ctx = torch.enable_grad() if grad else torch.no_grad()
        with ctx:
            new = model.cmu(self.M[torch.from_numpy(nodes).to(d)], self.M[torch.from_numpy(other).to(d)],
                            torch.from_numpy(dt.astype(np.float32)).to(d), torch.from_numpy(st.f[ev]).to(d))
            M2 = self.M.clone()
            M2[torch.from_numpy(nodes).to(d)] = new
        self.last_t[nodes] = st.t[ev]
        return M2

    def commit(self, M2, batch_idx):
        """Memory after the pending batch becomes the state; the current batch becomes pending; neighbour
        buffers learn the current batch's edges (user <- resource, resource <- user)."""
        self.M = M2.detach()
        st, K = self.st, self.cfg.n_neighbors
        for i in batch_idx:
            for a, b in ((st.u[i], st.r[i]), (st.r[i], st.u[i])):
                p = self.ptr[a] % K
                self.nbr[a, p], self.nbr_t[a, p], self.nbr_f[a, p] = b, st.t[i], st.f[i]
                self.ptr[a] += 1
        self.pending = batch_idx

    def embed(self, model, M, nodes: np.ndarray, t: np.ndarray):
        """SGE embeddings h_v(t) for nodes at times t (resources also see their application as a neighbour)."""
        d, K = self.dev, self.cfg.n_neighbors
        nb = self.nbr[nodes]; nt = self.nbr_t[nodes]; nf = self.nbr_f[nodes]
        mask = nb >= 0
        dt = np.where(mask, t[:, None] - nt, 0.0).astype(np.float32)
        nbc = np.where(mask, nb, 0)
        nb_t = torch.from_numpy(nbc).to(d)
        m_n = M[nb_t] * torch.from_numpy(mask).to(d).unsqueeze(-1)
        h = model.sge(M[torch.from_numpy(nodes).to(d)], self.ntype[torch.from_numpy(nodes).to(d)], m_n,
                      self.ntype[nb_t], torch.from_numpy(nf).to(d), torch.from_numpy(dt).to(d), torch.from_numpy(mask).to(d))
        return h


def _bce(logit, target, weight=None):
    return F.binary_cross_entropy_with_logits(logit, target, weight=weight)


def train_tgn_v3(st: StreamV3, cfg: TGNv3Config, device, seed: int, log=print):
    torch.manual_seed(seed); rng = np.random.default_rng(seed)
    model = TGNv3(cfg).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)
    tr = np.where(st.split == "train")[0]
    res_nodes = np.array(sorted(st.res_app))
    hist, best, best_ap, bad = [], copy.deepcopy(model.state_dict()), -1.0, 0
    for ep in range(cfg.epochs):
        supervised = ep >= cfg.self_supervised_epochs
        model.train()
        ms = MemoryStateV3(st, cfg, device)
        t0, tot, n = time.time(), 0.0, 0
        for b0 in range(0, len(tr), cfg.batch_size):
            idx = tr[b0:b0 + cfg.batch_size]
            M2 = ms.apply_pending(model, grad=True)
            u, r, t = st.u[idx], st.r[idx], st.t[idx]
            neg = rng.choice(res_nodes, len(idx))
            neg = np.where(neg == r, rng.choice(res_nodes, len(idx)), neg)
            nodes = np.concatenate([u, r, neg]); tt = np.concatenate([t, t, t])
            h = ms.embed(model, M2, nodes, tt)
            B = len(idx)
            hu, hr, hn = h[:B], h[B:2 * B], h[2 * B:]
            f = torch.from_numpy(st.f[idx]).to(device)
            coll = torch.from_numpy(st.coll[idx]).to(device)
            lp, ln = model.dec(hu, hr, f), model.dec(hu, hn, f)
            if supervised:
                w = 1.0 + (cfg.sup_weight * 20.0 - 1.0) * coll           # collusion edges are rare: up-weight
                loss = _bce(lp, coll, w) + _bce(ln, torch.ones_like(ln))
            else:
                keep = (coll == 0).float()                               # stage A: benign edges only
                loss = (_bce(lp, torch.zeros_like(lp), keep) + _bce(ln, torch.ones_like(ln)))
            opt.zero_grad(); loss.backward(); opt.step()
            ms.commit(M2, idx)
            tot += float(loss.detach()); n += 1
        ap, _ = edge_ap_v3(model, st, cfg, device, "val")
        hist.append({"epoch": ep, "stage": "joint" if supervised else "self_supervised", "loss": tot / max(n, 1),
                     "val_edge_pr_auc": ap, "seconds": time.time() - t0})
        log(f"  [tgn-v3] epoch {ep} ({'joint' if supervised else 'self-sup'}) loss {tot / max(n, 1):.4f} "
            f"val collusion-edge PR-AUC {ap:.4f} ({time.time() - t0:.0f}s)")
        if supervised:
            if ap > best_ap:
                best_ap, best, bad = ap, copy.deepcopy(model.state_dict()), 0
            else:
                bad += 1
                if bad >= 5:
                    break
    model.load_state_dict(best)
    return model, hist, best_ap


@torch.no_grad()
def edge_ap_v3(model, st: StreamV3, cfg: TGNv3Config, device, split: str):
    """Replay the stream up to the end of `split` (memory continues across splits, as deployed) and score
    every edge of `split` at its time: PR-AUC of eps against the collusion labels."""
    model.eval()
    ms = MemoryStateV3(st, cfg, device)
    end = np.where(st.split == split)[0]
    stop = end.max() + 1 if len(end) else 0
    scores, labels = [], []
    for b0 in range(0, stop, 512):
        idx = np.arange(b0, min(b0 + 512, stop))
        M2 = ms.apply_pending(model, grad=False)
        sel = idx[st.split[idx] == split]
        if len(sel):
            h = ms.embed(model, M2, np.concatenate([st.u[sel], st.r[sel]]), np.concatenate([st.t[sel], st.t[sel]]))
            B = len(sel)
            scores.append(torch.sigmoid(model.dec(h[:B], h[B:], torch.from_numpy(st.f[sel]).to(device))).cpu().numpy())
            labels.append(st.coll[sel])
        ms.commit(M2, idx)
    if not scores:
        return float("nan"), None
    s, y = np.concatenate(scores), np.concatenate(labels)
    return (float(average_precision_score(y, s)) if y.sum() else float("nan")), (s, y)


# ---------------------------------------------------------------------------------------------- collective score
def coordination_groups(edges: List[dict], cfg: TGNv3Config, deg: Dict[str, int], res_s: Dict[str, int]):
    """H_W and groups (Eqs. 15-17). edges: [{u, r, t, eps}] in the window. Returns (groups, w, shared)."""
    by_r = defaultdict(lambda: defaultdict(list))
    for e in edges:
        if deg.get(e["r"], 0) <= cfg.d_cap:
            by_r[e["r"]][e["u"]].append(e["t"])
    pair_res = defaultdict(dict)                                  # (u, v) -> r -> min |t_u - t_v|
    for r, us in by_r.items():
        ul = sorted(us)
        for i in range(len(ul)):
            for j in range(i + 1, len(ul)):
                a, b = ul[i], ul[j]
                gap = min(abs(x - y) for x in us[a] for y in us[b])
                if gap <= cfg.delta_c:
                    pair_res[(a, b)][r] = gap
    w = {}
    for (a, b), rs in pair_res.items():
        if len(rs) >= cfg.h_min:
            w[(a, b)] = sum(res_s.get(r, 0) / math.log(1 + max(deg.get(r, 1), 1)) * math.exp(-gap / cfg.tau) for r, gap in rs.items())
    adj = defaultdict(set)
    for (a, b) in w:
        adj[a].add(b); adj[b].add(a)
    groups, seen = [], set()
    for start in list(adj):
        if start in seen:
            continue
        comp, stack = set(), [start]
        while stack:
            x = stack.pop()
            if x in comp:
                continue
            comp.add(x); stack.extend(adj[x] - comp)
        seen |= comp
        n = len(comp)
        n_edges = sum(1 for (a, b) in w if a in comp and b in comp)
        density = n_edges / (n * (n - 1) / 2) if n > 1 else 0.0
        shared = {r for r, us in by_r.items() if res_s.get(r, 0) == 1 and len(set(us) & comp) >= 2}
        if n >= cfg.n_min and len(shared) >= cfg.ns_min and density >= cfg.delta_min:
            groups.append({"members": sorted(comp), "density": density, "shared_sensitive": sorted(shared)})
    return groups, w


def member_features(u: str, g: dict, edges: List[dict], w: Dict, cfg: TGNv3Config, first_access: Dict = None) -> List[float]:
    """[TM_pi(eps of u's edges), A_u (Eq. 18), Nov(u, g)]."""
    eps = sorted(e["eps"] for e in edges if e["u"] == u)
    k = int(len(eps) * cfg.trim_pi)
    core = eps[k:len(eps) - k] if len(eps) > 2 * k else eps
    tm = float(np.mean(core)) if core else 0.0
    others = [v for v in g["members"] if v != u]
    a_u = sum(w.get((min(u, v), max(u, v)), 0.0) for v in others) / max(len(others), 1)
    sh = [e for e in edges if e["u"] == u and e["r"] in g["shared_sensitive"]]
    nov = float(np.mean([float(e.get("fam", 1) == 0) for e in sh])) if sh else 0.0      # first-day access (live parity)
    return [tm, a_u, nov]


@torch.no_grad()
def anchor_replay_v3(model, st: StreamV3, cfg: TGNv3Config, device, anchors: List[Tuple[float, str]]):
    """Replay the whole stream (no gradients); at each anchor (t, seed user) compute eps for the window edges of
    the seed and of the users sharing its resources, then H_W, groups and member features.
    Nov uses the graph index's familiarity (fam == 0: first day the user accessed the resource), as live.
    Returns one record per anchor: {seed, t, edges, groups, w}."""
    model.eval()
    ms = MemoryStateV3(st, cfg, device)
    anchors = sorted(anchors)
    out, ai = [], 0
    seen_access = set()
    first_access = {}                                             # (user, r*) -> 1 if the user's first access
    W = cfg.window
    for b0 in range(0, len(st.t), 256):
        idx = np.arange(b0, min(b0 + 256, len(st.t)))
        bt = st.t[idx[0]] if len(idx) else float("inf")
        M2 = ms.apply_pending(model, grad=False)
        while ai < len(anchors) and anchors[ai][0] <= bt:
            t, seed = anchors[ai]; ai += 1
            out.append(_score_anchor(model, ms, M2, st, cfg, device, t, seed, first_access))
        if not len(idx):
            break
        for i in idx:
            key = (st.u[i], st.r[i])
            if key not in seen_access:
                seen_access.add(key); first_access[(st.users[st.u[i]], st.res[st.r[i] - st.U])] = 1
            else:
                first_access.setdefault((st.users[st.u[i]], st.res[st.r[i] - st.U]), 0)
        ms.commit(M2, idx)
    if ai < len(anchors):                                         # anchors after the last batch's first event
        M2 = ms.apply_pending(model, grad=False)
        while ai < len(anchors):
            t, seed = anchors[ai]; ai += 1
            out.append(_score_anchor(model, ms, M2, st, cfg, device, t, seed, first_access))
    return out


def _score_anchor(model, ms, M2, st, cfg, device, t, seed, first_access):
    W = cfg.window
    lo, hi = np.searchsorted(st.t, [t - W, t], side="right")
    win = np.arange(lo, hi)
    if seed not in st.uid or not len(win):
        return {"seed": seed, "t": t, "edges": [], "groups": [], "w": {}}
    su = st.uid[seed]
    seed_res = set(st.r[win][st.u[win] == su])
    users = {su}
    for r in seed_res:                                            # sampled neighbourhood: K most recent users per r
        us = st.u[win][st.r[win] == r]
        users |= set(us[-cfg.n_neighbors:].tolist())
    sel = win[np.isin(st.u[win], list(users))]
    if not len(sel):
        return {"seed": seed, "t": t, "edges": [], "groups": [], "w": {}}
    nodes = np.unique(np.concatenate([st.u[sel], st.r[sel]]))
    h = ms.embed(model, M2, nodes, np.full(len(nodes), t))
    pos = {n: i for i, n in enumerate(nodes)}
    hu = h[torch.tensor([pos[x] for x in st.u[sel]], device=device)]
    hr = h[torch.tensor([pos[x] for x in st.r[sel]], device=device)]
    eps = torch.sigmoid(model.dec(hu, hr, torch.from_numpy(st.f[sel]).to(device))).cpu().numpy()
    edges = [{"u": st.users[st.u[i]], "r": st.res[st.r[i] - st.U], "t": float(st.t[i]), "eps": float(s), "coll": float(st.coll[i]),
              "fam": int(st.fam[i])}
             for i, s in zip(sel, eps)]
    deg = defaultdict(set)
    for i in win:
        deg[st.res[st.r[i] - st.U]].add(st.u[i])
    deg = {r: len(v) for r, v in deg.items()}
    res_s = {st.res[k - st.U]: v for k, v in st.res_s.items()}
    groups, w = coordination_groups(edges, cfg, deg, res_s)
    feats = {}
    for g in groups:
        for m in g["members"]:
            feats[m] = member_features(m, g, edges, w, cfg, first_access)
    return {"seed": seed, "t": t, "edges": edges, "groups": groups, "w": {f"{a}|{b}": v for (a, b), v in w.items()},
            "features": feats}

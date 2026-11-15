"""Supervised retraining data from ANALYST-VERIFIED feedback only (D3 Step 9: "only analyst-verified outcomes are
promoted to the training set; unverified model decisions are retained for monitoring and drift measurement but
are excluded from supervised updates").

Source: the HDFS `analyst-feedback` dataset (written by the HDFS sink from Kafka `analyst-feedback`, which only the
dashboard API publishes, for authenticated analysts). Model outputs (risk scores, group scores, PDP decisions,
incidents without feedback) are never read here.

Label mapping:  confirmed_malicious -> 1   false_positive, benign_explained -> 0 (hard negatives)
                needs_more_info / anything else -> excluded
Scope: individual feedback labels the users' access events inside the feedback window (TCN windows);
collective feedback labels the users' access edges inside the window (TGN graphs).
"""
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np

LABELS = {"confirmed_malicious": 1, "false_positive": 0, "benign_explained": 0}
HISTORY_DAYS = 14


def verified_feedback(lake, t0: float = 0.0, t1: Optional[float] = None) -> List[dict]:
    """Feedback payloads with a label-bearing verdict, newest version per feedback id."""
    out = {}
    for r in lake.read_records("analyst-feedback", t0, t1 or time.time() + 60):
        p = r["payload"]
        if p.get("verdict") in LABELS and p.get("analyst") and p.get("users"):
            out[p["id"]] = p
    return sorted(out.values(), key=lambda p: p["created_at"])


def summary(rows: List[dict]) -> dict:
    c = defaultdict(int)
    for p in rows:
        c[f"{p['scope']}:{p['verdict']}"] += 1
    return {"feedback": len(rows), "by_scope_verdict": dict(sorted(c.items())),
            "feedback_ids": [p["id"] for p in rows]}


def tcn_examples(rows: List[dict], lake, pre, L: int = 20) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    """Windows ending at each event of a labelled user inside the feedback window (history replayed for context)."""
    from .features import UserProfile, observe
    X, y, fid = [], [], []
    for p in rows:
        if p["scope"] != "individual":
            continue
        lab = LABELS[p["verdict"]]
        for u in p["users"]:
            hist = lake.user_history(u, p["window_t0"] - HISTORY_DAYS * 86400, p["window_t1"])
            prof = UserProfile()
            for e in hist:
                observe(prof, e, L)
                if p["window_t0"] <= e.t <= p["window_t1"]:
                    X.append(pre.encode(list(prof.window))); y.append(lab); fid.append(p["id"])
    if not X:
        return np.zeros((0, L, 50), np.float32), np.zeros(0, np.int64), []
    return np.stack(X).astype(np.float32), np.array(y, dtype=np.int64), fid


def tgn_items(rows: List[dict], lake, gp, anchors_per_feedback: int = 4):
    """Graph samples around collective feedback; edge label = verdict for the feedback users' edges in the window."""
    import torch
    from .tgn_graph import EdgeAnnotator, MemoryEdgeIndex, build_sample, edge_endpoints, featurize
    from .tgn_train import Item
    items = []
    for p in rows:
        if p["scope"] != "collective":
            continue
        lab = LABELS[p["verdict"]]
        t0, t1 = p["window_t0"], p["window_t1"]
        in_window = lake.read_events(t0 - gp.window, t1)
        involved = sorted({e.u for e in in_window})
        hist = lake.read_events(t0 - HISTORY_DAYS * 86400, t1, users=involved)
        ann = EdgeAnnotator()
        edges = [ann.annotate(e) for e in sorted(hist, key=lambda e: (e.t, e.event_id))]
        index = MemoryEdgeIndex(edges)
        targets = set(p["users"])
        anchors = np.linspace(t0 + gp.window / 2, t1, num=anchors_per_feedback) if t1 > t0 else [t1]
        for a in anchors:
            for seed in sorted(targets)[:2]:
                s = build_sample([seed], index, float(a), gp)
                if not s.edges:
                    continue
                yv = np.array([float(lab) if (e["u"] in targets and t0 <= e["t"] <= t1) else 0.0 for e in s.edges],
                              dtype=np.float32)
                s.label = yv
                X, A = featurize(s, gp)
                eu, er = edge_endpoints(s)
                items.append(Item(s, torch.from_numpy(X), torch.from_numpy(A), torch.from_numpy(eu), torch.from_numpy(er),
                                  torch.from_numpy(yv), f"feedback:{p['verdict']}", int(lab == 1 and seed in targets),
                                  sorted(targets) if lab == 1 else []))
    return items


def holdout_split(rows: List[dict], frac: float = 0.25, seed: int = 0) -> Tuple[List[dict], List[dict]]:
    """Split by feedback record (never the same incident on both sides). Fewer than 4 records: no holdout."""
    if len(rows) < 4:
        return rows, []
    rng = np.random.default_rng(seed)
    idx = rng.permutation(len(rows))
    k = max(1, int(round(frac * len(rows))))
    hold = {int(i) for i in idx[:k]}
    return [r for i, r in enumerate(rows) if i not in hold], [r for i, r in enumerate(rows) if i in hold]

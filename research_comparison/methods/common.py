"""Shared, label-safe helpers for AccessSentinel reference-method adapters."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Any
import hashlib
import numpy as np

def event_time(e) -> float:
    return float(getattr(e, "t", 0.0))

def user_id(e) -> str:
    return str(getattr(e, "u", ""))

def resource_id(e) -> str:
    return f"{getattr(e, 'tenant', 'default')}/{getattr(e, 'a', '')}:{getattr(e, 'r', '')}"

def operation(e) -> str:
    return str(getattr(e, "o", "unknown"))

def event_token(e) -> list[str]:
    """Categorical, non-label tokens available in the AccessSentinel event schema."""
    c = getattr(e, "c", {}) or {}
    return [
        f"app={getattr(e, 'a', '')}",
        f"resource={getattr(e, 'r', '')}",
        f"operation={getattr(e, 'o', '')}",
        f"permission={getattr(e, 'p', '')}",
        f"sensitivity={int(getattr(e, 's', 0))}",
        f"decision={'allow' if int(getattr(e, 'y', 1)) else 'deny'}",
        f"device={c.get('device', 'unknown')}",
        f"ip_prefix={str(c.get('ip', 'unknown')).rsplit('.', 1)[0]}",
    ]

def stable_hash(text: str, buckets: int = 128) -> int:
    return int(hashlib.blake2b(text.encode("utf-8"), digest_size=8).hexdigest(), 16) % buckets

@dataclass
class AccessGraph:
    node_ids: list[str]
    node_types: list[str]
    features: np.ndarray
    edge_src: np.ndarray
    edge_dst: np.ndarray
    edge_types: list[str]
    edge_times: np.ndarray
    edge_labels: np.ndarray | None = None

def build_user_resource_graph(events: Iterable[Any], feature_dim: int = 128) -> AccessGraph:
    """Bipartite user-resource graph; labels are deliberately not encoded as features."""
    events = sorted(events, key=lambda e: (event_time(e), str(getattr(e, "event_id", ""))))
    nodes, types, feats, src, dst, etypes, times, labels = [], [], [], [], [], [], [], []
    index = {}
    def add_node(key, typ):
        if key not in index:
            index[key] = len(nodes); nodes.append(key); types.append(typ); feats.append(np.zeros(feature_dim, np.float32))
        return index[key]
    for e in events:
        ukey = f"user:{user_id(e)}"
        rkey = f"resource:{resource_id(e)}"
        ui, ri = add_node(ukey, "user"), add_node(rkey, "resource")
        for token in event_token(e):
            feats[ui][stable_hash(token, feature_dim)] += 1.0
            feats[ri][stable_hash(token, feature_dim)] += 1.0
        src.append(ui); dst.append(ri); etypes.append(operation(e)); times.append(event_time(e))
        src.append(ri); dst.append(ui); etypes.append(operation(e) + "_rev"); times.append(event_time(e))
        # Never use labels as model inputs; attach only as optional edge-level evaluation target.
        lab = getattr(e, "label", None)
        if lab is not None: labels.append(int(lab))
    edge_labels = np.asarray(labels, dtype=np.int64) if len(labels) == len(src) else None
    return AccessGraph(nodes, types, np.asarray(feats, np.float32),
                       np.asarray(src, np.int64), np.asarray(dst, np.int64),
                       etypes, np.asarray(times, np.float64), edge_labels)

"""Leak-free window dataset for the TCN.

* Events are replayed in time order through per-user profiles exactly as the live worker does (`observe`).
* The Preprocessor (vocabulary + scaler) is fitted on records of TRAINING-period events only.
* Sample i = the window ending at event i; its label = individual label of event i; its split = the split of
  event i's timestamp. Windows may include earlier events from a previous period (that is the past, which is
  also available in deployment); no window's label comes from another split.
* Memory: each event is encoded once (`rows`); windows are gathered per batch from index arrays.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

import numpy as np

from ..events import AccessEvent
from .features import D, L_DEFAULT, Preprocessor, UserProfile, observe


@dataclass
class WindowIndex:
    rows: np.ndarray            # (N, D) encoded events, grouped by user in time order
    end: np.ndarray             # (N,) row index of the newest event of sample i
    user_start: np.ndarray      # (N,) first row of that user's block
    y: np.ndarray               # (N,) individual label
    split: np.ndarray           # (N,) "train" | "val" | "test"
    users: np.ndarray
    t: np.ndarray
    event_ids: np.ndarray
    scenario: np.ndarray
    instance: np.ndarray
    L: int = L_DEFAULT

    def __len__(self):
        return len(self.end)

    def gather(self, idx: np.ndarray) -> np.ndarray:
        """(B, L, D) windows for samples idx, zero-padded at the front."""
        offs = np.arange(self.L - 1, -1, -1)
        r = self.end[idx][:, None] - offs[None, :]
        valid = r >= self.user_start[idx][:, None]
        X = self.rows[np.where(valid, r, 0)]
        X[~valid] = 0.0
        return X.astype(np.float32, copy=False)

    def last_step(self, idx: np.ndarray) -> np.ndarray:
        return self.rows[self.end[idx]]

    def mask(self, split: str) -> np.ndarray:
        return np.where(self.split == split)[0]


def replay(events: Sequence[AccessEvent], L: int = L_DEFAULT):
    """Online feature extraction over a time-ordered event list. Returns raw records in input order."""
    profiles: Dict[str, UserProfile] = {}
    recs = []
    for ev in events:
        p = profiles.setdefault(ev.u, UserProfile())
        recs.append(observe(p, ev, L))
    return recs, profiles


def build(events: Sequence[AccessEvent], labels: Dict[str, object], split_of, L: int = L_DEFAULT,
          pre: Optional[Preprocessor] = None):
    """labels: event_id -> object with .individual/.scenario/.instance; split_of: t -> split name."""
    events = sorted(events, key=lambda e: (e.t, e.event_id))
    recs, _ = replay(events, L)
    split = np.array([split_of(e.t) for e in events])
    if pre is None:
        pre = Preprocessor.fit([r for r, s in zip(recs, split) if s == "train"], L)
    enc = np.stack([pre.encode_record(r) for r in recs]) if recs else np.zeros((0, D), np.float32)
    users = np.array([e.u for e in events])
    order = np.lexsort((np.arange(len(events)), users))       # group rows by user, keep time order
    pos = np.empty_like(order); pos[order] = np.arange(len(order))
    rows = enc[order]
    sorted_users = users[order]
    first = {}
    for i, u in enumerate(sorted_users):
        first.setdefault(u, i)
    end = pos
    user_start = np.array([first[u] for u in users])
    lab = [labels.get(e.event_id) for e in events]
    idx = WindowIndex(
        rows=rows, end=end, user_start=user_start,
        y=np.array([int(getattr(l, "individual", 0) or 0) for l in lab], dtype=np.int64),
        split=split, users=users, t=np.array([e.t for e in events]), event_ids=np.array([e.event_id for e in events]),
        scenario=np.array([getattr(l, "scenario", "unlabelled") for l in lab]),
        instance=np.array([getattr(l, "instance", "") for l in lab]), L=L)
    return idx, pre, recs

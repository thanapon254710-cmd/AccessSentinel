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


# ============================================================================================================
# Revised v3 (Phase 3): L = 128 windows of Table III records, chronological split with a gap (Eq. 7)
# ============================================================================================================
"""
Eq. 7: D_train = [t0, t1), D_val = [t1 + g, t2), D_test = [t2 + g, t3] with g >= L events.
Implemented per user: a validation / test window is EVALUATED only if all of its L events lie inside its own
split, i.e. the first L - 1 events of each user in that split form the gap (they are still context for later
windows of the same split). No evaluated window therefore shares an event with a training window, and
self-supervised next-event targets never cross a split boundary. The encoder (vocabularies, scaler, peer
table) is fitted on training records only. Incidents stay inside one split by construction of the generator
(attacks are scheduled inside their split, after the gap: SynthConfig.attack_margin_days).
"""
from .features import L_V3, N_NUM, N_Z, EncoderV3, UserStateV3, observe_v3  # noqa: E402


@dataclass
class WindowIndexV3:
    res: np.ndarray             # (N,) categorical rows grouped by user in time order
    op: np.ndarray
    net: np.ndarray
    num: np.ndarray             # (N, 19)
    z: np.ndarray               # (N, 4)  timer context at each event
    end: np.ndarray             # (N,) sample i -> row of its newest event
    user_start: np.ndarray      # (N,) first row of the user's block
    split_start: np.ndarray     # (N,) first row of the user's run in the sample's split
    next_res: np.ndarray        # (N,) targets of the self-supervised heads (by sample)
    next_op: np.ndarray
    next_gap: np.ndarray
    next_mask: np.ndarray       # (N,) bool: a next event exists in the same split
    evaluable: np.ndarray       # (N,) bool: train sample, or window fully inside its val/test split (gap rule)
    y: np.ndarray
    split: np.ndarray
    users: np.ndarray
    t: np.ndarray
    event_ids: np.ndarray
    scenario: np.ndarray
    instance: np.ndarray
    L: int = L_V3

    def __len__(self):
        return len(self.end)

    def mask(self, split: str, evaluable_only: bool = True) -> np.ndarray:
        m = self.split == split
        if evaluable_only:
            m &= self.evaluable
        return np.where(m)[0]

    def gather(self, idx: np.ndarray) -> dict:
        offs = np.arange(self.L - 1, -1, -1)
        r = self.end[idx][:, None] - offs[None, :]
        valid = r >= self.user_start[idx][:, None]
        rr = np.where(valid, r, 0)
        res, op, net = self.res[rr], self.op[rr], self.net[rr]
        num = self.num[rr]
        res[~valid] = 0; op[~valid] = 0; net[~valid] = 0; num[~valid] = 0.0
        return {"res": res, "op": op, "net": net, "num": num.astype(np.float32, copy=False),
                "z": self.z[self.end[idx]], "y": self.y[idx].astype(np.float32),
                "next_res": self.next_res[idx], "next_op": self.next_op[idx],
                "next_gap": self.next_gap[idx].astype(np.float32), "next_mask": self.next_mask[idx]}


def build_v3(events: Sequence[AccessEvent], labels: Dict[str, object], split_of, threat_fn, posture_fn,
             L: int = L_V3, enc: Optional[EncoderV3] = None):
    events = sorted(events, key=lambda e: (e.t, e.event_id))
    states: Dict[str, UserStateV3] = {}
    recs = []
    for ev in events:
        st = states.setdefault(ev.u, UserStateV3())
        rec, _ = observe_v3(st, ev, threat_fn, posture_fn, L)
        recs.append(rec)
    split = np.array([split_of(e.t) for e in events])
    if enc is None:
        enc = EncoderV3.fit([r for r, s in zip(recs, split) if s == "train"])
    n = len(events)
    res = np.zeros(n, np.int64); op = np.zeros(n, np.int64); net = np.zeros(n, np.int64)
    num = np.zeros((n, N_NUM), np.float32); z = np.zeros((n, N_Z), np.float32)
    for i, r in enumerate(recs):
        res[i], op[i], net[i], num[i], z[i] = enc.encode_record(r)
    users = np.array([e.u for e in events])
    order = np.lexsort((np.arange(n), users))                  # rows grouped by user, time order kept
    pos = np.empty_like(order); pos[order] = np.arange(n)
    su, ssplit = users[order], split[order]
    ts = np.array([e.t for e in events])
    user_start_row = np.zeros(n, np.int64); split_start_row = np.zeros(n, np.int64)
    for k in range(n):
        new_user = k == 0 or su[k] != su[k - 1]
        user_start_row[k] = k if new_user else user_start_row[k - 1]
        split_start_row[k] = k if (new_user or ssplit[k] != ssplit[k - 1]) else split_start_row[k - 1]
    # self-supervised targets: next row of the same user in the same split
    nxt_ok = np.zeros(n, bool); nxt_ok[:-1] = (su[1:] == su[:-1]) & (ssplit[1:] == ssplit[:-1])
    rows_res, rows_op, rows_t = res[order], op[order], ts[order]
    nres = np.zeros(n, np.int64); nop = np.zeros(n, np.int64); ngap = np.zeros(n, np.float32)
    nres[:-1], nop[:-1] = rows_res[1:], rows_op[1:]
    ngap[:-1] = np.log1p(np.maximum(rows_t[1:] - rows_t[:-1], 0.0))
    end = pos
    evaluable = (split == "train") | ((end - (L - 1)) >= split_start_row[end])
    lab = [labels.get(e.event_id) for e in events]
    idx = WindowIndexV3(
        res=rows_res, op=rows_op, net=net[order], num=num[order], z=z[order], end=end,
        user_start=user_start_row[end], split_start=split_start_row[end],
        next_res=nres[end], next_op=nop[end], next_gap=ngap[end], next_mask=nxt_ok[end], evaluable=evaluable,
        y=np.array([int(getattr(l, "individual", 0) or 0) for l in lab], dtype=np.int64), split=split, users=users,
        t=ts, event_ids=np.array([e.event_id for e in events]),
        scenario=np.array([getattr(l, "scenario", "unlabelled") for l in lab]),
        instance=np.array([getattr(l, "instance", "") for l in lab]), L=L)
    return idx, enc, recs

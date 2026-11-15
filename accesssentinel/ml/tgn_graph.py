"""Temporal interaction graph for the TGN (D3 Step 7, Eq. 5: G_t = (V_t, E_t)).

Nodes V = users ∪ resources ∪ applications (events are NOT nodes). Edges E = access (user -> resource, with
timestamp and attributes) ∪ membership (resource -> application) ∪ co-access (users who touched the same
resource, or the same application, inside the observation window).

Shared by offline training and the live workers (same code -> same features):
  * `EdgeAnnotator`  turns each access event, in time order, into an edge with label-free attributes:
                     s, o, y, hour, `fam` = number of distinct EARLIER days the user accessed this resource
                     (a habit measured in days, so one burst cannot make a resource "familiar"), and `seq` =
                     object id adjacent to the user's previous object on that resource (enumeration).
  * `build_sample`   bounded neighbourhood N(u) of the seed users in [t0, t1]: their resources (most recent
                     first, <= max_resources), co-accessors of those resources (<= max_users_per_resource
                     each, most recent first), application nodes.
  * `featurize`      T = 10 time slices; per slice a normalised adjacency (access edges active in the slice +
                     static membership/co-access edges) and NODE_FEATURES features per node.
"""
import bisect
import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

from ..events import AccessEvent

NODE_FEATURES = 17
CONCURRENCY_S = 600.0            # [IMPL] two users' accesses to one resource within 10 min are "concurrent"
OFF_HOURS = (7.0, 20.0)


@dataclass
class GraphParams:
    window: float = 3600.0               # [IMPL] observation window ending at the anchor time
    max_resources: int = 20              # [IMPL] neighbourhood bounds (keep N small: dense GCN)
    max_users_per_resource: int = 8      # [IMPL]
    app_coaccess_weight: float = 0.25    # [IMPL] weight of user-user edges through a shared application
    T: int = 10                          # [D3] attention window, time steps


class EdgeAnnotator:
    """Online, label-free edge attributes. `state` maps (u, rkey) -> [distinct_days, last_day, last_obj];
    a dict offline, a Redis-backed mapping live (same logic)."""

    def __init__(self, state=None):
        self.state = state if state is not None else {}

    def annotate(self, ev: AccessEvent) -> dict:
        key = (ev.u, ev.rkey)
        days, last_day, last_obj = self.state.get(key, (0, -1, None))
        day = int(ev.t // 86400)
        fam = days - (1 if last_day == day else 0)              # distinct days BEFORE today
        obj = ev.meta.get("object_id") if isinstance(ev.meta, dict) else None
        try:
            obj = int(obj) if obj is not None else None
        except (TypeError, ValueError):
            obj = None
        seq = int(obj is not None and last_obj is not None and abs(obj - last_obj) == 1)
        self.state[key] = (days + (0 if last_day == day else 1), day, obj if obj is not None else last_obj)
        return {"id": ev.event_id, "u": ev.u, "r": ev.rkey, "a": ev.a, "t": float(ev.t), "s": int(ev.s), "o": ev.o,
                "y": int(ev.y), "fam": int(fam), "seq": seq, "hour": (ev.t / 3600.0) % 24.0}


class MemoryEdgeIndex:
    """Offline edge store with time-range queries by user and by resource."""

    def __init__(self, edges: Sequence[dict]):
        self.by_user, self.by_res = defaultdict(list), defaultdict(list)
        for e in sorted(edges, key=lambda e: (e["t"], e["id"])):
            self.by_user[e["u"]].append(e); self.by_res[e["r"]].append(e)
        self._ut = {u: [e["t"] for e in es] for u, es in self.by_user.items()}
        self._rt = {r: [e["t"] for e in es] for r, es in self.by_res.items()}

    @staticmethod
    def _slice(lst, ts, t0, t1):
        return lst[bisect.bisect_left(ts, t0): bisect.bisect_right(ts, t1)]

    def user_edges(self, u: str, t0: float, t1: float) -> List[dict]:
        return self._slice(self.by_user.get(u, []), self._ut.get(u, []), t0, t1)

    def resource_edges(self, r: str, t0: float, t1: float) -> List[dict]:
        return self._slice(self.by_res.get(r, []), self._rt.get(r, []), t0, t1)


@dataclass
class GraphSample:
    users: List[str]
    resources: List[str]
    apps: List[str]
    seeds: List[str]
    t0: float
    t1: float
    edges: List[dict]                       # access edges (dicts from EdgeAnnotator), time-ordered
    co_res: Dict[Tuple[int, int], int] = field(default_factory=dict)
    co_app: Dict[Tuple[int, int], int] = field(default_factory=dict)
    label: Optional[np.ndarray] = None      # per access edge (training/evaluation only)

    @property
    def n_users(self): return len(self.users)
    @property
    def n_nodes(self): return len(self.users) + len(self.resources) + len(self.apps)


def build_sample(seeds: Sequence[str], index, t1: float, gp: GraphParams) -> GraphSample:
    """Bounded neighbourhood N(seeds) over [t1 - window, t1]; deterministic for a given index state."""
    t0 = t1 - gp.window
    seeds = sorted(set(seeds))
    last_use: Dict[str, float] = {}
    for u in seeds:
        for e in index.user_edges(u, t0, t1):
            last_use[e["r"]] = max(last_use.get(e["r"], -1.0), e["t"])
    resources = sorted(last_use, key=lambda r: (-last_use[r], r))[: gp.max_resources]
    users = set(seeds)
    res_edges = {}
    for r in resources:
        es = index.resource_edges(r, t0, t1)
        res_edges[r] = es
        last = {}
        for e in es:
            if e["u"] not in seeds:
                last[e["u"]] = max(last.get(e["u"], -1.0), e["t"])
        users |= set(sorted(last, key=lambda u: (-last[u], u))[: gp.max_users_per_resource])
    edges = sorted((e for r in resources for e in res_edges[r] if e["u"] in users), key=lambda e: (e["t"], e["id"]))
    users_l = sorted({e["u"] for e in edges} | set(seeds))
    res_l = sorted({e["r"] for e in edges})
    apps_l = sorted({e["a"] for e in edges})
    ui = {u: i for i, u in enumerate(users_l)}
    by_res, by_app = defaultdict(set), defaultdict(set)
    for e in edges:
        by_res[e["r"]].add(ui[e["u"]]); by_app[e["a"]].add(ui[e["u"]])
    co_res, co_app = defaultdict(int), defaultdict(int)
    for group, out in ((by_res, co_res), (by_app, co_app)):
        for us in group.values():
            us = sorted(us)
            for x in range(len(us)):
                for y in range(x + 1, len(us)):
                    out[(us[x], us[y])] += 1
    return GraphSample(users_l, res_l, apps_l, [s for s in seeds if s in ui], t0, t1, edges, dict(co_res), dict(co_app))


def edge_endpoints(s: GraphSample) -> Tuple[np.ndarray, np.ndarray]:
    ui = {u: i for i, u in enumerate(s.users)}
    ri = {r: s.n_users + i for i, r in enumerate(s.resources)}
    return (np.array([ui[e["u"]] for e in s.edges], dtype=np.int64),
            np.array([ri[e["r"]] for e in s.edges], dtype=np.int64))


def featurize(s: GraphSample, gp: GraphParams) -> Tuple[np.ndarray, np.ndarray]:
    """X: (T, N, NODE_FEATURES), A: (T, N, N) symmetric-normalised with self-loops."""
    T, N, U, R = gp.T, s.n_nodes, s.n_users, len(s.resources)
    X = np.zeros((T, N, NODE_FEATURES), dtype=np.float32)
    A = np.zeros((T, N, N), dtype=np.float32)
    X[:, :U, 0] = 1; X[:, U:U + R, 1] = 1; X[:, U + R:, 2] = 1
    eu, er = edge_endpoints(s)
    ai = {a: U + R + i for i, a in enumerate(s.apps)}
    ea = np.array([ai[e["a"]] for e in s.edges], dtype=np.int64)
    static = np.zeros((N, N), dtype=np.float32)
    for e, r_node in zip(s.edges, er):
        static[r_node, ai[e["a"]]] = static[ai[e["a"]], r_node] = 1.0           # membership
    for (i, j), c in s.co_res.items():
        static[i, j] = static[j, i] = max(static[i, j], min(c, 3) / 3.0)        # co-access via a resource
    for (i, j), c in s.co_app.items():
        if static[i, j] == 0:
            static[i, j] = static[j, i] = gp.app_coaccess_weight                # co-access via an application
    span = max(s.t1 - s.t0, 1e-6)
    sl = np.clip(((np.array([e["t"] for e in s.edges]) - s.t0) / span * T).astype(int), 0, T - 1) if s.edges else np.zeros(0, int)
    # whole-window node statistics
    nbrs = [set() for _ in range(N)]
    deg = np.zeros(N); apps_of = defaultdict(set); res_of = defaultdict(set); users_of = defaultdict(set)
    conc = np.zeros(N); times_by_res = defaultdict(list)
    for k, e in enumerate(s.edges):
        u, r = eu[k], er[k]
        nbrs[u].add(r); nbrs[r].add(u); deg[u] += 1; deg[r] += 1; deg[ea[k]] += 1
        apps_of[u].add(e["a"]); res_of[u].add(e["r"]); users_of[r].add(e["u"])
        times_by_res[r].append((e["t"], u))
    for k, e in enumerate(s.edges):                                             # synchronised co-access
        u, r = eu[k], er[k]
        if any(v != u and abs(t - e["t"]) <= CONCURRENCY_S for t, v in times_by_res[r]):
            conc[u] += 1; conc[r] += 1
    burst = np.zeros(N)
    ts_by_node = defaultdict(list)
    for k, e in enumerate(s.edges):
        ts_by_node[eu[k]].append(e["t"]); ts_by_node[er[k]].append(e["t"])
    for n, ts in ts_by_node.items():
        ts = sorted(ts)
        best = max(bisect.bisect_right(ts, t + 900) - i for i, t in enumerate(ts))
        burst[n] = best / len(ts)
    for t in range(T):
        A[t] = static
        cnt = np.zeros(N); acc = np.zeros((N, 6))
        for k in np.where(sl == t)[0]:
            e = s.edges[k]
            u, r = eu[k], er[k]
            A[t, u, r] = A[t, r, u] = 1.0
            vals = (e["s"], 1 - e["y"], e["o"] == "export", e["fam"] <= 1, e["seq"],
                    not (OFF_HOURS[0] <= e["hour"] < OFF_HOURS[1]))
            for n in (u, r, ea[k]):
                cnt[n] += 1; acc[n] += vals
        nz = np.maximum(cnt, 1)[:, None]
        X[t, :, 3] = np.log1p(cnt)
        X[t, :, 4:9] = (acc[:, :5] / nz)
        X[t, :, 11] = acc[:, 5] / nz[:, 0]
        X[t, :, 12] = t / max(T - 1, 1)
        Ah = A[t] + np.eye(N, dtype=np.float32)
        d = Ah.sum(1)
        A[t] = Ah / np.sqrt(np.outer(d, d))
    X[:, :, 9] = np.log1p(deg)[None]
    X[:, :, 10] = np.log1p([len(x) for x in nbrs])[None]
    for i in range(U):
        X[:, i, 13] = math.log1p(len(res_of[i])); X[:, i, 14] = math.log1p(len(apps_of[i]))
    for j in range(R):
        X[:, U + j, 13] = math.log1p(len(users_of[U + j]))
    X[:, :, 15] = burst[None]
    X[:, :, 16] = (conc / np.maximum(deg, 1))[None]
    return X, A

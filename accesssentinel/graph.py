"""Step 7 graph: nodes = users, resources, applications (events are NOT nodes).
Edges = access (user->resource, timestamped, with attributes), membership (resource->app),
co-access (users sharing a resource, or an application, in the observation window)."""
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Tuple
import numpy as np
from .config import GraphConfig
from .events import AccessEvent


@dataclass
class GraphSample:
    users: List[str]
    resources: List[str]
    apps: List[str]
    seeds: List[str]
    t0: float
    t1: float
    e_user: np.ndarray            # access edges: node indices
    e_res: np.ndarray
    e_app: np.ndarray
    e_t: np.ndarray
    e_s: np.ndarray
    e_y: np.ndarray
    e_eid: np.ndarray
    mem_res: np.ndarray           # membership edges
    mem_app: np.ndarray
    co_res: Dict[Tuple[int, int], int]    # (user_i, user_j) -> #shared resources
    co_sens: Dict[Tuple[int, int], int]   # ... of which sensitive
    co_app: Dict[Tuple[int, int], int]    # -> #shared applications
    e_label: np.ndarray = None    # ground-truth collusion label per access edge (training only)

    @property
    def n_users(self): return len(self.users)
    @property
    def n_nodes(self): return len(self.users) + len(self.resources) + len(self.apps)
    @property
    def n_edges(self): return len(self.e_user)
    def res_node(self, i): return self.n_users + i
    def app_node(self, i): return self.n_users + len(self.resources) + i


class NeighborhoodBuilder:
    def __init__(self, cfg: GraphConfig):
        self.cfg = cfg

    def build(self, seeds, events: List[AccessEvent], t0: float, t1: float) -> GraphSample:
        ev = [e for e in events if t0 <= e.t <= t1]
        by_user, by_res = defaultdict(list), defaultdict(list)
        for e in ev:
            by_user[e.u].append(e); by_res[e.rkey].append(e)
        seeds = sorted(set(seeds))
        last = {}
        for u in seeds:
            for e in by_user.get(u, []): last[e.rkey] = max(last.get(e.rkey, -1), e.t)
        res_sel = sorted(last, key=lambda k: (-last[k], k))[: self.cfg.max_resources]
        res_set = set(res_sel)
        users = set(seeds)
        for k in res_sel:
            others = sorted({e.u for e in by_res[k]} - set(seeds))
            users |= set(others[: self.cfg.max_users_per_resource])
        sel = [e for e in ev if e.u in users and e.rkey in res_set]
        users = sorted({e.u for e in sel} | (set(seeds) & {e.u for e in sel}))
        resources = sorted({e.rkey for e in sel})
        apps = sorted({e.a for e in sel})
        ui = {u: i for i, u in enumerate(users)}; ri = {r: i for i, r in enumerate(resources)}
        ai = {a: i for i, a in enumerate(apps)}
        arr = lambda f, dt=float: np.array([f(e) for e in sel], dtype=dt)
        mem = sorted({(ri[e.rkey], ai[e.a]) for e in sel})
        # co-access edges
        ures, uapp, sens = defaultdict(set), defaultdict(set), {}
        for e in sel:
            ures[e.rkey].add(ui[e.u]); uapp[e.a].add(ui[e.u]); sens[e.rkey] = e.s
        co_res, co_sens, co_app = defaultdict(int), defaultdict(int), defaultdict(int)
        for k, us in ures.items():
            us = sorted(us)
            for x in range(len(us)):
                for y in range(x + 1, len(us)):
                    co_res[(us[x], us[y])] += 1; co_sens[(us[x], us[y])] += sens[k]
        for a, us in uapp.items():
            us = sorted(us)
            for x in range(len(us)):
                for y in range(x + 1, len(us)):
                    co_app[(us[x], us[y])] += 1
        return GraphSample(
            users, resources, apps, [s for s in seeds if s in ui], t0, t1,
            arr(lambda e: ui[e.u], int), arr(lambda e: ri[e.rkey], int), arr(lambda e: ai[e.a], int),
            arr(lambda e: e.t), arr(lambda e: e.s), arr(lambda e: e.y), arr(lambda e: e.eid, int),
            np.array([m[0] for m in mem], dtype=int), np.array([m[1] for m in mem], dtype=int),
            dict(co_res), dict(co_sens), dict(co_app), np.zeros(len(sel)))


def groups_from_graph(sample: GraphSample, min_shared_sensitive: int):
    """Behaviourally coupled groups g(u): connected components of the user co-access graph
    restricted to pairs sharing >= min_shared_sensitive sensitive resources."""
    parent = list(range(sample.n_users))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]; x = parent[x]
        return x
    for (i, j), c in sample.co_sens.items():
        if c >= min_shared_sensitive:
            parent[find(i)] = find(j)
    comps = defaultdict(list)
    for i in range(sample.n_users): comps[find(i)].append(i)
    return list(comps.values())

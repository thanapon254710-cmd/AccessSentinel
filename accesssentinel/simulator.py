"""Synthetic multi-application access logs with ground truth, for development only (not real data).
Contains: normal users, individually-anomalous attackers (compromised accounts), benign flash crowds,
and colluding groups whose members each look normal on their own."""
from dataclasses import dataclass, field
from typing import Dict, List
import numpy as np
from .events import AccessEvent

DAY = 86400.0


@dataclass
class SimConfig:
    n_users: int = 120
    n_apps: int = 3
    res_per_app: int = 25
    n_sens: int = 5                 # first n_sens resources of every app are sensitive
    days: int = 4
    events_per_day: float = 25.0
    n_train_attackers: int = 4
    n_test_attackers: int = 4
    attack_hours: float = 2.0
    attack_rate_per_h: float = 40.0
    group_size: int = 4
    group_targets: int = 4
    bursts_per_group: int = 4
    n_benign_crowds: int = 8
    seed: int = 0


@dataclass
class SimResult:
    events: List[AccessEvent]
    truth: Dict[int, tuple]                      # eid -> (individual_anomaly, collusion)
    attackers: Dict[str, list]                   # user -> [(t0, t1)]
    groups: Dict[str, list]                      # group -> members
    split_t: float
    users: List[str]
    train_groups: List[str]
    test_groups: List[str]
    train_attackers: List[str]
    test_attackers: List[str]

    @property
    def colluders(self): return {u for g in self.groups.values() for u in g}


def simulate(cfg: SimConfig) -> SimResult:
    rng = np.random.default_rng(cfg.seed)
    apps = [f"app{i+1}" for i in range(cfg.n_apps)]
    res = lambda i: f"res{i:02d}"
    sens = lambda i: 1 if i < cfg.n_sens else 0
    users = [f"u{i:03d}" for i in range(cfg.n_users)]
    home = {u: (f"10.{rng.integers(0,200)}.{rng.integers(0,250)}", f"dev{rng.integers(0,5000)}") for u in users}
    habit = {}
    for u in users:
        non = [(a, i) for a in apps for i in rng.choice(np.arange(cfg.n_sens, cfg.res_per_app), 3, replace=False)]
        habit[u] = {"non": [non[i] for i in rng.choice(len(non), 6, replace=False)],
                    "sens": (apps[rng.integers(cfg.n_apps)], int(rng.integers(cfg.n_sens)))}
    raw = []   # (event, ind, coll)

    def mk(u, t, a, i, p, o, ip, dev, y, ind=0, coll=0):
        raw.append((AccessEvent(u, float(t), a, res(i), sens(i), p, o, {"ip": ip, "device": dev}, y), ind, coll))

    # --- normal behaviour: working hours, habitual resources, home ip/device
    for u in users:
        ip, dev = home[u]
        for d in range(cfg.days):
            n = rng.poisson(cfg.events_per_day)
            ts = np.sort(d * DAY + 8 * 3600 + rng.random(n) * 10 * 3600)
            for t in ts:
                x = rng.random()
                if x < 0.93: a, i = habit[u]["non"][rng.integers(6)]
                elif x < 0.96: a, i = habit[u]["sens"]
                elif x < 0.97: a, i = apps[rng.integers(cfg.n_apps)], int(rng.integers(cfg.n_sens))
                else: a, i = apps[rng.integers(cfg.n_apps)], int(rng.integers(cfg.n_sens, cfg.res_per_app))
                mk(u, t, a, i, rng.choice(["read", "write"], p=[.8, .2]), rng.choice(["view", "edit"], p=[.85, .15]),
                   ip, dev, int(rng.random() > 0.02))
    # --- benign flash crowds (several users, ONE sensitive resource, short time): should NOT be flagged
    for _ in range(cfg.n_benign_crowds):
        a, i = apps[rng.integers(cfg.n_apps)], int(rng.integers(cfg.n_sens))
        t0 = rng.integers(0, cfg.days) * DAY + 9 * 3600 + rng.random() * 8 * 3600
        for u in rng.choice(users, 5, replace=False):
            mk(u, t0 + rng.random() * 600, a, i, "read", "view", *home[u], 1)
    # --- attackers: compromised accounts, new ip/device, many sensitive resources, denials, off-hours
    split_t = (cfg.days // 2) * DAY
    pool = list(rng.permutation(users))
    tr_att = pool[: cfg.n_train_attackers]; te_att = pool[cfg.n_train_attackers: cfg.n_train_attackers + cfg.n_test_attackers]
    attackers = {}
    def attack(u, t0):
        t1 = t0 + cfg.attack_hours * 3600
        attackers.setdefault(u, []).append((t0, t1))
        ip, dev = f"172.{rng.integers(16,31)}.{rng.integers(250)}", f"dev{rng.integers(5000, 9000)}"
        for t in np.sort(t0 + rng.random(int(cfg.attack_rate_per_h * cfg.attack_hours)) * (t1 - t0)):
            mk(u, t, apps[rng.integers(cfg.n_apps)], int(rng.integers(cfg.n_sens)) if rng.random() < .8 else int(rng.integers(cfg.res_per_app)),
               rng.choice(["admin", "read", "write"]), rng.choice(["export", "delete", "view"]), ip, dev,
               int(rng.random() > 0.4), ind=1)
    train_days, test_days = list(range(0, cfg.days // 2)), list(range(cfg.days // 2, cfg.days))
    for u in tr_att:
        for d in train_days: attack(u, d * DAY + rng.random() * 20 * 3600)
    for u in te_att: attack(u, rng.choice(test_days) * DAY + rng.random() * 18 * 3600)
    # --- colluders: individually normal, but a group hits the same sensitive targets in 10-min bursts
    groups, tg, eg = {}, [], []
    members_pool = [u for u in pool[cfg.n_train_attackers + cfg.n_test_attackers:]]
    for gi in range(4):
        gname = f"G{gi}"; members = members_pool[gi * cfg.group_size:(gi + 1) * cfg.group_size]
        groups[gname] = members
        train = gi < 2; (tg if train else eg).append(gname)
        targets = [(apps[rng.integers(cfg.n_apps)], int(rng.integers(cfg.n_sens))) for _ in range(cfg.group_targets)]
        days = train_days if train else test_days
        for b in range(cfg.bursts_per_group):
            t0 = rng.choice(days) * DAY + 9 * 3600 + rng.random() * 8 * 3600
            for u in members:
                for a, i in targets:
                    mk(u, t0 + rng.random() * 600, a, i, "read", "view", *home[u], 1, coll=1)
    raw.sort(key=lambda r: r[0].t)
    events, truth = [], {}
    last, seen = {}, {}
    for k, (e, ind, coll) in enumerate(raw):
        e.eid = k
        gap = e.t - last.get(e.u, -1e9); last[e.u] = e.t
        e.c["session"] = "new" if gap > 1800 else "active"
        truth[k] = (ind, coll); events.append(e)
    return SimResult(events, truth, attackers, groups, split_t, users, tg, eg, tr_att, te_att)


def tcn_dataset(events, truth, cfg):
    """Replay events through the state store; label = individual-anomaly flag of the newest event."""
    from .features import FeatureEncoder
    from .state_store import UserStateStore
    st = UserStateStore(cfg.window_len, cfg.feature_dim, FeatureEncoder())
    X = np.zeros((len(events), cfg.window_len, cfg.feature_dim), dtype=np.float32)
    y = np.zeros(len(events), dtype=np.int64)
    for i, e in enumerate(events):
        X[i] = st.update(e); y[i] = truth[e.eid][0]
    return X, y


def tgn_training_samples(events, truth, builder, t_end, window=900.0, stride=450.0, max_seeds=25, seed=0):
    """Sliding observation windows over [0, t_end); seeds = users on multi-user sensitive resources + random users."""
    from collections import defaultdict
    rng = np.random.default_rng(seed)
    out, t = [], 0.0
    evs = [e for e in events if e.t < t_end]
    while t + window <= t_end:
        w = [e for e in evs if t <= e.t < t + window]
        if w:
            per = defaultdict(set)
            for e in w:
                if e.s == 1: per[e.rkey].add(e.u)
            seeds = {u for us in per.values() if len(us) >= 2 for u in us}
            allu = sorted({e.u for e in w})
            seeds |= set(rng.choice(allu, min(5, len(allu)), replace=False).tolist())
            seeds = sorted(seeds)[:max_seeds]
            s = builder.build(seeds, w, t, t + window)
            if s.n_edges:
                s.e_label = np.array([truth[int(i)][1] for i in s.e_eid], dtype=np.float32)
                out.append(s)
        t += stride
    return out

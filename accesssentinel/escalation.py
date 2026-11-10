"""Step 6: four-path escalation, U_t = U_high | U_res | U_sweep | U_samp (Eq. 4)."""
from collections import defaultdict, deque
from dataclasses import dataclass, field
import numpy as np
from .config import EscalationConfig
from .events import AccessEvent


@dataclass
class EscalationResult:
    high: set = field(default_factory=set)
    res: set = field(default_factory=set)
    sweep: set = field(default_factory=set)
    samp: set = field(default_factory=set)

    @property
    def union(self):
        return self.high | self.res | self.sweep | self.samp

    @property
    def realtime(self):
        return self.high | self.res

    @property
    def background(self):
        return (self.sweep | self.samp) - self.realtime


class ResourceWindowTracker:
    """A sensitive (s=1) resource accessed by more than kappa distinct users within delta_res
    escalates all participants regardless of individual score."""
    def __init__(self, kappa, delta_res):
        self.kappa, self.delta = kappa, delta_res
        self._acc = defaultdict(deque)
        self.pending = set()

    def observe(self, ev: AccessEvent):
        if ev.s != 1:
            return
        q = self._acc[ev.rkey]
        q.append((ev.t, ev.u))
        while q and q[0][0] < ev.t - self.delta:
            q.popleft()
        users = {u for _, u in q}
        if len(users) > self.kappa:
            self.pending |= users

    def drain(self):
        out, self.pending = self.pending, set()
        return out


class EscalationPolicy:
    def __init__(self, cfg: EscalationConfig, rng: np.random.Generator):
        self.cfg, self.rng = cfg, rng
        self.tracker = ResourceWindowTracker(cfg.kappa, cfg.delta_res)

    def observe(self, ev: AccessEvent):
        self.tracker.observe(ev)

    def sweep(self, t_end, store):
        """Batch pass over history: sensitive resources touched by >= sweep_min_users distinct users
        across the long window (low-and-slow / threshold-evasion). Selection rule is an [IMPL] choice."""
        evs = store.read_range(t_end - self.cfg.t_sweep, t_end)
        per = defaultdict(set)
        for e in evs:
            if e.s == 1: per[e.rkey].add(e.u)
        cands = sorted((k for k, us in per.items() if len(us) >= self.cfg.sweep_min_users),
                       key=lambda k: -len(per[k]))
        out = set()
        for k in cands:
            out |= per[k]
            if len(out) >= self.cfg.sweep_batch: break
        return set(sorted(out)[: self.cfg.sweep_batch])

    def sample(self, low_active):
        low = sorted(low_active)
        if not low or self.cfg.gamma <= 0: return set()
        k = max(1, int(round(self.cfg.gamma * len(low))))
        return set(self.rng.choice(low, size=min(k, len(low)), replace=False).tolist())

    def compute(self, t_end, risk, active_users, sweep_due, store) -> EscalationResult:
        from .risk import Tier
        res = EscalationResult()
        res.high = risk.drain_high()
        res.res = self.tracker.drain()
        res.sweep = self.sweep(t_end, store) if sweep_due else set()
        low_active = {u for u in active_users if risk.tier.get(u, Tier.LOW) == Tier.LOW}
        res.samp = self.sample(low_active)
        return res

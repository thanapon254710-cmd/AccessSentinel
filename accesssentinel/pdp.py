"""Step 8: Policy Decision Point, D_u = h(S_ind, S_coll, C) with outcomes Allow | Block."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Set
from .config import PDPConfig, RiskConfig


class Decision(str, Enum):
    ALLOW = "Allow"
    BLOCK = "Block"


@dataclass
class PolicyContext:                      # C: organisational / contextual policy
    blocklist: Set[str] = field(default_factory=set)


@dataclass
class DecisionRecord:
    u: str
    t: float
    decision: Decision
    reason: str
    s_ind: Optional[float]
    s_coll: Optional[float]


class PDP:
    def __init__(self, cfg: PDPConfig, rcfg: RiskConfig):
        self.cfg, self.rcfg = cfg, rcfg
        self._coll = {}                    # u -> (score, t)
        self.current = {}                  # u -> DecisionRecord (cache the Gateway reads at request time)

    def set_collective(self, u, s, t):
        self._coll[u] = (float(s), t)

    def collective(self, u, now):
        v = self._coll.get(u)
        if v is None or now - v[1] > self.cfg.coll_ttl: return None
        return v[0]

    def decide(self, u, s_ind, t, ctx: PolicyContext):
        """Returns (record, changed)."""
        s_coll = self.collective(u, t)
        if u in ctx.blocklist: d, why = Decision.BLOCK, "policy"
        elif s_ind is not None and s_ind >= self.rcfg.theta_high: d, why = Decision.BLOCK, "individual"
        elif s_coll is not None and s_coll >= self.cfg.theta_coll: d, why = Decision.BLOCK, "collective"
        else: d, why = Decision.ALLOW, "ok"
        rec = DecisionRecord(u, t, d, why, s_ind, s_coll)
        prev = self.current.get(u)
        self.current[u] = rec
        return rec, (prev is None or prev.decision != d)

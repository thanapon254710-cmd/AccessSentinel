"""Step 5: three-tier individual risk evaluation with Medium-tier forced re-scoring (T_recheck)."""
from enum import IntEnum
from .config import RiskConfig


class Tier(IntEnum):
    LOW = 0
    MEDIUM = 1
    HIGH = 2


class RiskMonitor:
    def __init__(self, cfg: RiskConfig):
        self.cfg = cfg
        self.score, self.tier, self.last_scored = {}, {}, {}
        self._touched_high = set()

    def classify(self, s: float) -> Tier:
        if s >= self.cfg.theta_high: return Tier.HIGH
        if s >= self.cfg.theta_low: return Tier.MEDIUM
        return Tier.LOW

    def update(self, u: str, s: float, t: float) -> Tier:
        tier = self.classify(s)
        self.score[u], self.tier[u], self.last_scored[u] = float(s), tier, t
        if tier == Tier.HIGH: self._touched_high.add(u)
        return tier

    def due_rechecks(self, now: float):
        return [u for u, tr in self.tier.items()
                if tr == Tier.MEDIUM and now - self.last_scored[u] >= self.cfg.t_recheck]

    def drain_high(self):
        out, self._touched_high = self._touched_high, set()
        return out

    def users_in(self, tier: Tier):
        return {u for u, t in self.tier.items() if t == tier}

"""Runtime-editable settings (dashboard Settings page) and their propagation to running services.

The dashboard writes validated values into the Redis hash `settings:runtime` (and a PostgreSQL history row) and
bumps `settings:version`. The PDP, escalation and TCN services poll the version on every tick and apply the new
effective values to their in-memory configuration - no restart needed. Unset keys keep the research defaults
from config.py ([D3] / [IMPL]).
"""
import json
import time
from dataclasses import replace
from typing import Any, Dict, Optional, Tuple

from .config import EscalationConfig, PDPConfig, RiskConfig

K_HASH = "settings:runtime"
K_VERSION = "settings:version"

# key -> (type, min, max, default-from-config, description)
SCHEMA: Dict[str, Tuple[type, float, float, Any, str]] = {
    "risk.theta_low": (float, 0.0, 1.0, RiskConfig().theta_low, "Low/Medium boundary of S_ind [D3 0.4]"),
    "risk.theta_high": (float, 0.0, 1.0, RiskConfig().theta_high, "Medium/High boundary of S_ind; Path A and PDP block [D3 0.6]"),
    "risk.t_recheck": (float, 30.0, 86400.0, RiskConfig().t_recheck, "Medium-tier re-scoring interval, s [D3 600]"),
    "pdp.theta_coll": (float, 0.0, 1.0, None, "Collective block threshold; empty = TGN model's calibrated value"),
    "pdp.block_ttl": (float, 60.0, 30 * 86400.0, PDPConfig().block_ttl, "Risk block expiry, s"),
    "pdp.release_grace": (float, 0.0, 7 * 86400.0, PDPConfig().release_grace, "Grace after an analyst release, s"),
    "escalation.kappa": (int, 0, 10000, EscalationConfig().kappa, "Path B: more than kappa distinct users"),
    "escalation.delta_res": (float, 60.0, 86400.0, EscalationConfig().delta_res, "Path B window, s [D3 900]"),
    "escalation.t_sweep": (float, 300.0, 7 * 86400.0, EscalationConfig().t_sweep, "Path C sweep interval, s [D3 7200]"),
    "escalation.gamma": (float, 0.0, 1.0, EscalationConfig().gamma, "Path D sampling rate"),
    "escalation.analysis_window": (float, 10.0, 3600.0, EscalationConfig().analysis_window, "Background window W, s"),
}


class SettingsError(ValueError):
    pass


def validate(updates: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in updates.items():
        if k not in SCHEMA:
            raise SettingsError(f"unknown setting {k}")
        typ, lo, hi, _, _ = SCHEMA[k]
        if v is None or v == "":
            if k != "pdp.theta_coll":
                raise SettingsError(f"{k} cannot be empty")
            out[k] = None
            continue
        try:
            v = typ(v)
        except (TypeError, ValueError):
            raise SettingsError(f"{k} must be a {typ.__name__}") from None
        if not (lo <= v <= hi):
            raise SettingsError(f"{k} must be between {lo} and {hi}")
        out[k] = v
    eff = {**current, **out}
    if eff["risk.theta_low"] >= eff["risk.theta_high"]:
        raise SettingsError("risk.theta_low must be lower than risk.theta_high")
    return out


def effective(redis) -> Dict[str, Any]:
    raw = redis.hgetall(K_HASH) or {}
    vals = {(k.decode() if isinstance(k, bytes) else k): json.loads(v) for k, v in raw.items()}
    return {k: vals.get(k, spec[3]) for k, spec in SCHEMA.items()}


def explicit(redis) -> set:
    """Keys an operator has set (as opposed to research defaults)."""
    return {(k.decode() if isinstance(k, bytes) else k) for k in (redis.hkeys(K_HASH) or [])}


def write(redis, updates: Dict[str, Any]) -> int:
    pipe = redis.pipeline()
    for k, v in updates.items():
        pipe.hset(K_HASH, k, json.dumps(v))
    pipe.incr(K_VERSION)
    return int(pipe.execute()[-1])


def apply_risk(rc: RiskConfig, eff: Dict[str, Any]) -> RiskConfig:
    return replace(rc, theta_low=eff["risk.theta_low"], theta_high=eff["risk.theta_high"], t_recheck=eff["risk.t_recheck"])


def apply_escalation(cfg: EscalationConfig, eff: Dict[str, Any]) -> EscalationConfig:
    return replace(cfg, kappa=eff["escalation.kappa"], delta_res=eff["escalation.delta_res"], t_sweep=eff["escalation.t_sweep"],
                   gamma=eff["escalation.gamma"], analysis_window=eff["escalation.analysis_window"])


def apply_pdp(cfg: PDPConfig, eff: Dict[str, Any]) -> PDPConfig:
    return replace(cfg, block_ttl=eff["pdp.block_ttl"], release_grace=eff["pdp.release_grace"])


class Watcher:
    """Cheap per-tick check: returns the effective settings only when `settings:version` changed."""

    def __init__(self, redis, every: float = 5.0, clock=time.monotonic):
        self.r, self.every, self.clock = redis, every, clock
        self._seen, self._next = None, 0.0

    def poll(self) -> Optional[Dict[str, Any]]:
        if self.clock() < self._next:
            return None
        self._next = self.clock() + self.every
        v = self.r.get(K_VERSION)
        v = int(v) if v is not None else 0
        if v == self._seen:
            return None
        self._seen = v
        return effective(self.r)

"""Per-event behavioural features for the TCN (D3 Step 4: X_u in R^{L x d}, L = 20, d = 50).

Two stages, so that the SAME code serves training and the live workers (train/serve parity):
  1. `observe(profile, ev)` updates a per-user profile (the stream-processing state that D3 keeps outside
     the workers; serialisable to Redis) and returns a RAW record: model-independent values describing the
     event relative to that user's own history. Uses only the event and the past - never labels.
  2. `Preprocessor.encode(records)` turns the last L raw records into the L x 50 matrix using a vocabulary
     and a numeric scaler that are FITTED ON THE TRAINING PERIOD ONLY and saved with the model.

Layout of the 50 dimensions (FEATURE_LAYOUT_VERSION = 1):
   0-3   application one-hot (3 catalog apps by training frequency + OOV)
   4-19  resource one-hot (15 catalog resources + OOV/unmapped)       no hashing -> no collisions (audit D11)
  20-23  operation one-hot (view, edit, delete, export); permission p is implied by o (schema.REQUIRED_PERMISSION)
  24 s sensitivity   25 y allowed   26 unmapped path
  27-28  hour of day sin/cos (UTC)   29-30 day of week sin/cos
  31 *log inter-arrival seconds      32 first request of a session
  33 new /24 network for this user   34 new device for this user   35 context missing (ip or device)
  36 first access to this resource   37 *log familiarity (past accesses to this resource)
  38 *deny rate (EWMA)   39 *events last 10 min   40 *events last hour   41 *distinct resources last hour
  42 *sensitive accesses last hour   43 *exports last 24 h   44 sequential object id (enumeration)
  45 has object id   46 *hour deviation from the user's usual hours   47 *log rate ratio vs usual hourly rate
  48 cold start (< 30 events of history)   49 *denials last hour
  (* = z-scored with training-period mean/std; missing numeric values -> training mean, i.e. 0 after scaling)
"""
import math
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Sequence

import numpy as np

from ..events import AccessEvent

FEATURE_LAYOUT_VERSION = 1
L_DEFAULT, D = 20, 50
OPS = ("view", "edit", "delete", "export")
N_APPS, N_RES = 3, 15
RECENT_WINDOW = 24 * 3600.0
RECENT_CAP = 400
COLD_START = 30
NUMERIC = ("dt", "familiarity", "deny_ewma", "n10m", "n1h", "distinct1h", "sens1h", "exports24h", "hour_dev",
           "rate_ratio", "denies1h")
NUMERIC_COLS = (31, 37, 38, 39, 40, 41, 42, 43, 46, 47, 49)


def _subnet(ip: Optional[str]) -> Optional[str]:
    if not ip or ip.count(".") != 3:
        return ip or None
    return ".".join(ip.split(".")[:3])


@dataclass
class UserProfile:
    n: int = 0
    last_t: Optional[float] = None
    nets: List[str] = field(default_factory=list)
    devices: List[str] = field(default_factory=list)
    res_counts: Dict[str, int] = field(default_factory=dict)
    deny_ewma: float = 0.0
    recent: List[list] = field(default_factory=list)          # [t, rkey, s, o, y] within 24 h
    last_obj: Dict[str, int] = field(default_factory=dict)
    hour_sin: float = 0.0
    hour_cos: float = 0.0
    rate_ewma: float = 0.0
    window: List[dict] = field(default_factory=list)          # last L raw records
    seen_ids: List[str] = field(default_factory=list)         # recent event ids (redelivery guard)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "UserProfile":
        return cls(**d)


def observe(p: UserProfile, ev: AccessEvent, L: int = L_DEFAULT) -> dict:
    """Update the profile with `ev` and return the event's raw feature record."""
    rkey = ev.rkey
    net, dev = _subnet(ev.c.get("ip")), ev.c.get("device") or None
    dt = 0.0 if p.last_t is None else max(ev.t - p.last_t, 0.0)
    hour = (ev.t / 3600.0) % 24.0
    dow = (int(ev.t // 86400) + 3) % 7                        # 1970-01-01 was a Thursday -> Monday = 0
    recent = [x for x in p.recent if x[0] > ev.t - RECENT_WINDOW]
    in_10m = [x for x in recent if x[0] > ev.t - 600]
    in_1h = [x for x in recent if x[0] > ev.t - 3600]
    obj = ev.meta.get("object_id") if isinstance(ev.meta, dict) else None
    try:
        obj_i = int(obj) if obj is not None else None
    except (TypeError, ValueError):
        obj_i = None
    prev_obj = p.last_obj.get(rkey)
    cold = p.n < COLD_START
    if cold:
        hour_dev = 0.0
    else:
        mean_angle = math.atan2(p.hour_sin, p.hour_cos)
        ang = 2 * math.pi * hour / 24.0
        hour_dev = abs(math.atan2(math.sin(ang - mean_angle), math.cos(ang - mean_angle))) / math.pi
    n1h = len(in_1h) + 1
    rec = {
        "a": ev.a, "r": ev.r, "o": ev.o, "s": int(ev.s), "y": int(ev.y), "unmapped": int(ev.r.startswith("unmapped:")),
        "hour": hour, "dow": dow, "dt": math.log1p(dt), "session_new": int(ev.c.get("session") == "new"),
        "new_net": int(net is not None and p.n > 0 and net not in p.nets),
        "new_device": int(dev is not None and p.n > 0 and dev not in p.devices),
        "ctx_missing": int(net is None or dev is None),
        "new_resource": int(p.n > 0 and rkey not in p.res_counts),
        "familiarity": math.log1p(p.res_counts.get(rkey, 0)),
        "deny_ewma": p.deny_ewma,
        "n10m": math.log1p(len(in_10m)), "n1h": math.log1p(len(in_1h)),
        "distinct1h": math.log1p(len({x[1] for x in in_1h} | {rkey})),
        "sens1h": math.log1p(sum(x[2] for x in in_1h) + int(ev.s)),
        "exports24h": math.log1p(sum(1 for x in recent if x[3] == "export") + int(ev.o == "export")),
        "seq_obj": int(obj_i is not None and prev_obj is not None and abs(obj_i - prev_obj) == 1),
        "has_obj": int(obj_i is not None),
        "hour_dev": hour_dev,
        "rate_ratio": 0.0 if cold else math.log((n1h + 1.0) / (p.rate_ewma + 1.0)),
        "cold": int(cold),
        "denies1h": math.log1p(sum(1 for x in in_1h if x[4] == 0) + (1 - int(ev.y))),
    }
    # ---- update state (after computing the record: features describe the event against the PAST)
    p.n += 1
    p.last_t = ev.t if p.last_t is None else max(p.last_t, ev.t)
    if net is not None and net not in p.nets:
        p.nets = (p.nets + [net])[-50:]
    if dev is not None and dev not in p.devices:
        p.devices = (p.devices + [dev])[-50:]
    p.res_counts[rkey] = p.res_counts.get(rkey, 0) + 1
    p.deny_ewma = 0.97 * p.deny_ewma + 0.03 * (1 - int(ev.y))
    recent.append([ev.t, rkey, int(ev.s), ev.o, int(ev.y)])
    p.recent = recent[-RECENT_CAP:]
    if obj_i is not None:
        p.last_obj[rkey] = obj_i
    ang = 2 * math.pi * hour / 24.0
    p.hour_sin = 0.98 * p.hour_sin + 0.02 * math.sin(ang)
    p.hour_cos = 0.98 * p.hour_cos + 0.02 * math.cos(ang)
    p.rate_ewma = 0.99 * p.rate_ewma + 0.01 * n1h if p.n > 1 else float(n1h)
    p.window = (p.window + [rec])[-L:]
    if ev.event_id:
        p.seen_ids = (p.seen_ids + [ev.event_id])[-64:]
    return rec


def window_for_recheck(p: UserProfile, now: float) -> List[dict]:
    """Medium-tier re-scoring (D3 Step 5): the same window with the newest step's inter-arrival feature
    replaced by the time since the user's last event, so inactivity drift changes the score."""
    w = [dict(r) for r in p.window]
    if w and p.last_t is not None:
        w[-1]["dt"] = math.log1p(max(now - p.last_t, 0.0))
    return w


@dataclass
class Preprocessor:
    apps: List[str] = field(default_factory=list)
    resources: List[str] = field(default_factory=list)       # "app:resource"
    mean: List[float] = field(default_factory=lambda: [0.0] * len(NUMERIC))
    std: List[float] = field(default_factory=lambda: [1.0] * len(NUMERIC))
    L: int = L_DEFAULT
    layout_version: int = FEATURE_LAYOUT_VERSION

    @classmethod
    def fit(cls, records: Sequence[dict], L: int = L_DEFAULT) -> "Preprocessor":
        """Fit vocabulary and scaler on TRAINING-period records only."""
        apps, res = {}, {}
        for r in records:
            apps[r["a"]] = apps.get(r["a"], 0) + 1
            if not r["unmapped"]:
                res[f"{r['a']}:{r['r']}"] = res.get(f"{r['a']}:{r['r']}", 0) + 1
        top = lambda d, k: [x for x, _ in sorted(d.items(), key=lambda kv: (-kv[1], kv[0]))[:k]]
        vals = np.array([[r[k] for k in NUMERIC] for r in records], dtype=np.float64) if records else np.zeros((1, len(NUMERIC)))
        mean, std = np.nanmean(vals, axis=0), np.nanstd(vals, axis=0)
        std[std < 1e-6] = 1.0
        return cls(top(apps, N_APPS), top(res, N_RES), mean.tolist(), std.tolist(), L)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Preprocessor":
        if d.get("layout_version") != FEATURE_LAYOUT_VERSION:
            raise ValueError(f"feature layout v{d.get('layout_version')} != code v{FEATURE_LAYOUT_VERSION}")
        return cls(**d)

    def encode_record(self, r: dict) -> np.ndarray:
        v = np.zeros(D, dtype=np.float32)
        ai = self.apps.index(r["a"]) if r["a"] in self.apps else N_APPS
        v[ai] = 1
        rk = f"{r['a']}:{r['r']}"
        v[4 + (self.resources.index(rk) if rk in self.resources else N_RES)] = 1
        v[20 + (OPS.index(r["o"]) if r["o"] in OPS else 0)] = 1
        v[24], v[25], v[26] = r["s"], r["y"], r["unmapped"]
        h = 2 * math.pi * r["hour"] / 24.0
        w = 2 * math.pi * r["dow"] / 7.0
        v[27], v[28], v[29], v[30] = math.sin(h), math.cos(h), math.sin(w), math.cos(w)
        v[32], v[33], v[34], v[35], v[36] = r["session_new"], r["new_net"], r["new_device"], r["ctx_missing"], r["new_resource"]
        v[44], v[45], v[48] = r["seq_obj"], r["has_obj"], r["cold"]
        for k, col, m, s in zip(NUMERIC, NUMERIC_COLS, self.mean, self.std):
            x = r.get(k)
            v[col] = 0.0 if x is None or (isinstance(x, float) and math.isnan(x)) else (x - m) / s
        return v

    def encode(self, records: Sequence[dict]) -> np.ndarray:
        """Last L records -> (L, 50), zero-padded at the front for users with a short history."""
        out = np.zeros((self.L, D), dtype=np.float32)
        recs = list(records)[-self.L:]
        for i, r in enumerate(recs):
            out[self.L - len(recs) + i] = self.encode_record(r)
        return out

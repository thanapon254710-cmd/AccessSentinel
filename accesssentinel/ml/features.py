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
import hashlib
import math
import zlib
from dataclasses import asdict, dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

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


# ============================================================================================================
# Revised v3 (Phase 3)
# ============================================================================================================
"""Revised v3 Table III event encoding (d = 43) and timer context z_u(t) (4), with train/serve parity.

Per event (dimension in the model input):
  application / resource r*        learned embedding (vocabulary from training + 16 hash buckets)    12
  permission / operation (p, o)    categorical embedding                                               6
  network prefix /24 (from c)      categorical embedding over 64 hash buckets (never the raw IP)       6
  device / novelty                 new device, new network, context missing, new session (binary)     4
  inter-arrival time dt            log1p(seconds)                                                      1
  time of day / day of week        sin / cos                                                           4
  gateway result y                 binary                                                              1
  sensitivity s                    binary                                                              1
  historical context               log frequency of r* for the user, first-seen, peer rarity of r*,   6
                                   deny-rate EWMA, sensitive accesses in the last hour, rate ratio
  schema / policy version          schema version, hash of v_policy                                    2
                                                                                                    = 43
Timer context z_u(t) (scoring head only): log1p(inactivity), log1p(session age), ThreatFeed, DevPosture.

`UserState` is the per-user stream state (JSON-serialisable: Redis in the live worker, a dict offline);
`observe` updates it and returns the event's RAW record (model-independent values against the user's past);
`Encoder` (fitted on TRAINING records only) turns raw records into categorical indices + scaled numbers.

Timer context z_u(t) = [inactivity, session age, ThreatFeed(u, t), DevPosture(u, t)] (Revised v3 Eq. 9,
Table III). It enters the TCN at the scoring head, not the event window X_u.

ThreatFeed and DevPosture need external systems (threat intelligence, device management / MDM) that the
synthetic data and the demo deployment do not have. v3 Limitation 4 assumes their logged histories exist; here
they are SIMULATED, deterministically, from information such systems would really hold, never from labels:

  SimulatedThreatFeed   reputation of the source IP. External addresses in the attacker range 203.0.113.0/24
                        are listed with probability `p_listed` per address (threat feeds are incomplete);
                        the VPN range 10.99.0.0/24 has false listings with probability `p_false`; office
                        networks are never listed. Score ~0.9 when listed, ~0.02 otherwise.
  SimulatedDevPosture   compliance of the device per a simulated MDM registry: devices IT issued to the user
                        (including replacement laptops, recorded by the generator) are managed, score ~0.9,
                        with `p_degraded` of readings degraded (overdue patches) to 0.4-0.7. Any other device
                        (an attacker's, a personal travel device) is unmanaged, score ~0.3.

Live deployment: `LiveTimerContext` reads `threat:ip:<ip>` and `posture:device:<id>` from Redis when an
integration writes them, and otherwise uses neutral defaults (threat 0.0, posture 1.0): no signal.
"""

L_V3 = 128
EMB_RES, EMB_OP, EMB_NET = 12, 6, 6
N_NUM = 19                                   # 4 + 1 + 4 + 1 + 1 + 6 + 2
D_V3 = EMB_RES + EMB_OP + EMB_NET + N_NUM    # 43
N_Z = 4
RES_HASH_BUCKETS, NET_BUCKETS = 16, 64
SESSION_GAP = 30 * 60.0                      # [IMPL] a new session after 30 min without events
HISTORY_WINDOW = 3600.0
NUM_FIELDS = ("new_device", "new_net", "ctx_missing", "session_new", "dt", "tod_sin", "tod_cos", "dow_sin",
              "dow_cos", "y", "s", "log_freq", "first_seen", "peer_rarity", "deny_ewma", "sens1h", "rate_ratio",
              "schema_v", "policy_h")
assert len(NUM_FIELDS) == N_NUM and D_V3 == 43
# Scaling (fixed in Phase 3 after the live check, see EncoderV3): binary and cyclic features are used as is;
# continuous ones are standardised on training data and clipped to +-CLIP; a feature that was CONSTANT in the
# training data is neutralised (always 0), because the model cannot have learned anything from it.
UNSCALED_FIELDS = ("new_device", "new_net", "ctx_missing", "session_new", "tod_sin", "tod_cos", "dow_sin", "dow_cos",
                   "y", "s", "first_seen")
CLIP = 8.0


def _h(s: str, n: int) -> int:
    return zlib.crc32(s.encode()) % n


@dataclass
class UserStateV3:
    n: int = 0
    last_t: Optional[float] = None
    session_start: Optional[float] = None
    nets: List[str] = field(default_factory=list)
    devices: List[str] = field(default_factory=list)
    res_counts: Dict[str, int] = field(default_factory=dict)
    deny_ewma: float = 0.0
    recent: List[list] = field(default_factory=list)          # [t, s] within the last hour
    rate_ewma: float = 0.0
    window: List[dict] = field(default_factory=list)          # last L raw records (event-time order)
    seen_ids: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "UserStateV3":
        return cls(**d)


def observe_v3(st: UserStateV3, ev: AccessEvent, threat_fn, posture_fn, L: int = L_V3) -> Tuple[dict, List[float]]:
    """Update the state with `ev`; return (raw record, raw timer context z at the event's time)."""
    ip, dev = ev.c.get("ip"), ev.c.get("device") or None
    net = _subnet(ip)
    rstar = ev.rstar
    dt = 0.0 if st.last_t is None else max(ev.t - st.last_t, 0.0)
    new_session = st.last_t is None or dt > SESSION_GAP or ev.c.get("session") == "new"
    session_start = ev.t if (new_session or st.session_start is None) else st.session_start
    hour = (ev.t / 3600.0) % 24.0
    dow = ((int(ev.t // 86400) + 3) % 7)
    recent = [x for x in st.recent if x[0] > ev.t - HISTORY_WINDOW]
    n1h = len(recent) + 1
    cold = st.n < 30
    rec = {
        "eid": ev.event_id, "te": float(ev.t), "u": ev.u, "res": rstar, "op": f"{ev.p}:{ev.o}", "net": net or "none",
        "new_device": int(dev is not None and st.n > 0 and dev not in st.devices),
        "new_net": int(net is not None and st.n > 0 and net not in st.nets),
        "ctx_missing": int(net is None or dev is None),
        "session_new": int(bool(new_session)),
        "dt": math.log1p(dt),
        "tod_sin": math.sin(2 * math.pi * hour / 24), "tod_cos": math.cos(2 * math.pi * hour / 24),
        "dow_sin": math.sin(2 * math.pi * dow / 7), "dow_cos": math.cos(2 * math.pi * dow / 7),
        "y": int(ev.y), "s": int(ev.s),
        "log_freq": math.log1p(st.res_counts.get(rstar, 0)),
        "first_seen": int(st.n > 0 and rstar not in st.res_counts),
        "peer_rarity": 0.0,                                   # filled by the Encoder from its training table
        "deny_ewma": st.deny_ewma,
        "sens1h": math.log1p(sum(x[1] for x in recent) + int(ev.s)),
        "rate_ratio": 0.0 if cold else math.log((n1h + 1.0) / (st.rate_ewma + 1.0)),
        "schema_v": float(ev.schema_version or 1) / 2.0,
        "policy_h": (_h(ev.v_policy, 1000) / 1000.0) if ev.v_policy else 0.0,
    }
    z = [math.log1p(dt), math.log1p(max(ev.t - session_start, 0.0)), float(threat_fn(ev.u, ip, ev.t)),
         float(posture_fn(ev.u, dev, ev.t))]
    rec["z"] = z
    # ---- state update (after the record: features describe the event against the PAST)
    st.n += 1
    st.last_t = ev.t if st.last_t is None else max(st.last_t, ev.t)
    st.session_start = session_start
    if net is not None and net not in st.nets:
        st.nets = (st.nets + [net])[-50:]
    if dev is not None and dev not in st.devices:
        st.devices = (st.devices + [dev])[-50:]
    st.res_counts[rstar] = st.res_counts.get(rstar, 0) + 1
    st.deny_ewma = 0.97 * st.deny_ewma + 0.03 * (1 - int(ev.y))
    recent.append([ev.t, int(ev.s)])
    st.recent = recent[-400:]
    st.rate_ewma = 0.99 * st.rate_ewma + 0.01 * n1h if st.n > 1 else float(n1h)
    st.window = insert_by_time(st.window, rec, L)
    if ev.event_id:
        st.seen_ids = (st.seen_ids + [ev.event_id])[-64:]
    return rec, z


def insert_by_time(window: List[dict], rec: dict, L: int) -> List[dict]:
    """Append in event-time order (a late event is inserted at its position: v3 late policy 'rescore')."""
    if not window or rec["te"] >= window[-1]["te"]:
        return (window + [rec])[-L:]
    i = len(window)
    while i > 0 and window[i - 1]["te"] > rec["te"]:
        i -= 1
    return (window[:i] + [rec] + window[i:])[-L:]


def timer_context_at(st: UserStateV3, now: float, threat: float, posture: float) -> List[float]:
    """z_u(t) for a timer-triggered re-score of a Medium-tier user (Eq. 9): inactivity grows with time."""
    last = st.last_t if st.last_t is not None else now
    start = st.session_start if st.session_start is not None else now
    return [math.log1p(max(now - last, 0.0)), math.log1p(max(now - start, 0.0)), threat, posture]


class EncoderV3:
    """Vocabularies, peer-rarity table and numeric scaler, fitted on TRAINING records only."""

    def __init__(self, res_vocab: Dict[str, int], op_vocab: Dict[str, int], peer: Dict[str, float],
                 mean: List[float], std: List[float], z_mean: List[float], z_std: List[float],
                 neutral: Optional[List[int]] = None, z_neutral: Optional[List[int]] = None):
        self.res_vocab, self.op_vocab, self.peer = res_vocab, op_vocab, peer
        self.mean, self.std = np.array(mean, np.float32), np.array(std, np.float32)
        self.z_mean, self.z_std = np.array(z_mean, np.float32), np.array(z_std, np.float32)
        self.neutral = list(neutral or [])           # numeric columns constant in training -> always 0
        self.z_neutral = list(z_neutral or [])
        self._keep = np.ones(N_NUM, np.float32); self._keep[self.neutral] = 0.0
        self._zkeep = np.ones(N_Z, np.float32); self._zkeep[self.z_neutral] = 0.0

    # sizes include index 0 = padding
    @property
    def n_res(self): return 1 + len(self.res_vocab) + RES_HASH_BUCKETS

    @property
    def n_op(self): return 2 + len(self.op_vocab)

    @property
    def n_net(self): return 1 + NET_BUCKETS

    @classmethod
    def fit(cls, records: Sequence[dict]) -> "EncoderV3":
        res = sorted({r["res"] for r in records})
        ops = sorted({r["op"] for r in records})
        users_by_res: Dict[str, set] = {}
        users = set()
        for r in records:
            users_by_res.setdefault(r["res"], set()).add(r["u"])
            users.add(r["u"])
        n_users = max(1, len(users))
        peer = {k: -math.log((len(v) + 1) / (n_users + 1)) for k, v in users_by_res.items()}
        enc = cls({k: i + 1 for i, k in enumerate(res)}, {k: i + 1 for i, k in enumerate(ops)}, peer,
                  [0.0] * N_NUM, [1.0] * N_NUM, [0.0] * N_Z, [1.0] * N_Z)
        X = np.array([enc._num(r) for r in records], np.float32) if records else np.zeros((1, N_NUM), np.float32)
        Z = np.array([r["z"] for r in records], np.float32) if records else np.zeros((1, N_Z), np.float32)
        sd, zsd = X.std(0), Z.std(0)
        neutral = [i for i in range(N_NUM) if sd[i] < 1e-6]
        z_neutral = [i for i in range(N_Z) if zsd[i] < 1e-6]
        mean, std = X.mean(0), np.maximum(sd, 1e-6)
        for i, f in enumerate(NUM_FIELDS):
            if f in UNSCALED_FIELDS or i in neutral:
                mean[i], std[i] = 0.0, 1.0
        z_mean, z_std = Z.mean(0), np.maximum(zsd, 1e-6)
        for i in z_neutral:
            z_mean[i], z_std[i] = 0.0, 1.0
        return cls(enc.res_vocab, enc.op_vocab, peer, mean.tolist(), std.tolist(), z_mean.tolist(), z_std.tolist(),
                   neutral, z_neutral)

    def scale(self, num: np.ndarray) -> np.ndarray:
        return np.clip((num - self.mean) / self.std, -CLIP, CLIP) * self._keep

    def scale_z(self, z: np.ndarray) -> np.ndarray:
        return np.clip((z - self.z_mean) / self.z_std, -CLIP, CLIP) * self._zkeep

    def _num(self, r: dict) -> List[float]:
        v = [float(r[k]) for k in NUM_FIELDS]
        v[NUM_FIELDS.index("peer_rarity")] = self.peer.get(r["res"], self.unseen_rarity())
        return v

    def unseen_rarity(self) -> float:
        return max(self.peer.values()) if self.peer else 0.0

    def res_index(self, res: str) -> int:
        i = self.res_vocab.get(res)
        return i if i is not None else 1 + len(self.res_vocab) + _h(res, RES_HASH_BUCKETS)

    def op_index(self, op: str) -> int:
        return self.op_vocab.get(op, 1 + len(self.op_vocab))

    def net_index(self, net: str) -> int:
        return 1 + _h(net, NET_BUCKETS)

    def encode_record(self, r: dict) -> Tuple[int, int, int, np.ndarray, np.ndarray]:
        num = self.scale(np.array(self._num(r), np.float32))
        z = self.scale_z(np.array(r["z"], np.float32))
        return self.res_index(r["res"]), self.op_index(r["op"]), self.net_index(r["net"]), num, z

    def encode_window(self, window: Sequence[dict], z: Optional[Sequence[float]] = None, L: int = L_V3):
        """One window (oldest first) -> front-padded arrays (res, op, net: (L,), num: (L, 19)) and z: (4,)."""
        res = np.zeros(L, np.int64); op = np.zeros(L, np.int64); net = np.zeros(L, np.int64)
        num = np.zeros((L, N_NUM), np.float32)
        w = list(window)[-L:]
        off = L - len(w)
        for i, r in enumerate(w):
            a, b, c, x, _ = self.encode_record(r)
            res[off + i], op[off + i], net[off + i], num[off + i] = a, b, c, x
        zz = w[-1]["z"] if (z is None and w) else (z if z is not None else [0.0] * N_Z)
        zs = self.scale_z(np.array(zz, np.float32))
        return res, op, net, num, zs

    def to_dict(self) -> dict:
        return {"res_vocab": self.res_vocab, "op_vocab": self.op_vocab, "peer": self.peer,
                "mean": self.mean.tolist(), "std": self.std.tolist(), "z_mean": self.z_mean.tolist(),
                "z_std": self.z_std.tolist(), "neutral": self.neutral, "z_neutral": self.z_neutral,
                "neutral_fields": [NUM_FIELDS[i] for i in self.neutral], "clip": CLIP}

    @classmethod
    def from_dict(cls, d: dict) -> "EncoderV3":
        return cls(d["res_vocab"], d["op_vocab"], d["peer"], d["mean"], d["std"], d["z_mean"], d["z_std"],
                   d.get("neutral"), d.get("z_neutral"))

    def fingerprint(self) -> str:
        return hashlib.sha256(repr(sorted(self.res_vocab.items())).encode()).hexdigest()[:12]




def _unit(*parts: str) -> float:
    """Deterministic pseudo-random number in [0, 1) from strings (a 'logged history')."""
    h = hashlib.sha256("|".join(parts).encode()).digest()
    return int.from_bytes(h[:8], "big") / 2 ** 64


class SimulatedThreatFeed:
    def __init__(self, p_listed: float = 0.5, p_false: float = 0.03, attacker_prefix: str = "203.0.113.",
                 vpn_prefix: str = "10.99.0."):
        self.p_listed, self.p_false, self.att, self.vpn = p_listed, p_false, attacker_prefix, vpn_prefix

    def __call__(self, u: str, ip: Optional[str], t: float) -> float:
        if not ip:
            return 0.0
        day = str(int(t // 86400))
        if ip.startswith(self.att):
            listed = _unit("tf", ip) < self.p_listed
        elif ip.startswith(self.vpn):
            listed = _unit("tf", ip, day) < self.p_false
        else:
            listed = False
        noise = 0.05 * (_unit("tfn", ip, day) - 0.5)
        return min(1.0, max(0.0, (0.9 if listed else 0.02) + noise))


class SimulatedDevPosture:
    def __init__(self, managed: Dict[str, set], p_degraded: float = 0.08):
        self.managed, self.p_degraded = managed, p_degraded

    @classmethod
    def from_dataset(cls, ds, **kw) -> "SimulatedDevPosture":
        return cls({u: set(p.get("managed_devices", [p.get("device")])) for u, p in ds.users.items()}, **kw)

    def __call__(self, u: str, device: Optional[str], t: float) -> float:
        if not device:
            return 0.5
        day = str(int(t // 86400))
        if device in self.managed.get(u, ()):
            if _unit("pd", device, day) < self.p_degraded:
                return 0.4 + 0.3 * _unit("pdv", device, day)
            return 0.85 + 0.1 * _unit("pdv", device, day)
        return 0.2 + 0.2 * _unit("pdv", device, day)


class LiveTimerContext:
    """Threat / posture lookups for the deployed worker (neutral defaults without an integration)."""

    def __init__(self, redis=None, default_threat: float = 0.0, default_posture: float = 1.0):
        self.r, self.dt, self.dp = redis, default_threat, default_posture

    def threat(self, u: str, ip: Optional[str], t: float) -> float:
        return self._get(f"threat:ip:{ip}", self.dt) if ip else self.dt

    def posture(self, u: str, device: Optional[str], t: float) -> float:
        return self._get(f"posture:device:{device}", self.dp) if device else self.dp

    def _get(self, key: str, default: float) -> float:
        if self.r is None:
            return default
        try:
            v = self.r.get(key)
            return float(v) if v is not None else default
        except Exception:  # noqa: BLE001 - a lookup failure must not stop scoring
            return default


ThreatFn = Callable[[str, Optional[str], float], float]
PostureFn = Callable[[str, Optional[str], float], float]

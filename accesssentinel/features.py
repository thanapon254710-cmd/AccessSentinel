"""Per-event feature encoding, d = 50 (D3 Step 4). Layout is an implementation choice."""
import math
import zlib
import numpy as np
from .events import AccessEvent

# layout offsets
APP, RES, PERM, OP, IP, DEV, SESS = 0, 6, 22, 26, 30, 36, 40
DT, TOD, DOW, Y, S, FIRST = 42, 43, 45, 47, 48, 49
D = 50
_PERMS = {"read": 0, "write": 1, "admin": 2, "export": 3}
_OPS = {"view": 0, "edit": 1, "delete": 2, "export": 3}


def _h(s: str, n: int) -> int:
    return zlib.crc32(s.encode()) % n


def dt_feature(dt: float) -> float:
    return math.log1p(max(dt, 0.0)) / 10.0


class FeatureEncoder:
    dim = D
    dt_index = DT

    def encode(self, ev: AccessEvent, dt: float, first_seen: bool) -> np.ndarray:
        v = np.zeros(D, dtype=np.float32)
        v[APP + _h(ev.a, 6)] = 1
        v[RES + _h(ev.rkey, 16)] = 1
        v[PERM + _PERMS.get(ev.p, _h(ev.p, 4))] = 1
        v[OP + _OPS.get(ev.o, _h(ev.o, 4))] = 1
        ip = ".".join(ev.c.get("ip", "0.0.0.0").split(".")[:3])
        v[IP + _h(ip, 6)] = 1
        v[DEV + _h(ev.c.get("device", ""), 4)] = 1
        v[SESS + (0 if ev.c.get("session", "active") == "new" else 1)] = 1
        v[DT] = dt_feature(dt)
        hour = (ev.t / 3600.0) % 24; dow = int(ev.t / 86400) % 7
        v[TOD] = math.sin(2 * math.pi * hour / 24); v[TOD + 1] = math.cos(2 * math.pi * hour / 24)
        v[DOW] = math.sin(2 * math.pi * dow / 7); v[DOW + 1] = math.cos(2 * math.pi * dow / 7)
        v[Y] = float(ev.y); v[S] = float(ev.s); v[FIRST] = 1.0 if first_seen else 0.0
        return v

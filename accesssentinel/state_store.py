"""Stream-processing keyed state: sliding window X_u (Eq. 2). Keeps TCN workers stateless."""
from collections import deque
import numpy as np
from .events import AccessEvent
from .features import FeatureEncoder, dt_feature


class UserStateStore:
    def __init__(self, L: int, d: int, encoder: FeatureEncoder):
        self.L, self.d, self.enc = L, d, encoder
        self._win, self._last_t, self._seen = {}, {}, {}

    def update(self, ev: AccessEvent) -> np.ndarray:
        """Append the event, return padded window of shape (L, d)."""
        u = ev.u
        last = self._last_t.get(u)
        dt = 0.0 if last is None else max(ev.t - last, 0.0)
        seen = self._seen.setdefault(u, set())
        first = ev.rkey not in seen
        seen.add(ev.rkey)
        self._win.setdefault(u, deque(maxlen=self.L)).append(self.enc.encode(ev, dt, first))
        self._last_t[u] = ev.t
        return self.window(u)

    def window(self, u: str) -> np.ndarray:
        out = np.zeros((self.L, self.d), dtype=np.float32)
        w = self._win.get(u)
        if w:
            arr = np.stack(w)
            out[self.L - len(arr):] = arr
        return out

    def window_at(self, u: str, now: float) -> np.ndarray:
        """Window with the newest step's inter-arrival feature refreshed to `now - last event`
        so a timer-triggered re-score sees inactivity drift (static X_u would give an identical score)."""
        out = self.window(u)
        last = self._last_t.get(u)
        if last is not None:
            out[-1, self.enc.dt_index] = dt_feature(now - last)
        return out

    def users(self):
        return self._last_t.keys()

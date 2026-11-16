"""Event-time ordering per user key (Revised v3 Step 2, Eq. (2), requirement R1, Table IV "per-user ordering").

Kafka keeps APPEND order per key, not event-time order: two gateways, two sessions, or a retried publish can
deliver a user's events out of te order. This module releases each user's events to the TCN / CMU path in
event-time order, using the per-key watermark

    W_t = max_{t' <= t} te(t') - delta_late                                                         (Eq. 2)

Release rules (per user):
  1. Watermark: buffered events with te <= W are released in (te, seq) order.
  2. Sequence fast path: an event whose sequence number nu is the next one of its gateway/session stream, when
     nothing else of this user is buffered, te is not older than the last released event, and NO OTHER stream
     of this user has been active within `multi_stream_window`, is released immediately. In the common case
     (one session, one gateway) ordering therefore adds no delay. A user with concurrent sessions goes through
     the watermark (rule 1), because another session's in-flight events may carry an earlier te. (Found in the
     live check: with the fast path on for two interleaved sessions, in-lateness events were overtaken.)
  3. Idle timeout: an event never waits more than delta_late of PROCESSING time. When the oldest-arrived
     buffered event has waited that long, every buffered event up to its te is released in order. Without this
     rule a user's last event would wait forever for a later event to advance the watermark. Together, rules 1-3
     bound the reorder delay: Lat_reorder <= delta_late (Eq. 31).

An arriving event is LATE when te < W (Eq. 2), or when te is older than an event already released (it can no
longer be put in order). Late events are not dropped silently: one of the four v3 policies is applied, chosen
by the resource sensitivity s:
  history_only   update history only (the event is passed on, flagged, without re-scoring)
  rescore        correct the window and re-score (passed on, flagged; the scorer re-scores the corrected window)
  retro_alert    trigger a retrospective alert (an alert record is emitted; the event is passed on for history)
  discard        discard with an audit record (only the audit record is emitted)
Duplicates (same event id still buffered) are ignored here; already-released duplicates are removed by the
downstream eid de-duplication.

The state is plain data so the transactional stage can checkpoint it in its changelog (to_state/from_state).
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

POLICIES = ("history_only", "rescore", "retro_alert", "discard")


@dataclass
class Item:
    te: float
    seq: int
    stream: str
    eid: str
    s: int
    arrived: float              # processing time the event reached this stage
    payload: dict               # the event (JSON-able dict)

    def sort_key(self):
        return (self.te, self.seq, self.eid)


@dataclass
class Released:
    item: Item
    late: Optional[str] = None  # None = in order; else the policy applied
    reason: str = ""            # "watermark" | "fast_path" | "idle" | "behind_watermark" | "behind_released"
    wait: float = 0.0           # processing-time seconds spent in the buffer


@dataclass
class UserOrder:
    max_te: float = float("-inf")
    last_released_te: float = float("-inf")
    last_seen: float = 0.0                                   # processing time of the last arrival (state eviction)
    streams: Dict[str, int] = field(default_factory=dict)    # stream -> last released seq
    stream_seen: Dict[str, float] = field(default_factory=dict)  # stream -> last arrival (processing time)
    buf: List[Item] = field(default_factory=list)

    def watermark(self, delta: float) -> float:
        return self.max_te - delta


class EventTimeOrderer:
    def __init__(self, delta_late: float, policy_s0: str = "history_only", policy_s1: str = "rescore",
                 idle_state_seconds: float = 86400.0, multi_stream_window: float = 60.0):
        for p in (policy_s0, policy_s1):
            if p not in POLICIES:
                raise ValueError(f"late-event policy must be one of {POLICIES}")
        if delta_late < 0:
            raise ValueError("delta_late must be >= 0")
        self.delta, self.policy = float(delta_late), {0: policy_s0, 1: policy_s1}
        self.idle_state, self.multi_stream_window = idle_state_seconds, multi_stream_window
        self.users: Dict[str, UserOrder] = {}
        self.pending: set = set()                       # users with buffered events (visited by tick)
        self.stats = {"in_order": 0, "fast_path": 0, "late": 0, "duplicates": 0, "released": 0}

    # ------------------------------------------------------------------ input
    def add(self, u: str, item: Item) -> List[Released]:
        st = self.users.setdefault(u, UserOrder())
        st.last_seen = max(st.last_seen, item.arrived)
        if any(b.eid == item.eid for b in st.buf):
            self.stats["duplicates"] += 1
            return []
        w = st.watermark(self.delta)
        if item.te < w or item.te < st.last_released_te:
            self.stats["late"] += 1
            reason = "behind_watermark" if item.te < w else "behind_released"
            return [Released(item, late=self.policy[1 if item.s else 0], reason=reason)]
        st.max_te = max(st.max_te, item.te)
        if item.stream:
            st.stream_seen[item.stream] = item.arrived
            for k in [k for k, t in st.stream_seen.items() if item.arrived - t > self.multi_stream_window]:
                del st.stream_seen[k]
                st.streams.pop(k, None)
        concurrent = any(k != item.stream for k in st.stream_seen)
        prev = st.streams.get(item.stream)
        contiguous = item.seq >= 1 and item.stream and ((prev is None and item.seq == 1) or (prev is not None and item.seq == prev + 1))
        if contiguous and not concurrent and not st.buf and item.te >= st.last_released_te:
            self.stats["fast_path"] += 1
            return [self._release(st, item, "fast_path", item.arrived)]
        st.buf.append(item)
        out = self._release_watermark(st, item.arrived)
        if st.buf:
            self.pending.add(u)
        return out

    def tick(self, now: float) -> List[Tuple[str, Released]]:
        """Idle-timeout release (rule 3) for users with buffered events."""
        out = []
        for u in list(self.pending):
            st = self.users.get(u)
            if st is None:
                self.pending.discard(u)
                continue
            while st.buf:
                oldest = min(st.buf, key=lambda b: b.arrived)
                if now - oldest.arrived < self.delta:
                    break
                cut = oldest.te
                ready = sorted((b for b in st.buf if b.te <= cut), key=Item.sort_key)
                st.buf = [b for b in st.buf if b.te > cut]
                out += [(u, self._release(st, b, "idle", now)) for b in ready]
            if not st.buf:
                self.pending.discard(u)
        return out

    def evict(self, now: float) -> List[str]:
        """Forget users idle for longer than idle_state_seconds (bounded state). Returns the evicted users."""
        gone = [u for u, st in self.users.items() if not st.buf and now - st.last_seen > self.idle_state]
        for u in gone:
            del self.users[u]
        return gone

    # ------------------------------------------------------------------ internals
    def _release_watermark(self, st: UserOrder, now: float) -> List[Released]:
        w = st.watermark(self.delta)
        ready = sorted((b for b in st.buf if b.te <= w), key=Item.sort_key)
        if ready:
            st.buf = [b for b in st.buf if b.te > w]
        return [self._release(st, b, "watermark", now) for b in ready]

    def _release(self, st: UserOrder, item: Item, reason: str, now: float) -> Released:
        st.last_released_te = max(st.last_released_te, item.te)
        if item.stream and item.seq >= 1:
            st.streams[item.stream] = max(st.streams.get(item.stream, 0), item.seq)
        self.stats["released"] += 1
        self.stats["in_order"] += 1
        return Released(item, None, reason, max(0.0, now - item.arrived))

    # ------------------------------------------------------------------ checkpoint
    def user_state(self, u: str) -> Optional[dict]:
        st = self.users.get(u)
        if st is None:
            return None
        return {"max_te": st.max_te if st.max_te != float("-inf") else None,
                "last_released_te": st.last_released_te if st.last_released_te != float("-inf") else None,
                "last_seen": st.last_seen, "streams": st.streams, "stream_seen": st.stream_seen,
                "buf": [b.__dict__ for b in st.buf]}

    def restore_user(self, u: str, d: Optional[dict]) -> None:
        if d is None:
            self.users.pop(u, None)
            self.pending.discard(u)
            return
        self.users[u] = UserOrder(max_te=d["max_te"] if d.get("max_te") is not None else float("-inf"),
                                  last_released_te=d["last_released_te"] if d.get("last_released_te") is not None else float("-inf"),
                                  last_seen=d.get("last_seen", 0.0), streams=dict(d.get("streams") or {}),
                                  stream_seen=dict(d.get("stream_seen") or {}),
                                  buf=[Item(**b) for b in d.get("buf") or []])
        if self.users[u].buf:
            self.pending.add(u)

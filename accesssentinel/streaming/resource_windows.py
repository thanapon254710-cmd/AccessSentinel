"""Resource-keyed view with two-stage shard-and-merge aggregation (Revised v3 Step 2; Table V "hot resource
partition"; Table IV "resource-trigger correctness"). All three stages are exactly-once Kafka transactions
(accesssentinel/streaming/transactional.py).

  Repartition  access-events            -> access-events-by-resource   key = r* | b | hash(u) mod J_r
  Stage 1      access-events-by-resource -> resource-partials          key = r* | b
               per shard: the distinct users of a sensitive resource r* in event-time window b (width Delta_res);
               a shard's partial result is emitted ONCE, when the window is final (the partition's watermark
               max te - delta_late has passed the window end + grace, or, for idle input, the window end has
               passed in processing time and the shard has been quiet for delta_late).
  Stage 2      resource-partials        -> resource-windows            key = r* | b
               merges the shards' participant sets: |U_{r*,b,1} u ... u U_{r*,b,J}| and the participant list
               (exact sets, no sketch, so the participant list needed for escalation is available, v3 Step 2).
               A window result is emitted EXACTLY ONCE per (r*, b), when all J_r shards that saw traffic have
               reported or after a merge timeout. A partial that arrives after that (a late shard) produces a
               "correction" record, never a second "final" one.

Hashing users over J_r shards spreads a popular resource's window over several partitions (no single hot
partition). Non-sensitive resources (s = 0) are forwarded by the repartitioner (the graph index uses them)
but not aggregated: only s = 1 resources can trigger escalation path B.

Late events (te behind a shard's watermark, for a window already finalised) are counted and sent to
`late-events` as audit records (policy "retro_alert" for s = 1 resources).

Watermarks across the re-partition. A by-resource partition merges events from ALL source partitions of
`access-events`, and consumers fetch those in per-partition chunks, so "max te seen - delta_late" would race
ahead of a lagging source and declare its events late. Stage 1 therefore keeps, per by-resource partition, the
max event time of each SOURCE partition (carried in meta.src_p by the repartitioner) and uses the MINIMUM over
the sources (as Flink does with input watermarks). A source not seen for `source_idle` seconds (processing
time) is treated as idle and ignored, so quiet partitions cannot stall windows forever; a source never seen
holds the watermark back only for the first `source_idle` seconds after the stage starts.
"""
import json
import logging
import os
import time
import zlib
from typing import Dict, Optional, Set, Tuple

from .codec import decode_access_dict
from .consumer import DecodeError, Record

log = logging.getLogger("accesssentinel.resource_windows")


def window_of(te: float, width: float) -> int:
    return int(te // width)


def shard_of(u: str, shards: int) -> int:
    return zlib.crc32(u.encode()) % shards


def rstar_of(d: dict) -> str:
    return f"{d.get('tenant', 'default')}/{d['a']}:{d['r']}"


def decode_event(rec: Record) -> dict:
    d = decode_access_dict(rec.value)
    if rec.timestamp_ms and rec.timestamp_ms > 0 and not d.get("ti"):
        d["ti"] = rec.timestamp_ms / 1000.0
    return d


# ---------------------------------------------------------------------------------------------------- repartition
class RepartitionProcessor:
    """Stateless re-key of every event onto the Stage-1 key (r*, b, hash(u) mod J_r)."""

    def __init__(self, out_topic: str, width: float, shards: int):
        self.out, self.width, self.shards = out_topic, width, shards     # source partition goes into meta.src_p
        self.stats = {"forwarded": 0}

    def decode(self, rec):
        return decode_event(rec)

    def process(self, rec, emit):
        d = rec.data
        d.setdefault("meta", {})["src_p"] = rec.partition
        key = f"{rstar_of(d)}|{window_of(float(d['t']), self.width)}|{shard_of(d['u'], self.shards)}"
        emit(self.out, key.encode(), json.dumps(d).encode(),
             [("as-source", f"{rec.topic}:{rec.partition}:{rec.offset}".encode())])
        self.stats["forwarded"] += 1

    def on_tick(self, now, emit):
        pass

    def take_dirty(self):
        return {}

    def restore(self, *a):
        pass

    def drop(self, parts):
        pass


# ---------------------------------------------------------------------------------------------------- stage 1
class ShardStage:
    def __init__(self, out_topic: str, late_topic: str, width: float, delta_late: float, grace: float,
                 clock=time.time, sensitive_only: bool = True, source_partitions: int = 8, source_idle: float = 10.0):
        self.out, self.late, self.width, self.delta, self.grace = out_topic, late_topic, width, delta_late, grace
        self.clock, self.sensitive_only = clock, sensitive_only
        self.n_src, self.source_idle, self.started = source_partitions, source_idle, clock()
        self.src: Dict[int, Dict[int, list]] = {}    # by-resource partition -> source partition -> [max te, seen at]
        self.state: Dict[str, dict] = {}            # "r*|b|j" -> {users:{u: first_te}, events, s, last_arrival, final}
        self.part_of: Dict[str, int] = {}
        self._dirty: Set[str] = set()
        self.stats = {"events": 0, "partials": 0, "late": 0, "ignored_non_sensitive": 0}

    def decode(self, rec):
        d = decode_event(rec)
        if not rec.key or rec.key.count(b"|") != 2:
            raise DecodeError("Stage-1 key must be r*|b|shard")
        return d

    def watermark(self, part: int, now: float) -> float:
        srcs = self.src.get(part, {})
        active = []
        for sp in range(self.n_src):
            v = srcs.get(sp)
            if v is None:
                if now - self.started < self.source_idle:
                    return float("-inf")               # an unseen source may still deliver older events
                continue
            if now - v[1] <= self.source_idle:
                active.append(v[0])
        return (min(active) - self.delta) if active else float("-inf")

    def _final_reached(self, part: int, b: int, now: float) -> bool:
        return self.watermark(part, now) >= (b + 1) * self.width + self.grace

    def process(self, rec, emit):
        d, now = rec.data, self.clock()
        key = rec.key.decode()
        rstar, b, j = key.rsplit("|", 2)
        b = int(b)
        sp = int((d.get("meta") or {}).get("src_p", -1))
        if sp >= 0:
            v = self.src.setdefault(rec.partition, {}).setdefault(sp, [float("-inf"), now])
            v[0], v[1] = max(v[0], float(d["t"])), now
        if self.sensitive_only and not int(d["s"]):
            self.stats["ignored_non_sensitive"] += 1
            return
        self.stats["events"] += 1
        st = self.state.get(key)
        # late = the window is already final for this shard, OR (no state yet for this shard) the partition's
        # watermark has already passed the window end: judged per window, not per shard key
        if (st is not None and st.get("final")) or (st is None and self._final_reached(rec.partition, b, now)):
            self.stats["late"] += 1
            emit(self.late, d["u"].encode(), json.dumps({
                "kind": "late_event", "policy": "retro_alert", "reason": "window_already_final", "stage": "resource-shard",
                "u": d["u"], "event_id": d["event_id"], "te": d["t"], "resource": rstar, "window": b,
                "t": now, "record_id": f"late-res:{d['event_id']}"}).encode())
            return
        if st is None:
            st = self.state[key] = {"users": {}, "events": 0, "s": int(d["s"]), "last_arrival": now, "final": False}
        st["users"].setdefault(d["u"], float(d["t"]))
        st["events"] += 1
        st["last_arrival"] = now
        self.part_of[key] = rec.partition
        self._dirty.add(key)

    def on_tick(self, now, emit):
        for key, st in list(self.state.items()):
            rstar, b, j = key.rsplit("|", 2)
            b = int(b)
            p = self.part_of.get(key)
            if st.get("final"):
                if now - st.get("final_at", now) > 3 * self.width + 3600:       # forget old finalised windows
                    del self.state[key]
                    self._dirty.add(key)
                continue
            by_watermark = p is not None and self._final_reached(p, b, now)
            by_idle = now >= (b + 1) * self.width + self.grace and now - st["last_arrival"] >= self.delta
            if not (by_watermark or by_idle):
                continue
            emit(self.out, f"{rstar}|{b}".encode(), json.dumps({
                "rstar": rstar, "window": b, "window_start": b * self.width, "window_end": (b + 1) * self.width,
                "shard": int(j), "users": sorted(st["users"]), "first_te": st["users"], "events": st["events"],
                "s": st["s"], "final_by": "watermark" if by_watermark else "idle", "t": now}).encode())
            self.stats["partials"] += 1
            self.state[key] = {"final": True, "final_at": now}
            self._dirty.add(key)

    def take_dirty(self):
        out = {}
        for key in self._dirty:
            p = self.part_of.get(key)
            if p is None:
                continue
            st = self.state.get(key)
            out[(p, key.encode())] = None if st is None else json.dumps(st).encode()
            if st is None:
                self.part_of.pop(key, None)
        self._dirty.clear()
        return out

    def restore(self, partition, key, value):
        k = key.decode()
        if value is None:
            self.state.pop(k, None)
            self.part_of.pop(k, None)
        else:
            self.state[k] = json.loads(value)
            self.part_of[k] = partition

    def drop(self, parts):
        parts = set(parts)
        for k in [k for k, p in self.part_of.items() if p in parts]:
            self.state.pop(k, None)
            del self.part_of[k]
            self._dirty.discard(k)
        for p in parts:
            self.src.pop(p, None)


# ---------------------------------------------------------------------------------------------------- stage 2
class MergeStage:
    def __init__(self, out_topic: str, shards: int, merge_timeout: float, kappa_fn, clock=time.time):
        self.out, self.shards, self.timeout, self.kappa_fn, self.clock = out_topic, shards, merge_timeout, kappa_fn, clock
        self.state: Dict[str, dict] = {}            # "r*|b" -> {parts:{j: [users]}, first, emitted, meta}
        self.part_of: Dict[str, int] = {}
        self._dirty: Set[str] = set()
        self.stats = {"partials": 0, "windows": 0, "corrections": 0, "triggered": 0}

    def decode(self, rec):
        try:
            d = json.loads(rec.value)
            assert {"rstar", "window", "shard", "users"} <= set(d)
            return d
        except Exception as e:  # noqa: BLE001
            raise DecodeError(f"invalid partial: {type(e).__name__}") from None

    def process(self, rec, emit):
        d, now = rec.data, self.clock()
        key = f"{d['rstar']}|{d['window']}"
        st = self.state.setdefault(key, {"parts": {}, "first": now, "emitted": False, "meta": {
            "rstar": d["rstar"], "window": d["window"], "window_start": d["window_start"],
            "window_end": d["window_end"], "s": d.get("s", 1)}})
        st["parts"][str(d["shard"])] = d["users"]
        self.part_of[key] = rec.partition
        self.stats["partials"] += 1
        self._dirty.add(key)
        if st["emitted"]:
            self._emit(key, st, emit, now, kind="correction")
        elif len(st["parts"]) >= self.shards:
            self._emit(key, st, emit, now, kind="final")

    def on_tick(self, now, emit):
        for key, st in list(self.state.items()):
            if not st["emitted"] and now - st["first"] >= self.timeout:
                self._emit(key, st, emit, now, kind="final")
            elif st["emitted"] and now - st["first"] > 24 * 3600:
                del self.state[key]
                self._dirty.add(key)

    def _emit(self, key, st, emit, now, kind):
        users = sorted(set().union(*[set(v) for v in st["parts"].values()]))
        kappa = self.kappa_fn()
        rec = {**st["meta"], "kind": kind, "users": users, "distinct_users": len(users), "shards_reported": len(st["parts"]),
               "kappa": kappa, "above_kappa": len(users) > kappa, "t": now, "record_id": f"{key}:{kind}:{len(users)}"}
        if kind == "final":
            st["emitted"] = True
            self.stats["windows"] += 1
            self.stats["triggered"] += int(rec["above_kappa"])
        else:
            self.stats["corrections"] += 1
        emit(self.out, key.encode(), json.dumps(rec).encode())
        self._dirty.add(key)

    def take_dirty(self):
        out = {}
        for key in self._dirty:
            p = self.part_of.get(key)
            if p is None:
                continue
            st = self.state.get(key)
            out[(p, key.encode())] = None if st is None else json.dumps(st).encode()
            if st is None:
                self.part_of.pop(key, None)
        self._dirty.clear()
        return out

    def restore(self, partition, key, value):
        k = key.decode()
        if value is None:
            self.state.pop(k, None)
            self.part_of.pop(k, None)
        else:
            self.state[k] = json.loads(value)
            self.part_of[k] = partition

    def drop(self, parts):
        parts = set(parts)
        for k in [k for k, p in self.part_of.items() if p in parts]:
            self.state.pop(k, None)
            del self.part_of[k]
            self._dirty.discard(k)


# ---------------------------------------------------------------------------------------------------- services
def _stage(s, name, input_topic, processor, changelog, inst):
    from confluent_kafka import Consumer, Producer
    from .runtime import Heartbeat
    from .transactional import (TransactionalStage, restore_consumer_config, transactional_consumer_config,
                                transactional_producer_config)
    return TransactionalStage(
        Consumer(transactional_consumer_config(s.kafka_bootstrap, name, f"{name}-{inst}")),
        Producer(transactional_producer_config(s.kafka_bootstrap, f"{name}-{inst}")),
        (lambda: Consumer(restore_consumer_config(s.kafka_bootstrap, f"{name}-{inst}"))) if changelog else None,
        input_topic, processor, changelog, batch=s.txn_batch, on_iteration=Heartbeat(),
        stats_fn=lambda: dict(processor.stats), group_id=name)


def main(argv=None) -> int:
    """`python -m accesssentinel.streaming.resource_windows repartition|shard|merge|windows|all`
    (windows = shard + merge; the repartitioner runs as its own service)"""
    import sys
    import threading
    from ..config import EscalationConfig
    from ..settings import get_settings
    from ..topics import (ACCESS_EVENTS, ACCESS_EVENTS_BY_RESOURCE, LATE_EVENTS, RES_MERGE_CHANGELOG,
                          RES_SHARD_CHANGELOG, RESOURCE_PARTIALS, RESOURCE_WINDOWS, topic_name)
    from .runtime import install_stop, setup
    which = (argv or sys.argv[1:] or ["all"])[0]
    s = get_settings()
    setup(s.log_level)
    t = lambda n: topic_name(n, s.kafka_topic_prefix)
    inst = os.environ.get("HOSTNAME", "local")

    def kappa():
        try:
            import redis as redis_lib
            from .. import runtime_settings as rs
            return int(rs.effective(redis_lib.Redis(**s.redis_kwargs()))["escalation.kappa"])
        except Exception:  # noqa: BLE001
            return s.esc_kappa or EscalationConfig().kappa

    stages = []
    if which in ("repartition", "all"):
        stages.append(_stage(s, "repartitioner", t(ACCESS_EVENTS),
                             RepartitionProcessor(t(ACCESS_EVENTS_BY_RESOURCE), s.res_window_seconds, s.res_shards), None, inst))
    if which in ("shard", "windows", "all"):
        stages.append(_stage(s, "resource-shard", t(ACCESS_EVENTS_BY_RESOURCE),
                             ShardStage(t(RESOURCE_PARTIALS), t(LATE_EVENTS), s.res_window_seconds, s.delta_late,
                                        s.res_window_grace, source_partitions=s.kafka_partitions),
                             t(RES_SHARD_CHANGELOG), inst))
    if which in ("merge", "windows", "all"):
        stages.append(_stage(s, "resource-merge", t(RESOURCE_PARTIALS),
                             MergeStage(t(RESOURCE_WINDOWS), s.res_shards, s.res_merge_timeout, kappa), t(RES_MERGE_CHANGELOG), inst))
    install_stop(lambda: [st.stop() for st in stages])
    log.info("resource windows (%s): Delta_res=%.0fs J_r=%d delta_late=%.1fs", which, s.res_window_seconds,
             s.res_shards, s.delta_late)
    threads = [threading.Thread(target=st.run, name=f"stage-{i}", daemon=True) for i, st in enumerate(stages)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

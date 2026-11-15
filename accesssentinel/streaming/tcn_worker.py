"""TCN worker (D3 / Revised v3 Steps 4-5): individual risk S_ind = f_TCN(X_u) for every access event, three
risk tiers, and periodic re-scoring of Medium-risk users. Run several replicas; Kafka splits the user partitions.

Revised v3 (Phase 3): when the served model is a v3 TCN (manifest arch "tcn-v3") the worker keeps the v3 state
(`tcn3:state:<u>`: last L = 128 raw Table III records + history statistics), scores [h_T ; z_u(t)], uses the
model's CALIBRATED thresholds (theta_low / theta_high / theta_B, Eq. 8) unless an operator set explicit ones,
handles the late-event policies exactly ("history_only": state only; "rescore": the event is inserted at its
event-time position and the corrected window re-scored), and re-scores Medium users with z_u(t) at the re-check
time (Eq. 9). D3 models keep the D3 path below (baseline).

    python -m accesssentinel.streaming.tcn_worker

Stateless workers: each user's profile and last-L window live in Redis (`tcn:profile:<u>`), so any replica
can score any user after a rebalance without state migration (D3 Step 4, audit D6).

Per batch: load profiles -> update + build windows (accesssentinel.ml.features, identical to training) ->
one forward pass -> publish to `risk-scores` and wait for Kafka's acknowledgement -> save profiles and risk
state -> (StreamConsumer) commit offsets. A crash before the profile write recomputes identical scores
(same score_id, deduplicated downstream); a crash after it skips the already-applied events by event id.

Risk tiers (D3 Step 5): S_ind < 0.4 low, < 0.6 medium, else high. High scores are published immediately
(the escalation policy and PDP consume `risk-scores`). Medium users of LIVE traffic are queued in
`tcn:recheck` (sorted set, due time) and re-scored every T_recheck with the inactivity-refreshed window;
replayed/historical events (older than tcn_live_window_seconds) are not re-checked.

No trained model -> the worker does not consume at all (events wait in Kafka); it never emits scores from an
untrained network. Every score carries model_version.
"""
import json
import logging
import os
import socket
import time
from dataclasses import replace
from typing import Callable, Dict, List, Optional

from ..config import RiskConfig
from ..ml.features import (L_V3, LiveTimerContext, UserProfile, UserStateV3, observe, observe_v3, timer_context_at,
                           window_for_recheck)
from ..topics import ACCESS_EVENTS_ORDERED, RISK_SCORES, topic_name
from .codec import decode_access_event
from .consumer import Record

log = logging.getLogger("accesssentinel.tcn_worker")

PROFILE = "tcn:profile:{}"
STATE_V3 = "tcn3:state:{}"
RISK = "risk:{}"
RECHECK = "tcn:recheck"
SCORE_SCHEMA_VERSION = 1


def tier_of(s: float, rc: RiskConfig) -> str:
    return "high" if s >= rc.theta_high else ("medium" if s >= rc.theta_low else "low")



def _trace(ev, t_deq: float, t_scored: float) -> dict:
    """Per-stage timestamps (wall clock, one host) for the latency decomposition Lat_ind (Eq. 31, Exp. C)."""
    m = ev.meta if isinstance(ev.meta, dict) else {}
    return {"te": ev.t, "ts_gw_in": m.get("ts_gw_in"), "ts_gw_pub": m.get("ts_gw_pub"), "ti": ev.ti or None,
            "ts_seq_in": m.get("ts_seq_in"), "ts_seq_out": m.get("ts_seq_out"), "reorder_wait": m.get("reorder_wait"),
            "order": m.get("order"), "ts_tcn_dequeue": t_deq, "ts_tcn_scored": t_scored}

class ModelProvider:
    """Serves the CURRENT registry model and hot-swaps when a new version is promoted."""

    def __init__(self, model_dir: str, poll_seconds: float = 30.0, loader=None, clock=time.monotonic):
        from ..ml import registry
        self.model_dir, self.poll, self.clock = model_dir, poll_seconds, clock
        self._loader = loader or (lambda v: registry.TcnScorer.load(model_dir, v))
        self._current = registry.current_version
        self.scorer, self._checked = None, -1e18

    def get(self):
        if self.clock() - self._checked >= self.poll or self.scorer is None:
            self._checked = self.clock()
            v = self._current(self.model_dir)
            if v and (self.scorer is None or self.scorer.version != v):
                self.scorer = self._loader(v)
                log.info("serving TCN model %s", v)
        return self.scorer


class TcnWorkerHandler:
    def __init__(self, models: ModelProvider, redis, producer, out_topic: str, risk_cfg: RiskConfig = None,
                 recheck_seconds: Optional[float] = None, live_window: float = 3600.0, L: int = 20,
                 worker_id: str = "tcn", clock: Callable[[], float] = time.time):
        self.models, self.r, self.p, self.out_topic = models, redis, producer, out_topic
        self.rc = risk_cfg or RiskConfig()
        self.recheck_s = recheck_seconds if recheck_seconds is not None else self.rc.t_recheck
        self.live_window, self.L, self.worker_id, self.clock = live_window, L, worker_id, clock
        self._errors: List[str] = []
        self.settings = None                      # runtime_settings.Watcher
        self.explicit_thresholds = False          # an operator set risk.theta_low / theta_high (v3: overrides calibration)
        self.timer = LiveTimerContext(redis)
        self.stats = {"scored": 0, "skipped_redelivered": 0, "rechecks": 0, "high": 0, "medium": 0, "low": 0,
                      "last_batch_ms": 0.0}

    # ------------------------------------------------------------------ StreamConsumer protocol
    def decode(self, rec: Record):
        return decode_access_event(rec.value)

    def checkpoint(self, force: bool) -> bool:
        return True                       # handle() returns only after scores are acknowledged and state saved

    def reset(self) -> None:
        pass

    def _thresholds(self, scorer) -> RiskConfig:
        """Tier thresholds for this scorer: calibrated (v3 manifest) unless an operator set explicit ones."""
        th = getattr(scorer, "thresholds", None) or {}
        if th and not self.explicit_thresholds:
            return replace(self.rc, theta_low=float(th["theta_low"]), theta_high=float(th["theta_high"]))
        return self.rc

    def _threshold_fields(self, scorer, rc: RiskConfig) -> dict:
        th = getattr(scorer, "thresholds", None) or {}
        return {"theta_low": rc.theta_low, "theta_high": rc.theta_high,
                "theta_B": float(th["theta_B"]) if th.get("theta_B") is not None else None,
                "thresholds": "explicit" if (self.explicit_thresholds or not th) else "calibrated"}

    def handle(self, records: List[Record]):
        t_deq = self.clock()                                      # trace: TCN dequeue time (Exp. C)
        scorer = self.models.get()
        if getattr(scorer, "arch", "d3") == "tcn-v3":
            return self._handle_v3(records, scorer, t_deq)
        users = sorted({r.data.u for r in records})
        raw = self.r.mget([PROFILE.format(u) for u in users])
        profiles: Dict[str, UserProfile] = {u: UserProfile.from_dict(json.loads(v)) if v else UserProfile()
                                            for u, v in zip(users, raw)}
        todo = []
        for rec in records:
            ev = rec.data
            p = profiles[ev.u]
            if ev.event_id and ev.event_id in p.seen_ids:
                self.stats["skipped_redelivered"] += 1
                continue
            observe(p, ev, self.L)
            late = ev.meta.get("late_policy") if isinstance(ev.meta, dict) else None
            if late == "history_only":                                # v3 late-event policy 1: history only
                self.stats["late_history_only"] = self.stats.get("late_history_only", 0) + 1
                continue
            # policies "rescore" / "retro_alert": the event is added to the history and the window re-scored.
            # The D3 incremental profile cannot re-order already-observed events; exact window correction comes
            # with the v3 TCN state (Phase 3).
            todo.append((ev, list(p.window)))
        if not todo:
            return []
        t0 = time.perf_counter()
        scores = scorer.score_windows([w for _, w in todo])
        ms = (time.perf_counter() - t0) * 1000.0
        now = self.clock()
        out = []
        for (ev, w), s in zip(todo, scores):
            tier = tier_of(float(s), self.rc)
            out.append({"score_id": ev.event_id, "schema_version": SCORE_SCHEMA_VERSION, "u": ev.u, "t": ev.t,
                        "event_id": ev.event_id, "a": ev.a, "r": ev.r, "s_ind": float(s), "tier": tier,
                        "trigger": "event", "model_version": scorer.version, "model_kind": "tcn",
                        "scored_at": now, "batch_inference_ms": round(ms, 3), "batch_size": len(todo),
                        "window_len": len(w), "source": ev.meta.get("source", "live") if isinstance(ev.meta, dict) else "live",
                        "late_policy": ev.meta.get("late_policy") if isinstance(ev.meta, dict) else None,
                        "trace": _trace(ev, t_deq, now)})
        self._publish(out)                                    # raises on failure -> batch retried, nothing saved
        pipe = self.r.pipeline(transaction=False)
        for u in users:
            pipe.set(PROFILE.format(u), json.dumps(profiles[u].to_dict()))
        latest = {}
        for rec in out:
            latest[rec["u"]] = rec
        for u, rec in latest.items():
            self._risk_update(pipe, rec, live=now - rec["t"] <= self.live_window)
        pipe.execute()
        self.stats["scored"] += len(out)
        self.stats["last_batch_ms"] = round(ms, 3)
        for rec in out:
            self.stats[rec["tier"]] += 1
        return []

    # ------------------------------------------------------------------ Revised v3 path
    def _handle_v3(self, records: List[Record], scorer, t_deq: float):
        rc = self._thresholds(scorer)
        users = sorted({r.data.u for r in records})
        raw = self.r.mget([STATE_V3.format(u) for u in users])
        states: Dict[str, UserStateV3] = {u: UserStateV3.from_dict(json.loads(v)) if v else UserStateV3()
                                          for u, v in zip(users, raw)}
        todo = []
        for rec in records:
            ev = rec.data
            st = states[ev.u]
            if ev.event_id and ev.event_id in st.seen_ids:
                self.stats["skipped_redelivered"] += 1
                continue
            raw_rec, z = observe_v3(st, ev, self.timer.threat, self.timer.posture, scorer.L)
            late = ev.meta.get("late_policy") if isinstance(ev.meta, dict) else None
            if late == "history_only":                                # v3 late-event policy 1
                self.stats["late_history_only"] = self.stats.get("late_history_only", 0) + 1
                continue
            # "rescore" / "retro_alert": observe_v3 inserted the event at its event-time position; the corrected
            # window is re-scored with the context of its newest event
            todo.append((ev, list(st.window), st.window[-1]["z"]))
        if not todo:
            self._save_v3(states)
            return []
        t0 = time.perf_counter()
        scores = scorer.score_windows([w for _, w, _ in todo], [z for _, _, z in todo])
        ms = (time.perf_counter() - t0) * 1000.0
        now = self.clock()
        thf = self._threshold_fields(scorer, rc)
        out = []
        for (ev, w, _), s in zip(todo, scores):
            out.append({"score_id": ev.event_id, "schema_version": SCORE_SCHEMA_VERSION, "u": ev.u, "t": ev.t,
                        "event_id": ev.event_id, "a": ev.a, "r": ev.r, "s_ind": float(s), "tier": tier_of(float(s), rc),
                        "trigger": "event", "model_version": scorer.version, "model_kind": "tcn-v3", **thf,
                        "scored_at": now, "batch_inference_ms": round(ms, 3), "batch_size": len(todo), "window_len": len(w),
                        "source": ev.meta.get("source", "live") if isinstance(ev.meta, dict) else "live",
                        "late_policy": ev.meta.get("late_policy") if isinstance(ev.meta, dict) else None,
                        "trace": _trace(ev, t_deq, now)})
        self._publish(out)                                    # raises on failure -> batch retried, nothing saved
        pipe = self._save_v3(states, execute=False)
        latest = {}
        for rec in out:
            latest[rec["u"]] = rec
        for u, rec in latest.items():
            self._risk_update(pipe, rec, live=now - rec["t"] <= self.live_window)
        pipe.execute()
        self.stats["scored"] += len(out)
        self.stats["last_batch_ms"] = round(ms, 3)
        for rec in out:
            self.stats[rec["tier"]] += 1
        return []

    def _save_v3(self, states: Dict[str, UserStateV3], execute: bool = True):
        pipe = self.r.pipeline(transaction=False)
        for u, st in states.items():
            pipe.set(STATE_V3.format(u), json.dumps(st.to_dict(), separators=(",", ":")))
        if execute:
            pipe.execute()
        return pipe

    def _recheck_v3(self, users: List[str], scorer, now: float) -> List[dict]:
        rc = self._thresholds(scorer)
        raw = self.r.mget([STATE_V3.format(u) for u in users])
        items = [(u, UserStateV3.from_dict(json.loads(v))) for u, v in zip(users, raw) if v]
        items = [(u, st) for u, st in items if st.window]
        if not items:
            return []
        zs = [timer_context_at(st, now, st.window[-1]["z"][2], st.window[-1]["z"][3]) for _, st in items]   # Eq. 9
        scores = scorer.score_windows([st.window for _, st in items], zs)
        thf = self._threshold_fields(scorer, rc)
        return [{"score_id": f"recheck:{u}:{int(now * 1000)}", "schema_version": SCORE_SCHEMA_VERSION, "u": u, "t": now,
                 "event_id": None, "s_ind": float(s), "tier": tier_of(float(s), rc), "trigger": "recheck",
                 "seconds_since_last_event": round(now - (st.last_t or now), 1), "model_version": scorer.version,
                 "model_kind": "tcn-v3", **thf, "timer_context": [round(x, 4) for x in z], "scored_at": now,
                 "source": "live"} for (u, st), s, z in zip(items, scores, zs)]

    # ------------------------------------------------------------------ medium-tier re-scoring
    def apply_settings(self) -> None:
        from .. import runtime_settings as rs
        eff = self.settings.poll() if self.settings is not None else None
        if eff:
            self.rc = replace(self.rc, theta_low=eff["risk.theta_low"], theta_high=eff["risk.theta_high"])
            ex = rs.explicit(self.r)
            self.explicit_thresholds = "risk.theta_low" in ex or "risk.theta_high" in ex
            if "risk.t_recheck" in rs.explicit(self.r):          # env overrides stay unless an operator set one
                self.recheck_s = eff["risk.t_recheck"]
            log.info("runtime settings applied: theta_low=%s theta_high=%s recheck=%ss", self.rc.theta_low,
                     self.rc.theta_high, self.recheck_s)

    def tick(self, max_users: int = 200) -> int:
        """Re-score Medium users whose T_recheck elapsed (claimed atomically: ZPOPMIN, not-due pushed back)."""
        self.apply_settings()
        now = self.clock()
        popped = self.r.zpopmin(RECHECK, max_users)
        due = [m for m, sc in popped if sc <= now]
        later = {m: sc for m, sc in popped if sc > now}
        if later:
            self.r.zadd(RECHECK, later)
        if not due:
            self._status(now)
            return 0
        scorer = self.models.get()
        users = [m.decode() if isinstance(m, bytes) else m for m in due]
        if getattr(scorer, "arch", "d3") == "tcn-v3":
            out = self._recheck_v3(users, scorer, now)
            if out:
                self._publish(out)
                pipe = self.r.pipeline(transaction=False)
                for rec in out:
                    self._risk_update(pipe, rec, live=True)
                pipe.execute()
                self.stats["rechecks"] += len(out)
            self._status(now)
            return len(out)
        raw = self.r.mget([PROFILE.format(u) for u in users])
        items = [(u, UserProfile.from_dict(json.loads(v))) for u, v in zip(users, raw) if v]
        items = [(u, p) for u, p in items if p.window]
        if not items:
            return 0
        scores = scorer.score_windows([window_for_recheck(p, now) for _, p in items])
        out = [{"score_id": f"recheck:{u}:{int(now * 1000)}", "schema_version": SCORE_SCHEMA_VERSION, "u": u, "t": now,
                "event_id": None, "s_ind": float(s), "tier": tier_of(float(s), self.rc), "trigger": "recheck",
                "seconds_since_last_event": round(now - (p.last_t or now), 1), "model_version": scorer.version,
                "model_kind": "tcn", "scored_at": now, "source": "live"} for (u, p), s in zip(items, scores)]
        self._publish(out)
        pipe = self.r.pipeline(transaction=False)
        for rec in out:
            self._risk_update(pipe, rec, live=True)
        pipe.execute()
        self.stats["rechecks"] += len(out)
        self._status(now)
        return len(out)

    # ------------------------------------------------------------------ helpers
    def _risk_update(self, pipe, rec: dict, live: bool) -> None:
        m = {"s_ind": rec["s_ind"], "tier": rec["tier"], "t": rec["t"], "trigger": rec["trigger"],
             "model_version": rec["model_version"], "scored_at": rec["scored_at"]}
        for k in ("theta_low", "theta_high", "theta_B"):
            if rec.get(k) is not None:
                m[k] = rec[k]
        pipe.hset(RISK.format(rec["u"]), mapping=m)
        if rec["tier"] == "medium" and live:
            pipe.zadd(RECHECK, {rec["u"]: rec["scored_at"] + self.recheck_s})
        else:
            pipe.zrem(RECHECK, rec["u"])

    def _cb(self, err, msg) -> None:
        if err is not None:
            self._errors.append(str(err))

    def _publish(self, recs: List[dict]) -> None:
        self._errors = []
        for rec in recs:
            while True:
                try:
                    self.p.produce(self.out_topic, key=rec["u"].encode(), value=json.dumps(rec).encode(),
                                   on_delivery=self._cb)
                    break
                except BufferError:
                    self.p.poll(0.5)
        remaining = self.p.flush(30)
        if remaining or self._errors:
            raise RuntimeError(f"risk scores not acknowledged: remaining={remaining} errors={self._errors[:3]}")

    def _status(self, now: float) -> None:
        sc = self.models.scorer
        self.r.hset(f"tcn:worker:{self.worker_id}", mapping={"model_version": sc.version if sc else "",
                                                              "updated_at": now, **{k: v for k, v in self.stats.items()}})
        self.r.expire(f"tcn:worker:{self.worker_id}", 120)


def main() -> int:
    import redis as redis_lib
    import torch
    from confluent_kafka import Consumer, Producer

    from ..settings import get_settings
    from .consumer import DeadLetterQueue, StreamConsumer, consumer_config, producer_config
    from .runtime import Heartbeat, install_stop, setup

    s = get_settings()
    setup(s.log_level)
    torch.set_num_threads(s.torch_threads)
    worker_id = f"tcn-{socket.gethostname()}"
    hb = Heartbeat()
    models = ModelProvider(s.model_dir, s.tcn_model_poll_seconds)
    stop = {"flag": False}
    install_stop(lambda: stop.__setitem__("flag", True))
    while models.get() is None and not stop["flag"]:                 # never score with an untrained network
        log.warning("no trained TCN in %s/tcn (CURRENT missing); not consuming. Train with "
                    "`python -m accesssentinel.ml.train_tcn --promote`", s.model_dir)
        for _ in range(10):
            hb(); time.sleep(1)
        models._checked = -1e18
    if stop["flag"]:
        return 0
    group = "tcn-v3"                                     # new group: input is the event-time ordered stream
    r = redis_lib.Redis(**s.redis_kwargs())
    handler = TcnWorkerHandler(models, r, Producer(producer_config(s.kafka_bootstrap, worker_id)),
                               topic_name(RISK_SCORES, s.kafka_topic_prefix), RiskConfig(), s.tcn_recheck_seconds,
                               s.tcn_live_window_seconds, worker_id=worker_id)
    from ..runtime_settings import Watcher
    handler.settings = Watcher(r)
    consumer = Consumer(consumer_config(s.kafka_bootstrap, group, worker_id))
    dlq = DeadLetterQueue(Producer(producer_config(s.kafka_bootstrap, f"{worker_id}-dlq")), group)
    sc = StreamConsumer(consumer, [topic_name(ACCESS_EVENTS_ORDERED, s.kafka_topic_prefix)], handler, dlq,
                        batch_size=s.consumer_batch, heartbeat=hb, tick=handler.tick)
    install_stop(sc.stop)
    log.info("%s scoring with %s (recheck every %.0fs)", worker_id, models.scorer.version, handler.recheck_s)
    sc.run()
    log.info("%s stopped: %s %s", worker_id, sc.stats, handler.stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

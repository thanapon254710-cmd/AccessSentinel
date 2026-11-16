"""Escalation service (D3 Step 6): consumes `risk-scores` (Path A, Path D activity/tiers) and the resource-keyed
view `access-events-by-resource` (Path B), runs the periodic HDFS sweep (Path C) and samples Low-risk users
(Path D); emits the union to `escalations.realtime` (A, B) and `escalations.background` (C, D) for the TGN.

    python -m accesssentinel.streaming.escalation_service                    # the service
    python -m accesssentinel.streaming.escalation_service sweep-now --at 2026-10-01T12:00 [--publish]

`sweep-now` runs Path C immediately at a chosen reference time over the HDFS history (experiments, demos,
or an operator-triggered sweep) and optionally publishes the resulting background escalations.
"""
import argparse
import json
import logging
import random
import socket
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone
from typing import List

from ..config import EscalationConfig, RiskConfig
from ..escalation_engine import Escalator, baseline_range
from ..topics import (ACCESS_EVENTS_BY_RESOURCE, ESCALATIONS_BACKGROUND, ESCALATIONS_REALTIME, RESOURCE_WINDOWS,
                      RISK_SCORES, topic_name)
from .codec import decode_access_event, decode_json_object
from .consumer import DecodeError, Record

log = logging.getLogger("accesssentinel.escalation")


def lake_loader(lake, cfg: EscalationConfig):
    def load(now: float) -> dict:
        lb0 = now - cfg.sweep_lookback
        return {"lookback_events": lake.read_events(lb0, now),
                "baseline_events": lake.read_events(*baseline_range(lb0, cfg)),
                "lookback_scores": [r["payload"] for r in lake.read_records("risk-scores", lb0, now)]}
    return load


class EscalationHandler:
    def __init__(self, esc: Escalator, producer, realtime_topic: str, background_topic: str, scores_topic: str,
                 resource_topic: str, sweep_loader=None):
        self.esc, self.p = esc, producer
        self.rt_topic, self.bg_topic = realtime_topic, background_topic
        self.scores_topic, self.resource_topic = scores_topic, resource_topic
        self.sweep_loader = sweep_loader
        self.settings = None                      # runtime_settings.Watcher
        self._errors: List[str] = []
        self.stats = {"scores": 0, "resource_events": 0, "realtime": 0, "background": 0, "sweeps": 0}

    def decode(self, rec: Record):
        if rec.topic == self.resource_topic and not self.esc.cfg.use_resource_windows:
            return decode_access_event(rec.value)
        if rec.topic == self.resource_topic:
            d = decode_json_object(rec.value)
            if "users" not in d or "rstar" not in d:
                raise DecodeError("resource window without users / rstar")
            return d
        d = decode_json_object(rec.value)
        if not isinstance(d.get("u"), str) or not isinstance(d.get("s_ind"), (int, float)):
            raise DecodeError("risk score without u / s_ind")
        return d

    def handle(self, records: List[Record]):
        for rec in records:
            if rec.topic == self.resource_topic:
                if self.esc.cfg.use_resource_windows:
                    self.esc.on_resource_window(rec.data)          # v3: finalised event-time window (Alg. 1, l. 2)
                else:
                    self.esc.on_event(rec.data)                    # D3 baseline: sliding count per event
                self.stats["resource_events"] += 1
            else:
                self.esc.on_score(rec.data)
                self.stats["scores"] += 1
        return []

    def checkpoint(self, force: bool) -> bool:
        return True                                   # inputs are applied to Redis inside handle()

    def reset(self) -> None:
        pass

    def tick(self) -> None:
        from .. import runtime_settings as rs
        eff = self.settings.poll() if self.settings is not None else None
        if eff:
            self.esc.cfg = rs.apply_escalation(self.esc.cfg, eff)
            self.esc.rc = rs.apply_risk(self.esc.rc, eff)
            self.esc.theta_high_override = eff["risk.theta_high"] if "risk.theta_high" in rs.explicit(self.esc.r) else None
            log.info("runtime settings applied: %s", self.esc.config_snapshot())
        self._emit(self.esc.flush_realtime(), self.rt_topic, "realtime")
        if self.sweep_loader is not None and self.esc.sweep_due():
            info = self.esc.run_sweep(self.sweep_loader)
            if info:
                self.stats["sweeps"] += 1
                log.info("sweep: %s", info)
        self._emit(self.esc.flush_background(), self.bg_topic, "background")

    def _emit(self, recs: List[dict], topic: str, kind: str) -> None:
        if not recs:
            return
        self._errors = []
        for rec in recs:
            self.p.produce(topic, key=rec["u"].encode(), value=json.dumps(rec).encode(), on_delivery=self._cb)
        remaining = self.p.flush(30)
        if remaining or self._errors:
            self.esc.restage(recs)
            raise RuntimeError(f"escalations not acknowledged ({kind}): remaining={remaining} errors={self._errors[:3]}")
        self.stats[kind] += len(recs)
        log.info("%s escalations: %s", kind, ", ".join(f"{r['u']}{r['paths']}" for r in recs[:10]))

    def _cb(self, err, msg) -> None:
        if err is not None:
            self._errors.append(str(err))


def neighbourhood_size_fn(redis, tenant: str = "default", window: float = 3600.0, k: int = 10):
    """(|E(N_h(u))|, |V(N_h(u))|) for Eq. 26, from the live edge index, matching what the SGE embeds (Phase 6):
    the user's window edges + up to K recent users per touched resource (1 hop) and all of their window edges.
    The neighbours' own window edges are counted too (ZCOUNT, no decoding), because the SGE embeds all of them:
    counting one edge per neighbour under-estimated |E| ~7x on dense neighbourhoods (Phase 6 cost fit)."""
    def raw(ids):
        return [json.loads(v) for v in redis.mget([f"g:e:{i.decode() if isinstance(i, bytes) else i}" for i in ids]) if v] if ids else []

    def size(u: str, now: float):
        t0 = now - window
        seed = raw(redis.zrangebyscore(f"g:user:{u}", t0, now))
        res = sorted({e["r"] for e in seed})
        users = {u}
        for rk in res:
            recent = []
            for e in raw(redis.zrevrangebyscore(f"g:res:{rk}", now, t0)):        # most recent first, as the SGE takes them
                if e["u"] not in recent:
                    recent.append(e["u"])
                if len(recent) >= k:
                    break
            users |= set(recent)
        others = sorted(users - {u})
        pipe = redis.pipeline(transaction=False)
        for v in others:
            pipe.zcount(f"g:user:{v}", t0, now)
        n_other = sum(int(c) for c in pipe.execute()) if others else 0
        return len(seed) + n_other, len(users) + len(res)
    return size


def escalation_config(s) -> EscalationConfig:
    cfg = EscalationConfig()
    over = {"gamma": s.esc_gamma, "kappa": s.esc_kappa, "t_sweep": s.esc_t_sweep, "analysis_window": s.esc_analysis_window}
    return replace(cfg, **{k: v for k, v in over.items() if v is not None})


def _ts(v: str) -> float:
    try:
        return float(v)
    except ValueError:
        dt = datetime.fromisoformat(v)
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()


def main(argv=None) -> int:
    import redis as redis_lib
    from confluent_kafka import Consumer, Producer

    from ..adapters.hdfs_lake import make_lake
    from ..settings import get_settings
    from .consumer import DeadLetterQueue, StreamConsumer, consumer_config, producer_config
    from .runtime import Heartbeat, install_stop, setup

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")
    sn = sub.add_parser("sweep-now"); sn.add_argument("--at", default=None); sn.add_argument("--publish", action="store_true")
    args = ap.parse_args(argv)
    s = get_settings()
    setup(s.log_level)
    cfg = escalation_config(s)
    r = redis_lib.Redis(**s.redis_kwargs())
    lake = make_lake(s)
    client = f"escalation-{socket.gethostname()}"
    producer = Producer(producer_config(s.kafka_bootstrap, client))
    topics = {k: topic_name(v, s.kafka_topic_prefix) for k, v in
              {"rt": ESCALATIONS_REALTIME, "bg": ESCALATIONS_BACKGROUND, "scores": RISK_SCORES,
               "res": RESOURCE_WINDOWS if cfg.use_resource_windows else ACCESS_EVENTS_BY_RESOURCE}.items()}

    if args.cmd == "sweep-now":
        at = _ts(args.at) if args.at else time.time()
        esc = Escalator(r, cfg, RiskConfig(), clock=lambda: at)
        info = esc.run_sweep(lake_loader(lake, cfg), force=True)
        recs = esc.drain_sweep()                         # live cooldowns/sampling do not apply to a swept period
        h = EscalationHandler(esc, producer, topics["rt"], topics["bg"], topics["scores"], topics["res"])
        if args.publish:
            h._emit(recs, topics["bg"], "background")
        print(json.dumps({"at": at, "sweep": info, "escalations": [{"u": x["u"], "paths": x["paths"],
                          "rules": sorted(x["evidence"].get("sweep", {}).get("rules", {}))} for x in recs],
                          "published": bool(args.publish)}))
        return 0

    esc = Escalator(r, cfg, RiskConfig(), rng=random.Random())
    esc.cost_fn = neighbourhood_size_fn(r, s.tenant)
    handler = EscalationHandler(esc, producer, topics["rt"], topics["bg"], topics["scores"], topics["res"],
                                sweep_loader=lake_loader(lake, cfg))
    from ..runtime_settings import Watcher
    handler.settings = Watcher(r)
    group = "escalation"
    consumer = Consumer(consumer_config(s.kafka_bootstrap, group, client))
    dlq = DeadLetterQueue(Producer(producer_config(s.kafka_bootstrap, f"{client}-dlq")), group)
    sc = StreamConsumer(consumer, [topics["scores"], topics["res"]], handler, dlq, batch_size=s.consumer_batch,
                        heartbeat=Heartbeat(), tick=handler.tick)
    install_stop(sc.stop)
    log.info("escalation service: %s", esc.config_snapshot())
    sc.run()
    log.info("escalation service stopped: %s %s", sc.stats, handler.stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())

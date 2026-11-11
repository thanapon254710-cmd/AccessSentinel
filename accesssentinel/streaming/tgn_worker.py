"""TGN worker (D3 Step 7): collective risk S_coll(g(u)) = f_TGN(G_t[N(u)]) for escalated users.

    python -m accesssentinel.streaming.tgn_worker realtime      # group tgn-rt on escalations.realtime
    python -m accesssentinel.streaming.tgn_worker background    # group tgn-bg on escalations.background

Real-time and background escalations are served by separate consumer groups/processes, so background graph
analysis never delays urgent analysis (D3 Step 10). For each escalation: the neighbourhood is anchored at the
latest event time in the escalation's evidence (so replayed history is analysed in its own time frame),
built from the live edge index (graph_index), scored by the promoted TGN, and the result is published to
`group-scores`. When a coupled group exists, every member's `coll:<u>` hash is updated for the PDP (group-wide
enforcement, Phase 8). No trained TGN -> the worker does not consume.
"""
import hashlib
import json
import logging
import socket
import sys
import time
from typing import List

from ..ml.tgn_graph import build_sample
from ..topics import ESCALATIONS_BACKGROUND, ESCALATIONS_REALTIME, GROUP_SCORES, topic_name
from .codec import decode_json_object
from .consumer import DecodeError, Record

log = logging.getLogger("accesssentinel.tgn_worker")
COLL = "coll:{}"


def anchor_time(esc: dict) -> float:
    ts = [float(v["t"]) for v in (esc.get("evidence") or {}).values() if isinstance(v, dict) and isinstance(v.get("t"), (int, float))]
    return max(ts) if ts else float(esc["t"])


class TgnModels:
    def __init__(self, model_dir: str, poll: float = 30.0, loader=None, clock=time.monotonic):
        from ..ml import tgn_registry
        self.model_dir, self.poll, self.clock = model_dir, poll, clock
        self._loader = loader or (lambda v: tgn_registry.TgnScorer.load(model_dir, v))
        self._current = tgn_registry.current_version
        self.scorer, self._checked = None, -1e18

    def get(self):
        if self.scorer is None or self.clock() - self._checked >= self.poll:
            self._checked = self.clock()
            v = self._current(self.model_dir)
            if v and (self.scorer is None or self.scorer.version != v):
                self.scorer = self._loader(v)
                log.info("serving TGN model %s", v)
        return self.scorer


class TgnWorkerHandler:
    def __init__(self, models, index, redis, producer, out_topic: str, coll_ttl: float = 3600.0, clock=time.time):
        self.models, self.index, self.r, self.p, self.out_topic = models, index, redis, producer, out_topic
        self.coll_ttl, self.clock = coll_ttl, clock
        self._errors: List[str] = []
        self.stats = {"analysed": 0, "groups": 0, "no_group": 0}

    def decode(self, rec: Record):
        d = decode_json_object(rec.value)
        if not isinstance(d.get("u"), str) or not d.get("escalation_id"):
            raise DecodeError("escalation without u / escalation_id")
        return d

    def checkpoint(self, force: bool) -> bool:
        return True

    def reset(self) -> None:
        pass

    def handle(self, records: List[Record]):
        scorer = self.models.get()
        out, coll = [], []
        for rec in records:
            esc = rec.data
            t0 = time.perf_counter()
            anchor = anchor_time(esc)
            sample = build_sample([esc["u"]], self.index, anchor, scorer.gp)
            res = scorer.analyse(sample).get(esc["u"]) if sample.edges else None
            ms = (time.perf_counter() - t0) * 1000
            g = res or {"members": [esc["u"]], "s_coll": None, "reason": "no recent interactions"}
            group_id = None
            if g["s_coll"] is not None:
                group_id = hashlib.sha256(f"{','.join(g['members'])}|{int(sample.t0)}|{int(sample.t1)}".encode()).hexdigest()[:16]
            r = {"score_id": f"coll:{esc['escalation_id']}", "schema_version": 1, "u": esc["u"], "t": self.clock(),
                 "anchor_t": anchor, "window": [sample.t0, sample.t1], "escalation_id": esc["escalation_id"],
                 "paths": esc.get("paths"), "priority": esc.get("priority"), "s_coll": g["s_coll"],
                 "above_theta_coll": bool(g["s_coll"] is not None and g["s_coll"] >= scorer.theta_coll),
                 "theta_coll": scorer.theta_coll, "tau_edge": scorer.tau_edge, "group_id": group_id,
                 "members": g["members"], "member_scores": g.get("member_scores"), "resources": g.get("resources"),
                 "top_edges": g.get("top_edges"), "reason": g.get("reason"),
                 "graph": {"users": len(sample.users), "resources": len(sample.resources), "apps": len(sample.apps),
                           "access_edges": len(sample.edges)},
                 "model_version": scorer.version, "model_kind": "tgn", "inference_ms": round(ms, 2)}
            out.append(r)
            if group_id:
                coll.append(r)
        self._publish(out)
        if coll:
            pipe = self.r.pipeline(transaction=False)
            for r in coll:
                for m in r["members"]:
                    pipe.hset(COLL.format(m), mapping={"s_coll": r["s_coll"], "group_id": r["group_id"],
                                                        "members": json.dumps(r["members"]), "t": r["t"],
                                                        "above_theta_coll": int(r["above_theta_coll"]),
                                                        "model_version": r["model_version"], "score_id": r["score_id"]})
                    pipe.expire(COLL.format(m), int(self.coll_ttl))
            pipe.execute()
        self.stats["analysed"] += len(out)
        self.stats["groups"] += len(coll)
        self.stats["no_group"] += len(out) - len(coll)
        return []

    def _cb(self, err, msg):
        if err is not None:
            self._errors.append(str(err))

    def _publish(self, recs):
        self._errors = []
        for r in recs:
            self.p.produce(self.out_topic, key=r["u"].encode(), value=json.dumps(r).encode(), on_delivery=self._cb)
        remaining = self.p.flush(30)
        if remaining or self._errors:
            raise RuntimeError(f"group scores not acknowledged: remaining={remaining} errors={self._errors[:3]}")


def main(argv=None) -> int:
    import redis as redis_lib
    import torch
    from confluent_kafka import Consumer, Producer
    from ..config import PDPConfig
    from ..settings import get_settings
    from .consumer import DeadLetterQueue, StreamConsumer, consumer_config, producer_config
    from .graph_index import RedisEdgeIndex
    from .runtime import Heartbeat, install_stop, setup

    argv = sys.argv[1:] if argv is None else argv
    priority = argv[0] if argv else "realtime"
    if priority not in ("realtime", "background"):
        print("usage: tgn_worker realtime|background"); return 2
    s = get_settings()
    setup(s.log_level)
    torch.set_num_threads(s.torch_threads)
    client = f"tgn-{priority}-{socket.gethostname()}"
    models = TgnModels(s.model_dir, s.tcn_model_poll_seconds)
    hb = Heartbeat()
    stop = {"flag": False}
    install_stop(lambda: stop.__setitem__("flag", True))
    while models.get() is None and not stop["flag"]:
        log.warning("no trained TGN in %s/tgn (CURRENT missing); not consuming. Train with "
                    "`python -m accesssentinel.ml.train_tgn --promote`", s.model_dir)
        for _ in range(10):
            hb(); time.sleep(1)
        models._checked = -1e18
    if stop["flag"]:
        return 0
    r = redis_lib.Redis(**s.redis_kwargs())
    topic = ESCALATIONS_REALTIME if priority == "realtime" else ESCALATIONS_BACKGROUND
    group = "tgn-rt" if priority == "realtime" else "tgn-bg"
    h = TgnWorkerHandler(models, RedisEdgeIndex(r), r, Producer(producer_config(s.kafka_bootstrap, client)),
                         topic_name(GROUP_SCORES, s.kafka_topic_prefix), PDPConfig().coll_ttl)
    sc = StreamConsumer(Consumer(consumer_config(s.kafka_bootstrap, group, client)), [topic_name(topic, s.kafka_topic_prefix)],
                        h, DeadLetterQueue(Producer(producer_config(s.kafka_bootstrap, f"{client}-dlq")), group),
                        batch_size=50, heartbeat=hb)
    install_stop(sc.stop)
    log.info("%s analysing %s with %s", client, topic, models.scorer.version)
    sc.run()
    log.info("%s stopped: %s %s", client, sc.stats, h.stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Temporal edge index for the TGN (D3: "Kafka -> resource-keyed event stream -> temporal graph construction").

    python -m accesssentinel.streaming.graph_index

Consumes `access-events-by-resource` (group graph-index). That view is keyed by resource AND time bucket, so one
user's accesses to one resource can sit in different partitions and arrive OUT OF ORDER. The index therefore
stores only order-independent state and derives the time-dependent edge attributes when a neighbourhood is
queried (found in the Phase 7 live parity test: annotating on arrival corrupted the familiarity counts):
    g:e:<event_id>          raw edge JSON (u, r, a, t, s, o, y, hour, obj)      expires after the retention
    g:user:<u>, g:res:<r>   zset event_id -> t                                  trimmed to the retention
    g:days:<u>|<r>          set of UTC days on which u accessed r               (for `fam`)
    g:obj:<u>|<r>           zset "obj|event_id" -> t, newest 500               (for `seq`)
At query time: fam(e) = |{days < day(e)}|, seq(e) = |obj(e) - obj(previous access by t)| == 1 - the same values
ml.tgn_graph.EdgeAnnotator computes when it sees the events in time order (offline training).
"""
import json
import logging
from typing import List

from ..topics import ACCESS_EVENTS_BY_RESOURCE, topic_name
from .codec import decode_access_event
from .consumer import Record

log = logging.getLogger("accesssentinel.graph_index")
STATE_TTL = 40 * 86400


def _s(x):
    return x.decode() if isinstance(x, bytes) else x


def _obj(ev):
    o = ev.meta.get("object_id") if isinstance(ev.meta, dict) else None
    try:
        return int(o) if o is not None else None
    except (TypeError, ValueError):
        return None


class RedisEdgeIndex:
    """Same query interface (and results) as ml.tgn_graph.MemoryEdgeIndex, over the live Redis index."""

    def __init__(self, redis):
        self.r = redis

    def _edges(self, zkey: str, t0: float, t1: float) -> List[dict]:
        ids = self.r.zrangebyscore(zkey, t0, t1)
        if not ids:
            return []
        raw = [json.loads(v) for v in self.r.mget([f"g:e:{_s(i)}" for i in ids]) if v]
        pairs = sorted({(e["u"], e["r"]) for e in raw})
        pipe = self.r.pipeline(transaction=False)
        for u, r in pairs:
            pipe.smembers(f"g:days:{u}|{r}")
        days = {p: sorted(int(_s(d)) for d in ds) for p, ds in zip(pairs, pipe.execute())}
        pipe = self.r.pipeline(transaction=False)
        with_obj = [e for e in raw if e.get("obj") is not None]
        for e in with_obj:
            pipe.zrevrangebyscore(f"g:obj:{e['u']}|{e['r']}", f"({e['t']}", "-inf", start=0, num=1)
        prev = {e["id"]: (int(_s(res[0]).split("|", 1)[0]) if res else None) for e, res in zip(with_obj, pipe.execute())}
        out = []
        for e in raw:
            day = int(e["t"] // 86400)
            fam = sum(1 for d in days[(e["u"], e["r"])] if d < day)
            p = prev.get(e["id"])
            seq = int(e.get("obj") is not None and p is not None and abs(e["obj"] - p) == 1)
            out.append({"id": e["id"], "u": e["u"], "r": e["r"], "a": e["a"], "t": e["t"], "s": e["s"], "o": e["o"],
                        "y": e["y"], "fam": fam, "seq": seq, "hour": e["hour"]})
        return sorted(out, key=lambda e: (e["t"], e["id"]))

    def user_edges(self, u: str, t0: float, t1: float) -> List[dict]:
        return self._edges(f"g:user:{u}", t0, t1)

    def resource_edges(self, r: str, t0: float, t1: float) -> List[dict]:
        return self._edges(f"g:res:{r}", t0, t1)


class GraphIndexHandler:
    def __init__(self, redis, retention: float = 3 * 3600.0):
        self.r, self.retention = redis, retention
        self.stats = {"indexed": 0, "redelivered": 0}

    def decode(self, rec: Record):
        return decode_access_event(rec.value)

    def handle(self, records: List[Record]):
        for rec in records:
            ev = rec.data
            ekey = f"g:e:{ev.event_id}"
            if self.r.exists(ekey):
                self.stats["redelivered"] += 1
                continue
            obj = _obj(ev)
            edge = {"id": ev.event_id, "u": ev.u, "r": ev.rkey, "a": ev.a, "t": float(ev.t), "s": int(ev.s), "o": ev.o,
                    "y": int(ev.y), "hour": (ev.t / 3600.0) % 24.0, "obj": obj}
            ttl = int(self.retention) + 3600
            pair = f"{ev.u}|{ev.rkey}"
            pipe = self.r.pipeline(transaction=False)
            pipe.set(ekey, json.dumps(edge), ex=ttl)
            for z in (f"g:user:{ev.u}", f"g:res:{ev.rkey}"):
                pipe.zadd(z, {ev.event_id: ev.t})
                pipe.zremrangebyscore(z, "-inf", ev.t - self.retention)
                pipe.expire(z, ttl)
            pipe.sadd(f"g:days:{pair}", int(ev.t // 86400))
            pipe.expire(f"g:days:{pair}", STATE_TTL)
            if obj is not None:
                pipe.zadd(f"g:obj:{pair}", {f"{obj}|{ev.event_id}": ev.t})
                pipe.zremrangebyrank(f"g:obj:{pair}", 0, -501)
                pipe.expire(f"g:obj:{pair}", STATE_TTL)
            pipe.execute()
            self.stats["indexed"] += 1
        return []

    def checkpoint(self, force: bool) -> bool:
        return True

    def reset(self) -> None:
        pass


def main() -> int:
    import socket
    import redis as redis_lib
    from confluent_kafka import Consumer, Producer
    from ..settings import get_settings
    from .consumer import DeadLetterQueue, StreamConsumer, consumer_config, producer_config
    from .runtime import Heartbeat, install_stop, setup

    s = get_settings()
    setup(s.log_level)
    client = f"graph-index-{socket.gethostname()}"
    h = GraphIndexHandler(redis_lib.Redis(**s.redis_kwargs()))
    group = "graph-index"
    sc = StreamConsumer(Consumer(consumer_config(s.kafka_bootstrap, group, client)),
                        [topic_name(ACCESS_EVENTS_BY_RESOURCE, s.kafka_topic_prefix)], h,
                        DeadLetterQueue(Producer(producer_config(s.kafka_bootstrap, f"{client}-dlq")), group),
                        batch_size=s.consumer_batch, heartbeat=Heartbeat())
    install_stop(sc.stop)
    log.info("graph index: retention %.0f s", h.retention)
    sc.run()
    log.info("graph index stopped: %s %s", sc.stats, h.stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

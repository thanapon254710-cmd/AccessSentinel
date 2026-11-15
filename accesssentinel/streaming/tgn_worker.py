"""TGN worker (D3 Step 7): collective risk S_coll(g(u)) = f_TGN(G_t[N(u)]) for escalated users.

    python -m accesssentinel.streaming.tgn_worker realtime      # group tgn-rt on escalations.realtime
    python -m accesssentinel.streaming.tgn_worker background    # group tgn-bg on escalations.background
    python -m accesssentinel.streaming.tgn_worker cmu-user      # v3 CMU: user memories, every event
    python -m accesssentinel.streaming.tgn_worker cmu-resource  # v3 CMU: resource memories, every event

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
    def __init__(self, models, index, redis, producer, out_topic: str, coll_ttl: float = 3600.0, clock=time.time,
                 tenant: str = "default"):
        self.models, self.index, self.r, self.p, self.out_topic = models, index, redis, producer, out_topic
        self.tenant = tenant
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

    def _handle_v3(self, records: List[Record], scorer):
        out, coll = [], []
        for rec in records:
            esc = rec.data
            anchor = anchor_time(esc)
            g = analyse_v3(scorer, self.index, self.r, esc["u"], anchor, self.tenant)
            group_id = None
            if g["s_coll"] is not None:
                group_id = hashlib.sha256(f"{','.join(g['members'])}|{int(anchor // scorer.cfg.window)}".encode()).hexdigest()[:16]
            r = {"score_id": f"coll:{esc['escalation_id']}", "schema_version": 2, "u": esc["u"], "t": self.clock(),
                 "anchor_t": anchor, "window": [anchor - scorer.cfg.window, anchor], "escalation_id": esc["escalation_id"],
                 "paths": esc.get("paths"), "priority": esc.get("priority"), "cost": esc.get("cost"),
                 "s_coll": g["s_coll"], "A_u": g.get("A_u"),
                 "above_theta_coll": bool(g["s_coll"] is not None and g["s_coll"] >= scorer.theta_coll),
                 "theta_coll": scorer.theta_coll, "theta_A": scorer.theta_attr, "group_id": group_id,
                 "members": g["members"], "member_scores": g.get("member_scores"), "resources": g.get("resources"),
                 "top_edges": g.get("top_edges"), "reason": g.get("reason"),
                 "graph": {"access_edges": g["edges"], "nodes": g["nodes"]}, "memory_staleness_s": g.get("staleness_s"),
                 "model_version": scorer.version, "model_kind": "tgn-v3", "inference_ms": g["inference_ms"]}
            out.append(r)
            if group_id:
                coll.append(r)
        return out, coll

    def handle(self, records: List[Record]):
        scorer = self.models.get()
        if getattr(scorer, "arch", "") == "tgn-v3":
            out, coll = self._handle_v3(records, scorer)
            return self._finish(out, coll)
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
        return self._finish(out, coll)

    def _finish(self, out, coll):
        self._publish(out)
        if coll:
            pipe = self.r.pipeline(transaction=False)
            for r in coll:
                for m in r["members"]:
                    ms_ = (r.get("member_scores") or {}).get(m)
                    ms_ = ms_ if isinstance(ms_, dict) else {}             # D3 records: member -> float
                    pipe.hset(COLL.format(m), mapping={"s_coll": ms_.get("s_coll", r["s_coll"]), "A_u": ms_.get("A_u", ""),
                                                        "group_id": r["group_id"],
                                                        "members": json.dumps(r["members"]), "t": r["t"],
                                                        "above_theta_coll": int(r["above_theta_coll"]),
                                                        "theta_coll": r.get("theta_coll") if r.get("theta_coll") is not None else "",
                                                        "theta_A": r.get("theta_A") if r.get("theta_A") is not None else "",
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


# ============================================================================================================
# Revised v3 Step 7a: Continuous Node Memory Updater (CMU), Phase 4
#     python -m accesssentinel.streaming.tgn_worker cmu-user        # group cmu-user on access-events.ordered
#     python -m accesssentinel.streaming.tgn_worker cmu-resource    # group cmu-resource on access-events-by-rstar
# ============================================================================================================
"""
Every access event updates the memory of its user (user-keyed task) and of its resource r* (resource-keyed
task), regardless of escalation, with the trained CMU (Eqs. 11-12): m(t) = GRU(m(t-), phi_msg(m(t-), m_other,
Phi(dt), f(e))). Single writer: a node's memory and neighbour buffer are written only by the task that owns its
key. The counterpart's memory is read from its latest published snapshot in Redis, so it may be stale; the
staleness (event time of the update minus the counterpart's last update time) is measured and published in
`cmu:stats` (v3 Limitation 5). Idempotent: an event already applied to a node (event id in the node's recent-id
ring, or Kafka (partition, offset) not beyond the last record applied to that node: a node's events are all on one
partition, the stream being keyed by it) is skipped, so redelivery after a crash never applies an event twice,
however large the batch. State: Redis hashes
cmu:u:<u> / cmu:r:<r*> {m: float32 bytes, t: last event time, ids: recent ids} and lists cmu:nbr:<side>:<id>
(K most recent neighbours: {n, t, f}). Without a trained v3 TGN the CMU does not consume (events wait in Kafka).
"""
import numpy as _np  # noqa: E402

CMU_KEY = {"u": "cmu:u:{}", "r": "cmu:r:{}"}
CMU_NBR = {"u": "cmu:nbr:u:{}", "r": "cmu:nbr:r:{}"}
CMU_STATS = "cmu:stats"


def cmu_read(redis, side: str, ids: List[str], dim: int):
    pipe = redis.pipeline(transaction=False)
    for i in ids:
        pipe.hmget(CMU_KEY[side].format(i), "m", "t", "ids", "pos")
    out = {}
    for i, (m, t, rid, pos) in zip(ids, pipe.execute()):
        out[i] = {"m": _np.frombuffer(m, dtype=_np.float32).copy() if m else _np.zeros(dim, _np.float32),
                  "t": float(t) if t else None, "ids": json.loads(rid) if rid else [],
                  "pos": json.loads(pos) if pos else None}             # [partition, offset] of the last applied record
    return out


class CmuHandler:
    def __init__(self, models, redis, side: str, tenant: str = "default", clock=time.time):
        assert side in ("u", "r")
        self.models, self.r, self.side, self.tenant, self.clock = models, redis, side, tenant, clock
        self.other = "r" if side == "u" else "u"
        self.stats = {"events": 0, "updates": 0, "skipped_applied": 0, "staleness_max": 0.0, "staleness_sum": 0.0,
                      "staleness_n": 0, "batches": 0}
        self._reported = 0

    def decode(self, rec: Record):
        from .codec import decode_access_event
        return decode_access_event(rec.value)

    def checkpoint(self, force: bool) -> bool:
        return True                    # handle() returns after Redis holds the new memories (then offsets commit)

    def reset(self) -> None:
        pass

    def _own(self, ev) -> str:
        return ev.u if self.side == "u" else ev.rstar

    def _counter(self, ev) -> str:
        return ev.rstar if self.side == "u" else ev.u

    def handle(self, records: List[Record]):
        from ..ml.tgn_train import edge_feat_v3
        scorer = self.models.get()
        if scorer is None or getattr(scorer, "arch", "") != "tgn-v3":
            raise RuntimeError("no trained v3 TGN (CMU) yet")       # transient: the batch waits, nothing applied
        dim = scorer.cfg.mem_dim
        pos_of = {id(r.data): (r.partition, r.offset) for r in records}
        evs = sorted((r.data for r in records), key=lambda e: (e.t, e.event_id))
        own = sorted({self._own(e) for e in evs}); other = sorted({self._counter(e) for e in evs})
        st = cmu_read(self.r, self.side, own, dim)
        snap = cmu_read(self.r, self.other, other, dim)          # counterpart: latest published snapshot
        rounds: dict = {}
        seen_n: dict = {}
        for e in evs:                                            # k-th event of each node goes to round k
            k = seen_n.get(self._own(e), 0); seen_n[self._own(e)] = k + 1
            rounds.setdefault(k, []).append(e)
        nbr_new = {}
        for k in sorted(rounds):
            def applied(e):
                s_ = st[self._own(e)]
                p_, o_ = pos_of.get(id(e), (None, None))
                return e.event_id in s_["ids"] or (s_["pos"] is not None and p_ == s_["pos"][0] and o_ <= s_["pos"][1])
            batch = [e for e in rounds[k] if not applied(e)]
            self.stats["skipped_applied"] += len(rounds[k]) - len(batch)
            if not batch:
                continue
            m_self = _np.stack([st[self._own(e)]["m"] for e in batch])
            m_oth = _np.stack([snap[self._counter(e)]["m"] for e in batch])
            dt = _np.array([0.0 if st[self._own(e)]["t"] is None else max(e.t - st[self._own(e)]["t"], 0.0) for e in batch])
            f = _np.array([edge_feat_v3(e) for e in batch], _np.float32)
            new = scorer.cmu_step(m_self, m_oth, dt, f)
            for e, m in zip(batch, new):
                s = st[self._own(e)]
                s["m"], s["t"] = m.astype(_np.float32), max(e.t, s["t"] or e.t)
                s["ids"] = (s["ids"] + [e.event_id])[-64:]
                p_, o_ = pos_of.get(id(e), (None, None))
                if o_ is not None and (s["pos"] is None or s["pos"][0] != p_ or o_ > s["pos"][1]):
                    s["pos"] = [p_, o_]
                ct = snap[self._counter(e)]["t"]
                if ct is not None:
                    lag = max(e.t - ct, 0.0)
                    self.stats["staleness_max"] = max(self.stats["staleness_max"], lag)
                    self.stats["staleness_sum"] += lag; self.stats["staleness_n"] += 1
                nbr_new.setdefault(self._own(e), []).append(json.dumps({"n": self._counter(e), "t": e.t, "f": edge_feat_v3(e)}))
                self.stats["updates"] += 1
        K = scorer.cfg.n_neighbors
        pipe = self.r.pipeline(transaction=False)
        for i, s in st.items():
            if s["t"] is None:
                continue
            pipe.hset(CMU_KEY[self.side].format(i), mapping={"m": s["m"].tobytes(), "t": s["t"], "ids": json.dumps(s["ids"]),
                                                            "pos": json.dumps(s["pos"]), "v": scorer.version})
        for i, items in nbr_new.items():
            pipe.rpush(CMU_NBR[self.side].format(i), *items)
            pipe.ltrim(CMU_NBR[self.side].format(i), -K, -1)
        n = max(self.stats["staleness_n"], 1)
        pipe.hincrby(CMU_STATS, f"{self.side}_updates", self.stats["updates"] - self._reported)   # cumulative across restarts
        self._reported = self.stats["updates"]
        pipe.hset(CMU_STATS, mapping={f"{self.side}_staleness_max_s": round(self.stats["staleness_max"], 3),
                                      f"{self.side}_staleness_mean_s": round(self.stats["staleness_sum"] / n, 3),
                                      f"{self.side}_updated_at": self.clock(), f"{self.side}_model": scorer.version})
        pipe.execute()
        self.stats["events"] += len(evs); self.stats["batches"] += 1
        return []


def cmu_main(side: str) -> int:
    import redis as redis_lib
    import torch
    from confluent_kafka import Consumer, Producer
    from ..settings import get_settings
    from ..topics import ACCESS_EVENTS_BY_RSTAR, ACCESS_EVENTS_ORDERED
    from .consumer import DeadLetterQueue, StreamConsumer, consumer_config, producer_config
    from .runtime import Heartbeat, install_stop, setup
    s = get_settings()
    setup(s.log_level)
    torch.set_num_threads(s.torch_threads)
    client = f"cmu-{side}-{socket.gethostname()}"
    models = TgnModels(s.model_dir, s.tcn_model_poll_seconds)
    hb = Heartbeat()
    stop = {"flag": False}
    install_stop(lambda: stop.__setitem__("flag", True))
    while not stop["flag"] and getattr(models.get(), "arch", "") != "tgn-v3":
        log.warning("CMU: no trained v3 TGN in %s/tgn; not consuming (events wait in Kafka)", s.model_dir)
        for _ in range(10):
            hb(); time.sleep(1)
        models._checked = -1e18
    if stop["flag"]:
        return 0
    r = redis_lib.Redis(**s.redis_kwargs())
    side_key = "u" if side == "user" else "r"
    topic = ACCESS_EVENTS_ORDERED if side == "user" else ACCESS_EVENTS_BY_RSTAR
    group = f"cmu-{side}"
    h = CmuHandler(models, r, side_key, s.tenant)
    sc = StreamConsumer(Consumer(consumer_config(s.kafka_bootstrap, group, client)), [topic_name(topic, s.kafka_topic_prefix)],
                        h, DeadLetterQueue(Producer(producer_config(s.kafka_bootstrap, f"{client}-dlq")), group),
                        batch_size=s.consumer_batch, heartbeat=hb)
    install_stop(sc.stop)
    log.info("%s updating %s memories from %s with %s", client, side, topic, models.scorer.version)
    sc.run()
    return 0


# ============================================================================================================
# Revised v3 Step 7b: Selective Graph Encoder (SGE) + collective score for ADMITTED users (Phase 6)
# ============================================================================================================
def _edge_feat_live(e: dict):
    return [float(e["s"]), float(1 - e["y"])] + [float(e.get("o") == o) for o in ("view", "edit", "delete", "export")] + \
           [float(str(e["r"]).split(":", 1)[-1].startswith("unmapped:"))]


def analyse_v3(scorer, index, redis, seed: str, t: float, tenant: str = "default") -> dict:
    """Eqs. 13-19 for one admitted user at time t, from the live state: window edges (graph index), node
    memories and neighbour buffers (CMU, Redis). Returns members, per-member scores and the cost inputs."""
    from ..ml.tgn_train import coordination_groups, member_features
    cfg = scorer.cfg
    W, K = cfg.window, cfg.n_neighbors
    t0 = time.perf_counter()
    rs = lambda rkey: f"{tenant}/{rkey}"
    seed_edges = index.user_edges(seed, t - W, t)
    seed_res = sorted({e["r"] for e in seed_edges})
    users, deg = {seed}, {}
    for rk in seed_res:
        re_ = index.resource_edges(rk, t - W, t)
        recent = []
        for e in sorted(re_, key=lambda x: -x["t"]):
            if e["u"] not in recent:
                recent.append(e["u"])
        users |= set(recent[:K])
        deg[rs(rk)] = len(set(x["u"] for x in re_))
    edges = []
    for v in sorted(users):
        for e in (seed_edges if v == seed else index.user_edges(v, t - W, t)):
            edges.append(e)
    for e in edges:
        if rs(e["r"]) not in deg:
            deg[rs(e["r"])] = len({x["u"] for x in index.resource_edges(e["r"], t - W, t)})
    if not edges:
        return {"members": [seed], "s_coll": None, "reason": "no recent interactions", "edges": 0, "nodes": 1,
                "inference_ms": round((time.perf_counter() - t0) * 1000, 3)}
    u_nodes = sorted({e["u"] for e in edges}); r_nodes = sorted({rs(e["r"]) for e in edges})
    mem_u = cmu_read(redis, "u", u_nodes, cfg.mem_dim); mem_r = cmu_read(redis, "r", r_nodes, cfg.mem_dim)
    nodes = [("u", x) for x in u_nodes] + [("r", x) for x in r_nodes]
    pipe = redis.pipeline(transaction=False)
    for side, x in nodes:
        pipe.lrange(CMU_NBR[side].format(x), 0, -1)
    nbr_lists = [[json.loads(v) for v in lst] for lst in pipe.execute()]
    need_u = sorted({n["n"] for (side, _), lst in zip(nodes, nbr_lists) if side == "r" for n in lst} - set(u_nodes))
    need_r = sorted({n["n"] for (side, _), lst in zip(nodes, nbr_lists) if side == "u" for n in lst} - set(r_nodes))
    mem_u.update(cmu_read(redis, "u", need_u, cfg.mem_dim)); mem_r.update(cmu_read(redis, "r", need_r, cfg.mem_dim))
    B, M, E = len(nodes), cfg.mem_dim, cfg.edge_feat_dim
    m_v = _np.zeros((B, M), _np.float32); vt = _np.zeros((B, 3), _np.float32)
    nm = _np.zeros((B, K, M), _np.float32); nt = _np.zeros((B, K, 3), _np.float32)
    nf = _np.zeros((B, K, E), _np.float32); nd = _np.zeros((B, K), _np.float32); mk = _np.zeros((B, K), bool)
    for i, ((side, x), lst) in enumerate(zip(nodes, nbr_lists)):
        m_v[i] = (mem_u if side == "u" else mem_r)[x]["m"]; vt[i, 0 if side == "u" else 1] = 1.0
        for j, n in enumerate(lst[-K:]):
            other = mem_r if side == "u" else mem_u
            nm[i, j] = other.get(n["n"], {"m": _np.zeros(M, _np.float32)})["m"]
            nt[i, j, 1 if side == "u" else 0] = 1.0
            nf[i, j] = n["f"]; nd[i, j] = max(t - float(n["t"]), 0.0); mk[i, j] = True
    h = scorer.embed(m_v, vt, nm, nt, nf, nd, mk)
    pos = {nd_: i for i, nd_ in enumerate(nodes)}
    hu = h[[pos[("u", e["u"])] for e in edges]]; hr = h[[pos[("r", rs(e["r"]))] for e in edges]]
    eps = scorer.edge_eps(hu, hr, _np.array([_edge_feat_live(e) for e in edges], _np.float32))
    wedges = [{"u": e["u"], "r": rs(e["r"]), "t": float(e["t"]), "eps": float(x), "fam": int(e.get("fam", 1))}
              for e, x in zip(edges, eps)]
    res_s = {rs(e["r"]): int(e["s"]) for e in edges}
    groups, w = coordination_groups(wedges, cfg, deg, res_s)
    g = next((x for x in groups if seed in x["members"]), None)
    ms = (time.perf_counter() - t0) * 1000
    base = {"edges": len(edges), "nodes": len(nodes), "inference_ms": round(ms, 3),
            "staleness_s": round(max([t - (mem_u[x]["t"] or t) for x in u_nodes] + [t - (mem_r[x]["t"] or t) for x in r_nodes] + [0.0]), 3)}
    if g is None:
        return {"members": [seed], "s_coll": None, "reason": "no coordination group", **base}
    member_scores = {}
    for m in g["members"]:
        f = member_features(m, g, wedges, w, cfg)
        member_scores[m] = {"s_coll": round(float(scorer.s_coll(f)), 6), "A_u": round(f[1], 6), "TM_eps": round(f[0], 6), "Nov": round(f[2], 6)}
    top = sorted(((x["u"], x["r"], x["eps"]) for x in wedges if x["u"] in g["members"]), key=lambda z: -z[2])[:5]
    return {"members": g["members"], "s_coll": member_scores[seed]["s_coll"], "member_scores": member_scores,
            "A_u": member_scores[seed]["A_u"], "resources": g["shared_sensitive"], "density": round(g["density"], 4),
            "top_edges": [{"u": a, "r": b, "eps": round(c, 4)} for a, b, c in top], **base}


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
    if priority in ("cmu-user", "cmu-resource"):
        return cmu_main(priority.split("-", 1)[1])
    if priority not in ("realtime", "background"):
        print("usage: tgn_worker realtime|background|cmu-user|cmu-resource"); return 2
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
                         topic_name(GROUP_SCORES, s.kafka_topic_prefix), PDPConfig().coll_ttl, tenant=s.tenant)
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

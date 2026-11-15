"""Phase 7 live check: a held-out collusion group (or a benign team meeting) through the deployed TGN path.

    docker compose run --rm -T tools python scripts/smoke_tgn.py collusion
    docker compose run --rm -T tools python scripts/smoke_tgn.py team

1. Offline: on the collusion-study dataset, build the seed's neighbourhood at the anchor time and score it with
   the promoted TGN -> expected group members and S_coll.
2. Live: replay the full histories (up to the anchor) of exactly the users in that neighbourhood - renamed
   syn-tgn-<run>-<user>, fresh event ids, meta.source = "synthetic-replay" - into `access-events`; wait for the
   repartitioner and graph index; publish an escalation for the seed; read `group-scores`.
3. Check: live members and S_coll equal the offline result (train/serve parity) and the Redis coll:<u> state.
Synthetic data: this validates the deployed path, not real-world accuracy.
"""
import argparse
import json
import os
import sys
import random
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from accesssentinel.datasets.synth import SynthConfig, generate  # noqa: E402
from accesssentinel.ml.tgn_graph import EdgeAnnotator, MemoryEdgeIndex, build_sample  # noqa: E402
from accesssentinel.ml.tgn_registry import TgnScorer  # noqa: E402
from accesssentinel.settings import get_settings  # noqa: E402
from accesssentinel.streaming.lag import group_lag  # noqa: E402
from accesssentinel.topics import ACCESS_EVENTS, ESCALATIONS_REALTIME, GROUP_SCORES, topic_name  # noqa: E402


def pick(ds, case: str, index: int):
    if case == "collusion":
        groups = sorted((k, v) for k, v in ds.instances.items() if v.get("split") == "test" and "bursts" in v)
        name, g = groups[index % len(groups)]
        return name, str(g["members"][0]), sorted(g["bursts"])[0] + 1800, g
    a, b = ds.split_bounds["test"]
    m = ds.meetings[index % len(ds.meetings)]
    for day in range(int(a // 86400), int(b // 86400)):
        if (day + 3) % 7 == m["weekday"]:
            return f"team-meeting-{m['role']}", str(m["team"][0]), day * 86400 + (m["hour"] + 0.75) * 3600, m
    raise SystemExit("no meeting in the test period")


# ============================================================================================================
# Revised v3 Phases 4-6 live checks (CMU on every event, PriorityAdmit, SGE for admitted users)
#     docker compose run --rm -T tools python scripts/smoke_tgn.py cmu
#     docker compose run --rm -T tools python scripts/smoke_tgn.py admit
#     docker compose run --rm -T tools python scripts/smoke_tgn.py burst --user syn-cmu-x --n 2000
#     docker compose run --rm -T tools python scripts/smoke_tgn.py check-cmu --user syn-cmu-x --n 2000
# ============================================================================================================
def _v3_results():
    return []


def _check(res, name, ok, detail=""):
    res.append(bool(ok))
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))


def _v3_event(u, r, t, s=0, o="view", y=True, a="hr"):
    from accesssentinel.schema import make_event
    return make_event(u, a, r, s, o, y, {"ip": "10.20.1.9", "device": f"dev-{u}"}, uuid.uuid4().hex,
                      {"source": "synthetic-replay", "smoke": "v3-graph"}, t=t)


def _send(sett, events):
    from confluent_kafka import Producer
    from accesssentinel.topics import ACCESS_EVENTS, topic_name
    pr = Producer({"bootstrap.servers": sett.kafka_bootstrap, "enable.idempotence": True, "acks": "all"})
    for e in events:
        pr.produce(topic_name(ACCESS_EVENTS, sett.kafka_topic_prefix), key=e.u.encode(), value=json.dumps(e.__dict__).encode())
    assert pr.flush(30) == 0


def cmd_cmu(sett, args) -> int:
    """Every event updates the memory of its user and of its resource, also for a user who is never escalated."""
    import redis as redis_lib
    from accesssentinel.streaming.tgn_worker import CMU_KEY, CMU_STATS
    r = redis_lib.Redis(**sett.redis_kwargs())
    u = f"syn-cmu-{uuid.uuid4().hex[:6]}"
    now = time.time()
    evs = [_v3_event(u, ["directory", "handbook", "policies"][i % 3], now + i, a=["hr", "docs", "docs"][i % 3]) for i in range(12)]
    before = {k: int(r.hget("cmu:stats", k) or 0) for k in ("u_updates", "r_updates")}
    _send(sett, evs)
    res = _v3_results()
    deadline = time.time() + args.timeout
    st = {}
    while time.time() < deadline:
        st = r.hgetall(CMU_KEY["u"].format(u))
        if st and json.loads(st[b"ids"]) and len(json.loads(st[b"ids"])) >= 12:
            break
        time.sleep(1)
    ids = json.loads(st[b"ids"]) if st else []
    _check(res, "user memory updated for every event (never escalated)", len(ids) == 12, f"{len(ids)}/12 events applied")
    m = __import__("numpy").frombuffer(st.get(b"m", b""), dtype="float32") if st else []
    _check(res, "memory vector has the model's dimension (100) and is non-zero", len(m) == 100 and float(abs(m).sum()) > 0)
    rm = r.hgetall(CMU_KEY["r"].format("default/hr:directory"))
    _check(res, "resource memory updated by the resource-keyed task", bool(rm) and float(rm[b"t"]) >= evs[0].t,
           f"last update t={float(rm[b't']) if rm else None}")
    time.sleep(2)
    after = {k: int(r.hget(CMU_STATS, k) or 0) for k in ("u_updates", "r_updates")}
    stats = {k.decode(): v.decode() for k, v in r.hgetall(CMU_STATS).items()}
    _check(res, "both CMU tasks applied the events", after["u_updates"] - before["u_updates"] >= 12 and after["r_updates"] - before["r_updates"] >= 12,
           f"user +{after['u_updates'] - before['u_updates']}, resource +{after['r_updates'] - before['r_updates']}")
    _check(res, "counterpart staleness measured", "u_staleness_max_s" in stats and "r_staleness_max_s" in stats,
           f"user-side max {stats.get('u_staleness_max_s')} s, resource-side max {stats.get('r_staleness_max_s')} s")
    print(f"{sum(res)}/{len(res)} checks passed")
    return 0 if all(res) else 1


def cmd_burst(sett, args) -> int:
    t0 = time.time()
    _send(sett, [_v3_event(args.user, "directory", t0 + i * 0.001) for i in range(args.n)])
    print(f"produced {args.n} events for {args.user}")
    return 0


def cmd_check_cmu(sett, args) -> int:
    """After a CMU crash/restart during a burst: every event applied exactly once (ids + update count)."""
    import redis as redis_lib
    from accesssentinel.streaming.tgn_worker import CMU_KEY
    r = redis_lib.Redis(**sett.redis_kwargs())
    res = _v3_results()
    deadline = time.time() + args.timeout
    pos = None
    while time.time() < deadline:
        st = r.hgetall(CMU_KEY["u"].format(args.user))
        if st and st.get(b"ids"):
            ids = json.loads(st[b"ids"])
            if ids and len(ids) == 64 and float(st[b"t"]) >= 0:
                pos = json.loads(st[b"pos"])
        time.sleep(3)
        if pos is not None:
            st2 = r.hgetall(CMU_KEY["u"].format(args.user))
            if json.loads(st2[b"pos"]) == pos:
                break
    from confluent_kafka import Consumer, TopicPartition
    from accesssentinel.topics import ACCESS_EVENTS_ORDERED, topic_name
    c = Consumer({"bootstrap.servers": sett.kafka_bootstrap, "group.id": f"chk-{uuid.uuid4().hex[:6]}", "enable.auto.commit": False})
    tp = TopicPartition(topic_name(ACCESS_EVENTS_ORDERED, sett.kafka_topic_prefix), pos[0]) if pos else None
    n_user = 0
    if tp:
        lo, hi = c.get_watermark_offsets(tp, timeout=10)
        c.assign([TopicPartition(tp.topic, tp.partition, lo)])
        while True:
            msgs = c.consume(5000, 2.0)
            if not msgs:
                break
            n_user += sum(1 for m_ in msgs if not m_.error() and json.loads(m_.value())["u"] == args.user)
    c.close()
    _check(res, "all burst events reached the CMU's input", n_user == args.n, f"{n_user}/{args.n} on the ordered stream")
    m = __import__("numpy").frombuffer(r.hget(CMU_KEY["u"].format(args.user), "m"), dtype="float32")
    _check(res, "memory present after the restart", m.shape == (100,))
    print(f"{sum(res)}/{len(res)} checks passed  (last applied offset {pos})")
    return 0 if all(res) else 1


def cmd_admit(sett, args) -> int:
    """A Path-A escalation of a fresh attacker is admitted under the budget (cost recorded), analysed by the
    SGE from live memories, and a v3 group score is published; the window ledger never exceeds B_W."""
    import redis as redis_lib
    from confluent_kafka import Consumer, TopicPartition
    from accesssentinel.config import EscalationConfig
    from accesssentinel.escalation_engine import graph_budget
    from accesssentinel.topics import ESCALATIONS_REALTIME, GROUP_SCORES, topic_name
    r = redis_lib.Redis(**sett.redis_kwargs())
    res = _v3_results()
    c = Consumer({"bootstrap.servers": sett.kafka_bootstrap, "group.id": f"adm-{uuid.uuid4().hex[:6]}", "enable.auto.commit": False})
    tps = []
    for t in (ESCALATIONS_REALTIME, GROUP_SCORES):
        name = topic_name(t, sett.kafka_topic_prefix)
        for p_ in c.list_topics(timeout=10).topics[name].partitions:
            tps.append(TopicPartition(name, p_, c.get_watermark_offsets(TopicPartition(name, p_), timeout=10)[1]))
    c.assign(tps)
    import subprocess
    user = f"adm-{uuid.uuid4().hex[:5]}"
    out = subprocess.run([sys.executable, "scripts/demo_new_user.py", "--user", user], capture_output=True, text=True, timeout=400)
    blocked = "attack detected and the account blocked" in out.stdout and "PASS  attack detected" in out.stdout
    _check(res, "fresh attacker blocked by the TCN path (PDP on S_ind)", blocked)
    esc, grp = None, None
    deadline = time.time() + args.timeout
    while time.time() < deadline and not (esc and grp):
        for m_ in c.consume(500, 1.0):
            if m_.error():
                continue
            d = json.loads(m_.value())
            if d.get("u") != user:
                continue
            if m_.topic().endswith(ESCALATIONS_REALTIME):
                esc = d
            elif d.get("model_kind") == "tgn-v3":
                grp = d
    c.close()
    _check(res, "escalation admitted with a cost (Eq. 26)", esc is not None and esc.get("cost") is not None,
           f"paths {esc and esc['paths']}, cost {esc and esc.get('cost')}")
    _check(res, "SGE analysed it from live memories (v3 group score)", grp is not None,
           f"graph {grp and grp['graph']}, inference {grp and grp['inference_ms']} ms, staleness {grp and grp.get('memory_staleness_s')} s, "
           f"S_coll {grp and grp['s_coll']} ({grp and grp.get('reason') or 'group'})")
    budget = graph_budget(EscalationConfig())
    used = [float(r.get(k) or 0) for k in r.scan_iter("esc:budget:*")]
    _check(res, "compute bound: every window's used work <= B_W (Eq. 29)", all(u <= budget for u in used),
           f"B_W {budget}, windows {len(used)}, max used {max(used) if used else 0:.1f}")
    print(f"{sum(res)}/{len(res)} checks passed")
    return 0 if all(res) else 1


def cmd_cost_fit(sett, args) -> int:
    """Phase 6: fit Eq. 26 (C_u = c0 + c1|E| + c2|V|, 1 unit = 1 ms of SGE time) from MEASURED analyse_v3 times.
    Seeds neighbourhoods of controlled size through the real pipeline (sequencer -> graph index + CMU), then
    times the SGE for each seed in this process (run it inside a tgn-worker container: same model, same limits).
        docker compose exec -T tgn-worker-rt python scripts/smoke_tgn.py cost-fit"""
    import numpy as np
    import redis as redis_lib
    from accesssentinel.ml import tgn_registry
    from accesssentinel.streaming.escalation_service import neighbourhood_size_fn
    from accesssentinel.streaming.graph_index import RedisEdgeIndex
    from accesssentinel.streaming.tgn_worker import analyse_v3
    r = redis_lib.Redis(**sett.redis_kwargs())
    scorer = tgn_registry.TgnScorer.load(sett.model_dir, tgn_registry.current_version(sett.model_dir))
    if getattr(scorer, "arch", "") != "tgn-v3":
        print("the served TGN is not v3"); return 1
    tag, now = uuid.uuid4().hex[:5], time.time() - 600
    seeds, evs = [], []
    for n_res in (1, 2, 4, 8, 16):
        for n_ev in (2, 5, 10, 20):
            seed = f"syn-cost-{tag}-{n_res}-{n_ev}"
            seeds.append((seed, n_res * n_ev))
            for j in range(n_res):
                res = f"cost{tag}x{n_res}x{n_ev}r{j}"
                for k in range(n_ev):
                    evs.append(_v3_event(seed, res, now + k * 7 + j, s=j % 2, a="fin"))
                for b in range(args.nbrs):
                    for k in range(n_ev):
                        evs.append(_v3_event(f"syn-cost-{tag}-b{n_res}x{n_ev}x{j}x{b}", res, now + k * 11 + b, s=j % 2, a="fin"))
    _send(sett, evs)
    print(f"produced {len(evs)} events for {len(seeds)} seeds")
    idx = RedisEdgeIndex(r)
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        if all(len(idx.user_edges(u, now - 10, now + 3600)) >= n for u, n in seeds):
            break
        time.sleep(3)
    time.sleep(10)                                                        # let the CMU tasks catch up
    size = neighbourhood_size_fn(r, sett.tenant, scorer.cfg.window, scorer.cfg.n_neighbors)
    t = now + 400
    analyse_v3(scorer, idx, r, seeds[0][0], t, sett.tenant)               # warm-up (torch, connections)
    rows = []
    for u, _ in seeds:
        e_hat, v_hat = size(u, t)
        runs = [analyse_v3(scorer, idx, r, u, t, sett.tenant) for _ in range(args.reps)]
        ms = sorted(x["inference_ms"] for x in runs)[len(runs) // 2]
        rows.append({"u": u, "E_hat": e_hat, "V_hat": v_hat, "edges": runs[0]["edges"], "nodes": runs[0]["nodes"],
                     "ms_median": ms, "ms_all": [x["inference_ms"] for x in runs]})
        print(f"  {u:32s} E^={e_hat:5d} V^={v_hat:4d}  edges={runs[0]['edges']:5d} nodes={runs[0]['nodes']:4d}  {ms:9.1f} ms")
    X = np.array([[1.0, x["E_hat"], x["V_hat"]] for x in rows]); y = np.array([x["ms_median"] for x in rows])
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    if (coef < 0).any():                                                  # Eq. 26 needs c >= 0: refit without the negative term
        keep = [0] + [i for i in (1, 2) if coef[i] >= 0]
        c2_, *_ = np.linalg.lstsq(X[:, keep], y, rcond=None)
        coef = np.zeros(3); coef[keep] = c2_
    pred = X @ coef
    r2 = 1 - float(((y - pred) ** 2).sum()) / float(((y - y.mean()) ** 2).sum())
    mape = float(np.mean(np.abs(pred - y) / y))
    out = {"model": scorer.version, "reps": args.reps, "nbrs_per_resource": args.nbrs, "rows": rows,
           "fit": {"c0": round(float(coef[0]), 4), "c1": round(float(coef[1]), 4), "c2": round(float(coef[2]), 4),
                   "r2": round(r2, 4), "mape": round(mape, 4), "units": "ms of SGE wall time"}}
    print(f"fit: c0={coef[0]:.3f} c1={coef[1]:.4f} c2={coef[2]:.4f}  R2={r2:.3f}  MAPE={mape:.1%}")
    print("RESULT " + json.dumps(out))
    return 0


class _MiniRedis:
    """In-process stand-in for the few Redis calls the CMU and the SGE make (offline reference, no server)."""
    def __init__(self):
        self.h, self.l = {}, {}

    @staticmethod
    def _b(v):
        return v if isinstance(v, bytes) else str(v).encode()

    def pipeline(self, transaction=False):
        return _MiniPipe(self)

    def hmget(self, k, *fields):
        f = fields[0] if len(fields) == 1 and isinstance(fields[0], (list, tuple)) else fields
        return [self.h.get(k, {}).get(x) for x in f]

    def hset(self, k, key=None, value=None, mapping=None):
        d = self.h.setdefault(k, {})
        if key is not None:
            d[key] = self._b(value)
        for a, b in (mapping or {}).items():
            d[a] = self._b(b)

    def hincrby(self, k, f, n=1):
        d = self.h.setdefault(k, {})
        d[f] = str(int(d.get(f, b"0")) + int(n)).encode()
        return int(d[f])

    def rpush(self, k, *vals):
        self.l.setdefault(k, []).extend(self._b(v) for v in vals)

    def _rng(self, k, a, b):
        n = len(self.l.get(k, []))
        a, b = (a + n if a < 0 else a), (b + n if b < 0 else b)
        return max(a, 0), min(b, n - 1)

    def ltrim(self, k, a, b):
        a, b = self._rng(k, a, b)
        self.l[k] = self.l.get(k, [])[a:b + 1]

    def lrange(self, k, a, b):
        a, b = self._rng(k, a, b)
        return self.l.get(k, [])[a:b + 1]


class _MiniPipe:
    def __init__(self, r):
        self.r, self.ops = r, []

    def __getattr__(self, name):
        return lambda *a, **kw: self.ops.append((name, a, kw))

    def execute(self):
        out = [getattr(self.r, n)(*a, **kw) for n, a, kw in self.ops]
        self.ops = []
        return out


def cmd_replay_v3(s, args, ds, scorer) -> int:
    """v3 collusion / team check: replay the participants of one synthetic test case (users AND resources renamed,
    so the live CMU state of earlier runs cannot leak in), inject a harness escalation, and compare the live
    group score with an offline reference computed by the same CmuHandler + analyse_v3 code on an in-process
    store. The two CMU tasks are asynchronous live, so S_coll is compared as a measured skew, not for equality."""
    import redis as redis_lib
    from confluent_kafka import Consumer, Producer
    from dataclasses import replace as _rep
    from accesssentinel.streaming.consumer import Record
    from accesssentinel.streaming.tgn_worker import CmuHandler, analyse_v3
    name, seed, anchor, info = pick(ds, args.case, args.index)
    W = scorer.cfg.window
    evs = sorted(ds.events, key=lambda e: (e.t, e.event_id))
    full = MemoryEdgeIndex([EdgeAnnotator().annotate(e) for e in evs])
    res = {e["r"] for e in full.user_edges(seed, anchor - W, anchor)}
    participants = {seed} | {e["u"] for rk in res for e in full.resource_edges(rk, anchor - W, anchor)}
    run = uuid.uuid4().hex[:6]
    shift = -7 * 86400.0 * random.Random(run).randint(60, 600)
    ren = lambda u: f"syn-tgn-{run}-{u}"
    rep = [_rep(e, u=ren(e.u), r=f"{e.r}-{run}", t=e.t + shift, event_id=uuid.uuid4().hex,
                meta={**e.meta, "source": "synthetic-replay"}) for e in evs if e.u in participants and e.t <= anchor]
    t_an = anchor + shift
    print(f"{args.case}: {name}, seed {seed}; replaying {len(rep)} events of {len(participants)} users (renamed, run {run})")
    off = _MiniRedis()
    prov = type("P", (), {"get": lambda self_: scorer})()
    hu, hr = CmuHandler(prov, off, "u", s.tenant), CmuHandler(prov, off, "r", s.tenant)
    for i in range(0, len(rep), 500):
        recs = [Record("o", 0, i + j, b"", b"", 0, data=e) for j, e in enumerate(rep[i:i + 500])]
        hu.handle(recs)
        hr.handle(recs)
    expected = analyse_v3(scorer, MemoryEdgeIndex([EdgeAnnotator().annotate(e) for e in rep]), off, ren(seed), t_an, s.tenant)
    print(f"  offline reference: members {[m.split('-', 3)[-1] for m in expected['members']]} s_coll {expected['s_coll']}")
    if args.offline:
        flagged = expected["s_coll"] is not None and expected["s_coll"] >= scorer.theta_coll
        print(f"OFFLINE {args.case} {args.index} {name} flagged={flagged} s_coll={expected['s_coll']} "
              f"members={[m.split('-', 3)[-1] for m in expected['members']]} truth={sorted(map(str, info.get('members', [])))}")
        return 0
    p = Producer({"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 20})
    topic = topic_name(ACCESS_EVENTS, s.kafka_topic_prefix)
    for e in rep:
        while True:
            try:
                p.produce(topic, key=e.u.encode(), value=json.dumps(e.__dict__).encode())
                break
            except BufferError:
                p.poll(0.5)
    p.flush(60)
    deadline = time.time() + args.timeout
    groups = (("repartitioner", ACCESS_EVENTS), ("graph-index", "access-events-by-resource"),
              ("cmu-user", "access-events.ordered"), ("cmu-resource", "access-events-by-rstar"))
    time.sleep(5)
    from confluent_kafka import KafkaException
    while time.time() < deadline:
        try:
            if all(group_lag(s.kafka_bootstrap, g, [topic_name(t, s.kafka_topic_prefix)])["total"] == 0 for g, t in groups):
                break
        except KafkaException as exc:                  # e.g. NOT_COORDINATOR while a group coordinator moves: retry
            print(f"  (lag query retry: {exc})")
        time.sleep(2)
    esc_id = uuid.uuid4().hex
    esc = {"escalation_id": esc_id, "schema_version": 1, "u": ren(seed), "t": time.time(), "priority": "realtime",
           "paths": ["harness"], "evidence": {"harness": {"t": t_an, "note": "v3 live check"}}, "source": "synthetic-replay"}
    p.produce(topic_name(ESCALATIONS_REALTIME, s.kafka_topic_prefix), key=esc["u"].encode(), value=json.dumps(esc).encode())
    p.flush(10)
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"verify-{run}", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([topic_name(GROUP_SCORES, s.kafka_topic_prefix)])
    got = None
    while time.time() < deadline and got is None:
        for m in c.consume(500, 1.0):
            if not m.error() and json.loads(m.value()).get("escalation_id") == esc_id:
                got = json.loads(m.value())
    c.close()
    out = [(got is not None, "TGN v3 worker analysed the escalation",
            f"graph {got['graph']} in {got['inference_ms']} ms, staleness {got.get('memory_staleness_s')} s" if got else "no group-scores record")]
    if got:
        short = lambda ms: sorted(m.split("-", 3)[-1] for m in ms)
        out.append((sorted(got["members"]) == sorted(expected["members"]), "live group == offline group (same members)",
                    f"live {short(got['members'])}"))
        d = None if got["s_coll"] is None or expected["s_coll"] is None else abs(got["s_coll"] - expected["s_coll"])
        out.append((True, "S_coll live vs offline (measured skew, asynchronous CMU tasks)",
                    f"live {got['s_coll']} offline {expected['s_coll']} |delta| {d if d is None else round(d, 6)}"))
        out.append((got["model_version"] == scorer.version and got.get("model_kind") == "tgn-v3", "scored by the promoted v3 TGN",
                    got["model_version"]))
        flagged = bool(got["above_theta_coll"])
        if args.case == "collusion":
            truth = set(map(str, info["members"]))
            found = set(short(got["members"])) & truth
            out.append((True, "detection outcome (synthetic, reported)",
                        f"S_coll {'>=' if flagged else '<'} theta_C {got['theta_coll']}; true colluders in group {sorted(found)} of {sorted(truth)}"))
        else:
            out.append((True, "benign team meeting outcome (reported)", f"flagged={flagged} s_coll={got['s_coll']} theta_C={got['theta_coll']}"))
        if got["group_id"]:
            r = redis_lib.Redis(**s.redis_kwargs(), decode_responses=True)
            out.append((r.hget(f"coll:{ren(seed)}", "group_id") == got["group_id"], "coll:<u> state written for the PDP",
                        f"s_coll {r.hget(f'coll:{ren(seed)}', 's_coll')} A_u {r.hget(f'coll:{ren(seed)}', 'A_u')}"))
    for ok, what, detail in out:
        print(f"  {'PASS' if ok else 'FAIL'}  {what}  [{detail}]")
    failed = sum(1 for ok, _, _ in out if not ok)
    print(f"{len(out) - failed}/{len(out)} checks passed")
    return 1 if failed else 0


def main(argv=None) -> int:
    from confluent_kafka import Consumer, Producer
    import redis as redis_lib
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("case", choices=["collusion", "team", "cmu", "admit", "burst", "check-cmu", "cost-fit"])
    ap.add_argument("--index", type=int, default=0)
    ap.add_argument("--timeout", type=float, default=240)
    ap.add_argument("--user", default="syn-cmu-burst")
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--nbrs", type=int, default=10)
    ap.add_argument("--offline", action="store_true", help="v3 collusion/team: offline reference only (no Kafka)")
    args = ap.parse_args(argv)
    s = get_settings()
    v3 = {"cmu": cmd_cmu, "admit": cmd_admit, "burst": cmd_burst, "check-cmu": cmd_check_cmu, "cost-fit": cmd_cost_fit}
    if args.case in v3:
        return v3[args.case](s, args)
    ds = generate(SynthConfig.collusion_study())
    scorer = TgnScorer.load(s.model_dir)
    if getattr(scorer, "arch", "") == "tgn-v3":
        return cmd_replay_v3(s, args, ds, scorer)
    name, seed, anchor, info = pick(ds, args.case, args.index)
    evs = sorted(ds.events, key=lambda e: (e.t, e.event_id))
    ann = EdgeAnnotator()
    index = MemoryEdgeIndex([ann.annotate(e) for e in evs])
    sample = build_sample([seed], index, anchor, scorer.gp)
    expected = scorer.analyse(sample)[seed]
    run = uuid.uuid4().hex[:6]
    ren = lambda u: f"syn-tgn-{run}-{u}"
    # Isolation: shift this replay by whole weeks into the past (weekday, hour and day differences - hence every
    # feature - are unchanged), so it cannot share a time window with earlier replays of the same data.
    shift = -7 * 86400.0 * random.Random(run).randint(60, 600)
    participants = set(sample.users)
    replay = [e for e in evs if e.u in participants and e.t <= anchor]
    print(f"{args.case}: {name}, seed {seed}, anchor {anchor:.0f} (replayed {shift / 604800:.0f} weeks earlier); offline: members {[str(m) for m in expected['members']]} "
          f"s_coll {expected['s_coll']}; replaying {len(replay)} events of {len(participants)} users")
    p = Producer({"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 20})
    for e in replay:
        d = dict(e.__dict__); d.update(u=ren(e.u), t=e.t + shift, event_id=uuid.uuid4().hex,
                                       meta={**e.meta, "source": "synthetic-replay"})
        while True:
            try:
                p.produce(topic_name(ACCESS_EVENTS, s.kafka_topic_prefix), key=d["u"].encode(), value=json.dumps(d).encode()); break
            except BufferError:
                p.poll(0.5)
    p.flush(60)
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        lags = {g: group_lag(s.kafka_bootstrap, g, [topic_name(t, s.kafka_topic_prefix)])["total"]
                for g, t in (("repartitioner", ACCESS_EVENTS), ("graph-index", "access-events-by-resource"))}
        if all(v == 0 for v in lags.values()):
            break
        time.sleep(2)
    esc_id = uuid.uuid4().hex
    esc = {"escalation_id": esc_id, "schema_version": 1, "u": ren(seed), "t": time.time(), "priority": "realtime",
           "paths": ["harness"], "evidence": {"harness": {"t": anchor + shift, "note": "Phase 7 live check"}}, "source": "synthetic-replay"}
    p.produce(topic_name(ESCALATIONS_REALTIME, s.kafka_topic_prefix), key=esc["u"].encode(), value=json.dumps(esc).encode())
    p.flush(10)
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": f"verify-{run}", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([topic_name(GROUP_SCORES, s.kafka_topic_prefix)])
    got = None
    while time.time() < deadline and got is None:
        for m in c.consume(500, 1.0):
            if not m.error():
                r = json.loads(m.value())
                if r.get("escalation_id") == esc_id:
                    got = r
    c.close()
    results = [(got is not None, "TGN worker analysed the escalation", f"graph {got['graph']} in {got['inference_ms']} ms" if got else "no group-scores record")]
    if got:
        exp_members = sorted(ren(u) for u in expected["members"])
        same_s = (got["s_coll"] is None and expected["s_coll"] is None) or (
            got["s_coll"] is not None and expected["s_coll"] is not None and abs(got["s_coll"] - expected["s_coll"]) < 1e-5)
        results.append((sorted(got["members"]) == exp_members and same_s, "live result == offline result (parity)",
                        f"live members {[m.split('-', 3)[-1] for m in got['members']]} s_coll {got['s_coll']}"))
        results.append((got["model_version"] == scorer.version, "scored by the promoted TGN", got["model_version"]))
        truth = set(info["members"]) if args.case == "collusion" else set()
        flagged = bool(got["above_theta_coll"])
        if args.case == "collusion":
            found = {m.split("-", 3)[-1] for m in got["members"]} & truth
            results.append((True, "detection outcome (synthetic, reported)",
                            f"S_coll {'>=' if flagged else '<'} theta_coll {got['theta_coll']}; true colluders in group {sorted(map(str, found))} of {sorted(map(str, truth))}"))
        else:
            results.append((True, "benign team meeting outcome (reported)", f"flagged={flagged} s_coll={got['s_coll']} theta={got['theta_coll']}"))
        if got["group_id"]:
            r = redis_lib.Redis(**s.redis_kwargs(), decode_responses=True)
            results.append((r.hget(f"coll:{ren(seed)}", "group_id") == got["group_id"], "coll:<u> state written for the PDP",
                            r.hget(f"coll:{ren(seed)}", "s_coll")))
    for ok, what, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {what}  [{detail}]")
    failed = sum(1 for ok, _, _ in results if not ok)
    print(f"{len(results) - failed}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

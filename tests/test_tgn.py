"""Phase 7 tests: collusion-study data, edge annotation, neighbourhoods, features, grouping, two-stage training,
TGN registry, live edge index parity, TGN worker."""
import collections
import json
import random
from dataclasses import replace

import fakeredis
import numpy as np
import pytest
import torch

from accesssentinel.datasets.synth import SynthConfig, generate
from accesssentinel.ml import tgn_registry
from accesssentinel.ml.tgn_graph import (NODE_FEATURES, EdgeAnnotator, GraphParams, MemoryEdgeIndex, build_sample,
                                         featurize)
from accesssentinel.ml.tgn_train import (Corpus, calibrate, groups_for, rule_shared_sensitive, rule_uncommon_coaccess,
                                         stage1, stage2, tgn_config)
from accesssentinel.schema import make_event
from accesssentinel.streaming.consumer import Record
from accesssentinel.streaming.graph_index import GraphIndexHandler, RedisEdgeIndex
from accesssentinel.streaming.tgn_worker import COLL, TgnWorkerHandler, anchor_time
from accesssentinel.tgn import TGN

T0 = 1_794_000_000.0
SMALL = SynthConfig(n_users=320, days=10, split_days=(6, 2, 2), seed=5, collusion_v2=True, benign_teams=True,
                    attacks={"compromised_account": (1, 0, 0), "insider_exfiltration": (1, 0, 0), "bac_probe": (0, 0, 0),
                             "low_and_slow": (0, 0, 0)},
                    collusion_groups={"coordinated_collection": (2, 1, 1), "split_exfiltration": (2, 1, 1)})


@pytest.fixture(scope="module")
def ds():
    return generate(SMALL)


def ev(u, t, r="payroll", a="hr", obj=None, s=1):
    meta = {"object_id": str(obj)} if obj is not None else {}
    return make_event(u, a, r, s, "view", True, {"ip": "10.0.0.1", "device": "d", "session": "active"}, "c" * 32, meta, t=t)


# ------------------------------------------------------------------------------------------ data
def test_default_dataset_is_unchanged_by_phase7_options():
    assert generate().fingerprint() == "beeb9bffdb1d7160"          # Phase 5 results stay reproducible


def test_collusion_study_style_data_has_groups_in_every_split_and_respects_the_cap(ds):
    splits = collections.Counter(v["split"] for v in ds.instances.values() if "bursts" in v)
    assert set(splits) == {"train", "val", "test"}
    roles = collections.Counter(p["role"] for p in ds.users.values())
    att = collections.Counter(p["role"] for p in ds.users.values() if p.get("attacker"))
    assert all(att[r] / roles[r] <= SMALL.max_attacker_share_per_role for r in att)
    assert ds.meetings and any(l.scenario == "benign_team_coaccess" for l in ds.labels.values())
    for v in ds.instances.values():
        if "bursts" in v:
            assert {"members", "familiar"} <= set(v) and len(v["members"]) >= 2
    with pytest.raises(ValueError):                                 # cannot silently drop groups any more
        generate(replace(SMALL, n_users=40))


# ------------------------------------------------------------------------------------------ graph
def test_annotator_familiarity_counts_distinct_earlier_days_and_sequences():
    a = EdgeAnnotator()
    e1 = a.annotate(ev("u", T0, obj=5)); e2 = a.annotate(ev("u", T0 + 60, obj=6))
    e3 = a.annotate(ev("u", T0 + 86400, obj=9)); e4 = a.annotate(ev("u", T0 + 2 * 86400))
    assert [e["fam"] for e in (e1, e2, e3, e4)] == [0, 0, 1, 2]       # a burst does not create familiarity
    assert [e["seq"] for e in (e1, e2, e3)] == [0, 1, 0]


def test_neighbourhood_is_bounded_and_features_have_the_right_shape():
    a = EdgeAnnotator()
    edges = [a.annotate(ev(f"u{i}", T0 + i * 10, r=f"r{i % 30}")) for i in range(300)]
    edges += [a.annotate(ev("seed", T0 + 100 + k, r=f"r{k}")) for k in range(30)]
    gp = GraphParams(max_resources=5, max_users_per_resource=2)
    s = build_sample(["seed"], MemoryEdgeIndex(edges), T0 + 3000, gp)
    assert len(s.resources) <= 5 and s.n_users <= 1 + 5 * 2 and "seed" in s.seeds
    X, A = featurize(s, gp)
    assert X.shape == (gp.T, s.n_nodes, NODE_FEATURES) and A.shape == (gp.T, s.n_nodes, s.n_nodes)
    assert np.allclose(A, A.transpose(0, 2, 1))
    assert build_sample(["seed"], MemoryEdgeIndex(edges), T0 + 3000, gp).edges == s.edges     # deterministic


def test_concurrency_feature_marks_synchronised_co_access():
    a = EdgeAnnotator()
    edges = [a.annotate(ev("x", T0 + 10)), a.annotate(ev("y", T0 + 70)), a.annotate(ev("z", T0 + 3000, r="reviews"))]
    s = build_sample(["x"], MemoryEdgeIndex(edges), T0 + 3500, GraphParams())
    X, _ = featurize(s, GraphParams())
    ui = {u: i for i, u in enumerate(s.users)}
    assert X[0, ui["x"], 16] == 1.0 and X[0, ui["y"], 16] == 1.0


def test_grouping_needs_two_users_with_suspicious_edges_on_one_resource():
    a = EdgeAnnotator()
    edges = [a.annotate(ev(u, T0 + k, r=r)) for k, (u, r) in enumerate([("a", "payroll"), ("b", "payroll"), ("c", "reviews")])]
    s = build_sample(["a", "c"], MemoryEdgeIndex(edges), T0 + 100, GraphParams())
    sc = np.array([0.9 if e["u"] in ("a", "b") else 0.95 for e in s.edges])
    g = groups_for(s, sc, tau_edge=0.8)
    assert g["a"]["members"] == ["a", "b"] and g["a"]["s_coll"] == pytest.approx(0.9)
    assert g["c"]["s_coll"] is None                                   # suspicious, but alone -> no group
    assert groups_for(s, sc, tau_edge=0.99)["a"]["s_coll"] is None


def test_rule_baselines():
    a = EdgeAnnotator()
    es = [a.annotate(ev(u, T0 + k * 5, r=r)) for k, (u, r) in enumerate([("s", "payroll"), ("s", "reviews"), ("p", "payroll"), ("p", "reviews")])]
    smp = build_sample(["s"], MemoryEdgeIndex(es), T0 + 100, GraphParams())
    assert rule_shared_sensitive(smp, "s") == 0.5 and rule_uncommon_coaccess(smp, "s") == 0.5


# ------------------------------------------------------------------------------------------ training + registry
@pytest.fixture(scope="module")
def trained(tmp_path_factory, ds):
    gp = GraphParams()
    corpus = Corpus(ds, gp)
    benign = corpus.benign("train", 30, random.Random(0))
    train = corpus.evaluation_set("train", random.Random(1), anchors_per_burst=2, n_random=40, n_resource=40)
    val = corpus.evaluation_set("val", random.Random(2), anchors_per_burst=2, n_random=30, n_resource=30)
    cfg = replace(tgn_config(gp), emb_dim=8)
    torch.manual_seed(0)
    model = TGN(cfg)
    hist = stage1(model, benign, 4, cfg, 0, log=lambda *a: None)
    info = stage2(model, train, val, 3, 3e-4, cfg, 0, 3, log=lambda *a: None)
    cal = calibrate(model, val)
    d = str(tmp_path_factory.mktemp("models"))
    tgn_registry.save(d, "tgn-test-1", model, cfg, gp, cal["tau_edge"], cal["theta_coll"], {"note": "unit test"})
    tgn_registry.promote(d, "tgn-test-1")
    return d, model, corpus, val, hist, info, cal


def test_two_stage_training_runs_and_benign_graphs_are_benign(trained):
    d, model, corpus, val, hist, info, cal = trained
    assert hist[-1] < hist[0]                                         # self-supervised loss decreases
    assert 0 < cal["tau_edge"] < 1 and 0 < cal["theta_coll"] < 1 and "best_val_seed_pr_auc" in info
    for it in corpus.benign("train", 10, random.Random(9)):
        assert not any(corpus.lab[e["id"]].collusion for e in it.sample.edges)


def test_registry_roundtrip_and_no_model(trained, tmp_path):
    d, model, corpus, val, *_ = trained
    sc = tgn_registry.TgnScorer.load(d)
    it = next(i for i in val if i.sample.edges)
    with torch.no_grad():
        h = model.eval().embed(it.X, it.A)
        ref = torch.sigmoid(model.dec(torch.cat([h[it.eu], h[it.er]], -1)).squeeze(-1)).numpy()
    assert np.allclose(sc.edge_scores(it.sample), ref, atol=1e-6) and sc.version == "tgn-test-1"
    with pytest.raises(FileNotFoundError):
        tgn_registry.TgnScorer.load(str(tmp_path))


# ------------------------------------------------------------------------------------------ live services
def rec(e, i):
    return Record("access-events-by-resource", 0, i, b"k", json.dumps(e.__dict__).encode(), 0, data=e)


def test_redis_edge_index_matches_the_offline_index_and_skips_redelivery(ds):
    users = sorted({e.u for e in ds.events})[:15]
    evs = sorted([e for e in ds.events if e.u in users], key=lambda e: (e.t, e.event_id))[:1500]
    r = fakeredis.FakeRedis()
    h = GraphIndexHandler(r, retention=10 * 86400)
    h.handle([rec(e, i) for i, e in enumerate(evs)])
    h.handle([rec(e, i) for i, e in enumerate(evs[:50])])
    assert h.stats == {"indexed": len(evs), "redelivered": 50}
    ann = EdgeAnnotator()
    mem = MemoryEdgeIndex([ann.annotate(e) for e in evs])
    live = RedisEdgeIndex(r)
    t0, t1 = evs[0].t, evs[-1].t
    for u in users[:5]:
        assert live.user_edges(u, t0, t1) == mem.user_edges(u, t0, t1)
    rk = evs[100].rkey
    assert live.resource_edges(rk, t0, t1) == mem.resource_edges(rk, t0, t1)


def test_live_index_is_independent_of_arrival_order():
    """Regression (Phase 7 live parity test): the resource-keyed view can deliver one user's accesses to one
    resource out of time order; annotating on arrival corrupted `fam`. The index must not depend on order."""
    evs = [ev("u", T0 + d * 86400 + k * 600, r=["payroll", "reviews"][k % 2], obj=d * 10 + k) for d in range(6) for k in range(4)]
    evs += [ev("v", T0 + d * 86400 + 300, obj=d) for d in range(6)]
    ann = EdgeAnnotator()
    mem = MemoryEdgeIndex([ann.annotate(e) for e in sorted(evs, key=lambda e: (e.t, e.event_id))])
    shuffled = list(evs)
    random.Random(3).shuffle(shuffled)
    r = fakeredis.FakeRedis()
    GraphIndexHandler(r, retention=30 * 86400).handle([rec(e, i) for i, e in enumerate(shuffled)])
    live = RedisEdgeIndex(r)
    for u in ("u", "v"):
        assert live.user_edges(u, 0, 2e9) == mem.user_edges(u, 0, 2e9)
    assert live.resource_edges("hr:payroll", 0, 2e9) == mem.resource_edges("hr:payroll", 0, 2e9)
    assert max(e["fam"] for e in live.user_edges("u", 0, 2e9)) == 5


def test_edge_index_retention_trims_old_edges():
    r = fakeredis.FakeRedis()
    h = GraphIndexHandler(r, retention=3600)
    h.handle([rec(ev("u", T0), 0), rec(ev("u", T0 + 7200), 1)])
    assert len(RedisEdgeIndex(r).user_edges("u", 0, 2e9)) == 1


class Prod:
    def __init__(self, fail=False): self.fail, self.sent, self._cbs = fail, [], []
    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.sent.append(json.loads(value)); self._cbs.append(on_delivery)
    def flush(self, t=None):
        for cb in self._cbs:
            cb("down" if self.fail else None, None)
        self._cbs = []
        return 0


class Models:
    def __init__(self, scorer): self.scorer = scorer
    def get(self): return self.scorer


def esc_rec(u, t_evidence, i=0):
    d = {"escalation_id": f"e{i}", "u": u, "t": 2e9, "priority": "realtime", "paths": ["resource"],
         "evidence": {"resource": {"t": t_evidence}}}
    return Record("escalations.realtime", 0, i, u.encode(), json.dumps(d).encode(), 0, data=d)


def test_tgn_worker_publishes_group_scores_and_sets_member_state(trained):
    d, *_ = trained
    scorer = tgn_registry.TgnScorer.load(d)
    scorer.tau_edge, scorer.theta_coll = 0.0, 0.5                    # force: every edge suspicious -> a group
    r = fakeredis.FakeRedis()
    gi = GraphIndexHandler(r, retention=86400)
    gi.handle([rec(ev("a", T0 + 10), 0), rec(ev("b", T0 + 20), 1), rec(ev("lonely", T0 + 30, r="board-minutes", a="docs"), 2)])
    prod = Prod()
    h = TgnWorkerHandler(Models(scorer), RedisEdgeIndex(r), r, prod, "group-scores", clock=lambda: 2e9)
    h.handle([esc_rec("a", T0 + 60, 0), esc_rec("lonely", T0 + 60, 1)])
    by_u = {x["u"]: x for x in prod.sent}
    assert by_u["a"]["members"] == ["a", "b"] and by_u["a"]["s_coll"] is not None and by_u["a"]["group_id"]
    assert by_u["a"]["anchor_t"] == T0 + 60 and by_u["a"]["model_version"] == "tgn-test-1" and by_u["a"]["score_id"] == "coll:e0"
    assert by_u["lonely"]["s_coll"] is None and by_u["lonely"]["group_id"] is None
    assert json.loads(r.hget(COLL.format("b"), "members")) == ["a", "b"] and not r.exists(COLL.format("lonely"))


def test_tgn_worker_saves_nothing_when_publishing_fails(trained):
    d, *_ = trained
    scorer = tgn_registry.TgnScorer.load(d)
    scorer.tau_edge = 0.0
    r = fakeredis.FakeRedis()
    GraphIndexHandler(r).handle([rec(ev("a", T0 + 10), 0), rec(ev("b", T0 + 20), 1)])
    h = TgnWorkerHandler(Models(scorer), RedisEdgeIndex(r), r, Prod(fail=True), "group-scores")
    with pytest.raises(RuntimeError):
        h.handle([esc_rec("a", T0 + 60)])
    assert not r.exists(COLL.format("a"))
    assert anchor_time({"t": 5.0, "evidence": {}}) == 5.0


# ============================================================================================================
# Revised v3 Phase 4: Continuous Node Memory Updater (CMU) service
# ============================================================================================================
import json as _json  # noqa: E402

import fakeredis as _fakeredis  # noqa: E402
import numpy as _np  # noqa: E402

from accesssentinel.config import TGNv3Config  # noqa: E402
from accesssentinel.schema import make_event as _make_event  # noqa: E402
from accesssentinel.streaming.consumer import Record as _Record  # noqa: E402
from accesssentinel.streaming.tgn_worker import CMU_KEY, CMU_NBR, CMU_STATS, CmuHandler, cmu_read  # noqa: E402


class _CmuStub:
    """Deterministic stand-in for the trained CMU: m(t) = m(t-) + 1 + 0.001 * m_other[0] (records its calls)."""
    arch, version = "tgn-v3", "tgn3-stub"

    def __init__(self):
        self.cfg = TGNv3Config(n_neighbors=3)
        self.calls = 0

    def cmu_step(self, m_self, m_other, dt, f):
        self.calls += 1
        return m_self + 1.0 + 0.001 * m_other[:, :1]


class _Prov:
    def __init__(self, s): self.scorer = s
    def get(self): return self.scorer


def _cev(u, r, t):
    return _make_event(u, "hr", r, 1, "view", True, {"ip": "10.0.0.1", "device": "d"}, "c" * 32, t=t)


def _recs(evs):
    return [_Record("t", 0, i, e.u.encode(), b"", 0, data=e) for i, e in enumerate(evs)]


def test_cmu_updates_every_event_single_writer_per_side():
    r, sc = _fakeredis.FakeRedis(), _CmuStub()
    hu, hr = CmuHandler(_Prov(sc), r, "u"), CmuHandler(_Prov(sc), r, "r")
    evs = [_cev("alice", "payroll", 100.0), _cev("alice", "directory", 110.0), _cev("bob", "payroll", 120.0)]
    hu.handle(_recs(evs)); hr.handle(_recs(evs))
    mu = cmu_read(r, "u", ["alice", "bob"], 100)
    mr = cmu_read(r, "r", ["default/hr:payroll", "default/hr:directory"], 100)
    assert round(float(mu["alice"]["m"][1]), 3) == 2.0 and round(float(mu["bob"]["m"][1]), 3) == 1.0   # 2 and 1 updates
    pay = float(mr["default/hr:payroll"]["m"][1])
    assert 2.0 < pay < 2.01 and mu["alice"]["t"] == 110.0   # 2 updates + the user memories' snapshot (counterpart read)
    nb = [_json.loads(x) for x in r.lrange(CMU_NBR["u"].format("alice"), 0, -1)]
    assert [x["n"] for x in nb] == ["default/hr:payroll", "default/hr:directory"]
    assert hu.stats["updates"] == 3 and hr.stats["updates"] == 3


def test_cmu_is_idempotent_under_redelivery_and_caps_neighbours():
    r, sc = _fakeredis.FakeRedis(), _CmuStub()
    h = CmuHandler(_Prov(sc), r, "u")
    evs = [_cev("alice", f"res{i}", 100.0 + i) for i in range(5)]
    h.handle(_recs(evs))
    m1 = cmu_read(r, "u", ["alice"], 100)["alice"]["m"].copy()
    h2 = CmuHandler(_Prov(sc), r, "u")                       # a restarted worker re-reads the same batch
    h2.handle(_recs(evs))
    assert _np.allclose(cmu_read(r, "u", ["alice"], 100)["alice"]["m"], m1) and h2.stats["skipped_applied"] == 5
    assert r.llen(CMU_NBR["u"].format("alice")) == 3          # K most recent neighbours only


def test_cmu_measures_counterpart_staleness_and_needs_a_trained_v3_model():
    r, sc = _fakeredis.FakeRedis(), _CmuStub()
    CmuHandler(_Prov(sc), r, "r").handle(_recs([_cev("x", "payroll", 100.0)]))      # resource snapshot at t=100
    hu = CmuHandler(_Prov(sc), r, "u")
    hu.handle(_recs([_cev("alice", "payroll", 160.0)]))
    assert hu.stats["staleness_max"] == 60.0 and float(r.hget(CMU_STATS, "u_staleness_max_s")) == 60.0
    import pytest as _pytest
    with _pytest.raises(RuntimeError):
        CmuHandler(_Prov(None), r, "u").handle(_recs([_cev("alice", "payroll", 200.0)]))


def test_cmu_offset_idempotency_survives_a_redelivered_batch_larger_than_the_id_ring():
    r, sc = _fakeredis.FakeRedis(), _CmuStub()
    evs = [_cev("alice", "payroll", 100.0 + i) for i in range(200)]               # one node, 200 events, one partition
    recs = [_Record("t", 3, 1000 + i, b"alice", b"", 0, data=e) for i, e in enumerate(evs)]
    CmuHandler(_Prov(sc), r, "u").handle(recs)
    m1 = cmu_read(r, "u", ["alice"], 100)["alice"]["m"].copy()
    h2 = CmuHandler(_Prov(sc), r, "u")
    h2.handle(recs)                                                                  # crash before the offset commit
    assert _np.allclose(cmu_read(r, "u", ["alice"], 100)["alice"]["m"], m1) and h2.stats["skipped_applied"] == 200


def test_cost_size_estimate_matches_the_neighbourhood_the_sge_embeds():
    """Eq. 26 inputs: |E| counts the seed's edges and ALL window edges of up to K recent users per touched
    resource (what analyse_v3 gathers), |V| those users + the seed's resources."""
    from accesssentinel.streaming.escalation_service import neighbourhood_size_fn
    r = _fakeredis.FakeRedis()
    evs = [_cev("seed", "payroll", 1000.0 + i) for i in range(3)]
    evs += [_cev(f"n{j}", "payroll", 1100.0 + j) for j in range(4)]
    evs += [_cev("n0", "other", 1200.0 + i) for i in range(5)]                      # n0's edges on another resource
    evs += [_cev("far", "other", 1300.0)]                                            # 2 hops: not in N_1(seed)
    GraphIndexHandler(r, retention=86400).handle(_recs(evs))
    e, v = neighbourhood_size_fn(r, window=3600.0, k=10)("seed", 2000.0)
    assert (e, v) == (3 + 4 + 5, 1 + 4 + 1)
    e2, v2 = neighbourhood_size_fn(r, window=3600.0, k=2)("seed", 2000.0)          # K most recent: n3, n2
    assert (e2, v2) == (3 + 2, 1 + 2 + 1)

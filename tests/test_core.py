import numpy as np, pytest, torch
from accesssentinel.broker import DualViewBroker, partition_for
from accesssentinel.config import Config, TCNConfig, EscalationConfig, RiskConfig
from accesssentinel.escalation import ResourceWindowTracker, EscalationPolicy, EscalationResult
from accesssentinel.events import AccessEvent
from accesssentinel.features import FeatureEncoder, D
from accesssentinel.gateway import Gateway, AccessRequest
from accesssentinel.graph import NeighborhoodBuilder, groups_from_graph
from accesssentinel.pdp import PDP, PolicyContext, Decision
from accesssentinel.risk import RiskMonitor, Tier
from accesssentinel.scaling import n_tcn_workers, lambda_eff, expected_escalated, n_tgn_workers
from accesssentinel.state_store import UserStateStore
from accesssentinel.tcn import TCN, focal_loss, make_splits
from accesssentinel.tgn import TGN, TGNScorer, to_tensors


def ev(u="u1", t=0.0, a="app1", r="res00", s=1, y=1, i=0, ip="10.0.0.1"):
    return AccessEvent(u, t, a, r, s, "read", "view", {"ip": ip, "device": "d1", "session": "active"}, y, i)


def test_same_user_same_partition_and_resource_view_colocates():
    b = DualViewBroker(Config().stream)
    assert partition_for("u1", 8) == partition_for("u1", 8)
    e1, e2 = ev("u1", 10), ev("u2", 20)
    assert b.resource_key(e1) == b.resource_key(e2)            # same resource+bucket -> co-located
    assert b.resource_key(ev(t=10)) != b.resource_key(ev(t=10 + 900))  # time-bucket salt


def test_feature_dim_and_window_padding():
    assert FeatureEncoder().encode(ev(), 5.0, True).shape == (D,) == (50,)
    st = UserStateStore(20, 50, FeatureEncoder())
    w = st.update(ev(t=1)); assert w.shape == (20, 50) and (w[:-1] == 0).all() and w[-1].any()
    w2 = st.window_at("u1", 500.0); assert w2[-1, FeatureEncoder.dt_index] > w[-1, FeatureEncoder.dt_index]


def test_tcn_shapes_and_spec():
    c = TCNConfig(); m = TCN(c).eval()
    assert m(torch.zeros(3, 20, 50)).shape == (3,)
    assert c.dilations == (1, 2, 4, 6, 8, 10, 12, 14, 16, 18) and c.filters[0] == 512 and c.filters[-1] == 12
    assert focal_loss(torch.zeros(4), torch.tensor([0., 1., 0., 1.])) > 0


def test_split_is_72_18_10():
    test, folds = make_splits(1000, TCNConfig())
    tr, va = folds[0]; assert len(test) == 100 and len(tr) == 720 and len(va) == 180


def test_risk_tiers_and_recheck():
    r = RiskMonitor(RiskConfig())
    assert r.update("a", 0.2, 0) == Tier.LOW and r.update("b", 0.5, 0) == Tier.MEDIUM and r.update("c", 0.7, 0) == Tier.HIGH
    assert r.due_rechecks(5) == [] and r.due_rechecks(10) == ["b"]
    assert r.drain_high() == {"c"} and r.drain_high() == set()


def test_resource_trigger_needs_more_than_kappa_distinct_sensitive_users():
    t = ResourceWindowTracker(kappa=3, delta_res=900)
    for i, u in enumerate(["a", "b", "c"]): t.observe(ev(u, i))
    assert t.drain() == set()
    t.observe(ev("d", 4)); assert t.drain() == {"a", "b", "c", "d"}
    t2 = ResourceWindowTracker(3, 900)
    for i, u in enumerate("abcd"): t2.observe(ev(u, i, s=0))
    assert t2.drain() == set()                                  # non-sensitive never triggers


def test_union_dedups():
    r = EscalationResult(high={"a", "b"}, res={"b", "c"}, sweep={"c"}, samp={"d"})
    assert r.union == {"a", "b", "c", "d"} and r.background == {"d"}


def test_graph_has_no_event_nodes_and_groups():
    evs = [ev(u, t, r=r, i=k) for k, (u, t, r) in enumerate(
        [(u, 10 + j, rr) for j, u in enumerate("abc") for rr in ("res00", "res01")])]
    g = NeighborhoodBuilder(Config().graph).build(["a"], evs, 0, 100)
    assert g.n_nodes == len(g.users) + len(g.resources) + len(g.apps) == 3 + 2 + 1
    assert g.n_edges == 6 and len(g.mem_res) == 2
    comps = groups_from_graph(g, 2); assert sorted(map(len, comps)) == [3]


def test_tgn_forward_and_group_score():
    cfg = Config()
    evs = [ev(u, 10 + j, r=rr, i=j) for j, u in enumerate("abc") for rr in ("res00", "res01")]
    g = NeighborhoodBuilder(cfg.graph).build(["a"], evs, 0, 100)
    X, A, eu, er = to_tensors(g, cfg.tgn, 0.25)
    assert X.shape[0] == 10 and A.shape == (10, g.n_nodes, g.n_nodes)
    sc = TGNScorer(TGN(cfg.tgn), cfg.tgn, cfg.graph)
    es = sc.score_edges(g); assert es.shape == (6,) and ((es >= 0) & (es <= 1)).all()
    gs = sc.group_scores(g, es); assert set(gs) == {"a", "b", "c"} and len({v[0] for v in gs.values()}) == 1


def test_pdp_individual_collective_and_policy():
    c = Config(); p = PDP(c.pdp, c.risk); ctx = PolicyContext()
    assert p.decide("u", 0.3, 0, ctx)[0].decision == Decision.ALLOW
    assert p.decide("u", 0.7, 1, ctx)[0].reason == "individual"
    p.set_collective("v", 0.9, 5)
    r, ch = p.decide("v", 0.1, 6, ctx); assert r.decision == Decision.BLOCK and r.reason == "collective" and ch
    assert p.decide("v", 0.1, 6 + 4000, ctx)[0].decision == Decision.ALLOW        # collective score expired
    ctx.blocklist.add("w"); assert p.decide("w", 0.0, 0, ctx)[0].reason == "policy"


def test_gateway_enforcement():
    c = Config(); p = PDP(c.pdp, c.risk); published = []
    gw = Gateway({("app1", "res00"): {"staff"}}, published.append, p.current)
    req = AccessRequest("u", 1, "app1", "res00", 1, "read", "view", "staff")
    assert gw.handle(req).allowed
    assert not gw.handle(AccessRequest("u", 2, "app1", "res00", 1, "read", "view", "intern")).allowed
    rec, _ = p.decide("u", 0.9, 3, PolicyContext()); gw.apply_decision(rec)
    assert "u" in gw.terminated and not gw.handle(req).allowed and len(published) == 3   # every event forwarded


def test_scaling_equations():
    assert lambda_eff(3000, 600, 60) == pytest.approx(3010)
    assert n_tcn_workers(3010, 0.7, 500) == 9
    assert n_tgn_workers(220, 10, 0.7, 50) == 63
    assert expected_escalated(50, 80, 5000, 7200, 0.0005, 95000, window=60) == pytest.approx(219.2, abs=0.1)

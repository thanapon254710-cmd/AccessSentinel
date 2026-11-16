"""Phase 5 tests: synthetic generator, behavioural features, leak-free dataset, training + registry, TCN worker.
A deliberately tiny TCN is trained here for speed; production training uses the D3 configuration."""
import collections
import json

import fakeredis
import numpy as np
import pytest

from accesssentinel.config import RiskConfig, TCNConfig
from accesssentinel.datasets.synth import INDIVIDUAL, SynthConfig, generate
from accesssentinel.ml import registry
from accesssentinel.ml.dataset import build, replay
from accesssentinel.ml.features import D, Preprocessor, UserProfile, observe, window_for_recheck
from accesssentinel.ml.train_tcn import run
from accesssentinel.schema import make_event, validate
from accesssentinel.streaming.consumer import Record
from accesssentinel.streaming.tcn_worker import PROFILE, RECHECK, ModelProvider, TcnWorkerHandler, tier_of

SMALL = SynthConfig(n_users=90, days=10, split_days=(6, 2, 2), seed=3,
                    attacks={"compromised_account": (1, 1, 1), "insider_exfiltration": (1, 1, 1),
                             "bac_probe": (1, 0, 1), "low_and_slow": (1, 0, 1)},
                    collusion_groups={"coordinated_collection": (1, 0, 0), "split_exfiltration": (0, 0, 0)})
TINY = TCNConfig(filters=(16, 16, 8, 8, 8, 8, 8, 8, 8, 8), batch_size=256)


@pytest.fixture(scope="module")
def ds():
    return generate(SMALL)


@pytest.fixture(scope="module")
def trained(tmp_path_factory, ds):
    d = tmp_path_factory.mktemp("models")
    res = run(str(d / "models"), str(d / "results"), SMALL, TINY, epochs=2, patience=2, cv_folds=2, seed=0,
              promote=True, log=lambda *a: None)
    return str(d / "models"), str(d / "results"), res


# ------------------------------------------------------------------------------------------ generator
def test_generator_is_deterministic_valid_and_leak_free(ds):
    assert generate(SMALL).fingerprint() == ds.fingerprint()
    for e in ds.events:
        validate(e)
        assert not {"individual", "collusion", "scenario", "instance"} & set(e.meta)       # labels never in events
        assert e.meta["source"] == "synthetic" and e.u.startswith("syn-")
    by_split = collections.defaultdict(set)
    for inst in ds.instances.values():
        by_split[inst["split"]].update([inst["user"]] if "user" in inst else inst["members"])
    assert not (by_split["train"] & by_split["test"]) and not (by_split["train"] & by_split["val"])
    roles = collections.Counter(p["role"] for p in ds.users.values())
    att = collections.Counter(p["role"] for p in ds.users.values() if p.get("attacker"))
    assert all(att[r] / roles[r] <= SMALL.max_attacker_share_per_role for r in att)
    scen = {l.scenario for l in ds.labels.values()}
    assert set(INDIVIDUAL) <= scen and {"benign_overtime", "benign_travel"} <= scen


def test_attack_events_fall_inside_their_split(ds):
    for e in ds.events:
        lab = ds.labels[e.event_id]
        if lab.instance:
            assert ds.split_of(e.t) == ds.instances[lab.instance]["split"]


# ------------------------------------------------------------------------------------------ features
def ev(u="u1", t=1_794_000_000.0, a="hr", r="payroll", o="view", y=True, ip="10.0.0.5", dev="d1", obj=None):
    meta = {"object_id": str(obj)} if obj is not None else {}
    return make_event(u, a, r, 1, o, y, {"ip": ip, "device": dev, "session": "active"}, "c" * 32, meta, t=t)


def test_features_describe_the_event_against_the_users_past():
    p = UserProfile()
    r0 = observe(p, ev(t=1000.0))
    assert r0["cold"] == 1 and r0["new_net"] == 0 and r0["new_device"] == 0     # nothing to compare with yet
    r1 = observe(p, ev(t=1060.0, ip="10.0.0.9"))                                # same /24 -> not new
    r2 = observe(p, ev(t=1120.0, ip="203.0.113.7", dev="d2", r="reviews"))
    assert r1["new_net"] == 0 and (r2["new_net"], r2["new_device"], r2["new_resource"]) == (1, 1, 1)
    assert abs(r1["dt"] - np.log1p(60.0)) < 1e-9
    a = observe(p, ev(t=1130.0, obj=41)); b = observe(p, ev(t=1131.0, obj=42))
    assert (a["seq_obj"], b["seq_obj"]) == (0, 1)
    no_device = make_event("u1", "hr", "payroll", 1, "view", True, {"ip": "10.0.0.5", "session": "active"}, "c" * 32, t=1140.0)
    r_missing = observe(p, no_device)
    assert r_missing["ctx_missing"] == 1 and r_missing["new_device"] == 0       # missing value != novelty
    assert b["ctx_missing"] == 0
    assert len(p.window) == 6 and p.n == 6 and set(r0) >= {"a", "r", "o", "s", "y", "hour", "dt", "rate_ratio"}


def test_profile_roundtrip_gives_identical_features():
    p = UserProfile()
    for i in range(30):
        observe(p, ev(t=1000.0 + i * 50, r=["payroll", "reviews", "employees"][i % 3], obj=i))
    q = UserProfile.from_dict(json.loads(json.dumps(p.to_dict())))
    nxt = ev(t=3000.0, r="payroll", obj=7)
    assert observe(p, nxt) == observe(q, nxt)


def test_recheck_window_refreshes_inactivity():
    p = UserProfile()
    observe(p, ev(t=1000.0)); observe(p, ev(t=1010.0))
    w = window_for_recheck(p, now=1000.0 + 3600)
    assert w[-1]["dt"] == pytest.approx(np.log1p(3590.0)) and p.window[-1]["dt"] == pytest.approx(np.log1p(10.0))


def test_preprocessor_vocab_oov_and_shape():
    p = UserProfile()
    recs = [observe(p, ev(t=1000.0 + i, r=r)) for i, r in enumerate(["payroll", "payroll", "reviews"])]
    pre = Preprocessor.fit(recs)
    assert pre.apps == ["hr"] and pre.resources == ["hr:payroll", "hr:reviews"]
    x = pre.encode([observe(p, ev(t=2000.0, a="docs", r="unmapped:admin"))])
    assert x.shape == (20, D) and x[-1, 3] == 1 and x[-1, 19] == 1 and x[:-1].sum() == 0   # OOV app + resource
    assert Preprocessor.from_dict(json.loads(json.dumps(pre.to_dict()))) == pre


def test_dataset_split_and_train_only_preprocessing(ds):
    idx, pre, recs = build(ds.events, ds.labels, ds.split_of)
    evs = sorted(ds.events, key=lambda e: (e.t, e.event_id))
    assert all(idx.split[i] == ds.split_of(evs[i].t) for i in range(0, len(evs), 97))
    train_recs = [r for r, e in zip(recs, evs) if ds.split_of(e.t) == "train"]
    assert np.allclose(Preprocessor.fit(train_recs).mean, pre.mean)
    i = len(evs) // 2
    user_recs = [r for r, e in zip(recs, evs[: i + 1]) if e.u == evs[i].u]
    assert np.allclose(idx.gather(np.array([i]))[0], pre.encode(user_recs[-20:]))


# ------------------------------------------------------------------------------------------ training + registry
def test_training_produces_versioned_reloadable_model(trained, ds):
    model_dir, results_dir, res = trained
    v = registry.current_version(model_dir)
    assert v == res["version"] and v in registry.list_versions(model_dir)
    s = registry.TcnScorer.load(model_dir)
    idx, _, _ = build(ds.events, ds.labels, ds.split_of, pre=s.pre)
    X = idx.gather(idx.mask("test")[:200])
    a, b = s.score_X(X), registry.TcnScorer.load(model_dir, v).score_X(X)
    assert np.allclose(a, b) and ((a >= 0) & (a <= 1)).all()
    m = res["test"]["tcn"]["event_level"]
    assert {"roc_auc", "pr_auc", "at_0.4", "at_0.6"} <= set(m) and set(res["test"]) == {"tcn", "baseline_rule", "baseline_logreg_last_event"}
    assert res["data"]["source"].startswith("SYNTHETIC") and res["cross_validation"]["folds"]
    import os
    assert os.path.exists(os.path.join(results_dir, f"{v}.md")) and "SYNTHETIC" in open(os.path.join(results_dir, f"{v}.md"), encoding="utf-8").read()
    with pytest.raises(FileExistsError):
        registry.save(model_dir, v, s.model, s.platt, s.pre, TINY, {})


def test_no_model_means_no_scorer(tmp_path):
    with pytest.raises(FileNotFoundError):
        registry.TcnScorer.load(str(tmp_path))
    assert ModelProvider(str(tmp_path)).get() is None


# ------------------------------------------------------------------------------------------ worker
class FakeProducer:
    def __init__(self, fail=False): self.sent, self.fail, self._cbs = [], fail, []
    def produce(self, topic, key=None, value=None, on_delivery=None, headers=None):
        self.sent.append((topic, key, json.loads(value))); self._cbs.append(on_delivery)
    def poll(self, t=0): pass
    def flush(self, t=None):
        for cb in self._cbs:
            cb("down" if self.fail else None, None)
        self._cbs = []
        return 0


class StubScorer:
    def __init__(self, value, version="stub-v1"): self.value, self.version = value, version
    def score_windows(self, ws): return np.full(len(ws), self.value)


class StubProvider:
    def __init__(self, scorer): self.scorer = scorer
    def get(self): return self.scorer


def recs_for(events):
    return [Record("access-events", 0, i, e.u.encode(), b"", 0, data=e) for i, e in enumerate(events)]


def test_worker_matches_offline_scoring_and_persists_state(trained, ds):
    model_dir, _, _ = trained
    scorer = registry.TcnScorer.load(model_dir)
    r = fakeredis.FakeRedis()
    prod = FakeProducer()
    h = TcnWorkerHandler(ModelProvider(model_dir), r, prod, "risk-scores", clock=lambda: 2e9)
    users = sorted({e.u for e in ds.events})[:6]
    evs = sorted([e for e in ds.events if e.u in users], key=lambda e: (e.t, e.event_id))[:600]
    for i in range(0, len(evs), 97):                               # several batches, state carried in Redis
        h.handle(recs_for(evs[i:i + 97]))
    online = {rec["event_id"]: rec for _, _, rec in prod.sent}
    recs, _ = replay(evs)
    windows, per_user = [], collections.defaultdict(list)
    for e, rec in zip(evs, recs):
        per_user[e.u].append(rec); windows.append(per_user[e.u][-20:])
    offline = scorer.score_windows(windows)
    diff = max(abs(online[e.event_id]["s_ind"] - s) for e, s in zip(evs, offline))
    assert len(online) == len(evs) and diff < 1e-6                  # train/serve parity
    assert all(rec["model_version"] == scorer.version and rec["trigger"] == "event" for rec in online.values())
    assert all(r.exists(PROFILE.format(u)) for u in users) and r.hget(f"risk:{users[0]}", "model_version")


def test_worker_skips_redelivered_events_and_saves_nothing_on_publish_failure():
    r, prod = fakeredis.FakeRedis(), FakeProducer()
    h = TcnWorkerHandler(StubProvider(StubScorer(0.1)), r, prod, "risk-scores", clock=lambda: 1e9)
    batch = recs_for([ev(t=1e9 - 10 + i) for i in range(3)])
    h.handle(batch)
    assert len(prod.sent) == 3
    h.handle(batch)                                                 # redelivery after a crash before commit
    assert len(prod.sent) == 3 and h.stats["skipped_redelivered"] == 3
    r2, bad = fakeredis.FakeRedis(), FakeProducer(fail=True)
    h2 = TcnWorkerHandler(StubProvider(StubScorer(0.1)), r2, bad, "risk-scores", clock=lambda: 1e9)
    with pytest.raises(RuntimeError):
        h2.handle(recs_for([ev(t=1e9)]))
    assert not r2.exists(PROFILE.format("u1"))                      # retried batch recomputes from old state


def test_tiers_and_medium_recheck_schedule():
    rc = RiskConfig()
    assert [tier_of(x, rc) for x in (0.39, 0.4, 0.59, 0.6)] == ["low", "medium", "medium", "high"]
    now = {"t": 1e9}
    r, prod = fakeredis.FakeRedis(), FakeProducer()
    h = TcnWorkerHandler(StubProvider(StubScorer(0.5)), r, prod, "risk-scores", recheck_seconds=600,
                         clock=lambda: now["t"])
    h.handle(recs_for([ev(u="live", t=now["t"] - 5), ev(u="old", t=now["t"] - 86400)]))
    assert r.zscore(RECHECK, "live") == pytest.approx(now["t"] + 600) and r.zscore(RECHECK, "old") is None
    assert h.tick() == 0                                            # not due yet
    now["t"] += 601
    assert h.tick() == 1
    rec = prod.sent[-1][2]
    assert rec["trigger"] == "recheck" and rec["u"] == "live" and rec["seconds_since_last_event"] == pytest.approx(606)
    assert r.zscore(RECHECK, "live") == pytest.approx(now["t"] + 600)          # still medium -> rescheduled
    h.models.scorer.value = 0.2
    now["t"] += 601
    h.tick()
    assert r.zscore(RECHECK, "live") is None and r.hget("risk:live", "tier") == b"low"


# ============================================================================================================
# Revised v3 TCN (Phase 3)
# ============================================================================================================
import torch  # noqa: E402

from accesssentinel.config import TCNv3Config  # noqa: E402
from accesssentinel.ml.dataset import build_v3  # noqa: E402
from accesssentinel.ml.features import (D_V3, N_NUM, EncoderV3, SimulatedDevPosture, SimulatedThreatFeed,  # noqa: E402
                                        UserStateV3, observe_v3, timer_context_at)
from accesssentinel.ml.train_tcn import calibrate_v3, run_v3  # noqa: E402
from accesssentinel.pdp_engine import theta_high_for  # noqa: E402
from accesssentinel.streaming.tcn_worker import STATE_V3  # noqa: E402
from accesssentinel.tcn import TCNv3, multitask_loss  # noqa: E402

NEUTRAL = (lambda u, ip, t: 0.0, lambda u, d, t: 1.0)


def test_v3_architecture_matches_the_paper():
    cfg = TCNv3Config()
    assert (cfg.window_len, cfg.feature_dim, cfg.receptive_field()) == (128, 43, 127) and D_V3 == 43
    m = TCNv3(cfg, n_res=30, n_op=8, n_net=65)
    assert len(m.body) == 6 and all(hasattr(b, "c1") and hasattr(b, "c2") for b in m.body)   # two convs per block
    assert [b.c1.conv.dilation[0] for b in m.body] == [1, 2, 4, 8, 16, 32]
    assert [b.c2.conv.out_channels for b in m.body] == [256, 256, 128, 128, 64, 64]
    B = 4
    out = m(torch.zeros(B, 128, dtype=torch.long), torch.zeros(B, 128, dtype=torch.long),
            torch.zeros(B, 128, dtype=torch.long), torch.zeros(B, 128, N_NUM), torch.zeros(B, 4))
    assert out["logit"].shape == (B,) and out["next_res"].shape == (B, 30) and out["next_op"].shape == (B, 8)


def test_v3_is_causal_future_steps_do_not_change_earlier_outputs():
    torch.manual_seed(0)
    m = TCNv3(TCNv3Config(dropout=0.0), n_res=10, n_op=4, n_net=65).eval()
    x = [torch.randint(1, 10, (1, 128)), torch.randint(1, 4, (1, 128)), torch.randint(1, 65, (1, 128)), torch.randn(1, 128, N_NUM)]
    h1 = m.body(m.embed(*x).transpose(1, 2))[:, :, :100]
    x2 = [t.clone() for t in x]
    x2[3][:, 100:] += 5.0                                                   # change only steps >= 100
    h2 = m.body(m.embed(*x2).transpose(1, 2))[:, :, :100]
    assert torch.allclose(h1, h2, atol=1e-6)


def test_v3_loss_focal_only_when_supervised_and_masked_targets():
    cfg = TCNv3Config()
    m = TCNv3(cfg, 10, 4, 65)
    B = 3
    batch = {"res": torch.ones(B, 128, dtype=torch.long), "op": torch.ones(B, 128, dtype=torch.long),
             "net": torch.ones(B, 128, dtype=torch.long), "num": torch.zeros(B, 128, N_NUM), "z": torch.zeros(B, 4),
             "y": torch.tensor([0.0, 1.0, 0.0]), "next_res": torch.tensor([1, 2, 3]), "next_op": torch.tensor([1, 1, 2]),
             "next_gap": torch.zeros(B), "next_mask": torch.tensor([True, False, True])}
    out = m(batch["res"], batch["op"], batch["net"], batch["num"], batch["z"])
    _, parts = multitask_loss(out, batch, cfg, supervised=False)
    assert "sup" not in parts and set(parts) == {"next_res", "next_op", "time_gap"}
    _, parts = multitask_loss(out, batch, cfg, supervised=True)
    assert "sup" in parts


def test_v3_features_late_insert_and_timer_context():
    st = UserStateV3()
    for t in (100.0, 200.0, 300.0):
        observe_v3(st, ev(t=t), *NEUTRAL)
    rec, z = observe_v3(st, ev(t=250.0), *NEUTRAL)                         # late event: inserted at its position
    assert [r["te"] for r in st.window] == [100.0, 200.0, 250.0, 300.0]
    assert len(z) == 4 and rec["first_seen"] == 0
    z1 = timer_context_at(st, 1000.0, 0.0, 1.0)
    z2 = timer_context_at(st, 5000.0, 0.0, 1.0)
    assert z2[0] > z1[0] and z2[1] > z1[1]                                 # inactivity and session age grow (Eq. 9)
    st2 = UserStateV3.from_dict(json.loads(json.dumps(st.to_dict())))
    assert st2.window == st.window


def test_v3_simulated_timer_context_is_deterministic_and_label_free():
    tf = SimulatedThreatFeed(p_listed=1.0)
    assert tf("u", "203.0.113.7", 1e9) > 0.8 and tf("u", "10.20.1.5", 1e9) < 0.1
    assert tf("u", "203.0.113.7", 1e9) == tf("u", "203.0.113.7", 1e9)
    dp = SimulatedDevPosture({"u": {"dev-ok"}}, p_degraded=0.0)
    assert dp("u", "dev-ok", 1e9) > 0.8 and dp("u", "dev-attacker", 1e9) < 0.5


def test_v3_dataset_gap_rule_and_train_only_encoder(ds):
    threat, posture = SimulatedThreatFeed(), SimulatedDevPosture.from_dataset(ds)
    idx, enc, recs = build_v3(ds.events, ds.labels, ds.split_of, threat, posture)
    for s in ("val", "test"):
        ids = idx.mask(s)
        if len(ids):
            assert np.all(idx.end[ids] - (idx.L - 1) >= idx.split_start[ids])   # whole window inside its split
    ordered = sorted(ds.events, key=lambda e: (e.t, e.event_id))
    train_res = {r["res"] for r, e in zip(recs, ordered) if ds.split_of(e.t) == "train"}
    assert set(enc.res_vocab) == train_res                                 # vocabulary from training only
    b = idx.gather(idx.mask("train")[:2])
    assert b["num"].shape == (2, 128, N_NUM) and b["z"].shape == (2, 4)


def test_v3_calibration_quantiles_are_ordered():
    s = np.linspace(0, 1, 10001)
    th = calibrate_v3(s, TCNv3Config())
    assert th["theta_low"] < th["theta_high"] < th["theta_B"]
    assert abs(th["theta_high"] - 0.99) < 1e-3 and abs(th["theta_low"] - 0.95) < 1e-3


def test_v3_refuses_to_save_an_untrained_model(tmp_path):
    with pytest.raises(ValueError):
        run_v3(str(tmp_path), str(tmp_path), SynthConfig(), TCNv3Config(finetune_epochs=0), "cpu", 0, False)


def test_pdp_and_escalation_use_the_calibrated_theta_high_unless_overridden():
    rc = RiskConfig()
    assert theta_high_for({"theta_high": 0.83}, rc, None) == 0.83
    assert theta_high_for({}, rc, None) == rc.theta_high                    # D3 scores: configured default
    assert theta_high_for({"theta_high": 0.83}, rc, 0.7) == 0.7             # explicit operator setting wins


class V3StubScorer:
    arch, version, L = "tcn-v3", "tcn3-stub", 128

    def __init__(self, value, thresholds):
        self.value, self.thresholds, self.calls = value, thresholds, []

    def score_windows(self, ws, zs=None):
        self.calls.append(([[r["te"] for r in w] for w in ws], zs))
        return np.full(len(ws), self.value)


def test_worker_v3_path_late_policies_thresholds_and_recheck():
    r, prod = fakeredis.FakeRedis(), FakeProducer()
    sc = V3StubScorer(0.5, {"theta_low": 0.3, "theta_high": 0.45, "theta_B": 0.9})
    h = TcnWorkerHandler(StubProvider(sc), r, prod, "risk-scores", clock=lambda: 1_794_000_100.0)
    e1, e2 = ev(t=1_794_000_000.0), ev(t=1_794_000_050.0)
    late_hist = ev(t=1_794_000_010.0); late_hist.meta["late_policy"] = "history_only"
    late_res = ev(t=1_794_000_020.0); late_res.meta["late_policy"] = "rescore"
    h.handle(recs_for([e1, e2, late_hist, late_res]))
    sent = [v for _, _, v in prod.sent]
    assert [x["event_id"] for x in sent] == [e1.event_id, e2.event_id, late_res.event_id]   # history_only not scored
    assert sent[0]["tier"] == "high" and sent[0]["theta_high"] == 0.45 and sent[0]["thresholds"] == "calibrated"
    last_window = sc.calls[0][0][-1]
    assert last_window[-1] == 1_794_000_050.0 and 1_794_000_020.0 in last_window and last_window == sorted(last_window)
    st = UserStateV3.from_dict(json.loads(r.get(STATE_V3.format("u1"))))
    assert [x["te"] for x in st.window] == sorted(x["te"] for x in st.window) and len(st.window) == 4
    sc.value = 0.35                                                         # Medium user re-check (Eq. 9)
    out = h._recheck_v3(["u1"], sc, 1_794_009_000.0)
    assert out[0]["trigger"] == "recheck" and out[0]["tier"] == "medium" and out[0]["timer_context"][0] > 8.0


def test_v3_encoder_is_robust_to_train_serve_skew():
    """Regression (Phase 3 live check): synthetic training events are schema v1 without v_policy, so the version
    features were constant; live schema-v2 events scaled them to ~5e5 and every live score was 0.0. Constant
    features are now neutralised, binary ones unscaled, continuous ones clipped."""
    from accesssentinel.ml.features import CLIP, NUM_FIELDS
    st = UserStateV3()
    train = [observe_v3(st, make_event("u1", "hr", ["payroll", "directory"][i % 2], i % 2 == 0, "view", i % 7 != 0,
                                       {"ip": "10.0.0.5", "device": "d1"}, "c" * 32, t=1_794_000_000.0 + 60 * i),
                        *NEUTRAL)[0] for i in range(50)]                     # schema v2 defaults: no v_policy
    enc = EncoderV3.fit(train)
    assert NUM_FIELDS.index("policy_h") in enc.neutral and NUM_FIELDS.index("schema_v") in enc.neutral
    assert NUM_FIELDS.index("s") not in enc.neutral and NUM_FIELDS.index("y") not in enc.neutral
    live = make_event("u1", "hr", "payroll", 1, "view", False, {"ip": "10.0.0.5", "device": "d1"}, "c" * 32,
                      t=1_794_009_000.0, seq=3, stream="gw1:x", v_policy="1cbb0e4395ea")
    rec, _ = observe_v3(st, live, *NEUTRAL)
    _, _, _, num, z = enc.encode_record(rec)
    assert np.all(np.abs(num) <= CLIP) and num[NUM_FIELDS.index("schema_v")] == 0.0
    assert num[NUM_FIELDS.index("y")] == 0.0 and num[NUM_FIELDS.index("s")] == 1.0        # binary: as is
    assert EncoderV3.from_dict(enc.to_dict()).neutral == enc.neutral


# ------------------------------------------------------------------------------------------ Revised v3 Phase 8 scenarios
def _v3_small():
    from accesssentinel.datasets.synth import SynthConfig as C
    return generate(C(n_users=420, days=28, split_days=(16, 5, 7), seed=5, collusion_v2=True, benign_teams=True,
                      benign_projects=True, collusion_group_sizes=(2, 5),
                      attacks={"horizontal_abuse": (1, 0, 1), "unusual_sequence": (1, 0, 1), "unusual_time": (1, 0, 1)},
                      collusion_groups={"cross_app_collection": (1, 0, 1), "low_slow_group": (2, 0, 2),
                                        "coordinated_collection": (1, 0, 1)}))


def test_v3_individual_scenarios_have_their_defining_signal():
    from datetime import datetime, timezone
    ds = _v3_small()
    by = lambda kind: [i for i, v in ds.instances.items() if v["kind"] == kind]
    for inst in by("horizontal_abuse"):                      # sequential enumeration of other people's records
        evs = [e for e in ds.events if ds.labels[e.event_id].instance == inst]
        objs = [int(e.meta["object_id"]) for e in evs]
        assert len(evs) >= 40 and objs == list(range(objs[0], objs[0] + len(objs))) and all(e.y for e in evs)
        assert objs[0] >= ds.users[ds.instances[inst]["user"]]["obj_pool"]           # beyond the user's usual records
    for inst in by("unusual_time"):                          # own device, night
        u = ds.instances[inst]["user"]
        evs = [e for e in ds.events if ds.labels[e.event_id].instance == inst]
        assert all(datetime.fromtimestamp(e.t, tz=timezone.utc).hour < 7 for e in evs)
        assert {e.c["device"] for e in evs} <= set(ds.users[u]["managed_devices"])
    for inst in by("unusual_sequence"):                      # permitted, but stronger operations than usual
        evs = [e for e in ds.events if ds.labels[e.event_id].instance == inst]
        assert {e.o for e in evs} - {"view"} and all(e.y for e in evs)


def test_v3_group_scenarios_sizes_cross_app_and_benign_projects():
    ds = _v3_small()
    groups = {i: v for i, v in ds.instances.items() if "members" in v}
    assert sorted({len(v["members"]) for v in groups.values()}) == [2, 5]               # collusion_group_sizes
    for v in groups.values():
        if v["kind"] == "cross_app_collection":
            assert len(v["apps"]) >= 2 and len(v["targets"]) >= 2
    slow = [v for v in groups.values() if v["kind"] == "low_slow_group"]
    assert slow and all(len(v["bursts"]) >= 5 for v in slow)                            # spread over >= 5 days
    assert ds.projects and {ds.labels[e.event_id].scenario for e in ds.events} >= {"benign_project", "benign_team_coaccess"}
    members = {m for v in groups.values() for m in v["members"]}
    assert not members & {m for p in ds.projects for m in p["team"]}                    # projects are benign users

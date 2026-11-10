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

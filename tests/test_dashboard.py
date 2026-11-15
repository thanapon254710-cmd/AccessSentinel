"""Phase 9 tests: incidents from PDP/TGN records, runtime settings (validation + propagation into a running
service), the retraining dataset (analyst-verified feedback only), and the dashboard API (security-staff-only
login, RBAC, CSRF, incident review -> feedback -> history lake -> retraining dataset, settings, catalog, models)."""
import json
import os
import time

import fakeredis
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from accesssentinel import runtime_settings as rs
from accesssentinel.adapters.hdfs_lake import HistoryLake, LocalFsClient
from accesssentinel.config import PDPConfig, RiskConfig
from accesssentinel.db.models import AclRule, Application, AuditLog, Base, Feedback, Incident, Resource, SettingsChange, User
from accesssentinel.incidents import IncidentStore
from accesssentinel.ml import feedback_data as fbd
from accesssentinel.pdp_engine import PolicyDecisionPoint
from accesssentinel.security import hash_password, verify_password
from accesssentinel.services.api.app import COOKIE, create_app
from accesssentinel.settings import Settings

T0 = 1_794_000_000.0
PW = "unit-test-password-1"
CSRF = {"X-Requested-With": "accesssentinel"}


def engine():
    e = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    Base.metadata.create_all(e)
    return e


def block_rec(u, did="d1", s=0.95, t=T0, enforced=True):
    return {"u": u, "transition": "block", "reason": "individual", "decision_id": did, "s_ind": s, "t": t,
            "enforced": enforced, "basis": "s_ind>=theta_high"}


def group_rec(members, gid="g1", s=0.97, above=True, sid="coll:1", t=T0):
    return {"u": members[0], "members": members, "group_id": gid, "s_coll": s, "theta_coll": 0.9,
            "above_theta_coll": above, "score_id": sid, "t": t, "resources": ["fin:ledger"], "paths": ["resource"]}


# ------------------------------------------------------------------------------------------ incidents
def test_individual_block_opens_one_incident_and_redelivery_is_idempotent():
    e = engine(); st = IncidentStore(e, clock=lambda: T0)
    a = st.on_decision(block_rec("ivan")); b = st.on_decision(block_rec("ivan"))
    assert a == b
    with Session(e) as db:
        inc = db.scalars(select(Incident)).one()
        assert (inc.kind, inc.severity, inc.status, inc.users, inc.source) == ("individual", "high", "open", ["ivan"], "live")
        assert len(inc.evidence) == 1 and inc.enforcement == {"ivan": "blocked"}
    st.on_decision({"u": "ivan", "transition": "release", "reason": "analyst_release", "decision_id": "d2",
                    "t": T0 + 60, "actor": "analyst1"})
    with Session(e) as db:
        inc = db.scalars(select(Incident)).one()
        assert inc.enforcement["ivan"] == "released by analyst1" and inc.last_seen == T0   # release is not activity


def test_incident_window_uses_event_time_not_decision_time():
    e = engine(); st = IncidentStore(e, clock=lambda: T0)
    old = T0 - 39 * 86400                                   # replayed / backlogged accesses scored much later
    r = block_rec("ivan"); r["basis"] = {"event_t": old}
    iid = st.on_decision(r)
    g = group_rec(["a", "b"]); g["window"] = [old - 3600, old]; g["anchor_t"] = old
    gid = st.on_group_score(g)
    with Session(e) as db:
        assert (db.get(Incident, iid).first_seen, db.get(Incident, iid).last_seen) == (old, old)
        assert (db.get(Incident, gid).first_seen, db.get(Incident, gid).last_seen) == (old - 3600, old)


def test_pdp_records_the_event_time_of_its_evidence():
    p = PolicyDecisionPoint(fakeredis.FakeRedis(), PDPConfig(), RiskConfig(), clock=lambda: T0)
    out = p.on_risk_score({"u": "x", "s_ind": 0.95, "score_id": "s1", "scored_at": T0, "t": T0 - 5, "trigger": "event"})
    assert out[0]["basis"]["event_t"] == T0 - 5
    out = p.on_group_score({"u": "y", "s_coll": 0.97, "theta_coll": 0.9, "members": ["y"], "group_id": "g", "t": T0,
                            "score_id": "c1", "window": [T0 - 900, T0 - 1]})
    assert out[0]["basis"]["event_window"] == [T0 - 900, T0 - 1]


def test_medium_severity_shadow_and_synthetic_labelling():
    e = engine(); st = IncidentStore(e, clock=lambda: T0)
    st.on_decision(block_rec("syn-u1", s=0.7, enforced=False))
    with Session(e) as db:
        inc = db.scalars(select(Incident)).one()
        assert inc.severity == "medium" and inc.source == "synthetic-replay"
        assert inc.enforcement["syn-u1"] == "shadow (not enforced)"


def test_group_scores_create_collective_incident_only_above_theta():
    e = engine(); st = IncidentStore(e, clock=lambda: T0)
    assert st.on_group_score(group_rec(["a", "b"], above=False)) is None
    iid = st.on_group_score(group_rec(["a", "b"]))
    st.on_decision({"u": "a", "transition": "block", "reason": "collective", "group_id": "g1", "decision_id": "d9",
                    "s_coll": 0.97, "t": T0 + 1, "enforced": True})
    st.on_group_score(group_rec(["a", "b", "c"], s=0.99, sid="coll:2", t=T0 + 5))
    with Session(e) as db:
        inc = db.get(Incident, iid)
        assert inc.kind == "collective" and inc.severity == "critical" and inc.users == ["a", "b", "c"]
        assert inc.s_coll == 0.99 and len(inc.evidence) == 3 and inc.enforcement["a"] == "blocked"
        assert inc.enforcement["c"] == "pending PDP decision" and "3 user" in inc.title


def test_rescored_groups_and_repeat_blocks_merge_into_the_open_case():
    e = engine(); st = IncidentStore(e, clock=lambda: T0)
    a = st.on_group_score(group_rec(["a", "b"], gid="g1"))
    assert st.on_group_score(group_rec(["a", "b", "c"], gid="g2", sid="coll:2", t=T0 + 600)) == a   # same group re-scored
    assert st.on_decision({"u": "c", "transition": "block", "reason": "collective", "group_id": "g2", "decision_id": "dc",
                           "t": T0 + 601, "enforced": True}) == a
    other = st.on_group_score(group_rec(["x", "y"], gid="g3", sid="coll:3"))
    later = st.on_group_score(group_rec(["a", "b"], gid="g4", sid="coll:4", t=T0 + 3 * 86400))
    assert len({a, other, later}) == 3                       # disjoint group / long after: separate cases
    i1 = st.on_decision(block_rec("ivan", did="d1", s=0.7))
    assert st.on_decision(block_rec("ivan", did="d2", s=0.95, t=T0 + 50)) == i1
    with Session(e) as db:
        inc = db.get(Incident, i1)
        assert len(inc.evidence) == 2 and inc.severity == "high" and inc.max_s_ind == 0.95
        inc.status = "closed"; db.commit()
    assert st.on_decision(block_rec("ivan", did="d3", t=T0 + 99)) != i1     # reviewed case stays closed


# ------------------------------------------------------------------------------------------ runtime settings
def test_settings_validation():
    cur = rs.effective(fakeredis.FakeRedis())
    assert cur["risk.theta_high"] == RiskConfig().theta_high and cur["pdp.theta_coll"] is None
    for bad in [{"nope": 1}, {"risk.theta_high": 1.5}, {"risk.theta_low": 0.7}, {"escalation.kappa": "x"},
                {"risk.theta_high": None}]:
        with pytest.raises(rs.SettingsError):
            rs.validate(bad, cur)
    assert rs.validate({"escalation.kappa": "7", "pdp.theta_coll": ""}, cur) == {"escalation.kappa": 7, "pdp.theta_coll": None}


def test_settings_propagate_into_a_running_pdp_service():
    from accesssentinel.streaming.pdp_service import PdpHandler
    r = fakeredis.FakeRedis()
    p = PolicyDecisionPoint(r, PDPConfig(), RiskConfig(), clock=lambda: T0)

    class Prod:
        def produce(self, *a, **k): pass
        def flush(self, t=None): return 0
    h = PdpHandler(p, Prod(), "pdp-decisions", None, None, poll_seconds=15, clock=lambda: T0)
    t = [0.0]
    h.settings = rs.Watcher(r, every=5, clock=lambda: t[0])
    h.apply_settings()                                       # version 0: defaults
    assert p.rc.theta_high == RiskConfig().theta_high
    rs.write(r, rs.validate({"risk.theta_high": 0.8, "pdp.theta_coll": 0.85, "pdp.block_ttl": 600}, rs.effective(r)))
    h.apply_settings()                                       # throttled: not re-read within `every`
    assert p.rc.theta_high == RiskConfig().theta_high
    t[0] = 6.0
    h.apply_settings()
    assert (p.rc.theta_high, p.theta_override, p.cfg.block_ttl) == (0.8, 0.85, 600.0)
    t[0] = 12.0
    assert h.settings.poll() is None                         # unchanged version: nothing to apply


# ------------------------------------------------------------------------------------------ retraining dataset
def lake(tmp_path):
    return HistoryLake(LocalFsClient(str(tmp_path / "lake")), "/as")


def fb_row(fid, verdict, analyst="analyst1", users=("ivan",), created=T0, scope="individual"):
    p = {"id": fid, "verdict": verdict, "analyst": analyst, "users": list(users), "scope": scope,
         "created_at": created, "window_t0": created - 900, "window_t1": created + 300}
    return {"record_id": f"{fid}:{created}", "ts": created, "payload": p}


def test_only_analyst_verified_verdicts_enter_the_training_set(tmp_path):
    lk = lake(tmp_path)
    lk.write_records("analyst-feedback", [
        fb_row("f1", "confirmed_malicious"), fb_row("f2", "false_positive"), fb_row("f3", "needs_more_info"),
        fb_row("f4", "confirmed_malicious", analyst=""), fb_row("f5", "model_said_so"),
        fb_row("f6", "benign_explained", users=()), fb_row("f2", "confirmed_malicious", created=T0 + 10)])
    rows = fbd.verified_feedback(lk, T0 - 10, T0 + 100)
    assert [(r["id"], r["verdict"]) for r in rows] == [("f1", "confirmed_malicious"), ("f2", "confirmed_malicious")]
    assert fbd.summary(rows)["by_scope_verdict"] == {"individual:confirmed_malicious": 2}
    # model outputs living in other datasets are never read
    lk.write_records("risk-scores", [{"record_id": "s1", "ts": T0, "payload": {"u": "x", "verdict": "confirmed_malicious"}}])
    assert len(fbd.verified_feedback(lk, T0 - 10, T0 + 100)) == 2


# ------------------------------------------------------------------------------------------ dashboard API
class FakeLive:
    def __init__(self): self.items = {"event": [], "score": [], "escalation": [], "group": [], "decision": []}
    def recent(self, kind, limit=200, pred=None): return [x for x in self.items[kind] if pred is None or pred(x)][:limit]
    def rate_history(self): return [{"t": T0, "access-events": 2.5}]
    def subscribe(self):
        import asyncio
        return asyncio.Queue()
    def unsubscribe(self, q): pass


class FakeProbe:
    def snapshot(self): return {"health": {"redis": "ok"}, "lag": {}}


@pytest.fixture
def api(tmp_path):
    e = engine()
    with Session(e) as db, db.begin():
        h = hash_password(PW)
        db.add_all([User(id="analyst1", display_name="A", role="security_analyst", password_hash=h),
                    User(id="secadmin", display_name="S", role="security_admin", password_hash=h),
                    User(id="alice", display_name="Alice", role="staff", password_hash=h),
                    Application(id="hr", name="HR", base_url="http://hr:8000")])
        db.flush()
        db.add(Resource(app_id="hr", resource_key="payroll", sensitivity=1))
        db.add_all([AclRule(app_id="hr", resource_key="directory", role="staff", permission="read"),
                    AclRule(app_id="hr", resource_key="payroll", role="hr_manager", permission="read")])
    srv = fakeredis.FakeServer()
    sync = fakeredis.FakeRedis(server=srv)
    aio = fakeredis.aioredis.FakeRedis(server=srv, decode_responses=True)
    lk = lake(tmp_path)
    published = []

    def publish(payload):                                    # stands in for Kafka analyst-feedback -> HDFS sink
        published.append(payload)
        lk.write_records("analyst-feedback", [{"record_id": payload["id"], "ts": payload["t"], "payload": payload}])
    models = tmp_path / "models"
    for kind, v in [("tcn", "tcn-a"), ("tcn", "tcn-b"), ("tgn", "tgn-a")]:
        os.makedirs(models / kind / v)
        (models / kind / v / "manifest.json").write_text(json.dumps({"version": v, "created_at": T0}))
    (models / "tcn" / "CURRENT").write_text("tcn-a")
    pdp = PolicyDecisionPoint(sync, PDPConfig(), RiskConfig())
    s = Settings(_env_file=None, upstream_token="u" * 32, internal_token="i" * 32)
    revoked = []
    app = create_app(s, e, aio, sync, FakeLive(), FakeProbe(), lambda: lk, publish, str(models), lambda: pdp,
                     revoke_sessions=lambda u, reason: (revoked.append((u, reason)), 2)[1])
    with TestClient(app) as c:
        c.engine, c.lake, c.published, c.pdp, c.redis, c.models = e, lk, published, pdp, sync, models
        c.revoked = revoked
        yield c


def login(c, u="analyst1", pw=PW):
    return c.post("/api/auth/login", json={"username": u, "password": pw}, headers=CSRF)


def test_only_security_staff_can_log_in_and_cookie_is_hardened(api):
    assert api.get("/api/overview").status_code == 401
    assert login(api, "alice").status_code == 401                         # monitored user: no dashboard access
    assert login(api, pw="wrong-password").status_code == 401
    r = login(api)
    assert r.status_code == 200 and r.json()["role"] == "security_analyst"
    sc = r.headers["set-cookie"].lower()
    assert COOKIE in sc and "httponly" in sc and "samesite=strict" in sc
    assert api.get("/api/auth/me").json()["user"] == "analyst1"
    assert api.get("/api/overview").status_code == 200
    with Session(api.engine) as db:
        acts = [a.action for a in db.scalars(select(AuditLog))]
    assert acts.count("dashboard_login_failed") == 2 and "dashboard_login" in acts
    assert PW not in json.dumps([a.detail for a in Session(api.engine).scalars(select(AuditLog))])


def test_password_typed_as_user_id_is_never_stored(api):
    secret = "Xq7pLm2vR9tZk4Wd"                              # pasted into the User ID field by mistake
    assert login(api, secret, "whatever").status_code == 401
    with Session(api.engine) as db:
        rows = [(a.actor, json.dumps(a.detail)) for a in db.scalars(select(AuditLog))]
    assert ("unknown-user", "{}") in rows and not any(secret in r[0] or secret in r[1] for r in rows)
    assert not any(secret in (k.decode() if isinstance(k, bytes) else k) for k in api.redis.keys("*"))


def test_login_lockout(api):
    for _ in range(5):
        login(api, pw="wrong-password")
    assert login(api).status_code == 429


def test_csrf_header_required_on_mutations(api):
    login(api)
    r = api.post("/api/auth/logout")                                        # no X-Requested-With
    assert r.status_code == 403
    assert api.post("/api/auth/logout", headers=CSRF).status_code == 200
    assert api.get("/api/auth/me").status_code == 401


def test_analyst_reviews_incident_feedback_reaches_lake_and_retraining_dataset(api):
    iid = IncidentStore(api.engine).on_decision(block_rec("ivan"))
    login(api)
    assert api.get("/api/incidents").json()["incidents"][0]["id"] == iid
    assert api.get(f"/api/incidents/{iid}").json()["evidence"][0]["kind"] == "pdp_block"
    assert api.post(f"/api/incidents/{iid}/feedback", json={"verdict": "guilty"}, headers=CSRF).status_code == 400
    r = api.post(f"/api/incidents/{iid}/feedback", json={"verdict": "confirmed_malicious", "note": "exfil"}, headers=CSRF)
    assert r.status_code == 200
    fb = r.json()["feedback"]
    assert (fb["analyst"], fb["users"], fb["scope"]) == ("analyst1", ["ivan"], "individual")
    assert fb["window_t0"] == T0 - 900 and fb["window_t1"] == T0 + 300
    inc = api.get(f"/api/incidents/{iid}").json()
    assert inc["status"] == "closed" and inc["verdict"] == "confirmed_malicious" and len(inc["feedback"]) == 1
    ds = api.get("/api/retraining/dataset").json()
    assert ds["feedback"] == 1 and ds["feedback_ids"] == [fb["id"]]
    with Session(api.engine) as db:
        assert db.scalars(select(Feedback)).one().verdict == "confirmed_malicious"


def test_needs_more_info_is_recorded_but_not_a_training_label(api):
    iid = IncidentStore(api.engine).on_decision(block_rec("ivan"))
    login(api)
    api.post(f"/api/incidents/{iid}/feedback", json={"verdict": "needs_more_info"}, headers=CSRF)
    assert api.get(f"/api/incidents/{iid}").json()["status"] == "triaged"
    assert len(api.published) == 1 and api.get("/api/retraining/dataset").json()["feedback"] == 0


def test_false_positive_with_release_unblocks_the_user(api):
    api.pdp.block_manual("bob", "pdp-test")
    iid = IncidentStore(api.engine).on_decision(block_rec("bob"))
    login(api)
    assert api.get("/api/users/bob/decision").json()["blocked"] is True
    r = api.post(f"/api/incidents/{iid}/feedback", json={"verdict": "false_positive", "release": True}, headers=CSRF)
    assert r.json()["released"] == ["bob"] and api.pdp.status("bob")["blocked"] is False


def test_missed_detection_feedback(api):
    login(api)
    bad = api.post("/api/feedback/missed", json={"users": ["x"], "window_t0": T0, "window_t1": T0 - 1}, headers=CSRF)
    assert bad.status_code == 400
    r = api.post("/api/feedback/missed", json={"users": ["syn-u9", "syn-u8"], "scope": "collective",
                                                "window_t0": T0, "window_t1": T0 + 3600}, headers=CSRF)
    p = r.json()["feedback"]
    assert p["kind"] == "missed_detection" and p["verdict"] == "confirmed_malicious" and p["source"] == "synthetic-replay"


def test_settings_are_admin_only_validated_and_versioned(api):
    login(api)
    assert api.get("/api/settings").status_code == 200
    assert api.put("/api/settings", json={"escalation.gamma": 0.1}, headers=CSRF).status_code == 403
    api.post("/api/auth/logout", headers=CSRF)
    login(api, "secadmin")
    assert api.put("/api/settings", json={"risk.theta_low": 0.9}, headers=CSRF).status_code == 400
    r = api.put("/api/settings", json={"escalation.gamma": 0.1}, headers=CSRF)
    assert r.status_code == 200 and rs.effective(api.redis)["escalation.gamma"] == 0.1
    rows = {x["key"]: x for x in api.get("/api/settings").json()["settings"]}
    assert rows["escalation.gamma"]["explicit"] and not rows["escalation.kappa"]["explicit"]
    hist = api.get("/api/settings/history").json()["history"]
    assert hist[0]["key"] == "escalation.gamma" and hist[0]["new"] == 0.1 and hist[0]["changed_by"] == "secadmin"


def test_catalog_and_model_management_are_admin_only(api):
    login(api)
    assert api.put("/api/catalog/resources/hr/payroll", json={"sensitivity": 0}, headers=CSRF).status_code == 403
    assert api.post("/api/models/tcn/promote", json={"version": "tcn-b"}, headers=CSRF).status_code == 403
    api.post("/api/auth/logout", headers=CSRF)
    login(api, "secadmin")
    assert api.put("/api/catalog/resources/hr/payroll", json={"sensitivity": 0}, headers=CSRF).status_code == 200
    assert api.put("/api/catalog/resources/hr/nope", json={"sensitivity": 0}, headers=CSRF).status_code == 404
    bad = api.post("/api/catalog/apps", json={"id": "Bad App", "name": "x", "base_url": "javascript:alert(1)"}, headers=CSRF)
    assert bad.status_code == 400
    ok = api.post("/api/catalog/apps", json={"id": "crm", "name": "CRM", "base_url": "http://crm:8000",
                                             "resources": [{"key": "accounts", "sensitivity": 1}]}, headers=CSRF)
    assert ok.status_code == 200
    assert api.post("/api/catalog/apps", json={"id": "crm", "name": "CRM", "base_url": "http://crm:8000"}, headers=CSRF).status_code == 409
    cat = api.get("/api/catalog").json()
    assert {"app": "hr", "resource": "payroll", "sensitivity": 0, "description": ""} in cat["resources"]
    assert any(a["id"] == "crm" for a in cat["applications"])
    m = api.get("/api/models").json()
    assert m["tcn"]["current"] == "tcn-a" and len(m["tcn"]["versions"]) == 2
    assert api.post("/api/models/tcn/promote", json={"version": "tcn-zzz"}, headers=CSRF).status_code == 404
    assert api.post("/api/models/tcn/promote", json={"version": "tcn-b"}, headers=CSRF).status_code == 200
    assert (api.models / "tcn" / "CURRENT").read_text() == "tcn-b"


def test_live_websocket_requires_a_dashboard_session(api):
    from starlette.websockets import WebSocketDisconnect
    with pytest.raises(WebSocketDisconnect):
        with api.websocket_connect("/ws/live") as ws:
            ws.receive_json()


def test_system_and_overview_report_backend_state(api):
    login(api)
    api.redis.hset("risk:ivan", mapping={"s_ind": 0.95, "tier": "high", "t": T0})
    api.redis.hset("risk:syn-u1", mapping={"s_ind": 0.2, "tier": "low", "t": T0})
    ov = api.get("/api/overview").json()
    assert ov["monitored_users"] == {"live": 1, "synthetic-replay": 1} and ov["events_per_second"] == 2.5
    assert ov["tiers"] == {"live:high": 1, "synthetic-replay:low": 1}
    assert [u["u"] for u in api.get("/api/risk/users").json()["users"]] == ["ivan", "syn-u1"]
    sysj = api.get("/api/system").json()
    assert sysj["health"]["redis"] == "ok" and sysj["tgn_latency_ms"]["n"] == 0
    assert api.get("/api/risk/users/..%2Fetc").status_code in (400, 404)


# ------------------------------------------------------------------------------------------ user accounts (admin)
def test_only_admins_provision_monitored_accounts(api):
    login(api)
    assert api.get("/api/accounts").status_code == 200                      # analysts can view
    assert api.post("/api/accounts", json={"id": "mallory", "name": "M", "role": "staff"}, headers=CSRF).status_code == 403
    api.post("/api/auth/logout", headers=CSRF)
    login(api, "secadmin")
    r = api.post("/api/accounts", json={"id": "mallory", "name": "Mallory Test", "department": "Sales", "role": "staff"},
                 headers=CSRF)
    assert r.status_code == 200
    pw = r.json()["initial_password"]
    assert pw and len(pw) >= 16
    with Session(api.engine) as db:
        u = db.get(User, "mallory")
        assert u.role == "staff" and u.is_active and verify_password(u.password_hash, pw)
        rows = [json.dumps(a.detail) for a in db.scalars(select(AuditLog).where(AuditLog.action == "user_created"))]
    assert rows and pw not in rows[0] and "generated" in rows[0]                # never the password itself
    listed = {u["id"]: u for u in api.get("/api/accounts").json()["users"]}
    assert "mallory" in listed and "secadmin" not in listed and "analyst1" not in listed
    assert api.get("/api/accounts").json()["roles"] == ["hr_manager", "staff"]


def test_account_validation_and_security_roles_are_protected(api):
    login(api, "secadmin")
    bad = [{"id": "Mallory", "name": "x", "role": "staff"}, {"id": "ab", "name": "x", "role": "staff"},
           {"id": "eve", "name": "", "role": "staff"}, {"id": "eve", "name": "Eve", "role": "security_admin"},
           {"id": "eve", "name": "Eve", "role": "no-such-role"}, {"id": "eve", "name": "Eve", "role": "staff", "password": "short"}]
    for b in bad:
        assert api.post("/api/accounts", json=b, headers=CSRF).status_code == 400, b
    ok = api.post("/api/accounts", json={"id": "eve", "name": "Eve", "role": "staff", "password": "a-chosen-password-1"},
                  headers=CSRF)
    assert ok.status_code == 200 and ok.json()["initial_password"] is None   # admin-chosen: not echoed back
    assert api.post("/api/accounts", json={"id": "alice", "name": "A", "role": "staff"}, headers=CSRF).status_code == 409
    for target in ("secadmin", "analyst1", "nobody"):
        assert api.post(f"/api/accounts/{target}/deactivate", json={}, headers=CSRF).status_code == 404


def test_deactivate_and_reset_end_open_sessions(api):
    login(api, "secadmin")
    r = api.post("/api/accounts/alice/deactivate", json={}, headers=CSRF)
    assert r.status_code == 200 and r.json()["sessions_ended"] == 2 and api.revoked[-1][0] == "alice"
    with Session(api.engine) as db:
        assert db.get(User, "alice").is_active is False
    assert api.post("/api/accounts/alice/activate", json={}, headers=CSRF).status_code == 200
    r = api.post("/api/accounts/alice/reset-password", json={}, headers=CSRF)
    pw = r.json()["initial_password"]
    with Session(api.engine) as db:
        a = db.get(User, "alice")
        assert a.is_active and verify_password(a.password_hash, pw) and not verify_password(a.password_hash, PW)
    assert api.revoked[-1] == ("alice", "password reset")

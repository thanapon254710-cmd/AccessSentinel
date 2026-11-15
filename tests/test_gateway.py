"""Phase 3 tests: request mapping, ACL, event schema, sessions, PDP decision cache, revocation, the gateway's
full request path against the real demo applications (in-process), and the non-blocking event publisher."""
import json
import time
from functools import partial

import fakeredis
import httpx
import pytest
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError

from accesssentinel.adapters.event_publisher import EventPublisher
from accesssentinel.db import demo_catalog as demo
from accesssentinel.schema import AccessEventV1, make_event
from accesssentinel.security import hash_password, one_way_ref, tokens_equal, verify_password
from accesssentinel.services.demo_app import create_demo_app
from accesssentinel.services.gateway.access import AppInfo, BadRequest, Catalog, MethodNotAllowed, resolve
from accesssentinel.services.gateway.app import COOKIE, create_app
from accesssentinel.services.gateway.store import MemoryGatewayStore, UserRecord
from accesssentinel.settings import Settings

PASSWORD = "unit-test-password-1"


def demo_catalog() -> Catalog:
    apps = {a: AppInfo(a, n, u, d) for a, (n, u, d) in demo.APPS.items()}
    sens = {(a, r): s for a, rs in demo.RESOURCES.items() for r, s, _ in rs}
    desc = {(a, r): d for a, rs in demo.RESOURCES.items() for r, _, d in rs}
    acl = {}
    for a, r, role, p in demo.acl_rows():
        acl.setdefault((a, r, role), set()).add(p)
    return Catalog(apps, sens, desc, {k: frozenset(v) for k, v in acl.items()})


CAT = demo_catalog()


# ------------------------------------------------------------------------------------------ pure logic
@pytest.mark.parametrize("method,path,op,perm,obj", [
    ("GET", "payroll", "view", "read", None), ("GET", "payroll/7", "view", "read", "7"),
    ("GET", "payroll/export", "export", "export", None), ("POST", "payroll", "edit", "write", None),
    ("PUT", "payroll/7", "edit", "write", "7"), ("PATCH", "payroll/7", "edit", "write", "7"),
    ("DELETE", "payroll/7", "delete", "admin", "7")])
def test_request_mapping(method, path, op, perm, obj):
    t = resolve(CAT, "hr", path, method)
    assert (t.resource, t.known, t.operation, t.permission, t.object_id) == ("payroll", True, op, perm, obj)


def test_unknown_and_invalid_paths():
    t = resolve(CAT, "hr", "secrets", "GET")
    assert t.resource == "unmapped:secrets" and not t.known
    for bad in ["", "a/b/c/d", "payroll/..", "pay roll", "payroll/7/x"]:
        with pytest.raises(BadRequest):
            resolve(CAT, "hr", bad, "GET")
    for m, p in [("DELETE", "payroll"), ("POST", "payroll/7"), ("PUT", "payroll")]:
        with pytest.raises(MethodNotAllowed):
            resolve(CAT, "hr", p, m)


def test_demo_policy_is_default_deny_and_role_based():
    assert CAT.allowed("hr", "directory", "staff", "read")
    assert not CAT.allowed("hr", "payroll", "staff", "read")
    assert CAT.allowed("hr", "payroll", "hr_manager", "export")
    assert CAT.allowed("hr", "payroll", "auditor", "read") and not CAT.allowed("hr", "payroll", "auditor", "write")
    assert not CAT.allowed("hr", "payroll", "no-such-role", "read")
    assert {s for s in CAT.sensitivity.values()} == {0, 1}
    assert {role for _, _, role, _ in demo.USERS} <= set(demo.ROLES)       # every demo user has a defined role


def test_event_schema_v2_and_v1_compatibility():
    ev = make_event("alice", "hr", "payroll", 1, "export", True, {"ip": "10.0.0.1"}, "a" * 32, {"reason": "allow"},
                    seq=7, stream="gw1:abcd", tenant="acme", v_policy="0123456789ab")
    assert ev.p == "export" and ev.y == 1 and ev.schema_version == 2 and len(ev.event_id) == 32
    assert (ev.seq, ev.stream, ev.rstar, ev.v_policy) == (7, "gw1:abcd", "acme/hr:payroll", "0123456789ab")
    AccessEventV1(**ev.__dict__)
    v1 = dict(ev.__dict__, schema_version=1)                  # D3 records (Kafka history, HDFS, synthetic) still load
    for k in ("seq", "stream", "tenant", "ti", "v_policy"):
        v1.pop(k)
    assert AccessEventV1(**v1).to_event().rstar == "default/hr:payroll"
    with pytest.raises(ValueError):
        AccessEventV1(**dict(ev.__dict__, schema_version=3))
    with pytest.raises(ValueError):
        AccessEventV1(**dict(ev.__dict__, tenant="bad tenant!"))
    with pytest.raises(ValueError):
        make_event("alice", "hr", "payroll", 1, "steal", True, {}, "a" * 32)
    with pytest.raises(ValueError):
        make_event("bad user!", "hr", "payroll", 1, "view", True, {}, "a" * 32)


def test_password_and_token_helpers():
    h = hash_password(PASSWORD)
    assert PASSWORD not in h and verify_password(h, PASSWORD) and not verify_password(h, "wrong-password")
    assert not verify_password(None, PASSWORD) and not verify_password("not-a-hash", PASSWORD)
    with pytest.raises(ValueError):
        hash_password("short")
    assert tokens_equal("abc", "abc") and not tokens_equal("abc", "abd") and not tokens_equal(None, "x")
    assert one_way_ref("x") != "x" and len(one_way_ref("x", 12)) == 12


# ------------------------------------------------------------------------------------------ gateway
class RecordingPublisher:
    def __init__(self): self.events = []
    def publish(self, ev): self.events.append(ev)
    def stats(self): return {"submitted": len(self.events)}


class HostRouter(httpx.AsyncBaseTransport):
    """Routes http://app-<id>:8000 to the in-process demo app, counting upstream calls."""
    def __init__(self, apps): self.apps, self.calls = {h: httpx.ASGITransport(app=a) for h, a in apps.items()}, []
    async def handle_async_request(self, request):
        self.calls.append((request.method, request.url.host, request.url.path, dict(request.headers)))
        return await self.apps[request.url.host].handle_async_request(request)


@pytest.fixture
def gw():
    settings = Settings(_env_file=None, upstream_token="u" * 32, internal_token="i" * 32,
                        login_max_failures=3, catalog_refresh_seconds=3600)
    users = {u: UserRecord(u, name, role, dept, hash_password(PASSWORD), True) for u, name, role, dept in demo.USERS}
    store = MemoryGatewayStore(CAT, users)
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    pub = RecordingPublisher()
    router = HostRouter({f"app-{a}": create_demo_app(a, settings) for a in demo.APPS})
    app = create_app(settings, store, redis, pub, httpx.AsyncClient(transport=router))
    with TestClient(app) as client:
        yield type("GW", (), dict(client=client, pub=pub, store=store, redis=redis, router=router,
                                  settings=settings))


def login(c, user, password=PASSWORD):
    return c.post("/auth/login", json={"username": user, "password": password})


def test_login_failure_lockout_and_audit(gw):
    c = gw.client
    assert login(c, "carol", "wrong-password").status_code == 401
    assert login(c, "nobody", "wrong-password").status_code == 401
    for _ in range(3):
        login(c, "dave", "wrong-password")
    r = login(c, "dave")                       # correct password, but locked after 3 failures
    assert r.status_code == 429 and COOKIE not in r.cookies
    actions = [a["action"] for a in gw.store.audit_rows]
    assert actions.count("login_failed") == 5 and "login_locked" in actions
    assert all(PASSWORD not in json.dumps(a) for a in gw.store.audit_rows)


def test_unauthenticated_requests_are_rejected_without_events(gw):
    r = gw.client.get("/apps/hr/directory")
    assert r.status_code == 401 and gw.pub.events == [] and gw.router.calls == []


def test_allowed_request_is_forwarded_and_recorded(gw):
    c = gw.client
    assert login(c, "alice").status_code == 200
    r1 = c.get("/apps/hr/payroll")
    r2 = c.get("/apps/hr/payroll/3")
    assert r1.status_code == 200 and r1.json()["viewer"] == "alice" and r1.json()["count"] == 12
    assert r2.status_code == 200 and r2.json()["id"] == "3"
    e1, e2 = gw.pub.events
    assert (e1.u, e1.a, e1.r, e1.s, e1.p, e1.o, e1.y) == ("alice", "hr", "payroll", 1, "read", "view", 1)
    assert e1.c["session"] == "new" and e2.c["session"] == "active" and e2.meta["object_id"] == "3"
    assert e1.correlation_id == r1.headers["X-Request-ID"] and e1.event_id != e2.event_id
    assert e1.c["ip"] and e1.c["device"] and len(e1.c["session_ref"]) == 16
    token = c.cookies.get(COOKIE)
    assert token and token not in json.dumps([e.__dict__ for e in gw.pub.events])   # no raw session token
    _, host, path, headers = gw.router.calls[0]
    assert host == "app-hr" and path == "/payroll" and headers["x-as-user"] == "alice"
    assert "cookie" not in headers                                                # cookies not forwarded


def test_acl_denial_is_recorded_and_not_forwarded(gw):
    c = gw.client
    login(c, "carol")
    r = c.get("/apps/hr/payroll")
    assert r.status_code == 403
    (ev,) = gw.pub.events
    assert (ev.y, ev.s, ev.meta["reason"]) == (0, 1, "acl-deny") and gw.router.calls == []
    assert c.get("/apps/hr/directory").status_code == 200 and gw.pub.events[-1].y == 1


def test_export_and_write_operations(gw):
    c = gw.client
    login(c, "alice")
    r = c.get("/apps/hr/payroll/export")
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/csv")
    assert "attachment" in r.headers["content-disposition"]
    r = c.post("/apps/hr/recruiting", json={"position": "Data Engineer", "status": "open"})
    assert r.status_code == 201
    assert c.delete(f"/apps/hr/recruiting/{r.json()['id']}").status_code == 204
    assert [(e.o, e.p, e.y) for e in gw.pub.events] == [("export", "export", 1), ("edit", "write", 1),
                                                         ("delete", "admin", 1)]


def test_unknown_resource_and_application(gw):
    c = gw.client
    login(c, "carol")
    assert c.get("/apps/hr/secret-salaries").status_code == 404
    (ev,) = gw.pub.events
    assert (ev.r, ev.s, ev.y, ev.meta["reason"]) == ("unmapped:secret-salaries", 1, 0, "unknown-resource")
    assert c.get("/apps/nope/x").status_code == 404 and len(gw.pub.events) == 1


def test_pdp_block_is_enforced_at_request_time_and_login(gw):
    c = gw.client
    login(c, "bob")
    assert c.get("/apps/finance/payments").status_code == 200
    c.portal.call(partial(gw.redis.hset, "decision:bob", mapping={"decision": "Block", "reason": "individual",
                                                                 "decided_at": time.time(), "version": 1}))
    r = c.get("/apps/finance/payments")
    assert r.status_code == 403 and gw.pub.events[-1].meta["reason"] == "pdp-block:individual"
    assert gw.pub.events[-1].y == 0
    c.cookies.clear()
    assert login(c, "bob").status_code == 403                 # no new session while blocked
    c.portal.call(gw.redis.hset, "decision:bob", "expires_at", time.time() - 1)   # explicit expiry passed
    assert login(c, "bob").status_code == 200 and c.get("/apps/finance/payments").status_code == 200


def test_session_revocation_terminates_active_session(gw):
    c = gw.client
    login(c, "erin")
    assert c.get("/apps/docs/contracts").status_code == 200
    assert c.post("/internal/sessions/revoke", json={"user": "erin"}).status_code == 403          # no token
    r = c.post("/internal/sessions/revoke", json={"user": "erin", "reason": "test"},
               headers={"X-AS-Internal-Token": "i" * 32})
    assert r.status_code == 200 and r.json()["revoked_sessions"] == 1
    assert c.get("/apps/docs/contracts").status_code == 401
    assert any(a["action"] == "sessions_revoked" for a in gw.store.audit_rows)


def test_logout(gw):
    c = gw.client
    login(c, "ivan")
    assert c.get("/auth/me").json()["user"] == "ivan"
    c.post("/auth/logout", json={})
    assert c.get("/auth/me").status_code == 401


def test_redis_outage_fails_closed(gw, monkeypatch):
    c = gw.client
    login(c, "carol")

    async def down(*a, **k):
        raise RedisConnectionError("down")
    monkeypatch.setattr(gw.redis, "hgetall", down)
    r = c.get("/apps/hr/directory")
    assert r.status_code == 503 and gw.router.calls == []


def test_html_flow(gw):
    c = gw.client
    assert "Sign in" in c.get("/").text
    r = c.post("/auth/login", data={"username": "frank", "password": PASSWORD}, follow_redirects=False)
    assert r.status_code == 303
    home = c.get("/")
    assert "Protected applications" in home.text and "board-minutes" in home.text
    assert home.headers["content-security-policy"] and home.headers["x-frame-options"] == "DENY"
    page = c.get("/apps/docs/board-minutes", headers={"Accept": "text/html"})
    assert page.status_code == 200 and "<table>" in page.text


def test_demo_app_rejects_requests_not_from_gateway():
    s = Settings(_env_file=None, upstream_token="u" * 32)
    with TestClient(create_demo_app("hr", s)) as c:
        assert c.get("/payroll").status_code == 401
        assert c.get("/payroll", headers={"X-AS-Gateway-Token": "wrong"}).status_code == 401
        assert c.get("/payroll", headers={"X-AS-Gateway-Token": "u" * 32}).status_code == 200
        assert c.get("/healthz").status_code == 200


# ------------------------------------------------------------------------------------------ publisher
class _Msg:
    def __init__(self, v): self._v = v
    def value(self): return self._v


class FlakyProducer:
    def __init__(self, mode): self.mode, self.sent = mode, []
    def __call__(self, conf): self.conf = conf; return self
    def produce(self, topic, key, value, on_delivery):
        if self.mode == "buffer-full":
            raise BufferError()
        self.sent.append((topic, key))
        on_delivery("broker down" if self.mode == "fail" else None, _Msg(value))
    def poll(self, t): time.sleep(0.01)
    def flush(self, t=None): pass
    def __len__(self): return 0


@pytest.mark.parametrize("mode,delivered,spooled", [("ok", 1, 0), ("fail", 0, 1), ("buffer-full", 0, 1)])
def test_publisher_never_raises_and_spools_failures(tmp_path, mode, delivered, spooled):
    prod = FlakyProducer(mode)
    pub = EventPublisher("fake:9092", "access-events", str(tmp_path), producer_factory=prod)
    ev = make_event("alice", "hr", "payroll", 1, "view", True, {}, "a" * 32)
    pub.publish(ev)
    st = pub.stats()
    pub.close()
    assert (st["submitted"], st["delivered"], st["spooled"]) == (1, delivered, spooled)
    assert prod.conf["enable.idempotence"] is True and prod.conf["acks"] == "all"
    files = list(tmp_path.iterdir())
    if spooled:
        row = json.loads(files[0].read_text().splitlines()[0])
        assert row["event"]["event_id"] == ev.event_id and row["reason"]
    else:
        assert files == [] and prod.sent == [("access-events", b"alice")]

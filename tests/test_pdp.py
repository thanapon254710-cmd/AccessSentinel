"""Phase 8 tests: decision rule, sticky Block lifecycle (expiry, analyst release + grace, policy), group-wide
enforcement, stale evidence, shadow mode, outbox/audit/revocation in the service, and the gateway reading the
decisions the PDP writes."""
import asyncio
import json
from dataclasses import replace

import fakeredis
import pytest

from accesssentinel.config import PDPConfig, RiskConfig
from accesssentinel.pdp_engine import K_BLOCKED, K_OUTBOX, PolicyDecisionPoint, PolicyLocked
from accesssentinel.services.gateway.decisions import active_block
from accesssentinel.streaming.consumer import Record
from accesssentinel.streaming.pdp_service import K_REVOKE, PdpHandler

T0 = 1_794_000_000.0


class Clock:
    def __init__(self, t=T0): self.t = t
    def __call__(self): return self.t


def pdp(clock, mode="enforce", server=None, **cfg):
    r = fakeredis.FakeRedis(server=server) if server else fakeredis.FakeRedis()
    return PolicyDecisionPoint(r, replace(PDPConfig(), **cfg), RiskConfig(), mode=mode, clock=clock)


def risk(u, s, t=T0, sid="s1"):
    return {"u": u, "s_ind": s, "score_id": sid, "scored_at": t, "t": t, "trigger": "event", "model_version": "tcn-v"}


def group(members, s, theta=0.9, t=T0):
    return {"u": members[0], "s_coll": s, "theta_coll": theta, "members": members, "group_id": "g1", "t": t,
            "score_id": "coll:e1", "model_version": "tgn-v"}


# ------------------------------------------------------------------------------------------ decision rule + stickiness
def test_individual_block_is_written_for_the_gateway_and_outboxed():
    c = Clock(); p = pdp(c)
    assert p.on_risk_score(risk("u", 0.59)) == []
    (rec,) = p.on_risk_score(risk("u", 0.83))
    assert (rec["decision"], rec["reason"], rec["enforced"]) == ("Block", "individual", True)
    st = p.status("u")
    assert st["blocked"] and st["reason"] == "individual" and float(st["expires_at"]) == pytest.approx(T0 + 86400)
    assert json.loads(p.r.lindex(K_OUTBOX, 0))["decision_id"] == rec["decision_id"]
    assert p.r.zscore(K_BLOCKED, "u") == pytest.approx(T0 + 86400)


def test_blocks_are_sticky_lower_or_missing_scores_never_release():
    """Audit defect D3."""
    c = Clock(); p = pdp(c)
    p.on_risk_score(risk("u", 0.9))
    for s in (0.1, 0.0, 0.3):
        c.t += 60
        assert p.on_risk_score(risk("u", s, t=c.t)) == []
    c.t += 3600                                           # an hour with no scores at all (ML unavailable)
    assert p.status("u")["blocked"] and p.expire_due() == []
    assert p.on_risk_score(risk("u", 0.95, t=c.t)) == []  # already blocked: no duplicate decision
    assert p.r.llen(K_OUTBOX) == 1


def test_stale_evidence_is_not_enforced():
    c = Clock(T0 + 3600); p = pdp(c)
    assert p.on_risk_score(risk("u", 0.99, t=T0)) == []   # scored an hour ago (e.g. backlog after a restart)
    assert p.on_group_score(group(["a", "b"], 0.99, t=T0)) == []
    assert not p.status("u")["blocked"]


def test_new_kind_of_evidence_on_a_blocked_user_is_recorded_not_dropped():
    """Phase 8 live finding: colluders already blocked individually silently dropped the collective evidence."""
    c = Clock(); p = pdp(c)
    p.on_risk_score(risk("a", 0.9))
    exp = p.status("a")["expires_at"]
    c.t += 60
    out = p.on_group_score(group(["a", "b"], 0.97, t=c.t))
    ev = {r["u"]: r for r in out}
    assert ev["a"]["transition"] == "evidence" and ev["a"]["reason"] == "collective" and ev["a"]["block_reason"] == "individual"
    assert ev["b"]["transition"] == "block" and ev["b"]["reason"] == "collective"
    st = p.status("a")
    assert st["reason"] == "individual" and st["expires_at"] == exp and json.loads(st["also"]) == ["collective"]
    assert p.on_group_score(group(["a", "b"], 0.98, t=c.t)) == []          # same evidence kind again: nothing new


def test_group_wide_enforcement_uses_the_calibrated_theta():
    c = Clock(); p = pdp(c)
    assert p.on_group_score(group(["a", "b", "c"], 0.85, theta=0.9)) == []
    out = p.on_group_score(group(["a", "b", "c"], 0.95, theta=0.9))
    assert [r["u"] for r in out] == ["a", "b", "c"] and all(r["reason"] == "collective" and r["group_id"] == "g1" for r in out)
    assert out[0]["basis"]["members"] == ["a", "b", "c"] and out[0]["s_coll"] == 0.95
    p2 = PolicyDecisionPoint(fakeredis.FakeRedis(), PDPConfig(), RiskConfig(), theta_coll=0.8, clock=c)
    assert len(p2.on_group_score(group(["a", "b"], 0.85, theta=0.9))) == 2     # operator override of theta


# ------------------------------------------------------------------------------------------ lifecycle
def test_expiry_ends_a_block_both_at_the_gateway_and_in_the_pdp():
    srv = fakeredis.FakeServer()
    c = Clock(); p = pdp(c, server=srv, block_ttl=600)
    p.on_risk_score(risk("u", 0.9))
    aio = fakeredis.aioredis.FakeRedis(server=srv, decode_responses=True)
    assert asyncio.run(active_block(aio, "u", now=T0 + 10))["reason"] == "individual"
    assert asyncio.run(active_block(aio, "u", now=T0 + 601)) is None     # gateway honours expires_at by itself
    c.t += 601
    (rec,) = p.expire_due()
    assert (rec["decision"], rec["transition"], rec["previous"]["reason"]) == ("Allow", "expire", "individual")
    assert p.r.zscore(K_BLOCKED, "u") is None


def test_analyst_release_with_grace_period():
    c = Clock(); p = pdp(c, release_grace=900)
    p.on_risk_score(risk("u", 0.9))
    (rec,) = p.release("u", actor="analyst1", note="false positive: month-end export")
    assert (rec["decision"], rec["reason"], rec["actor"]) == ("Allow", "analyst_release", "analyst1")
    c.t += 60
    assert p.on_risk_score(risk("u", 0.95, t=c.t)) == []                 # same ongoing evidence: no re-block
    c.t += 900
    assert p.on_risk_score(risk("u", 0.95, t=c.t))[0]["decision"] == "Block"


def test_policy_blocks_need_the_policy_lifted():
    c = Clock(); p = pdp(c)
    p.on_risk_score(risk("u", 0.9))
    (rec,) = p.sync_policy({"u": {"reason": "contract ended", "by": "hr-admin", "expires_at": None}})
    assert rec["reason"] == "policy" and rec["actor"] == "hr-admin"      # policy upgrades a risk block
    assert p.sync_policy({"u": {"reason": "contract ended", "by": "hr-admin", "expires_at": None}}) == []
    with pytest.raises(PolicyLocked):
        p.release("u", actor="analyst1")
    assert p.expire_due() == []                                          # no expiry for policy blocks
    (lift,) = p.sync_policy({})
    assert (lift["decision"], lift["transition"]) == ("Allow", "policy_lifted")
    out = p.sync_policy({"v": {"reason": "x", "by": "a", "expires_at": T0 + 100}})
    assert out[0]["expires_at"] == pytest.approx(T0 + 100)


def test_manual_block_with_ttl():
    c = Clock(); p = pdp(c)
    p.release("u", actor="a")                                             # sets a grace period ...
    (rec,) = p.block_manual("u", actor="analyst2", note="confirmed insider", ttl=120)   # ... which a manual block clears
    assert rec["reason"] == "analyst" and rec["expires_at"] == pytest.approx(T0 + 120)
    assert p.status("u")["override_until"] is None


def test_shadow_mode_decides_and_audits_without_enforcing():
    srv = fakeredis.FakeServer()
    c = Clock(); p = pdp(c, mode="shadow", server=srv)
    (rec,) = p.on_risk_score(risk("u", 0.9))
    assert rec["enforced"] is False and rec["mode"] == "shadow" and p.status("u")["blocked"]
    aio = fakeredis.aioredis.FakeRedis(server=srv, decode_responses=True)
    assert asyncio.run(active_block(aio, "u", now=T0)) is None             # the gateway sees nothing
    assert p.r.zscore(K_BLOCKED, "u") is None and p.r.zscore("shadow:" + K_BLOCKED, "u") is not None


# ------------------------------------------------------------------------------------------ service
class Prod:
    def __init__(self, fail=False): self.fail, self.sent, self._cbs = fail, [], []
    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.sent.append(json.loads(value)); self._cbs.append(on_delivery)
    def flush(self, t=None):
        for cb in self._cbs:
            cb("down" if self.fail else None, None)
        self._cbs = []
        return 0


class Policy:
    def __init__(self): self.rows, self.denied_map, self.fail = [], {}, False
    def denied(self):
        if self.fail:
            raise ConnectionError("postgres down")
        return dict(self.denied_map)
    def audit(self, recs): self.rows += recs


class Revoker:
    def __init__(self): self.calls, self.fail = [], False
    def revoke(self, u, reason):
        if self.fail:
            raise ConnectionError("gateway down")
        self.calls.append((u, reason)); return 1


def handler(c, **kw):
    p = pdp(c)
    prod, pol, rev = Prod(), Policy(), Revoker()
    return PdpHandler(p, prod, "pdp-decisions", pol, rev, poll_seconds=15, clock=c, **kw), p, prod, pol, rev


def rec(topic, d, i=0):
    return Record(topic, 0, i, d["u"].encode(), json.dumps(d).encode(), 0, data=d)


def test_service_publishes_audits_and_revokes_sessions():
    c = Clock(); h, p, prod, pol, rev = handler(c)
    h.handle([rec("risk-scores", risk("x", 0.95)), rec("group-scores", group(["a", "b"], 0.97), 1)])
    h.tick()
    assert sorted(r["u"] for r in prod.sent) == ["a", "b", "x"] and len(pol.rows) == 3
    assert sorted(u for u, _ in rev.calls) == ["a", "b", "x"] and p.r.llen(K_OUTBOX) == 0
    assert all(reason.startswith("pdp-block:") for _, reason in rev.calls)


def test_failed_revocation_is_retried_and_the_block_still_holds():
    c = Clock(); h, p, prod, pol, rev = handler(c)
    rev.fail = True
    h.handle([rec("risk-scores", risk("x", 0.95))]); h.tick()
    assert p.r.hexists(K_REVOKE, "x") and p.status("x")["blocked"] and h.stats["revoke_failures"] == 1
    rev.fail = False
    h.tick()
    assert rev.calls == [("x", "pdp-block:individual")] and not p.r.hexists(K_REVOKE, "x")


def test_failed_publish_keeps_the_outbox():
    c = Clock(); h, p, prod, pol, rev = handler(c)
    prod.fail = True
    h.handle([rec("risk-scores", risk("x", 0.95))])
    with pytest.raises(RuntimeError):
        h.tick()
    assert p.r.llen(K_OUTBOX) == 1 and pol.rows == [] and rev.calls == []
    prod.fail = False
    h.tick()
    assert len(pol.rows) == 1 and p.r.llen(K_OUTBOX) == 0


def test_policy_source_outage_keeps_existing_decisions():
    c = Clock(); h, p, prod, pol, rev = handler(c)
    pol.denied_map = {"x": {"reason": "offboarding", "by": "hr", "expires_at": None}}
    h.tick()
    assert p.status("x")["reason"] == "policy"
    pol.fail = True
    c.t += 30
    h.tick()                                                   # PostgreSQL unreachable: x must stay blocked
    assert p.status("x")["blocked"] and h.stats["policy_errors"] == 1

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
    (rec,) = p.on_risk_score(risk("u", 0.83))                              # D3 score (no theta_B): Terminate
    assert (rec["decision"], rec["reason"], rec["enforced"]) == ("Terminate", "individual", True)
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
    assert ev["b"]["transition"] == "block" and ev["b"]["reason"] == "collective" and ev["b"]["decision"] == "Restrict"
    st = p.status("a")
    assert st["reason"] == "individual" and st["expires_at"] == exp and json.loads(st["also"]) == ["collective"]
    assert p.on_group_score(group(["a", "b"], 0.98, t=c.t)) == []          # same evidence kind again: nothing new


def test_group_wide_enforcement_uses_the_calibrated_theta():
    c = Clock(); p = pdp(c)
    assert p.on_group_score(group(["a", "b", "c"], 0.85, theta=0.9)) == []
    out = p.on_group_score(group(["a", "b", "c"], 0.95, theta=0.9))
    assert [r["u"] for r in out] == ["a", "b", "c"] and all(r["reason"] == "collective" and r["group_id"] == "g1" for r in out)
    assert all(r["decision"] == "Restrict" for r in out)                    # D3 group score: no A_u -> Restrict
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
    assert p.on_risk_score(risk("u", 0.95, t=c.t))[0]["decision"] == "Terminate"


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
    assert rev.calls == [("x", "pdp-terminate:individual")] and p.r.llen(K_OUTBOX) == 0   # Restrict keeps sessions
    assert float(p.r.get("pdp:alive")) == T0                                 # heartbeat for the gateway TTL


def test_failed_revocation_is_retried_and_the_block_still_holds():
    c = Clock(); h, p, prod, pol, rev = handler(c)
    rev.fail = True
    h.handle([rec("risk-scores", risk("x", 0.95))]); h.tick()
    assert p.r.hexists(K_REVOKE, "x") and p.status("x")["blocked"] and h.stats["revoke_failures"] == 1
    rev.fail = False
    h.tick()
    assert rev.calls == [("x", "pdp-terminate:individual")] and not p.r.hexists(K_REVOKE, "x")


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



# ------------------------------------------------------------------------------------------ Revised v3 (Phase 7)
from accesssentinel.pdp_engine import ACTIONS, RANK, decide  # noqa: E402


def v3risk(u, s, t=T0, th=0.05, tb=0.09):
    return {**risk(u, s, t), "theta_high": th, "theta_B": tb, "model_version": "tcn3-x"}


def v3group(members, member_scores, s, theta=0.86, theta_a=0.36, t=T0):
    return {**group(members, s, theta, t), "model_kind": "tgn-v3", "theta_A": theta_a, "member_scores": member_scores,
            "model_version": "tgn3-x"}


@pytest.mark.parametrize("deny,s_ind,s_coll,a_u,expected", [
    (True, 0.99, 0.99, 0.99, "Block"),                 # row 1 wins over everything
    (False, 0.10, 0.99, 0.99, "Terminate"),            # row 2 before Restrict
    (False, 0.07, 0.90, 0.50, "Restrict"),             # row 3 before Throttle
    (False, 0.07, 0.90, 0.10, "StepUp"),               # row 4 before Throttle
    (False, 0.07, 0.50, 0.90, "Throttle"),             # S_coll below theta_C
    (False, 0.07, None, None, "Throttle"),
    (False, 0.02, 0.50, 0.90, "Allow"),
    (False, 0.09, None, None, "Terminate"),            # boundary: S_ind = theta_B
    (False, 0.05, None, None, "Throttle"),             # boundary: S_ind = theta_high
    (False, 0.02, 0.86, 0.36, "Restrict"),             # boundaries: S_coll = theta_C, A_u = theta_A
])
def test_eq21_mapping_top_down(deny, s_ind, s_coll, a_u, expected):
    assert decide(deny, s_ind, 0.05, 0.09, s_coll, a_u, 0.86, 0.36) == expected
    assert ACTIONS == ("Allow", "Throttle", "StepUp", "Restrict", "Terminate", "Block") and RANK["Block"] == 5


def test_v3_throttle_band_escalates_to_terminate_and_never_downgrades():
    c = Clock(); p = pdp(c, throttle_ttl=900)
    assert p.on_risk_score(v3risk("u", 0.04)) == []                        # below theta_high
    (thr,) = p.on_risk_score(v3risk("u", 0.06))
    assert (thr["decision"], thr["expires_at"]) == ("Throttle", pytest.approx(T0 + 900))
    assert not p.status("u")["blocked"] and p.status("u")["action"] == "Throttle"
    c.t += 30
    (term,) = p.on_risk_score(v3risk("u", 0.30, t=c.t))
    assert term["decision"] == "Terminate" and term["previous_action"] == "Throttle" and term["psi"] > thr["psi"]
    assert p.status("u")["blocked"] and p.on_risk_score(v3risk("u", 0.06, t=c.t)) == []   # no downgrade
    assert p.status("u")["action"] == "Terminate"


def test_v3_operator_theta_high_above_theta_b_leaves_no_throttle_band():
    c = Clock(); p = pdp(c)
    p.theta_high_override = 0.2
    assert p.on_risk_score(v3risk("u", 0.15)) == []
    assert p.on_risk_score(v3risk("u", 0.25))[0]["decision"] == "Terminate"


def test_v3_group_restricts_core_members_and_steps_up_peripheral_ones():
    c = Clock(); p = pdp(c)
    ms = {"core": {"s_coll": 0.95, "A_u": 0.80}, "edge": {"s_coll": 0.90, "A_u": 0.10},
          "low": {"s_coll": 0.40, "A_u": 0.90}}
    out = {r["u"]: r for r in p.on_group_score(v3group(["core", "edge", "low"], ms, 0.95))}
    assert out["core"]["decision"] == "Restrict" and out["edge"]["decision"] == "StepUp" and "low" not in out
    assert out["edge"]["basis"]["A_u"] == 0.10 and out["edge"]["basis"]["theta_A"] == 0.36
    assert p.status("edge")["expires_at"] and not p.status("core")["blocked"]
    c.t += 10                                                               # stronger attribution later: escalate
    ms2 = {"edge": {"s_coll": 0.92, "A_u": 0.50}}
    (up,) = p.on_group_score(v3group(["edge"], ms2, 0.92, t=c.t))
    assert (up["decision"], up["previous_action"]) == ("Restrict", "StepUp")


def test_v3_decisions_carry_the_version_triple_and_psi_is_strictly_increasing():
    c = Clock(); p = pdp(c)
    p.sync_policy({})
    recs = p.on_risk_score(v3risk("a", 0.5)) + p.on_risk_score(v3risk("b", 0.06)) + p.release("a", actor="x")
    psis = [r["psi"] for r in recs]
    assert psis == sorted(psis) and len(set(psis)) == 3 and psis[0] >= int(T0 * 1000)
    assert recs[0]["v_model"] == "tcn3-x" and len(recs[0]["v_policy"]) == 12
    assert int(p.status("b")["psi"]) == psis[1]
    p.r.delete("pdp:psi")                                                   # flushed counter: re-seeded from the clock
    c.t += 1
    assert p.on_risk_score(v3risk("c", 0.5, t=c.t))[0]["psi"] > psis[-1]

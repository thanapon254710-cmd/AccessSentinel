"""PDP service (Revised v3 Step 8): turns risk-scores (TCN) and group-scores (TGN) plus organisational policy C into
the six-action decisions of Eq. 21 for the gateway, and enforces Terminate / Block at session time.

    python -m accesssentinel.streaming.pdp_service

Each tick: (1) re-read policy C from PostgreSQL (policy_blocks, deactivated users) - if PostgreSQL is
unreachable, existing decisions simply stay in force; (2) end expired blocks; (3) drain the decision outbox:
publish to `pdp-decisions` (-> HDFS), insert into the `pdp_decisions` audit table, and terminate the sessions of
users newly under Terminate / Block through the gateway's internal API (failed revocations are retried; the cached
decision already denies every request meanwhile); (4) write the heartbeat pdp:alive (the gateway's decision TTL).
Request-time enforcement needs no call: the gateway reads decision:<u> (psi-ordered).
"""
import json
import logging
import socket
import time
from datetime import datetime, timezone
from typing import Dict, List

from ..pdp_engine import DENY_ALL, PolicyDecisionPoint
from ..topics import GROUP_SCORES, PDP_DECISIONS, RISK_SCORES, topic_name
from .codec import decode_json_object
from .consumer import DecodeError, Record

log = logging.getLogger("accesssentinel.pdp")
K_REVOKE = "pdp:revoke_pending"


class SqlPolicySource:
    """Organisational policy C from PostgreSQL."""

    def __init__(self, engine):
        self.engine = engine

    def denied(self) -> Dict[str, dict]:
        from sqlalchemy import select
        from sqlalchemy.orm import Session
        from ..db.models import PolicyBlock, User
        out = {}
        with Session(self.engine) as db:
            for pb in db.scalars(select(PolicyBlock).where(PolicyBlock.active.is_(True))):
                exp = pb.expires_at.timestamp() if pb.expires_at else None
                out[pb.user_id] = {"reason": pb.reason, "by": pb.created_by, "expires_at": exp}
            for u in db.scalars(select(User).where(User.is_active.is_(False))):
                out.setdefault(u.id, {"reason": "account deactivated", "by": "identity", "expires_at": None})
        return out

    def audit(self, records: List[dict]) -> None:
        from sqlalchemy.dialects.postgresql import insert
        from sqlalchemy.orm import Session
        from ..db.models import PdpDecision
        rows = [{"decision_id": r["decision_id"], "user_id": r["u"], "t": r["t"], "decision": r["decision"],
                 "transition": r["transition"], "reason": r["reason"], "mode": r["mode"], "enforced": r["enforced"],
                 "actor": r["actor"], "s_ind": r.get("s_ind"), "s_coll": r.get("s_coll"), "group_id": r.get("group_id"),
                 "expires_at": r.get("expires_at"), "version": r["version"], "record": r} for r in records]
        if rows:
            with Session(self.engine) as db, db.begin():
                db.execute(insert(PdpDecision).values(rows).on_conflict_do_nothing(index_elements=["decision_id"]))


class GatewayRevoker:
    def __init__(self, base_url: str, token: str, timeout: float = 5.0):
        self.base, self.token, self.timeout = base_url.rstrip("/"), token, timeout

    def revoke(self, user: str, reason: str) -> int:
        import httpx
        r = httpx.post(f"{self.base}/internal/sessions/revoke", json={"user": user, "reason": reason[:200]},
                       headers={"X-AS-Internal-Token": self.token}, timeout=self.timeout)
        r.raise_for_status()
        return int(r.json().get("revoked_sessions", 0))


class PdpHandler:
    def __init__(self, pdp: PolicyDecisionPoint, producer, out_topic: str, policy=None, revoker=None,
                 scores_topic: str = "risk-scores", group_topic: str = "group-scores", poll_seconds: float = 15.0,
                 clock=time.time):
        self.pdp, self.p, self.out_topic, self.policy, self.revoker = pdp, producer, out_topic, policy, revoker
        self.scores_topic, self.group_topic, self.poll, self.clock = scores_topic, group_topic, poll_seconds, clock
        self._next_policy = 0.0
        self.settings = None                      # runtime_settings.Watcher (dashboard Settings page)
        self._errors: List[str] = []
        self.stats = {"risk_scores": 0, "group_scores": 0, "decisions_published": 0, "sessions_revoked": 0,
                      "revoke_failures": 0, "policy_errors": 0}

    def decode(self, rec: Record):
        d = decode_json_object(rec.value)
        if not isinstance(d.get("u"), str):
            raise DecodeError("record without u")
        if rec.topic == self.scores_topic and not isinstance(d.get("s_ind"), (int, float)):
            raise DecodeError("risk score without s_ind")
        return d

    def handle(self, records: List[Record]):
        for rec in records:
            if rec.topic == self.group_topic:
                self.pdp.on_group_score(rec.data); self.stats["group_scores"] += 1
            else:
                self.pdp.on_risk_score(rec.data); self.stats["risk_scores"] += 1
        return []

    def checkpoint(self, force: bool) -> bool:
        return True                     # state transitions are in Redis (with their outbox records) already

    def reset(self) -> None:
        pass

    def apply_settings(self) -> None:
        from .. import runtime_settings as rs
        eff = self.settings.poll() if self.settings is not None else None
        if eff:
            self.pdp.rc = rs.apply_risk(self.pdp.rc, eff)
            self.pdp.cfg = rs.apply_pdp(self.pdp.cfg, eff)
            self.pdp.theta_override = eff["pdp.theta_coll"]
            self.pdp.theta_high_override = eff["risk.theta_high"] if "risk.theta_high" in rs.explicit(self.pdp.r) else None
            log.info("runtime settings applied: theta_high=%s theta_coll=%s block_ttl=%s", self.pdp.rc.theta_high,
                     self.pdp.theta_override or "model-calibrated", self.pdp.cfg.block_ttl)

    def tick(self) -> None:
        self.apply_settings()
        now = self.clock()
        if self.policy is not None and now >= self._next_policy:
            self._next_policy = now + self.poll
            try:
                self.pdp.sync_policy(self.policy.denied())
            except Exception as e:  # noqa: BLE001 - fail safe: existing decisions stay in force
                self.stats["policy_errors"] += 1
                log.warning("policy sync failed (%s); existing decisions stay in force", type(e).__name__)
        self.pdp.expire_due()
        self.pdp.heartbeat()
        self.drain()
        self.retry_revocations()

    def drain(self) -> int:
        recs = self.pdp.outbox_peek(200)
        if not recs:
            return 0
        self._errors = []
        for r in recs:
            self.p.produce(self.out_topic, key=r["u"].encode(), value=json.dumps(r).encode(), on_delivery=self._cb)
        remaining = self.p.flush(30)
        if remaining or self._errors:
            raise RuntimeError(f"pdp decisions not acknowledged: remaining={remaining} errors={self._errors[:3]}")
        if self.policy is not None:
            self.policy.audit(recs)                      # raises -> retried; Kafka duplicates carry the same id
        for r in recs:                                   # queue revocations BEFORE acknowledging the outbox
            if r["transition"] == "block" and r["decision"] in DENY_ALL and r["enforced"] and self.revoker is not None:
                self.pdp.r.hset(K_REVOKE, r["u"], f"{r['decision'].lower()}:{r['reason']}")
        self.pdp.outbox_ack(len(recs))
        self.stats["decisions_published"] += len(recs)
        for r in recs:
            log.info("decision %s %s %s (%s) enforced=%s", r["u"], r["decision"], r["transition"], r["reason"], r["enforced"])
        return len(recs)

    def retry_revocations(self) -> None:
        if self.revoker is None:
            return
        for u, reason in list(self.pdp.r.hgetall(K_REVOKE).items()):
            u, reason = (u.decode() if isinstance(u, bytes) else u), (reason.decode() if isinstance(reason, bytes) else reason)
            try:
                n = self.revoker.revoke(u, f"pdp-{reason}")
                self.pdp.r.hdel(K_REVOKE, u)
                self.stats["sessions_revoked"] += n
            except Exception as e:  # noqa: BLE001 - retried next tick; the cached Block already denies requests
                self.stats["revoke_failures"] += 1
                log.warning("session revocation for %s failed (%s); will retry", u, type(e).__name__)

    def _cb(self, err, msg):
        if err is not None:
            self._errors.append(str(err))


def build_pdp(s, redis, clock=time.time) -> PolicyDecisionPoint:
    from ..config import PDPConfig, RiskConfig
    return PolicyDecisionPoint(redis, PDPConfig(), RiskConfig(), mode=s.pdp_mode, clock=clock)


def main() -> int:
    import redis as redis_lib
    from confluent_kafka import Consumer, Producer
    from ..db.engine import make_engine
    from ..settings import get_settings
    from .consumer import DeadLetterQueue, StreamConsumer, consumer_config, producer_config
    from .runtime import Heartbeat, install_stop, setup

    s = get_settings()
    setup(s.log_level)
    client = f"pdp-{socket.gethostname()}"
    r = redis_lib.Redis(**s.redis_kwargs())
    pdp = build_pdp(s, r)
    revoker = GatewayRevoker(s.gateway_internal_url, s.secret("internal_token")) if s.pdp_mode == "enforce" else None
    h = PdpHandler(pdp, Producer(producer_config(s.kafka_bootstrap, client)), topic_name(PDP_DECISIONS, s.kafka_topic_prefix),
                   SqlPolicySource(make_engine(s)), revoker, topic_name(RISK_SCORES, s.kafka_topic_prefix),
                   topic_name(GROUP_SCORES, s.kafka_topic_prefix), pdp.cfg.policy_poll_seconds)
    from ..runtime_settings import Watcher
    h.settings = Watcher(r)
    group = "pdp"
    sc = StreamConsumer(Consumer(consumer_config(s.kafka_bootstrap, group, client)),
                        [topic_name(RISK_SCORES, s.kafka_topic_prefix), topic_name(GROUP_SCORES, s.kafka_topic_prefix)], h,
                        DeadLetterQueue(Producer(producer_config(s.kafka_bootstrap, f"{client}-dlq")), group),
                        batch_size=s.consumer_batch, heartbeat=Heartbeat(), tick=h.tick)
    install_stop(sc.stop)
    log.info("PDP mode=%s theta_high=%.2f theta_coll=%s block_ttl=%.0fs started %s", s.pdp_mode, pdp.rc.theta_high,
             pdp.theta_override or "model-calibrated", pdp.cfg.block_ttl, datetime.now(timezone.utc).isoformat())
    sc.run()
    log.info("PDP stopped: %s %s", sc.stats, h.stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

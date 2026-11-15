"""SQLAlchemy models. Schema version 1 (Phase 3). Later phases add incidents, feedback, model registry.

Schema changes are applied by `python -m accesssentinel.db.init_db` (create-if-missing). A migration tool
(Alembic) is deferred until a released schema has to change in place; documented in docs/PROGRESS.md.
"""
from datetime import datetime

from typing import Optional

from sqlalchemy import (JSON, BigInteger, Boolean, CheckConstraint, DateTime, Float, ForeignKey, ForeignKeyConstraint,
                        Integer, String, Text, UniqueConstraint, func)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SCHEMA_VERSION = 4              # v2 (Phase 8): policy_blocks, pdp_decisions; v3 (Phase 9): incidents, feedback,
                                #   settings_changes; v4 (Revised v3 Phase 7): pdp_decisions.decision VARCHAR(16)


class Base(DeclarativeBase):
    pass


class SchemaInfo(Base):
    __tablename__ = "schema_info"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(),
                                                 onupdate=func.now())


class User(Base):
    """Single identity u shared by every protected application (D3 Section III-A)."""
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)
    role: Mapped[str] = mapped_column(String(64), nullable=False)
    department: Mapped[str] = mapped_column(String(100), nullable=False, default="")
    password_hash: Mapped[str] = mapped_column(String(256), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Application(Base):
    __tablename__ = "applications"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    base_url: Mapped[str] = mapped_column(String(300), nullable=False)       # internal upstream URL
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class Resource(Base):
    """Policy catalog entry: resource r of application a with sensitivity s in {0,1} (D3 Eq. 1)."""
    __tablename__ = "resources"
    __table_args__ = (UniqueConstraint("app_id", "resource_key"),
                      CheckConstraint("sensitivity IN (0, 1)", name="ck_resources_sensitivity"))
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    app_id: Mapped[str] = mapped_column(ForeignKey("applications.id", ondelete="CASCADE"), nullable=False)
    resource_key: Mapped[str] = mapped_column(String(64), nullable=False)
    sensitivity: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")


class AclRule(Base):
    """First-line access-control policy: role may use `permission` on (app, resource). Default deny."""
    __tablename__ = "acl_rules"
    __table_args__ = (
        UniqueConstraint("app_id", "resource_key", "role", "permission"),
        ForeignKeyConstraint(["app_id", "resource_key"], ["resources.app_id", "resources.resource_key"],
                             ondelete="CASCADE"),
        CheckConstraint("permission IN ('read', 'write', 'admin', 'export')", name="ck_acl_permission"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    app_id: Mapped[str] = mapped_column(String(32), nullable=False)
    resource_key: Mapped[str] = mapped_column(String(64), nullable=False)
    role: Mapped[str] = mapped_column(String(64), nullable=False)
    permission: Mapped[str] = mapped_column(String(16), nullable=False)


class PolicyBlock(Base):
    """Organisational policy context C (D3 Step 8): an administrator's standing deny for a user. Only lifting
    the policy ends such a block; neither ML scores nor an analyst release do."""
    __tablename__ = "policy_blocks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class PdpDecision(Base):
    """Audit trail of every PDP decision transition (also published to Kafka `pdp-decisions` -> HDFS)."""
    __tablename__ = "pdp_decisions"
    decision_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    t: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    decision: Mapped[str] = mapped_column(String(16), nullable=False)       # v3 actions (Eq. 20), e.g. "Terminate"
    transition: Mapped[str] = mapped_column(String(24), nullable=False)
    reason: Mapped[str] = mapped_column(String(32), nullable=False)
    mode: Mapped[str] = mapped_column(String(8), nullable=False)
    enforced: Mapped[bool] = mapped_column(Boolean, nullable=False)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    s_ind: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    s_coll: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    group_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    expires_at: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    record: Mapped[dict] = mapped_column(JSON, nullable=False)


class Incident(Base):
    """A detection that needs analyst review: one per individual Block episode, one per TGN group."""
    __tablename__ = "incidents"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    dedup_key: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
    kind: Mapped[str] = mapped_column(String(16), nullable=False, index=True)        # individual | collective
    severity: Mapped[str] = mapped_column(String(12), nullable=False)                 # medium | high | critical
    status: Mapped[str] = mapped_column(String(24), nullable=False, index=True)       # open | triaged | closed
    verdict: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)         # analyst verdict
    title: Mapped[str] = mapped_column(String(300), nullable=False)
    users: Mapped[list] = mapped_column(JSON, nullable=False)
    group_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    first_seen: Mapped[float] = mapped_column(Float, nullable=False)
    last_seen: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    updated_at: Mapped[float] = mapped_column(Float, nullable=False)
    evidence: Mapped[list] = mapped_column(JSON, nullable=False)                      # decisions / scores / paths
    enforcement: Mapped[dict] = mapped_column(JSON, nullable=False)                   # per user: blocked/shadow/...
    max_s_ind: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    s_coll: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(24), nullable=False)                  # live | synthetic-replay
    assigned_to: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)


class Feedback(Base):
    """Analyst feedback. The ONLY source of supervised labels for retraining (D3 Step 9)."""
    __tablename__ = "feedback"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    incident_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    kind: Mapped[str] = mapped_column(String(24), nullable=False)          # incident_review | missed_detection
    scope: Mapped[str] = mapped_column(String(16), nullable=False)         # individual | collective
    verdict: Mapped[str] = mapped_column(String(32), nullable=False)       # confirmed_malicious | false_positive | ...
    users: Mapped[list] = mapped_column(JSON, nullable=False)
    window_t0: Mapped[float] = mapped_column(Float, nullable=False)
    window_t1: Mapped[float] = mapped_column(Float, nullable=False)
    note: Mapped[str] = mapped_column(Text, nullable=False, default="")
    analyst: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    group_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    source: Mapped[str] = mapped_column(String(24), nullable=False, default="live")


class SettingsChange(Base):
    """History of runtime setting changes made through the dashboard."""
    __tablename__ = "settings_changes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(64), nullable=False)
    old_value: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    new_value: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    changed_by: Mapped[str] = mapped_column(String(128), nullable=False)
    changed_at: Mapped[float] = mapped_column(Float, nullable=False)


class AuditLog(Base):
    """Security audit trail for gateway/admin actions (logins, logouts, revocations). Never stores secrets."""
    __tablename__ = "audit_log"
    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True,
                                    autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    target: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    ip: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    detail: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

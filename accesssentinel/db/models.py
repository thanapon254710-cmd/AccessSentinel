"""SQLAlchemy models. Schema version 1 (Phase 3). Later phases add incidents, feedback, model registry.

Schema changes are applied by `python -m accesssentinel.db.init_db` (create-if-missing). A migration tool
(Alembic) is deferred until a released schema has to change in place; documented in docs/PROGRESS.md.
"""
from datetime import datetime

from sqlalchemy import (JSON, BigInteger, Boolean, CheckConstraint, DateTime, ForeignKey, ForeignKeyConstraint,
                        Integer, String, Text, UniqueConstraint, func)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SCHEMA_VERSION = 1


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

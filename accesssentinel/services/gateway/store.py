"""Gateway access to PostgreSQL: catalog + ACL snapshot, user lookup, audit log (synchronous; called from
a thread pool). `MemoryGatewayStore` is the test double with the same interface."""
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from ...db.models import AclRule, Application, AuditLog, Resource, User
from .access import AppInfo, Catalog


@dataclass(frozen=True)
class UserRecord:
    id: str
    display_name: str
    role: str
    department: str
    password_hash: str
    is_active: bool


class SqlGatewayStore:
    def __init__(self, engine: Engine):
        self.engine = engine

    def load_catalog(self) -> Catalog:
        with Session(self.engine) as db:
            apps = {a.id: AppInfo(a.id, a.name, a.base_url, a.description)
                    for a in db.scalars(select(Application).where(Application.is_active.is_(True)))}
            sens, desc = {}, {}
            for r in db.scalars(select(Resource)):
                if r.app_id in apps:
                    sens[(r.app_id, r.resource_key)] = r.sensitivity
                    desc[(r.app_id, r.resource_key)] = r.description
            acl = defaultdict(set)
            for rule in db.scalars(select(AclRule)):
                acl[(rule.app_id, rule.resource_key, rule.role)].add(rule.permission)
        return Catalog(apps, sens, desc, {k: frozenset(v) for k, v in acl.items()})

    def get_user(self, user_id: str) -> Optional[UserRecord]:
        with Session(self.engine) as db:
            u = db.get(User, user_id)
            if u is None:
                return None
            return UserRecord(u.id, u.display_name, u.role, u.department, u.password_hash, u.is_active)

    def audit(self, actor: str, action: str, target: str = "", ip: str = "", detail: Optional[dict] = None) -> None:
        with Session(self.engine) as db, db.begin():
            db.add(AuditLog(actor=actor, action=action, target=target, ip=ip, detail=detail or {}))


class MemoryGatewayStore:
    def __init__(self, catalog: Catalog, users: Dict[str, UserRecord]):
        self.catalog, self.users = catalog, users
        self.audit_rows: List[dict] = []

    def load_catalog(self) -> Catalog:
        return self.catalog

    def get_user(self, user_id: str) -> Optional[UserRecord]:
        return self.users.get(user_id)

    def audit(self, actor, action, target="", ip="", detail=None) -> None:
        self.audit_rows.append(dict(actor=actor, action=action, target=target, ip=ip, detail=detail or {}))

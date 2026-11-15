"""Pure first-line access-control logic: catalog, request -> (resource, operation), ACL evaluation.
No I/O, so every rule here is unit-tested directly.

Request mapping (implementation choice, documented in docs/PROGRESS.md):
  /apps/<a>/<r>                  GET -> view      POST -> edit
  /apps/<a>/<r>/<id>             GET -> view      PUT/PATCH -> edit     DELETE -> delete
  /apps/<a>/<r>/export           GET -> export
  The permission p recorded in the event is the one the operation requires (schema.REQUIRED_PERMISSION).
  A path whose first segment is not in the catalog is denied and recorded as r = "unmapped:<segment>"
  with s = 1 (unknown resources are treated as sensitive: forced browsing is security-relevant).
"""
import re
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Tuple

from ...schema import REQUIRED_PERMISSION

SEGMENT = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


@dataclass(frozen=True)
class AppInfo:
    id: str
    name: str
    base_url: str
    description: str = ""


@dataclass(frozen=True)
class Catalog:
    apps: Dict[str, AppInfo] = field(default_factory=dict)
    sensitivity: Dict[Tuple[str, str], int] = field(default_factory=dict)     # (app, resource) -> s
    descriptions: Dict[Tuple[str, str], str] = field(default_factory=dict)
    acl: Dict[Tuple[str, str, str], FrozenSet[str]] = field(default_factory=dict)  # (app, res, role) -> perms

    def resources(self, app_id: str) -> List[str]:
        return sorted(r for a, r in self.sensitivity if a == app_id)

    def permissions(self, app_id: str, resource: str, role: str) -> FrozenSet[str]:
        return self.acl.get((app_id, resource, role), frozenset())

    def allowed(self, app_id: str, resource: str, role: str, permission: str) -> bool:
        """Default deny: only an explicit grant allows."""
        return permission in self.permissions(app_id, resource, role)

    @property
    def version(self) -> str:
        """v3 vpolicy: short stable hash of the sensitivity labels and ACL grants (Table III "policy version")."""
        v = self.__dict__.get("_version")
        if v is None:
            import hashlib
            body = repr((sorted(self.sensitivity.items()), sorted((k, sorted(p)) for k, p in self.acl.items())))
            v = self.__dict__["_version"] = hashlib.sha256(body.encode()).hexdigest()[:12]
        return v


@dataclass(frozen=True)
class Target:
    resource: str              # catalog key, or "unmapped:<segment>"
    known: bool
    object_id: Optional[str]
    operation: str
    permission: str


class BadRequest(ValueError):
    pass


class MethodNotAllowed(ValueError):
    pass


def resolve(catalog: Catalog, app_id: str, path: str, method: str) -> Target:
    parts = [p for p in path.split("/") if p]
    if not parts:
        raise BadRequest("no resource in path")
    if len(parts) > 3 or any(not SEGMENT.match(p) or p in (".", "..") for p in parts):
        raise BadRequest("invalid path")
    resource, rest = parts[0], parts[1:]
    m = method.upper()
    is_export = rest == ["export"]
    obj = None if (not rest or is_export) else rest[0]
    if len(rest) > 1:
        raise BadRequest("invalid path")
    if m in ("GET", "HEAD"):
        op = "export" if is_export else "view"
    elif m == "POST" and not rest:
        op = "edit"
    elif m in ("PUT", "PATCH") and obj is not None:
        op = "edit"
    elif m == "DELETE" and obj is not None:
        op = "delete"
    else:
        raise MethodNotAllowed(f"{m} not supported on this path")
    known = (app_id, resource) in catalog.sensitivity
    return Target(resource if known else f"unmapped:{resource}", known, obj, op, REQUIRED_PERMISSION[op])

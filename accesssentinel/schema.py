"""Validated access-event schema (v1 and v2) used at service boundaries: the gateway validates before publishing,
consumers validate before processing (invalid records go to the topic's DLQ, Phase 4).

Vocabulary (also used by features.py):
  operation o   view | edit | delete | export
  permission p  read | write | admin  | export     (permission required for an operation, see REQUIRED_PERMISSION)
"""
import re
import time
import uuid
from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .events import AccessEvent

EVENT_SCHEMA_VERSION = 2          # v2 = Revised v3 Eq. (1): adds seq (nu), stream, tenant, ti, v_policy
SUPPORTED_SCHEMA_VERSIONS = (1, 2)

OPERATIONS = ("view", "edit", "delete", "export")
PERMISSIONS = ("read", "write", "admin", "export")
REQUIRED_PERMISSION = {"view": "read", "edit": "write", "delete": "admin", "export": "export"}

_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}$")
_HEX = re.compile(r"^[0-9a-f]{32}$")


class AccessEventV1(BaseModel):
    """Validates schema v1 records (D3) and v2 records (Revised v3). Name kept for compatibility."""
    model_config = ConfigDict(extra="forbid", frozen=True)

    u: str
    t: float = Field(gt=0)
    a: str
    r: str
    s: int = Field(ge=0, le=1)
    p: str
    o: str
    c: Dict[str, str]
    y: int = Field(ge=0, le=1)
    eid: int = -1
    event_id: str
    schema_version: int = EVENT_SCHEMA_VERSION
    correlation_id: str
    meta: Dict[str, Any] = Field(default_factory=dict)
    seq: int = Field(default=-1, ge=-1)
    stream: str = Field(default="", max_length=64)
    tenant: str = "default"
    ti: float = Field(default=0.0, ge=0)
    v_policy: str = Field(default="", max_length=32)

    @field_validator("u", "a", "r", "tenant")
    @classmethod
    def _ident(cls, v: str) -> str:
        if not _ID.match(v):
            raise ValueError("identifier must be 1-128 chars of [A-Za-z0-9_.:@-]")
        return v

    @field_validator("p")
    @classmethod
    def _perm(cls, v: str) -> str:
        if v not in PERMISSIONS:
            raise ValueError(f"permission must be one of {PERMISSIONS}")
        return v

    @field_validator("o")
    @classmethod
    def _op(cls, v: str) -> str:
        if v not in OPERATIONS:
            raise ValueError(f"operation must be one of {OPERATIONS}")
        return v

    @field_validator("event_id", "correlation_id")
    @classmethod
    def _hex(cls, v: str) -> str:
        if not _HEX.match(v):
            raise ValueError("must be 32 lowercase hex characters")
        return v

    @field_validator("schema_version")
    @classmethod
    def _ver(cls, v: int) -> int:
        if v not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(f"unsupported schema_version {v}")
        return v

    @field_validator("c")
    @classmethod
    def _ctx(cls, v: Dict[str, str]) -> Dict[str, str]:
        if len(v) > 16 or any(len(k) > 32 or len(str(x)) > 128 for k, x in v.items()):
            raise ValueError("context too large")
        return v

    def to_event(self) -> AccessEvent:
        return AccessEvent(**self.model_dump())


def new_id() -> str:
    return uuid.uuid4().hex


def validate(ev: AccessEvent) -> AccessEvent:
    """Raise pydantic.ValidationError if `ev` is not a valid v1 event; return it unchanged otherwise."""
    AccessEventV1(**ev.__dict__)
    return ev


def make_event(u: str, a: str, r: str, s: int, o: str, allowed: bool, context: Dict[str, str],
               correlation_id: str, meta: Dict[str, Any] = None, t: float = None, *, seq: int = -1,
               stream: str = "", tenant: str = "default", v_policy: str = "") -> AccessEvent:
    """Build and validate a schema-v2 event. The permission is the one REQUIRED for the operation."""
    if o not in REQUIRED_PERMISSION:
        raise ValueError(f"operation must be one of {OPERATIONS}")
    m = AccessEventV1(u=u, t=time.time() if t is None else t, a=a, r=r, s=s, p=REQUIRED_PERMISSION[o], o=o,
                      c=dict(context), y=int(allowed), event_id=new_id(), correlation_id=correlation_id,
                      meta=dict(meta or {}), seq=seq, stream=stream, tenant=tenant, v_policy=v_policy)
    return m.to_event()

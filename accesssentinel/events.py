"""Eq. (1): e_t = (u, t, a, r, s, p, o, c, y).

The nine D3 fields come first and are unchanged. The trailing fields are implementation metadata
(not model inputs) and all have defaults, so events created by the simulator and files written before
schema v1 still load:
  eid            int label/join key used by the simulator and offline experiments
  event_id       globally unique id (UUID4 hex) set by the gateway; used for deduplication
  schema_version version of this record layout (see accesssentinel/schema.py)
  correlation_id request id shared by the gateway log, upstream app and event
  meta           non-feature audit details (HTTP method, object id, deny reason, upstream status)
"""
from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class AccessEvent:
    u: str            # user id (single id across applications)
    t: float          # timestamp (event time, seconds; gateway uses UTC epoch seconds)
    a: str            # application
    r: str            # accessed resource (local to application)
    s: int            # sensitivity label in {0,1} from the policy catalog
    p: str            # permission
    o: str            # requested operation
    c: Dict[str, str] = field(default_factory=dict)   # context: ip, device, session
    y: int = 1        # gateway access-control result (1 allow / 0 deny)
    eid: int = -1
    event_id: str = ""
    schema_version: int = 0                            # 0 = legacy/simulator record without metadata
    correlation_id: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)

    @property
    def rkey(self) -> str:
        return f"{self.a}:{self.r}"

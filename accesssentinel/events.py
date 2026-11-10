"""Eq. (1): e_t = (u, t, a, r, s, p, o, c, y). `eid` is an implementation helper (dedup / labels)."""
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class AccessEvent:
    u: str            # user id (single id across applications)
    t: float          # timestamp (event time, seconds)
    a: str            # application
    r: str            # accessed resource (local to application)
    s: int            # sensitivity label in {0,1} from the policy catalog
    p: str            # permission
    o: str            # requested operation
    c: Dict[str, str] = field(default_factory=dict)   # context: ip, device, session
    y: int = 1        # gateway access-control result (1 allow / 0 deny)
    eid: int = -1

    @property
    def rkey(self) -> str:
        return f"{self.a}:{self.r}"

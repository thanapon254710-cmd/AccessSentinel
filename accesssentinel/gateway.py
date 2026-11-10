"""Component 2: Proxy / Access-Control Gateway. Conventional ACL check, forwards EVERY event to the
pipeline, and applies PDP decisions (request-time: deny; session-time: terminate). It never scores."""
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Set, Tuple
from .events import AccessEvent
from .pdp import Decision, DecisionRecord


@dataclass
class AccessRequest:
    u: str; t: float; a: str; r: str; s: int; p: str; o: str
    role: str = "staff"; c: dict = field(default_factory=dict)


@dataclass
class GatewayResult:
    event: AccessEvent
    allowed: bool
    reason: str


class Gateway:
    def __init__(self, acl: Dict[Tuple[str, str], Set[str]], publish: Callable[[AccessEvent], None], pdp_current=None):
        self.acl, self.publish = acl, publish
        self.pdp_current = pdp_current if pdp_current is not None else {}
        self.sessions: Set[str] = set()
        self.terminated: Set[str] = set()
        self._eid = 0

    def handle(self, req: AccessRequest) -> GatewayResult:
        self.sessions.add(req.u)
        rec: Optional[DecisionRecord] = self.pdp_current.get(req.u)
        if rec is not None and rec.decision == Decision.BLOCK:
            allowed, reason = False, f"pdp-block:{rec.reason}"          # request-time enforcement
        elif req.role not in self.acl.get((req.a, req.r), set()):
            allowed, reason = False, "acl-deny"
        else:
            allowed, reason = True, "allow"
        ev = AccessEvent(req.u, req.t, req.a, req.r, req.s, req.p, req.o, dict(req.c), int(allowed), self._eid)
        self._eid += 1
        self.publish(ev)                                                  # forward every relevant event
        return GatewayResult(ev, allowed, reason)

    def apply_decision(self, rec: DecisionRecord):
        """Session-time enforcement: terminate the active session without waiting for the next request."""
        if rec.decision == Decision.BLOCK and rec.u in self.sessions:
            self.sessions.discard(rec.u); self.terminated.add(rec.u)

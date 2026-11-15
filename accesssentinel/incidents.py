"""Incidents for analyst review (D3 Step 9: "detected incidents"), derived from PDP decisions and TGN groups.

  individual  one open case per user: a new Block while the user's previous individual incident is still open
              (not yet reviewed) is merged into it          dedup key  individual:<u>:<first decision_id>
  collective  one open case per colluding group             dedup key  collective:<first group_id>
              The TGN derives group_id from members + window, so re-scoring the same group gives a new id; a
              record joins an open collective incident that already saw that group_id, or whose members overlap
              (|A n B| >= half of the smaller group) and that was active within MERGE_WINDOW.
Later transitions (release, expiry, policy lifted) update the per-user enforcement status of the user's
incidents. Every merge is idempotent by evidence id, so Kafka redelivery cannot duplicate evidence.
Incidents involving synthetic users are marked source = "synthetic-replay" so the dashboard labels them.
"""
import time
import uuid
from typing import List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from .db.models import Incident

OPEN_STATES = ("open", "triaged")
MERGE_WINDOW = 24 * 3600.0


def _source(users: List[str], rec: dict) -> str:
    if rec.get("source") == "synthetic-replay" or any(u.startswith("syn-") for u in users):
        return "synthetic-replay"
    return "live"


def _activity(rec: dict) -> Optional[Tuple[float, float]]:
    """EVENT-time span of the evidence (the accesses that were scored), not the decision time: feedback windows are
    derived from it and must cover the user's actual accesses, which can be much older than the decision (backlog,
    replayed history). Falls back to the record time for records written before event times were recorded."""
    b = rec.get("basis") if isinstance(rec.get("basis"), dict) else {}
    w = rec.get("window") or b.get("event_window")
    if isinstance(w, (list, tuple)) and len(w) == 2 and w[0] is not None and w[1] is not None:
        return float(w[0]), float(w[1])
    for t in (b.get("event_t"), rec.get("anchor_t"), rec.get("t")):
        if t:
            return float(t), float(t)
    return None


ACTION_LABEL = {"Block": "blocked", "Terminate": "terminated (sessions ended, access suspended)",
                "Restrict": "restricted (sensitive resources denied)", "StepUp": "step-up re-authentication required",
                "Throttle": "throttled (request rate limited)"}


def _label(rec: dict) -> str:
    """Enforcement status of one user from a decision record (v3 action set, Eq. 20)."""
    if not rec.get("enforced"):
        return "shadow (not enforced)"
    act = rec.get("decision") or "Block"                    # D3 records: every block transition was a Block
    return ACTION_LABEL.get(act, str(act))


def _ev(kind: str, rec: dict, **extra) -> dict:
    rid = rec.get("decision_id") or rec.get("score_id") or rec.get("escalation_id") or uuid.uuid4().hex
    return {"id": rid, "kind": kind, "t": rec.get("t"), **extra}


class IncidentStore:
    def __init__(self, engine, clock=time.time):
        self.engine, self.clock = engine, clock

    # ------------------------------------------------------------------ from PDP decisions
    def on_decision(self, rec: dict) -> Optional[str]:
        tr, reason, u = rec.get("transition"), rec.get("reason"), rec["u"]
        with Session(self.engine) as db, db.begin():
            if tr == "block" and reason == "individual":
                key = f"individual:{u}:{rec['decision_id']}"
                inc = db.scalar(select(Incident).where(Incident.dedup_key == key))
                if inc is None:
                    inc = next((i for i in db.scalars(select(Incident).where(Incident.kind == "individual",
                                                                             Incident.status.in_(OPEN_STATES)))
                                if u in i.users), None)
                    if inc is not None and (rec.get("s_ind") or 0) > (inc.max_s_ind or 0):
                        inc.max_s_ind = rec.get("s_ind")
                        if inc.max_s_ind >= 0.9:
                            inc.severity = "high"
                if inc is None:
                    s = rec.get("s_ind")
                    inc = self._new(db, key, "individual", "high" if (s or 0) >= 0.9 else "medium",
                                    f"High individual risk: {u}" + (f" (S_ind {s:.2f})" if s is not None else ""), [u], rec)
                    inc.max_s_ind = s
                self._merge(inc, _ev("pdp_block", rec, user=u, reason=reason, action=rec.get("decision"),
                                     s_ind=rec.get("s_ind"), basis=rec.get("basis"), enforced=rec.get("enforced")),
                            {u: _label(rec)}, rec)
                return inc.id
            if reason == "collective" and tr in ("block", "evidence") and rec.get("group_id"):
                inc = self._collective(db, rec["group_id"], rec, [u])
                status = _label(rec) if tr == "block" \
                    else f"already {ACTION_LABEL.get(rec.get('decision'), rec.get('decision'))} ({rec.get('block_reason')})"
                self._merge(inc, _ev("pdp_" + tr, rec, user=u, reason=reason, action=rec.get("decision"),
                                     s_coll=rec.get("s_coll"), group_id=rec["group_id"], enforced=rec.get("enforced")),
                            {u: status}, rec)
                return inc.id
            if tr in ("release", "expire", "policy_lifted"):
                label = {"release": f"released by {rec.get('actor')}", "expire": "action expired",
                         "policy_lifted": "policy lifted"}[tr]
                hit = None
                for inc in db.scalars(select(Incident).where(Incident.status.in_(OPEN_STATES))):
                    if u in inc.users:
                        self._merge(inc, _ev("pdp_" + tr, rec, user=u, actor=rec.get("actor"), note=rec.get("note")),
                                    {u: label}, rec, activity=False)
                        hit = inc.id
                return hit
        return None

    # ------------------------------------------------------------------ from TGN group scores
    def on_group_score(self, rec: dict) -> Optional[str]:
        if not rec.get("above_theta_coll") or not rec.get("group_id"):
            return None
        with Session(self.engine) as db, db.begin():
            inc = self._collective(db, rec["group_id"], rec, rec.get("members") or [rec["u"]])
            inc.s_coll = max(inc.s_coll or 0.0, float(rec["s_coll"]))
            self._merge(inc, _ev("tgn_group", rec, seed=rec["u"], group_id=rec["group_id"], s_coll=rec.get("s_coll"),
                                 theta_coll=rec.get("theta_coll"),
                                 members=rec.get("members"), resources=rec.get("resources"),
                                 top_edges=(rec.get("top_edges") or [])[:5], paths=rec.get("paths"),
                                 escalation_id=rec.get("escalation_id"), model_version=rec.get("model_version")),
                        {m: inc.enforcement.get(m, "pending PDP decision") for m in rec.get("members") or []}, rec)
            return inc.id

    # ------------------------------------------------------------------ internals
    def _collective(self, db, group_id: str, rec: dict, users: List[str]) -> Incident:
        key = f"collective:{group_id}"
        inc = db.scalar(select(Incident).where(Incident.dedup_key == key))
        if inc is None:
            t = (_activity(rec) or (self.clock(), self.clock()))[1]
            new = set(users)
            for cand in db.scalars(select(Incident).where(Incident.kind == "collective", Incident.status.in_(OPEN_STATES))
                                   .order_by(Incident.created_at)):
                seen = {e.get("group_id") for e in cand.evidence} | {cand.group_id}
                overlap = len(new & set(cand.users)) >= max(1, min(len(new), len(cand.users))) / 2
                if group_id in seen or (overlap and t - cand.last_seen <= MERGE_WINDOW and cand.first_seen - t <= MERGE_WINDOW):
                    return cand
        if inc is None:
            inc = self._new(db, key, "collective", "critical",
                            f"Possible collusion: group of {len(users)} user(s)", users, rec)
            inc.group_id = group_id
            inc.s_coll = rec.get("s_coll")
        return inc

    def _new(self, db, key, kind, severity, title, users, rec) -> Incident:
        now = self.clock()
        t0, t1 = _activity(rec) or (now, now)
        inc = Incident(id=uuid.uuid4().hex, dedup_key=key, kind=kind, severity=severity, status="open", title=title,
                       users=sorted(set(users)), first_seen=t0, last_seen=t1, created_at=now, updated_at=now, evidence=[],
                       enforcement={}, source=_source(users, rec))
        db.add(inc)
        return inc

    def _merge(self, inc: Incident, ev: dict, enforcement: dict, rec: dict, activity: bool = True) -> None:
        if any(e.get("id") == ev["id"] for e in inc.evidence):
            return                                          # redelivered record: already merged
        inc.evidence = inc.evidence + [ev]
        users = sorted(set(inc.users) | set(enforcement))
        if inc.kind == "collective" and len(users) != len(inc.users):
            inc.title = f"Possible collusion: group of {len(users)} user(s)"
        inc.users = users
        inc.enforcement = {**inc.enforcement, **enforcement}
        span = _activity(rec) if activity else None
        if span:
            inc.first_seen, inc.last_seen = min(inc.first_seen, span[0]), max(inc.last_seen, span[1])
        inc.updated_at = self.clock()
        if inc.source == "live" and _source(users, rec) == "synthetic-replay":
            inc.source = "synthetic-replay"
        for f in ("evidence", "users", "enforcement"):
            flag_modified(inc, f)

"""Dashboard backend (Phase 9): REST + WebSocket over the real backend state.

Security: separate login for security staff (roles security_analyst / security_admin; monitored users cannot log
in), Argon2id passwords, Redis server-side sessions in their own namespace, cookie `as_dash` (HttpOnly,
SameSite=Strict), CSRF guard (every non-GET /api request must carry `X-Requested-With: accesssentinel`, which
cross-site forms cannot send), role checks (admin-only: settings, catalog, model promotion), audit log entries.

Data sources: Redis (risk tiers, decisions, escalation/PDP counters, edge index), PostgreSQL (incidents, feedback,
catalog, audit, settings history), Kafka (live feed, lag), HDFS (history, analyst-feedback), model registries.
Records of synthetic users ("syn-*") carry source = "synthetic-replay" and are labelled in the UI.
"""
import json
import os
import re
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from ... import runtime_settings as rs
from ...db.demo_catalog import SECURITY_ROLES
from ...db.models import (AclRule, Application, AuditLog, Feedback, Incident, PdpDecision, Resource, SettingsChange, User)
from ...security import hash_password, verify_password
from ..gateway.sessions import UNKNOWN_USER, SessionStore

COOKIE = "as_dash"
CSRF_HEADER, CSRF_VALUE = "x-requested-with", "accesssentinel"
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}$")
VERDICTS = ("confirmed_malicious", "false_positive", "benign_explained", "needs_more_info")


def _src(u: str) -> str:
    return "synthetic-replay" if u.startswith("syn-") else "live"


def incident_dict(i: Incident, full: bool = False) -> dict:
    d = {"id": i.id, "kind": i.kind, "severity": i.severity, "status": i.status, "verdict": i.verdict, "title": i.title,
         "users": i.users, "group_id": i.group_id, "first_seen": i.first_seen, "last_seen": i.last_seen,
         "created_at": i.created_at, "updated_at": i.updated_at, "max_s_ind": i.max_s_ind, "s_coll": i.s_coll,
         "source": i.source, "assigned_to": i.assigned_to, "enforcement": i.enforcement,
         "paths": sorted({p for e in i.evidence for p in (e.get("paths") or [])}), "evidence_count": len(i.evidence)}
    if full:
        d["evidence"] = i.evidence
    return d


def create_app(settings, engine, aredis, redis, live, probe, lake_factory: Callable, feedback_publisher: Callable,
               model_dir: str, pdp_factory: Callable, static_dir: Optional[str] = None, cleanup=None,
               revoke_sessions: Optional[Callable[[str, str], int]] = None) -> FastAPI:
    sessions = SessionStore(aredis, settings.session_idle_seconds, settings.session_max_seconds, prefix="dsess")

    @asynccontextmanager
    async def lifespan(_app):
        import asyncio
        if live is not None and hasattr(live, "start"):
            live.start(asyncio.get_running_loop())
        try:
            yield
        finally:
            if live is not None and hasattr(live, "stop"):
                live.stop()
            for fn in cleanup or []:
                try:
                    await fn()
                except Exception:  # noqa: BLE001
                    pass

    app = FastAPI(title="AccessSentinel Dashboard API", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    def audit(actor, action, target="", detail=None, ip=""):
        with Session(engine) as db, db.begin():
            db.add(AuditLog(actor=actor, action=action, target=target, ip=ip, detail=detail or {}))

    # ------------------------------------------------------------------ security plumbing
    @app.middleware("http")
    async def guard(request: Request, call_next):
        if request.url.path.startswith("/api/") and request.method not in ("GET", "HEAD", "OPTIONS"):
            if request.headers.get(CSRF_HEADER) != CSRF_VALUE:
                return JSONResponse({"error": "missing CSRF header"}, status_code=403)
        resp = await call_next(request)
        resp.headers.setdefault("X-Content-Type-Options", "nosniff")
        resp.headers.setdefault("X-Frame-Options", "DENY")
        resp.headers.setdefault("Referrer-Policy", "same-origin")
        resp.headers.setdefault("Content-Security-Policy",
                                "default-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss:; img-src 'self' data:")
        if request.url.path.startswith("/api/"):
            resp.headers.setdefault("Cache-Control", "no-store")
        return resp

    async def who(request: Request, admin: bool = False) -> dict:
        s = await sessions.get(request.cookies.get(COOKIE))
        if not s:
            raise HTTPException(401, "authentication required")
        if admin and s["role"] != "security_admin":
            raise HTTPException(403, "security_admin role required")
        await sessions.touch(s["ref"])
        return s

    async def body(request: Request) -> dict:
        try:
            d = await request.json()
        except Exception:  # noqa: BLE001
            raise HTTPException(400, "JSON body required") from None
        if not isinstance(d, dict):
            raise HTTPException(400, "JSON object required")
        return d

    def uid(u: str) -> str:
        if not isinstance(u, str) or not ID_RE.match(u):
            raise HTTPException(400, "invalid user id")
        return u

    # ------------------------------------------------------------------ auth
    @app.post("/api/auth/login")
    async def login(request: Request):
        d = await body(request)
        u, pw = d.get("username"), d.get("password")
        if not (isinstance(u, str) and ID_RE.match(u) and isinstance(pw, str) and 0 < len(pw) <= 256):
            raise HTTPException(400, "user id and password required")
        if await sessions.login_locked(u, settings.login_max_failures):
            raise HTTPException(429, "too many failed attempts")

        def lookup():
            with Session(engine) as db:
                x = db.get(User, u)
                return None if x is None else (x.password_hash, x.role, x.is_active, x.display_name)
        rec = await run_in_threadpool(lookup)
        ok = await run_in_threadpool(verify_password, rec[0] if rec else None, pw)
        ip = request.client.host if request.client else ""
        if not ok or not rec[2] or rec[1] not in SECURITY_ROLES:
            await sessions.login_failed(u, settings.login_lock_seconds)
            # never log an unrecognised user ID: it may be a password typed into the wrong field
            await run_in_threadpool(audit, u if rec else UNKNOWN_USER, "dashboard_login_failed", "", {}, ip)
            raise HTTPException(401, "invalid credentials or not a security account")
        await sessions.login_succeeded(u)
        token = await sessions.create(u, rec[1], ip, "")
        await run_in_threadpool(audit, u, "dashboard_login", "", {"role": rec[1]}, ip)
        resp = JSONResponse({"user": u, "role": rec[1], "name": rec[3]})
        resp.set_cookie(COOKIE, token, httponly=True, samesite="strict", secure=settings.cookie_secure,
                        max_age=settings.session_max_seconds, path="/")
        return resp

    @app.post("/api/auth/logout")
    async def logout(request: Request):
        s = await sessions.get(request.cookies.get(COOKIE))
        if s:
            await sessions.revoke(s["ref"], s["u"])
        resp = JSONResponse({"logged_out": True})
        resp.delete_cookie(COOKIE, path="/")
        return resp

    @app.get("/api/auth/me")
    async def me(request: Request):
        s = await who(request)
        return {"user": s["u"], "role": s["role"], "pdp_mode": settings.pdp_mode}

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    # ------------------------------------------------------------------ overview
    def risk_rows(limit=None, tier=None, source=None):
        rows = []
        for k in redis.scan_iter("risk:*", count=1000):
            k = k.decode() if isinstance(k, bytes) else k
            h = {(a.decode() if isinstance(a, bytes) else a): (b.decode() if isinstance(b, bytes) else b)
                 for a, b in redis.hgetall(k).items()}
            if not h.get("s_ind"):
                continue
            u = k.split(":", 1)[1]
            r = {"u": u, "s_ind": float(h["s_ind"]), "tier": h.get("tier"), "t": float(h.get("t") or 0),
                 "scored_at": float(h.get("scored_at") or 0), "trigger": h.get("trigger"),
                 "model_version": h.get("model_version"), "source": _src(u)}
            if (tier is None or r["tier"] == tier) and (source is None or r["source"] == source):
                rows.append(r)
        rows.sort(key=lambda r: -r["s_ind"])
        return rows[:limit] if limit else rows

    @app.get("/api/overview")
    async def overview(request: Request):
        await who(request)

        def compute():
            rows = risk_rows()
            tiers = {}
            for r in rows:
                key = (r["source"], r["tier"])
                tiers[key] = tiers.get(key, 0) + 1
            with Session(engine) as db:
                apps = db.scalar(select(func.count()).select_from(Application).where(Application.is_active.is_(True)))
                open_inc = db.execute(select(Incident.source, func.count()).where(Incident.status.in_(("open", "triaged")))
                                      .group_by(Incident.source)).all()
                cutoff = datetime.now(timezone.utc) - timedelta(days=1)
                revoked = db.scalar(select(func.count()).select_from(AuditLog)
                                    .where(AuditLog.action == "sessions_revoked", AuditLog.ts >= cutoff))
            blocked = [x.decode() if isinstance(x, bytes) else x for x in redis.zrange("pdp:blocked", 0, -1)]
            rates = live.rate_history() if live else []
            return {"monitored_users": {"live": sum(1 for r in rows if r["source"] == "live"),
                                        "synthetic-replay": sum(1 for r in rows if r["source"] != "live")},
                    "applications": apps,
                    "tiers": {f"{s}:{t}": n for (s, t), n in sorted(tiers.items())},
                    "events_per_second": rates[-1].get("access-events", 0.0) if rates else None,
                    "rate_history": [{"t": x["t"], "events": x.get("access-events", 0.0)} for x in rates[-120:]],
                    "open_incidents": {s: n for s, n in open_inc},
                    "blocked_users": {"live": sum(1 for u in blocked if _src(u) == "live"),
                                      "synthetic-replay": sum(1 for u in blocked if _src(u) != "live")},
                    "sessions_revoked_24h": revoked, "pdp_mode": settings.pdp_mode,
                    "recent_decisions": live.recent("decision", 10) if live else []}
        return await run_in_threadpool(compute)

    # ------------------------------------------------------------------ activity
    @app.get("/api/activity/recent")
    async def activity(request: Request, limit: int = 200, user: str = "", app_id: str = "", denied: bool = False,
                       source: str = ""):
        await who(request)
        pred = lambda e: ((not user or e["u"] == user) and (not app_id or e["a"] == app_id) and (not denied or e["y"] == 0)
                          and (not source or (e.get("meta") or {}).get("source", "live").startswith(source[:5])))
        return {"events": live.recent("event", min(limit, 1000), pred) if live else []}

    # ------------------------------------------------------------------ individual risk
    @app.get("/api/risk/users")
    async def risk_users(request: Request, tier: str = "", source: str = "", limit: int = 300):
        await who(request)
        return {"users": await run_in_threadpool(risk_rows, min(limit, 2000), tier or None, source or None)}

    @app.get("/api/risk/users/{u}")
    async def risk_user(u: str, request: Request, hours: float = 24.0, until: Optional[float] = None):
        await who(request)
        u = uid(u)

        def compute():
            t1 = until or time.time()
            t0 = t1 - min(hours, 24 * 30) * 3600
            lake = lake_factory()
            scores = [r["payload"] for r in lake.read_records("risk-scores", t0, t1 + 600, keys=[u]) if r["payload"].get("u") == u]
            scores += [s for s in (live.recent("score", 1000, lambda s: s["u"] == u) if live else [])
                       if s.get("scored_at", 0) > t0 and s["score_id"] not in {x.get("score_id") for x in scores}]
            events = lake.user_history(u, t0, t1)
            pdp = pdp_factory().status(u)
            return {"user": u, "source": _src(u), "window": [t0, t1], "decision": pdp,
                    "scores": sorted(({"t": s.get("t"), "scored_at": s.get("scored_at"), "s_ind": s["s_ind"], "tier": s.get("tier"),
                                       "trigger": s.get("trigger"), "model_version": s.get("model_version")} for s in scores),
                                     key=lambda s: s["t"] or 0),
                    "events": [{"t": e.t, "a": e.a, "r": e.r, "s": e.s, "o": e.o, "y": e.y,
                                "reason": e.meta.get("reason"), "ip": e.c.get("ip")} for e in events][-500:]}
        return await run_in_threadpool(compute)

    # ------------------------------------------------------------------ escalation + collusion
    @app.get("/api/escalations/recent")
    async def escalations(request: Request, limit: int = 100):
        await who(request)
        return {"escalations": live.recent("escalation", min(limit, 1000)) if live else []}

    @app.get("/api/collusion/groups")
    async def groups(request: Request, limit: int = 100, only_groups: bool = False):
        await who(request)
        recs = live.recent("group", 1000) if live else []
        if only_groups:
            recs = [r for r in recs if r.get("group_id")]
        return {"group_scores": recs[:limit]}

    @app.get("/api/collusion/graph")
    async def graph(request: Request, u: str, t: Optional[float] = None):
        await who(request)
        u = uid(u)

        def compute():
            from ...ml.tgn_graph import GraphParams, build_sample
            from ...ml.tgn_registry import TgnScorer
            from ...streaming.graph_index import RedisEdgeIndex
            try:
                scorer = TgnScorer.load(model_dir)
            except FileNotFoundError:
                scorer = None
            d3 = scorer is not None and hasattr(scorer, "gp")
            # v3 model (CMU + SGE): no D3 per-edge scorer to call here; the view shows the live neighbourhood (v3 has
            # no user-user edges through a shared application) and the user's latest v3 collective result (Eqs. 18-19)
            gp = scorer.gp if d3 else GraphParams(app_coaccess_weight=0.0)
            s = build_sample([u], RedisEdgeIndex(redis), t or time.time(), gp)
            scores = scorer.edge_scores(s) if (d3 and s.edges) else None
            if d3:
                analysis = scorer.analyse(s).get(u) if s.edges else None
            else:
                rec = next((g for g in (live.recent("group", 1000) if live else [])
                            if u in (g.get("members") or [g.get("u")])), None)
                m = ((rec or {}).get("member_scores") or {}).get(u) or {}
                analysis = rec and {"model_kind": rec.get("model_kind"), "group_id": rec.get("group_id"),
                                    "members": rec.get("members"), "s_coll": m.get("s_coll", rec.get("s_coll")),
                                    "A_u": m.get("A_u"), "theta_coll": rec.get("theta_coll"), "theta_A": rec.get("theta_A"),
                                    "t": rec.get("t")}
            nodes = ([{"id": f"u:{x}", "label": x, "type": "user", "seed": x == u} for x in s.users]
                     + [{"id": f"r:{x}", "label": x, "type": "resource"} for x in s.resources]
                     + [{"id": f"a:{x}", "label": x, "type": "application"} for x in s.apps])
            agg = {}
            for k, e in enumerate(s.edges):
                key = (e["u"], e["r"])
                a = agg.setdefault(key, {"source": f"u:{e['u']}", "target": f"r:{e['r']}", "type": "access", "count": 0,
                                         "sensitive": e["s"], "denied": 0, "score": 0.0, "last_t": 0.0})
                a["count"] += 1; a["denied"] += 1 - e["y"]; a["last_t"] = max(a["last_t"], e["t"])
                if scores is not None:
                    a["score"] = max(a["score"], float(scores[k]))
            edges = list(agg.values())
            edges += [{"source": f"r:{r}", "target": f"a:{r.split(':', 1)[0]}", "type": "membership"} for r in s.resources]
            for (i, j), c in s.co_res.items():
                edges.append({"source": f"u:{s.users[i]}", "target": f"u:{s.users[j]}", "type": "co-access", "count": c})
            return {"seed": u, "window": [s.t0, s.t1], "nodes": nodes, "edges": edges, "model_version": scorer.version if scorer else None,
                    "tau_edge": scorer.tau_edge if d3 else None, "analysis": analysis, "source": _src(u)}
        return await run_in_threadpool(compute)

    # ------------------------------------------------------------------ incidents + feedback
    @app.get("/api/incidents")
    async def incidents(request: Request, status: str = "", kind: str = "", source: str = "", limit: int = 200):
        await who(request)

        def q():
            with Session(engine) as db:
                st = select(Incident).order_by(Incident.created_at.desc()).limit(min(limit, 1000))
                if status:
                    st = st.where(Incident.status == status)
                if kind:
                    st = st.where(Incident.kind == kind)
                if source:
                    st = st.where(Incident.source == source)
                return [incident_dict(i) for i in db.scalars(st)]
        return {"incidents": await run_in_threadpool(q)}

    @app.get("/api/incidents/{iid}")
    async def incident(iid: str, request: Request):
        await who(request)

        def q():
            with Session(engine) as db:
                i = db.get(Incident, iid)
                if i is None:
                    return None
                fb_rows = db.scalars(select(Feedback).where(Feedback.incident_id == iid).order_by(Feedback.created_at)).all()
                return {**incident_dict(i, True), "feedback": [{"id": f.id, "verdict": f.verdict, "note": f.note,
                                                                 "analyst": f.analyst, "created_at": f.created_at} for f in fb_rows]}
        out = await run_in_threadpool(q)
        if out is None:
            raise HTTPException(404, "incident not found")
        return out

    @app.post("/api/incidents/{iid}/assign")
    async def assign(iid: str, request: Request):
        s = await who(request)

        def q():
            with Session(engine) as db, db.begin():
                i = db.get(Incident, iid)
                if i is None:
                    return False
                i.assigned_to, i.updated_at = s["u"], time.time()
                if i.status == "open":
                    i.status = "triaged"
                return True
        if not await run_in_threadpool(q):
            raise HTTPException(404, "incident not found")
        return {"assigned_to": s["u"]}

    def record_feedback(analyst, incident_id, kind, scope, verdict, users, t0, t1, note, group_id, source):
        fid = uuid.uuid4().hex
        now = time.time()
        payload = {"id": fid, "record_id": fid, "t": now, "incident_id": incident_id, "kind": kind, "scope": scope,
                   "verdict": verdict, "users": users, "window_t0": t0, "window_t1": t1, "note": note[:2000],
                   "analyst": analyst, "created_at": now, "group_id": group_id, "source": source, "schema_version": 1}
        feedback_publisher(payload)                       # Kafka analyst-feedback -> HDFS (training source)
        with Session(engine) as db, db.begin():
            db.add(Feedback(id=fid, incident_id=incident_id, kind=kind, scope=scope, verdict=verdict, users=users,
                            window_t0=t0, window_t1=t1, note=note[:2000], analyst=analyst, created_at=now,
                            group_id=group_id, source=source))
            if incident_id:
                i = db.get(Incident, incident_id)
                i.verdict, i.updated_at = verdict, now
                i.status = "triaged" if verdict == "needs_more_info" else "closed"
                i.assigned_to = i.assigned_to or analyst
            db.add(AuditLog(actor=analyst, action="analyst_feedback", target=incident_id or ",".join(users),
                            detail={"feedback_id": fid, "verdict": verdict, "scope": scope}))
        return payload

    @app.post("/api/incidents/{iid}/feedback")
    async def feedback(iid: str, request: Request):
        s = await who(request)
        d = await body(request)
        verdict, note = d.get("verdict"), str(d.get("note") or "")
        if verdict not in VERDICTS:
            raise HTTPException(400, f"verdict must be one of {VERDICTS}")

        def q():
            with Session(engine) as db:
                i = db.get(Incident, iid)
                return None if i is None else (i.kind, list(i.users), i.first_seen, i.last_seen, i.group_id, i.source)
        inc = await run_in_threadpool(q)
        if inc is None:
            raise HTTPException(404, "incident not found")
        kind, users, first, last, gid, source = inc
        payload = await run_in_threadpool(record_feedback, s["u"], iid, "incident_review", kind, verdict, users,
                                          first - 900.0, last + 300.0, note, gid, source)
        released = []
        if d.get("release") and verdict in ("false_positive", "benign_explained"):
            pdp = pdp_factory()
            for u in users:
                try:
                    if await run_in_threadpool(pdp.release, u, s["u"], f"feedback {payload['id']}: {verdict}"):
                        released.append(u)
                except Exception:  # noqa: BLE001 - policy-locked users stay blocked
                    pass
        return {"feedback": payload, "released": released}

    @app.post("/api/feedback/missed")
    async def missed(request: Request):
        s = await who(request)
        d = await body(request)
        users = [uid(u) for u in (d.get("users") or [])]
        scope = d.get("scope", "individual")
        try:
            t0, t1 = float(d["window_t0"]), float(d["window_t1"])
        except (KeyError, TypeError, ValueError):
            raise HTTPException(400, "window_t0 and window_t1 (epoch seconds) required") from None
        if not users or scope not in ("individual", "collective") or not (0 < t1 - t0 <= 30 * 86400):
            raise HTTPException(400, "users, scope individual|collective and a window of at most 30 days required")
        payload = await run_in_threadpool(record_feedback, s["u"], None, "missed_detection", scope, "confirmed_malicious",
                                          users, t0, t1, str(d.get("note") or ""), None,
                                          "synthetic-replay" if any(_src(u) != "live" for u in users) else "live")
        return {"feedback": payload}

    @app.get("/api/feedback")
    async def feedback_list(request: Request, limit: int = 200):
        await who(request)

        def q():
            with Session(engine) as db:
                return [{"id": f.id, "incident_id": f.incident_id, "kind": f.kind, "scope": f.scope, "verdict": f.verdict,
                         "users": f.users, "analyst": f.analyst, "created_at": f.created_at, "note": f.note, "source": f.source}
                        for f in db.scalars(select(Feedback).order_by(Feedback.created_at.desc()).limit(min(limit, 1000)))]
        return {"feedback": await run_in_threadpool(q)}

    @app.get("/api/retraining/dataset")
    async def retraining_dataset(request: Request):
        await who(request)

        def q():
            from ...ml import feedback_data as fbd
            rows = fbd.verified_feedback(lake_factory())
            return {"source": "HDFS analyst-feedback (analyst-verified verdicts only)", **fbd.summary(rows),
                    "records": [{k: r.get(k) for k in ("id", "scope", "verdict", "users", "incident_id", "analyst", "created_at", "source")}
                                for r in rows][-100:]}
        return await run_in_threadpool(q)

    # ------------------------------------------------------------------ monitored user accounts (admin)
    # Accounts of the employees who use the protected applications. Provisioned by a security admin (as an IdP /
    # HR feed would be in production); there is deliberately no self-registration. Security accounts
    # (security_analyst / security_admin) cannot be created or changed here. Passwords are never logged; a
    # generated password is returned once, in the response only.
    NEW_USER_RE = re.compile(r"^[a-z][a-z0-9._-]{2,31}$")

    def monitored_roles(db) -> List[str]:
        return sorted({r for r in db.scalars(select(AclRule.role).distinct())} - set(SECURITY_ROLES))

    def new_password(d: dict):
        pw = d.get("password")
        if pw in (None, ""):
            return secrets.token_urlsafe(12), True
        if not (isinstance(pw, str) and 12 <= len(pw) <= 128):
            raise HTTPException(400, "password must be 12-128 characters (or leave it empty to generate one)")
        return pw, False

    def end_sessions(u: str, reason: str) -> Optional[int]:
        if revoke_sessions is None:
            return None
        try:
            return revoke_sessions(u, reason)
        except Exception:  # noqa: BLE001 - the account change stands; the gateway re-checks is_active at login
            return None

    @app.get("/api/accounts")
    async def accounts(request: Request):
        await who(request)

        def q():
            with Session(engine) as db:
                users = db.scalars(select(User).where(User.role.not_in(SECURITY_ROLES)).order_by(User.id)).all()
                roles = monitored_roles(db)
                grants = {r: [f"{a.app_id}:{a.resource_key} ({a.permission})" for a in
                              db.scalars(select(AclRule).where(AclRule.role == r)
                                         .order_by(AclRule.app_id, AclRule.resource_key))]
                          for r in roles}
                out = []
                for x in users:
                    h = {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
                         for k, v in redis.hgetall(f"risk:{x.id}").items()}
                    out.append({"id": x.id, "name": x.display_name, "role": x.role, "department": x.department,
                                "active": x.is_active, "created_at": x.created_at.timestamp() if x.created_at else None,
                                "tier": h.get("tier"), "s_ind": float(h["s_ind"]) if h.get("s_ind") else None,
                                "source": _src(x.id)})
            return {"users": out, "roles": roles, "grants": grants}
        return await run_in_threadpool(q)

    @app.post("/api/accounts")
    async def create_account(request: Request):
        s = await who(request, admin=True)
        d = await body(request)
        new_id, role = d.get("id"), d.get("role")
        name, dept = str(d.get("name") or "").strip(), str(d.get("department") or "").strip()
        if not (isinstance(new_id, str) and NEW_USER_RE.match(new_id)) or not name:
            raise HTTPException(400, "user id (3-32 chars: lowercase letters, digits, . _ -; starts with a letter) "
                                     "and name required")
        pw, generated = new_password(d)
        h = await run_in_threadpool(hash_password, pw)

        def q():
            with Session(engine) as db, db.begin():
                if role not in monitored_roles(db):
                    return "role"
                if db.get(User, new_id) is not None:
                    return "exists"
                db.add(User(id=new_id, display_name=name[:200], role=role, department=dept[:100],
                            password_hash=h, is_active=True))
                db.add(AuditLog(actor=s["u"], action="user_created", target=new_id,
                                detail={"role": role, "department": dept[:100],
                                        "password": "generated" if generated else "set by admin"}))
            return None
        err = await run_in_threadpool(q)
        if err == "role":
            raise HTTPException(400, "role must be one of the monitored roles defined in the access catalog")
        if err == "exists":
            raise HTTPException(409, "user id already exists")
        return {"created": new_id, "role": role, "initial_password": pw if generated else None,
                "note": "The user can sign in at the gateway now. Monitoring starts with an empty history (cold start)."}

    async def change_account(request: Request, u: str, fn):
        s = await who(request, admin=True)
        u = uid(u)

        def q():
            with Session(engine) as db, db.begin():
                x = db.get(User, u)
                if x is None or x.role in SECURITY_ROLES:
                    return False
                fn(db, x, s["u"])
                return True
        if not await run_in_threadpool(q):
            raise HTTPException(404, "no monitored user with that id")
        return s

    @app.post("/api/accounts/{u}/deactivate")
    async def deactivate(u: str, request: Request):
        def fn(db, x, actor):
            x.is_active = False
            db.add(AuditLog(actor=actor, action="user_deactivated", target=x.id, detail={}))
        await change_account(request, u, fn)
        return {"deactivated": u, "sessions_ended": await run_in_threadpool(end_sessions, u, "account deactivated")}

    @app.post("/api/accounts/{u}/activate")
    async def activate(u: str, request: Request):
        def fn(db, x, actor):
            x.is_active = True
            db.add(AuditLog(actor=actor, action="user_activated", target=x.id, detail={}))
        await change_account(request, u, fn)
        return {"activated": u}

    @app.post("/api/accounts/{u}/reset-password")
    async def reset_password(u: str, request: Request):
        d = await body(request)
        pw, generated = new_password(d)
        h = await run_in_threadpool(hash_password, pw)

        def fn(db, x, actor):
            x.password_hash = h
            db.add(AuditLog(actor=actor, action="user_password_reset", target=x.id,
                            detail={"password": "generated" if generated else "set by admin"}))
        await change_account(request, u, fn)
        return {"reset": u, "initial_password": pw if generated else None,
                "sessions_ended": await run_in_threadpool(end_sessions, u, "password reset")}

    # ------------------------------------------------------------------ PDP actions
    @app.get("/api/users/{u}/decision")
    async def decision(u: str, request: Request):
        await who(request)
        return await run_in_threadpool(pdp_factory().status, uid(u))

    @app.post("/api/users/{u}/release")
    async def release(u: str, request: Request):
        s = await who(request)
        d = await body(request)
        from ...pdp_engine import PolicyLocked
        try:
            out = await run_in_threadpool(pdp_factory().release, uid(u), s["u"], str(d.get("note") or "")[:500])
        except PolicyLocked as e:
            raise HTTPException(409, str(e)) from None
        return {"decisions": out}

    @app.post("/api/users/{u}/block")
    async def block(u: str, request: Request):
        s = await who(request)
        d = await body(request)
        ttl = d.get("ttl")
        out = await run_in_threadpool(pdp_factory().block_manual, uid(u), s["u"], str(d.get("note") or "")[:500],
                                      float(ttl) if ttl else None)
        return {"decisions": out}

    @app.get("/api/decisions/audit")
    async def decisions_audit(request: Request, user: str = "", limit: int = 100):
        await who(request)

        def q():
            with Session(engine) as db:
                st = select(PdpDecision).order_by(PdpDecision.t.desc()).limit(min(limit, 1000))
                if user:
                    st = st.where(PdpDecision.user_id == uid(user))
                return [x.record for x in db.scalars(st)]
        return {"decisions": await run_in_threadpool(q)}

    # ------------------------------------------------------------------ system monitoring
    @app.get("/api/system")
    async def system(request: Request):
        await who(request)
        snap = await run_in_threadpool(probe.snapshot) if probe else {}
        groups = live.recent("group", 500) if live else []
        lat = sorted(g["inference_ms"] for g in groups if g.get("inference_ms") is not None)
        snap = dict(snap)
        snap["tgn_latency_ms"] = {"n": len(lat), "median": lat[len(lat) // 2] if lat else None,
                                  "p95": lat[int(0.95 * (len(lat) - 1))] if lat else None}
        snap["rates"] = live.rate_history()[-120:] if live else []
        return snap

    # ------------------------------------------------------------------ settings, catalog, models
    @app.get("/api/settings")
    async def get_settings_(request: Request):
        await who(request)

        def q():
            eff, exp = rs.effective(redis), rs.explicit(redis)
            return {"pdp_mode": settings.pdp_mode, "settings": [
                {"key": k, "value": eff[k], "default": spec[3], "explicit": k in exp, "type": spec[0].__name__,
                 "min": spec[1], "max": spec[2], "description": spec[4]} for k, spec in rs.SCHEMA.items()]}
        return await run_in_threadpool(q)

    @app.put("/api/settings")
    async def put_settings(request: Request):
        s = await who(request, admin=True)
        d = await body(request)

        def q():
            current = rs.effective(redis)
            try:
                updates = rs.validate(d, current)
            except rs.SettingsError as e:
                return None, str(e)
            v = rs.write(redis, updates)
            with Session(engine) as db, db.begin():
                for k, val in updates.items():
                    db.add(SettingsChange(key=k, old_value={"v": current.get(k)}, new_value={"v": val},
                                          changed_by=s["u"], changed_at=time.time()))
                db.add(AuditLog(actor=s["u"], action="settings_changed", target=",".join(updates), detail={"version": v}))
            return v, None
        v, err = await run_in_threadpool(q)
        if err:
            raise HTTPException(400, err)
        return {"version": v, "applied_within_seconds": 15}

    @app.get("/api/settings/history")
    async def settings_history(request: Request, limit: int = 100):
        await who(request)

        def q():
            with Session(engine) as db:
                return [{"key": c.key, "old": (c.old_value or {}).get("v"), "new": (c.new_value or {}).get("v"),
                         "changed_by": c.changed_by, "changed_at": c.changed_at}
                        for c in db.scalars(select(SettingsChange).order_by(SettingsChange.changed_at.desc()).limit(limit))]
        return {"history": await run_in_threadpool(q)}

    @app.get("/api/catalog")
    async def catalog(request: Request):
        await who(request)

        def q():
            with Session(engine) as db:
                apps = [{"id": a.id, "name": a.name, "base_url": a.base_url, "description": a.description, "active": a.is_active}
                        for a in db.scalars(select(Application).order_by(Application.id))]
                res = [{"app": r.app_id, "resource": r.resource_key, "sensitivity": r.sensitivity, "description": r.description}
                       for r in db.scalars(select(Resource).order_by(Resource.app_id, Resource.resource_key))]
                grants = db.scalar(select(func.count()).select_from(AclRule))
            return {"applications": apps, "resources": res, "acl_rules": grants}
        return await run_in_threadpool(q)

    @app.put("/api/catalog/resources/{app_id}/{res}")
    async def set_sensitivity(app_id: str, res: str, request: Request):
        s = await who(request, admin=True)
        d = await body(request)
        if d.get("sensitivity") not in (0, 1):
            raise HTTPException(400, "sensitivity must be 0 or 1")

        def q():
            with Session(engine) as db, db.begin():
                n = db.execute(update(Resource).where(Resource.app_id == app_id, Resource.resource_key == res)
                               .values(sensitivity=int(d["sensitivity"]))).rowcount
                if n:
                    db.add(AuditLog(actor=s["u"], action="sensitivity_changed", target=f"{app_id}:{res}",
                                    detail={"sensitivity": int(d["sensitivity"])}))
                return n
        if not await run_in_threadpool(q):
            raise HTTPException(404, "resource not found")
        return {"app": app_id, "resource": res, "sensitivity": int(d["sensitivity"]), "gateway_reload_seconds": settings.catalog_refresh_seconds}

    @app.post("/api/catalog/apps")
    async def register_app(request: Request):
        s = await who(request, admin=True)
        d = await body(request)
        aid, name, url = d.get("id"), str(d.get("name") or ""), str(d.get("base_url") or "")
        if not (isinstance(aid, str) and re.match(r"^[a-z][a-z0-9-]{1,31}$", aid)) or not name or not re.match(r"^https?://[A-Za-z0-9.-]+(:\d+)?/?$", url):
            raise HTTPException(400, "id (lowercase, 2-32 chars), name and base_url (http(s)://host[:port]) required")
        resources = d.get("resources") or []
        for r in resources:
            if not (isinstance(r.get("key"), str) and re.match(r"^[a-z][a-z0-9-]{1,63}$", r["key"]) and r.get("sensitivity") in (0, 1)):
                raise HTTPException(400, "each resource needs key (lowercase) and sensitivity 0|1")

        def q():
            with Session(engine) as db, db.begin():
                if db.get(Application, aid):
                    return False
                db.add(Application(id=aid, name=name[:200], base_url=url, description=str(d.get("description") or "")[:500]))
                db.flush()
                for r in resources:
                    db.add(Resource(app_id=aid, resource_key=r["key"], sensitivity=int(r["sensitivity"]),
                                    description=str(r.get("description") or "")[:500]))
                db.add(AuditLog(actor=s["u"], action="application_registered", target=aid, detail={"resources": len(resources)}))
                return True
        if not await run_in_threadpool(q):
            raise HTTPException(409, "application id already exists")
        return {"registered": aid, "note": "no ACL grants yet: every request is denied until grants exist (default deny)"}

    @app.get("/api/models")
    async def models(request: Request):
        await who(request)

        def q():
            out = {}
            for kind in ("tcn", "tgn"):
                d = os.path.join(model_dir, kind)
                cur = None
                try:
                    cur = open(os.path.join(d, "CURRENT"), encoding="utf-8").read().strip()
                except OSError:
                    pass
                versions = []
                for v in sorted(os.listdir(d)) if os.path.isdir(d) else []:
                    mpath = os.path.join(d, v, "manifest.json")
                    if not os.path.isfile(mpath):
                        continue
                    m = json.load(open(mpath, encoding="utf-8"))
                    versions.append({"version": v, "current": v == cur, "created_at": m.get("created_at"),
                                     "parent_version": m.get("parent_version"), "gate": (m.get("gate") or {}).get("passed"),
                                     "data": (m.get("data") or {}).get("source"),
                                     "test_summary": m.get("test_summary") or (m.get("evaluation") or {}).get("candidate")})
                out[kind] = {"current": cur, "versions": versions}
            return out
        return await run_in_threadpool(q)

    @app.post("/api/models/{kind}/promote")
    async def promote(kind: str, request: Request):
        s = await who(request, admin=True)
        d = await body(request)
        if kind not in ("tcn", "tgn") or not isinstance(d.get("version"), str) or not ID_RE.match(d["version"]):
            raise HTTPException(400, "kind tcn|tgn and a version are required")

        def q():
            from ...ml import registry as tcn_reg, tgn_registry
            (tcn_reg if kind == "tcn" else tgn_registry).promote(model_dir, d["version"])
            audit(s["u"], "model_promoted", f"{kind}:{d['version']}")
        try:
            await run_in_threadpool(q)
        except FileNotFoundError:
            raise HTTPException(404, "unknown model version") from None
        return {"promoted": d["version"], "workers_reload_within_seconds": 30}

    # ------------------------------------------------------------------ live websocket
    @app.websocket("/ws/live")
    async def ws_live(ws: WebSocket):
        s = await sessions.get(ws.cookies.get(COOKIE))
        if not s:
            await ws.close(code=4401)
            return
        await ws.accept()
        q = live.subscribe()
        try:
            while True:
                msg = await q.get()
                await ws.send_json(msg)
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            live.unsubscribe(q)

    # ------------------------------------------------------------------ static single-page app
    if static_dir and os.path.isdir(static_dir):
        @app.get("/{path:path}")
        async def spa(path: str):
            f = os.path.normpath(os.path.join(static_dir, path))
            if path and f.startswith(os.path.normpath(static_dir)) and os.path.isfile(f):
                return FileResponse(f)
            return FileResponse(os.path.join(static_dir, "index.html"))

    return app

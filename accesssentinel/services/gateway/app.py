"""Gateway HTTP application (FastAPI). Dependencies are injected so the whole request path is unit-testable.

Per proxied request (/apps/<a>/<path>):
  1. authenticate the session (Redis)                       -> 401 if missing / revoked / expired
  2. resolve resource r, operation o, required permission p   (catalog snapshot from PostgreSQL)
  3. request-time enforcement: cached PDP Block               -> 403   (never waits for Kafka/ML)
  4. first-line ACL (role-based, default deny)                -> 403
  5. build e_t = (u,t,a,r,s,p,o,c,y) and publish to Kafka      (non-blocking; allowed AND denied requests)
  6. forward allowed requests to the application with the internal upstream token and identity headers
Unauthenticated requests produce no access event (D3 monitors post-authentication behaviour); failed
logins go to the audit log instead.
"""
import asyncio
import logging
import re
import time
from contextlib import asynccontextmanager
from typing import Awaitable, Callable, List, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from redis.exceptions import RedisError
from starlette.concurrency import run_in_threadpool

from ...schema import make_event, new_id
from ...security import one_way_ref, tokens_equal, verify_password
from ...settings import Settings
from .access import BadRequest, Catalog, MethodNotAllowed, resolve
from .decisions import active_block
from .pages import home_page, login_page, message_page
from .sessions import SessionStore

COOKIE = "as_session"
USER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}$")
HEX32 = re.compile(r"^[0-9a-f]{32}$")
FORWARD_REQUEST_HEADERS = ("accept", "content-type", "accept-language")
FORWARD_RESPONSE_HEADERS = ("content-type", "content-disposition", "cache-control", "etag", "last-modified")
CSP = "default-src 'self'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'; form-action 'self'"

log = logging.getLogger("accesssentinel.gateway")


class BodyTooLarge(Exception):
    pass


def _wants_html(request: Request) -> bool:
    return "text/html" in request.headers.get("accept", "")


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "0.0.0.0"   # X-Forwarded-For is not trusted


async def _read_body(request: Request, limit: int) -> bytes:
    size, chunks = 0, []
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise BodyTooLarge()
        chunks.append(chunk)
    return b"".join(chunks)


def create_app(settings: Settings, store, redis, publisher, upstream: httpx.AsyncClient,
               cleanup: Optional[List[Callable[[], Awaitable[None]]]] = None) -> FastAPI:
    sessions = SessionStore(redis, settings.session_idle_seconds, settings.session_max_seconds)
    upstream_token = settings.secret("upstream_token")
    internal_token = settings.secret("internal_token")
    state = {"catalog": None, "catalog_loaded_at": None,
             "counts": {"requests": 0, "allowed": 0, "denied": 0, "unauthenticated": 0, "upstream_errors": 0}}

    async def load_catalog() -> None:
        try:
            state["catalog"] = await run_in_threadpool(store.load_catalog)
            state["catalog_loaded_at"] = time.time()
        except Exception as e:  # noqa: BLE001
            log.warning("catalog load failed: %s", type(e).__name__)

    async def refresh_loop() -> None:
        while True:
            await asyncio.sleep(settings.catalog_refresh_seconds if state["catalog"] else 2.0)
            await load_catalog()

    async def spool_replay_loop() -> None:
        while True:
            await asyncio.sleep(settings.spool_replay_seconds)
            try:
                res = await run_in_threadpool(publisher.replay_spool)
                if res.get("files"):
                    log.info("spool replay: %s", res)
            except Exception as e:  # noqa: BLE001
                log.warning("spool replay failed: %s", type(e).__name__)

    @asynccontextmanager
    async def lifespan(_app):
        await load_catalog()
        tasks = [asyncio.create_task(refresh_loop())]
        if hasattr(publisher, "replay_spool"):
            tasks.append(asyncio.create_task(spool_replay_loop()))
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            for fn in cleanup or []:
                try:
                    await fn()
                except Exception:  # noqa: BLE001
                    pass

    app = FastAPI(title="AccessSentinel Gateway", lifespan=lifespan, docs_url=None, redoc_url=None,
                  openapi_url=None)
    app.state.gw = state

    def audit(actor: str, action: str, **kw) -> Awaitable[None]:
        return run_in_threadpool(store.audit, actor, action, **kw)

    def reply(request: Request, status: int, title: str, message: str) -> Response:
        if _wants_html(request):
            return HTMLResponse(message_page(title, message, f"HTTP {status}"), status_code=status)
        return JSONResponse({"error": title, "detail": message}, status_code=status)

    # ----------------------------------------------------------------- middleware / errors
    @app.middleware("http")
    async def correlation_and_headers(request: Request, call_next):
        cid = request.headers.get("x-request-id", "").lower()
        request.state.cid = cid if HEX32.match(cid) else new_id()
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.cid
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "same-origin")
        response.headers.setdefault("Content-Security-Policy", CSP)
        response.headers.setdefault("Cache-Control", "no-store")
        return response

    @app.exception_handler(RedisError)
    async def redis_down(request: Request, exc: RedisError):
        # Sessions and PDP decisions live in Redis: without it the gateway cannot authorise -> fail closed.
        log.error("redis unavailable: %s", type(exc).__name__)
        return reply(request, 503, "Service unavailable", "The access-control state store is unavailable.")

    # ----------------------------------------------------------------- health
    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz():
        checks = {"catalog": "ok" if state["catalog"] else "not-loaded"}
        try:
            await redis.ping()
            checks["redis"] = "ok"
        except RedisError:
            checks["redis"] = "down"
        ready = all(v == "ok" for v in checks.values())
        checks["event_publisher"] = publisher.stats()
        return JSONResponse({"ready": ready, "checks": checks}, status_code=200 if ready else 503)

    # ----------------------------------------------------------------- pages / auth
    @app.get("/")
    async def home(request: Request):
        sess = await sessions.get(request.cookies.get(COOKIE))
        if not sess:
            return HTMLResponse(login_page())
        user = await run_in_threadpool(store.get_user, sess["u"])
        return HTMLResponse(home_page(sess["u"], user.display_name if user else sess["u"], sess["role"],
                                      state["catalog"] or Catalog()))

    @app.post("/auth/login")
    async def login(request: Request):
        is_json = request.headers.get("content-type", "").startswith("application/json")
        try:
            data = await request.json() if is_json else dict(await request.form())
        except Exception:  # noqa: BLE001
            data = {}
        username, password = data.get("username"), data.get("password")
        ip = _client_ip(request)

        def fail(status: int, message: str) -> Response:
            if is_json:
                return JSONResponse({"error": message}, status_code=status)
            return HTMLResponse(login_page(message), status_code=status)

        if not (isinstance(username, str) and USER_RE.match(username) and isinstance(password, str)
                and 1 <= len(password) <= 256):
            return fail(400, "Enter a valid user ID and password.")
        if await sessions.login_locked(username, settings.login_max_failures):
            await audit(username, "login_locked", ip=ip)
            return fail(429, "Too many failed attempts. Try again later.")
        user = await run_in_threadpool(store.get_user, username)
        ok = await run_in_threadpool(verify_password, user.password_hash if user else None, password)
        if not ok or not user.is_active:
            n = await sessions.login_failed(username, settings.login_lock_seconds)
            await audit(username, "login_failed", ip=ip, detail={"consecutive_failures": n})
            return fail(401, "Invalid user ID or password.")
        if await active_block(redis, user.id):
            await audit(user.id, "login_refused_blocked", ip=ip)
            return fail(403, "Access is suspended by the risk policy. Contact the security team.")
        await sessions.login_succeeded(user.id)
        device = one_way_ref(request.headers.get("user-agent", ""), 12)
        token = await sessions.create(user.id, user.role, ip, device)
        await audit(user.id, "login", ip=ip, detail={"role": user.role})
        resp: Response = (JSONResponse({"user": user.id, "role": user.role}) if is_json
                          else RedirectResponse("/", status_code=303))
        resp.set_cookie(COOKIE, token, httponly=True, samesite="lax", secure=settings.cookie_secure,
                        max_age=settings.session_max_seconds, path="/")
        return resp

    @app.post("/auth/logout")
    async def logout(request: Request):
        sess = await sessions.get(request.cookies.get(COOKIE))
        if sess:
            await sessions.revoke(sess["ref"], sess["u"])
            await audit(sess["u"], "logout", ip=_client_ip(request))
        is_json = request.headers.get("content-type", "").startswith("application/json")
        resp: Response = JSONResponse({"logged_out": True}) if is_json else RedirectResponse("/", status_code=303)
        resp.delete_cookie(COOKIE, path="/")
        return resp

    @app.get("/auth/me")
    async def me(request: Request):
        sess = await sessions.get(request.cookies.get(COOKIE))
        if not sess:
            return JSONResponse({"error": "authentication required"}, status_code=401)
        return {"user": sess["u"], "role": sess["role"], "session_requests": int(sess.get("requests", 0))}

    # ----------------------------------------------------------------- internal API (service-to-service)
    def internal_ok(request: Request) -> bool:
        return tokens_equal(request.headers.get("x-as-internal-token"), internal_token)

    @app.post("/internal/sessions/revoke")
    async def revoke(request: Request):
        """Session-time enforcement hook (used by the PDP in Phase 8): terminate every session of a user."""
        if not internal_ok(request):
            return JSONResponse({"error": "forbidden"}, status_code=403)
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            body = None
        user = body.get("user") if isinstance(body, dict) else None
        reason = str(body.get("reason", ""))[:200] if isinstance(body, dict) else ""
        if not (isinstance(user, str) and USER_RE.match(user)):
            return JSONResponse({"error": "invalid user"}, status_code=400)
        n = await sessions.revoke_user(user)
        await audit("system", "sessions_revoked", target=user, ip=_client_ip(request),
                    detail={"count": n, "reason": reason})
        return {"user": user, "revoked_sessions": n}

    @app.get("/internal/stats")
    async def stats(request: Request):
        if not internal_ok(request):
            return JSONResponse({"error": "forbidden"}, status_code=403)
        return {"counts": state["counts"], "event_publisher": publisher.stats(),
                "catalog_loaded_at": state["catalog_loaded_at"]}

    # ----------------------------------------------------------------- the protected applications
    @app.api_route("/apps/{app_id}/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def proxy(app_id: str, path: str, request: Request):
        t0 = time.perf_counter()
        cid = request.state.cid
        counts = state["counts"]
        counts["requests"] += 1
        sess = await sessions.get(request.cookies.get(COOKIE))
        if not sess:
            counts["unauthenticated"] += 1
            if request.method == "GET" and _wants_html(request):
                return RedirectResponse("/", status_code=303)
            return JSONResponse({"error": "authentication required"}, status_code=401)
        catalog: Optional[Catalog] = state["catalog"]
        if catalog is None:
            return reply(request, 503, "Service unavailable", "The access policy has not been loaded yet.")
        app_info = catalog.apps.get(app_id)
        if app_info is None:
            return reply(request, 404, "Not found", "Unknown application.")
        try:
            target = resolve(catalog, app_id, path, request.method)
        except BadRequest as e:
            return reply(request, 400, "Bad request", str(e))
        except MethodNotAllowed as e:
            return reply(request, 405, "Method not allowed", str(e))
        try:
            body = await _read_body(request, settings.max_body_bytes)
        except BodyTooLarge:
            return reply(request, 413, "Request too large", f"Limit is {settings.max_body_bytes} bytes.")

        u, role = sess["u"], sess["role"]
        n = await sessions.touch(sess["ref"])
        ctx = {"ip": _client_ip(request), "device": sess.get("device", ""),
               "session": "new" if n == 1 else "active", "session_ref": sess["ref"][:16]}
        if not target.known:
            allowed, reason, s = False, "unknown-resource", 1
        else:
            s = catalog.sensitivity[(app_id, target.resource)]
            block = await active_block(redis, u)
            if block:
                allowed, reason = False, f"pdp-block:{block.get('reason', 'unspecified')}"
            elif not catalog.allowed(app_id, target.resource, role, target.permission):
                allowed, reason = False, "acl-deny"
            else:
                allowed, reason = True, "allow"
        meta = {"method": request.method, "reason": reason, "role": role}
        if target.object_id:
            meta["object_id"] = target.object_id
        event = make_event(u, app_id, target.resource, s, target.operation, allowed, ctx, cid, meta)
        publisher.publish(event)                                   # never blocks on Kafka
        counts["allowed" if allowed else "denied"] += 1

        def done(status: int) -> None:
            log.info("cid=%s u=%s a=%s r=%s o=%s y=%d reason=%s status=%d ms=%.1f", cid, u, app_id,
                     target.resource, target.operation, int(allowed), reason, status,
                     (time.perf_counter() - t0) * 1000)

        if not allowed:
            if reason.startswith("pdp-block"):
                status, msg = 403, "Access is suspended by the continuous-monitoring risk policy."
            elif reason == "unknown-resource":
                status, msg = 404, "Unknown resource."
            else:
                status, msg = 403, (f"Your role '{role}' does not have '{target.permission}' permission on "
                                    f"{app_id}/{target.resource}.")
            done(status)
            return reply(request, status, "Access denied" if status == 403 else "Not found", msg)

        headers = {k: v for k in FORWARD_REQUEST_HEADERS if (v := request.headers.get(k))}
        headers.update({"X-AS-Gateway-Token": upstream_token, "X-AS-User": u, "X-AS-Role": role,
                        "X-Request-ID": cid})
        url = f"{app_info.base_url.rstrip('/')}/{path.lstrip('/')}"
        if request.url.query:
            url += f"?{request.url.query}"
        try:
            r = await upstream.request(request.method, url, content=body, headers=headers)
        except httpx.HTTPError as e:
            counts["upstream_errors"] += 1
            log.warning("cid=%s upstream %s failed: %s", cid, app_id, type(e).__name__)
            done(502)
            return reply(request, 502, "Application unavailable", f"{app_info.name} did not respond.")
        done(r.status_code)
        out = {k: r.headers[k] for k in FORWARD_RESPONSE_HEADERS if k in r.headers}
        return Response(content=r.content, status_code=r.status_code, headers=out)

    return app

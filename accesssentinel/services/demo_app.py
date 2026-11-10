"""Demonstration protected web application (one module, three deployments: AS_APP_ID = hr | finance | docs).

All records are deterministic FAKE data generated at start-up; changes are kept in memory only.

Trust model (deliberate and documented): the application performs no authorisation of its own. It is
reachable only on the internal Docker network and accepts a request only if it carries the gateway's
upstream token (X-AS-Gateway-Token); identity comes from X-AS-User / X-AS-Role set by the gateway.
This mirrors the setting D3 targets: first-line enforcement sits at the gateway, and abuse that the
first-line policy allows (e.g. a legitimate role exporting far more than usual) is left for the
continuous-monitoring pipeline to detect.

    AS_APP_ID=hr uvicorn --factory accesssentinel.services.demo_app:build --port 8000
"""
import csv
import io
import os
import random
from datetime import date, timedelta
from html import escape
from typing import Dict, List

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from ..db.demo_catalog import APPS, RESOURCES
from ..security import tokens_equal
from ..settings import Settings, get_settings
from .gateway.pages import page

_NAMES = ["Ana Souza", "Ben Carter", "Chai Wong", "Dara Nguyen", "Eli Rosen", "Fatima Khan", "Gus Berg",
          "Hana Sato", "Ian Moore", "Jin Park", "Kofi Mensah", "Lena Vogel", "Mia Rossi", "Noor Aziz"]
_DEPTS = ["Engineering", "Sales", "Finance", "Human Resources", "Legal", "IT", "Operations"]
_COMPANIES = ["Northwind Ltd", "Contoso Co", "Fabrikam Inc", "Tailspin Toys", "Litware LLC", "Adatum Corp"]

# resource -> field generators(rng, i) ; every record also gets "id"
_FIELDS = {
    "directory": {"name": lambda g, i: g.choice(_NAMES), "department": lambda g, i: g.choice(_DEPTS),
                  "extension": lambda g, i: str(1000 + i)},
    "employees": {"name": lambda g, i: g.choice(_NAMES), "department": lambda g, i: g.choice(_DEPTS),
                  "title": lambda g, i: g.choice(["Engineer", "Analyst", "Manager", "Specialist"]),
                  "start_date": lambda g, i: str(date(2015, 1, 1) + timedelta(days=g.randint(0, 3500)))},
    "payroll": {"name": lambda g, i: g.choice(_NAMES), "monthly_salary": lambda g, i: g.randint(40, 180) * 1000,
                "bank_account": lambda g, i: f"FAKE-{g.randint(1000, 9999)}-{g.randint(100000, 999999)}"},
    "reviews": {"name": lambda g, i: g.choice(_NAMES), "period": lambda g, i: g.choice(["2025H1", "2025H2"]),
                "rating": lambda g, i: g.randint(1, 5)},
    "recruiting": {"position": lambda g, i: g.choice(["Backend Engineer", "Accountant", "Recruiter"]),
                   "department": lambda g, i: g.choice(_DEPTS), "status": lambda g, i: g.choice(["open", "closed"])},
    "invoices": {"customer": lambda g, i: g.choice(_COMPANIES), "amount": lambda g, i: g.randint(500, 90000),
                 "status": lambda g, i: g.choice(["paid", "due", "overdue"])},
    "budget": {"department": lambda g, i: g.choice(_DEPTS), "year": lambda g, i: 2026,
               "allocated": lambda g, i: g.randint(100, 900) * 1000},
    "payments": {"payee": lambda g, i: g.choice(_COMPANIES), "amount": lambda g, i: g.randint(1000, 250000),
                 "method": lambda g, i: g.choice(["wire", "ach", "card"])},
    "vendor-banking": {"vendor": lambda g, i: g.choice(_COMPANIES),
                       "account_no": lambda g, i: f"FAKE-{g.randint(10**7, 10**8 - 1)}",
                       "bank": lambda g, i: g.choice(["Example Bank", "Sample Trust"])},
    "audit-reports": {"title": lambda g, i: f"Audit {2024 + i % 3}-{i:02d}", "findings": lambda g, i: g.randint(0, 9),
                      "status": lambda g, i: g.choice(["draft", "final"])},
    "handbook": {"section": lambda g, i: f"Section {i}", "title": lambda g, i: g.choice(
        ["Leave", "Travel", "Equipment", "Conduct"])},
    "policies": {"title": lambda g, i: g.choice(["Acceptable Use", "Data Retention", "Access Control"]),
                 "version": lambda g, i: f"{g.randint(1, 4)}.{g.randint(0, 9)}"},
    "contracts": {"counterparty": lambda g, i: g.choice(_COMPANIES), "value": lambda g, i: g.randint(10, 900) * 1000,
                  "expires": lambda g, i: str(date(2026, 1, 1) + timedelta(days=g.randint(0, 900)))},
    "board-minutes": {"meeting_date": lambda g, i: str(date(2025, 1, 15) + timedelta(days=30 * i)),
                      "topic": lambda g, i: g.choice(["Acquisition", "Budget", "Restructuring", "Strategy"])},
    "ip-filings": {"title": lambda g, i: f"Invention disclosure {i}", "filing_no": lambda g, i: f"FAKE-PAT-{9000 + i}",
                   "status": lambda g, i: g.choice(["pending", "granted"])},
}


def generate(app_id: str, n: int = 12) -> Dict[str, List[dict]]:
    data = {}
    for key, _, _ in RESOURCES[app_id]:
        rng = random.Random(f"{app_id}:{key}")
        data[key] = [{"id": str(i), **{f: gen(rng, i) for f, gen in _FIELDS[key].items()}} for i in range(1, n + 1)]
    return data


def _table(resource: str, items: List[dict], viewer: str) -> str:
    if not items:
        return "<p class=muted>No records.</p>"
    cols = list(items[0])
    head = "".join(f"<th>{escape(c)}</th>" for c in cols)
    rows = "".join("<tr>" + "".join(f"<td>{escape(str(it.get(c, '')))}</td>" for c in cols) + "</tr>" for it in items)
    return (f"<p class=muted>Viewing as {escape(viewer)}. <a href='{escape(resource)}/export'>Export CSV</a></p>"
            f"<div class=card><table><tr>{head}</tr>{rows}</table></div>")


def create_demo_app(app_id: str, settings: Settings) -> FastAPI:
    if app_id not in APPS:
        raise ValueError(f"unknown AS_APP_ID {app_id!r}; expected one of {sorted(APPS)}")
    name = APPS[app_id][0]
    token = settings.secret("upstream_token")
    data = generate(app_id)
    app = FastAPI(title=name, docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def only_via_gateway(request: Request, call_next):
        if request.url.path != "/healthz" and not tokens_equal(request.headers.get("x-as-gateway-token"), token):
            return JSONResponse({"error": "requests must come through the AccessSentinel gateway"}, status_code=401)
        return await call_next(request)

    def viewer(request: Request) -> str:
        return request.headers.get("x-as-user", "unknown")

    def records(resource: str):
        return data.get(resource)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok", "app": app_id}

    @app.get("/{resource}")
    async def list_items(resource: str, request: Request):
        items = records(resource)
        if items is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        if "text/html" in request.headers.get("accept", ""):
            body = (f"<h1>{escape(name)} &middot; {escape(resource)}</h1><p><a href='/'>All applications</a></p>"
                    + _table(resource, items, viewer(request)))
            return HTMLResponse(page(f"{name} - {resource}", body))
        return {"app": app_id, "resource": resource, "viewer": viewer(request), "count": len(items), "items": items}

    @app.get("/{resource}/export")
    async def export(resource: str, request: Request):
        items = records(resource)
        if items is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=list(items[0]) if items else ["id"])
        w.writeheader()
        w.writerows(items)
        return Response(buf.getvalue(), media_type="text/csv",
                        headers={"Content-Disposition": f'attachment; filename="{app_id}-{resource}.csv"'})

    @app.get("/{resource}/{item_id}")
    async def get_item(resource: str, item_id: str):
        for it in records(resource) or []:
            if it["id"] == item_id:
                return it
        return JSONResponse({"error": "not found"}, status_code=404)

    async def _payload(request: Request, resource: str):
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            return None
        allowed = set(_FIELDS[resource])
        if not isinstance(body, dict) or not body or set(body) - allowed:
            return None
        if any(not isinstance(v, (str, int, float)) or len(str(v)) > 200 for v in body.values()):
            return None
        return body

    @app.post("/{resource}")
    async def create(resource: str, request: Request):
        items = records(resource)
        if items is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        body = await _payload(request, resource)
        if body is None:
            return JSONResponse({"error": f"body must be a JSON object with fields {sorted(_FIELDS[resource])}"},
                                status_code=422)
        item = {"id": str(max((int(i["id"]) for i in items), default=0) + 1), **body}
        items.append(item)
        return JSONResponse(item, status_code=201)

    @app.api_route("/{resource}/{item_id}", methods=["PUT", "PATCH"])
    async def update(resource: str, item_id: str, request: Request):
        body = await _payload(request, resource) if records(resource) is not None else None
        for it in records(resource) or []:
            if it["id"] == item_id:
                if body is None:
                    return JSONResponse({"error": "invalid body"}, status_code=422)
                it.update(body)
                return it
        return JSONResponse({"error": "not found"}, status_code=404)

    @app.delete("/{resource}/{item_id}")
    async def delete(resource: str, item_id: str):
        items = records(resource)
        for k, it in enumerate(items or []):
            if it["id"] == item_id:
                items.pop(k)
                return Response(status_code=204)
        return JSONResponse({"error": "not found"}, status_code=404)

    return app


def build() -> FastAPI:
    return create_demo_app(os.environ.get("AS_APP_ID", ""), get_settings())

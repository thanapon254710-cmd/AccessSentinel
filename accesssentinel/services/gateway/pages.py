"""Minimal server-rendered pages for the gateway (login + application launcher). Every dynamic value is
HTML-escaped. The analyst dashboard is a separate React app (Phase 9)."""
from html import escape
from typing import Optional

from .access import Catalog

_CSS = """
:root{--bg:#f6f7f9;--fg:#1d2330;--muted:#5d6675;--card:#fff;--line:#dfe3ea;--accent:#2f5bd3;--ok:#1d7a46;--no:#b42318}
@media (prefers-color-scheme:dark){:root{--bg:#12151b;--fg:#e6e9ef;--muted:#9aa3b2;--card:#1b1f27;--line:#2c323d;--accent:#7b9cff;--ok:#5cc489;--no:#ff8a80}}
*{box-sizing:border-box}body{margin:0;font:15px/1.5 system-ui,sans-serif;background:var(--bg);color:var(--fg)}
main{max-width:980px;margin:0 auto;padding:24px 16px}h1{font-size:22px;margin:0 0 4px}h2{font-size:17px;margin:0 0 8px}
.muted{color:var(--muted)}.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin:16px 0}
table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:6px 8px;border-top:1px solid var(--line);vertical-align:top}
a{color:var(--accent)}.ok{color:var(--ok)}.no{color:var(--no)}.tag{font-size:12px;border:1px solid var(--line);border-radius:999px;padding:1px 8px}
input{font:inherit;padding:8px;border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--fg);width:100%;max-width:320px}
button{font:inherit;padding:8px 14px;border:0;border-radius:6px;background:var(--accent);color:#fff;cursor:pointer}
form.inline{display:inline}header{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
"""


def page(title: str, body: str) -> str:
    return (f"<!doctype html><html lang=en><head><meta charset=utf-8>"
            f"<meta name=viewport content='width=device-width,initial-scale=1'><title>{escape(title)}</title>"
            f"<style>{_CSS}</style></head><body><main>{body}</main></body></html>")


def login_page(error: Optional[str] = None) -> str:
    err = f"<p class=no>{escape(error)}</p>" if error else ""
    return page("AccessSentinel sign in", f"""
<h1>AccessSentinel Gateway</h1><p class=muted>Sign in once to reach every protected application.</p>
<div class=card><form method=post action="/auth/login">
<p><label>User ID<br><input name=username autocomplete=username required maxlength=128></label></p>
<p><label>Password<br><input name=password type=password autocomplete=current-password required maxlength=256></label></p>
{err}<button type=submit>Sign in</button></form></div>""")


def home_page(user: str, display: str, role: str, catalog: Catalog) -> str:
    cards = []
    for app_id in sorted(catalog.apps):
        app = catalog.apps[app_id]
        rows = []
        for r in catalog.resources(app_id):
            s = catalog.sensitivity[(app_id, r)]
            perms = sorted(catalog.permissions(app_id, r, role))
            granted = f"<span class=ok>{escape(', '.join(perms))}</span>" if perms else "<span class=no>none</span>"
            href = f"/apps/{escape(app_id)}/{escape(r)}"
            rows.append(f"<tr><td><a href='{href}'>{escape(r)}</a></td>"
                        f"<td>{'<span class=tag>sensitive</span>' if s else 'normal'}</td><td>{granted}</td>"
                        f"<td class=muted>{escape(catalog.descriptions.get((app_id, r), ''))}</td></tr>")
        cards.append(f"<div class=card><h2>{escape(app.name)} <span class=muted>/apps/{escape(app_id)}</span></h2>"
                     f"<p class=muted>{escape(app.description)}</p><table><tr><th>Resource</th><th>Sensitivity</th>"
                     f"<th>Your permissions</th><th>Description</th></tr>{''.join(rows)}</table></div>")
    return page("AccessSentinel applications", f"""
<header><div><h1>Protected applications</h1>
<p class=muted>Signed in as <b>{escape(display)}</b> ({escape(user)}, role {escape(role)}). Every request is
checked against the access policy and recorded for continuous monitoring.</p></div>
<form class=inline method=post action="/auth/logout"><button type=submit>Sign out</button></form></header>
{''.join(cards)}""")


def message_page(title: str, message: str, status_hint: str = "") -> str:
    return page(title, f"<h1>{escape(title)}</h1><div class=card><p>{escape(message)}</p>"
                        f"<p class=muted>{escape(status_hint)}</p><p><a href='/'>Back to applications</a></p></div>")

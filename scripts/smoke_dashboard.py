"""Phase 9 live checks against the running dashboard API. Run INSIDE Docker:

    docker compose run --rm -T tools python scripts/smoke_dashboard.py

  1. login: monitored users are refused; security_analyst accepted; cookie HttpOnly + SameSite=Strict; CSRF enforced
  2. every page's endpoint answers from backend state
  3. analyst feedback on an incident -> PostgreSQL + Kafka analyst-feedback -> HDFS -> retraining dataset
  4. RBAC: analyst cannot change settings; admin change is versioned and picked up by the running services

The analyst/admin password is read from the environment (AS_ANALYST_PASSWORD) and never printed.
Feedback is given on a SYNTHETIC incident (syn-* users) unless --incident is passed.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

from accesssentinel.adapters.hdfs_lake import make_lake  # noqa: E402
from accesssentinel.ml.feedback_data import verified_feedback  # noqa: E402
from accesssentinel.settings import get_settings  # noqa: E402

API = os.environ.get("AS_API_URL", "http://api:8080")
CSRF = {"X-Requested-With": "accesssentinel"}
results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    return ok


def client():
    return httpx.Client(base_url=API, timeout=30, headers=CSRF)


def login(c, user, pw):
    return c.post("/api/auth/login", json={"username": user, "password": pw})


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--incident", help="incident id to review (default: newest open synthetic individual incident)")
    ap.add_argument("--verdict", default="confirmed_malicious")
    ap.add_argument("--wait", type=float, default=90.0, help="seconds to wait for the HDFS sink flush")
    ap.add_argument("--settle", type=float, default=15.0, help="seconds between a settings change and its restore")
    args = ap.parse_args(argv)
    s = get_settings()
    if s.analyst_password is None:
        print("AS_ANALYST_PASSWORD is not set"); return 2
    pw = s.secret("analyst_password")

    print("1. authentication")
    with client() as c:
        if s.demo_user_password is not None:            # correct password, wrong role -> still refused
            check("monitored user (alice), correct password, cannot use the dashboard",
                  login(c, "alice", s.secret("demo_user_password")).status_code == 401)
        r = login(c, "analyst1", pw)
        sc = r.headers.get("set-cookie", "").lower()
        check("security analyst signs in", r.status_code == 200 and r.json()["role"] == "security_analyst")
        check("session cookie is HttpOnly + SameSite=Strict", "httponly" in sc and "samesite=strict" in sc)
        bare = httpx.post(f"{API}/api/incidents/x/assign", cookies=c.cookies, timeout=10)
        check("mutation without CSRF header refused", bare.status_code == 403, f"HTTP {bare.status_code}")

        print("2. pages read backend state")
        for path in ["/api/overview", "/api/activity/recent", "/api/risk/users", "/api/escalations/recent",
                     "/api/collusion/groups", "/api/incidents", "/api/system", "/api/settings", "/api/catalog",
                     "/api/models", "/api/feedback", "/api/retraining/dataset"]:
            r = c.get(path)
            check(f"GET {path}", r.status_code == 200, f"HTTP {r.status_code}")
        sysj = c.get("/api/system").json()
        check("system health probes", all(v == "ok" for v in sysj["health"].values()), str(sysj["health"]))
        check("HDFS reachable from the API", sysj["hdfs"].get("reachable") is True)
        users = c.get("/api/risk/users").json()["users"]
        if users:
            g = c.get("/api/collusion/graph", params={"u": users[0]["u"]})
            check("collusion graph for the top-risk user", g.status_code == 200 and len(g.json()["nodes"]) >= 1,
                  f"{users[0]['u']}: {len(g.json()['nodes'])} nodes, {len(g.json()['edges'])} edges, model {g.json()['model_version']}")

        print("3. analyst feedback -> HDFS -> retraining dataset")
        incs = c.get("/api/incidents", params={"status": "open"}).json()["incidents"]
        inc = next((i for i in incs if i["id"] == args.incident), None) if args.incident else \
            next((i for i in incs if i["source"] == "synthetic-replay" and i["kind"] == "individual"), None)
        if not check("an open incident to review", inc is not None):
            return 1
        before = len(verified_feedback(make_lake(s)))
        r = c.post(f"/api/incidents/{inc['id']}/feedback",
                   json={"verdict": args.verdict, "note": "Phase 9 smoke test (synthetic incident)"})
        ok = check("feedback accepted", r.status_code == 200, f"incident {inc['id'][:8]} {inc['users']} -> {args.verdict}")
        if not ok:
            print(r.text); return 1
        fid = r.json()["feedback"]["id"]
        d = c.get(f"/api/incidents/{inc['id']}").json()
        check("incident closed with the analyst verdict", d["status"] == "closed" and d["verdict"] == args.verdict)
        t_end, found = time.time() + args.wait, False
        while time.time() < t_end and not found:
            found = any(p["id"] == fid for p in verified_feedback(make_lake(s)))
            if not found:
                time.sleep(5)
        check("feedback record landed in HDFS analyst-feedback", found, f"{len(verified_feedback(make_lake(s)))} labelled (was {before})")
        ds = c.get("/api/retraining/dataset").json()
        check("feedback is in the next retraining dataset", fid in ds["feedback_ids"], str(ds["by_scope_verdict"]))

        print("4. settings RBAC and propagation")
        r = c.put("/api/settings", json={"escalation.gamma": 0.05})
        check("analyst cannot change settings", r.status_code == 403, f"HTTP {r.status_code}")
    with client() as a:
        check("security admin signs in", login(a, "secadmin", pw).status_code == 200)
        cur = {x["key"]: x["value"] for x in a.get("/api/settings").json()["settings"]}
        bad = a.put("/api/settings", json={"risk.theta_low": cur["risk.theta_high"]})
        check("invalid change rejected (theta_low >= theta_high)", bad.status_code == 400)
        k = cur["escalation.kappa"]
        r = a.put("/api/settings", json={"escalation.kappa": k + 1})
        check("admin change accepted and versioned", r.status_code == 200, f"kappa {k} -> {k + 1}, version {r.json().get('version')}")
        print(f"     waiting {args.settle:.0f} s so the services apply it (see `docker compose logs escalation`)")
        time.sleep(args.settle)
        r = a.put("/api/settings", json={"escalation.kappa": k})
        check("restored", r.status_code == 200, f"kappa -> {k}, version {r.json().get('version')}")
        hist = a.get("/api/settings/history").json()["history"]
        check("changes recorded in history", len(hist) >= 2 and hist[0]["changed_by"] == "secadmin")
    print(f"{sum(results)}/{len(results)} checks passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())

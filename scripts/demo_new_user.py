"""Presentation scenario: "create a NEW user, attack with it - does AccessSentinel detect it?"

Runs the whole scenario against the live stack, through the real gateway, with an account that did not exist
before (so the TCN has no history for it - a cold start). Run INSIDE Docker:

    docker compose run --rm -T tools python scripts/demo_new_user.py                 # account demo-<random>
    docker compose run --rm -T tools python scripts/demo_new_user.py --user mallory2  # pick the name
    docker compose run --rm -T tools python scripts/demo_new_user.py --attack enumerate

  1. secadmin creates the account through the dashboard API (password generated, kept in memory, never printed)
  2. the new user signs in at the gateway and works normally at human pace       -> should NOT be blocked
  3. the same user attacks:
       probe      requests resources the role is not allowed to use              (access-control probing)
       enumerate  walks through record ids one after another at machine speed  (scraping / enumeration)
  4. checks: suspended by the PDP, open session ended, new login refused, incident opened for the analyst

The account is left as it ends (blocked by the PDP, incident open) so it can be shown in the dashboard.
"""
import argparse
import json
import os
import secrets
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

from accesssentinel.settings import get_settings  # noqa: E402

SUSPENDED = "suspended by the continuous-monitoring risk policy"
NORMAL = ["/apps/hr/directory", "/apps/docs/handbook", "/apps/docs/policies", "/apps/hr/directory/4",
          "/apps/docs/handbook/2", "/apps/docs/policies/3", "/apps/hr/directory/7", "/apps/docs/handbook"]
PROBES = ["/apps/hr/payroll", "/apps/finance/payments", "/apps/hr/admin", "/apps/finance/backup", "/apps/docs/contracts",
          "/apps/hr/reviews", "/apps/docs/board-minutes", "/apps/finance/vendor-banking", "/apps/docs/config",
          "/apps/hr/export-all"]
results = []


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    return ok


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--user", default=f"demo-{secrets.token_hex(3)}")
    ap.add_argument("--role", default="staff")
    ap.add_argument("--attack", choices=["probe", "enumerate"], default="probe")
    ap.add_argument("--normal-pause", type=float, default=4.0, help="seconds between normal requests (human pace)")
    ap.add_argument("--attack-pause", type=float, default=None,
                    help="seconds between attack requests (default 0.2 probe / 0.05 enumerate; ~2-3 = a person clicking)")
    ap.add_argument("--timeout", type=float, default=90)
    ap.add_argument("--api", default=os.environ.get("AS_API_URL", "http://api:8080"))
    ap.add_argument("--gateway", default=os.environ.get("AS_GATEWAY_URL", "http://gateway:8000"))
    args = ap.parse_args(argv)
    s = get_settings()
    h = {"X-Requested-With": "accesssentinel"}

    print(f"1. security admin creates a brand-new account '{args.user}' (role {args.role})")
    adm = httpx.Client(base_url=args.api, headers=h, timeout=30)
    if adm.post("/api/auth/login", json={"username": "secadmin", "password": s.secret("analyst_password")}).status_code != 200:
        print("cannot sign in as secadmin (AS_ANALYST_PASSWORD)"); return 2
    r = adm.post("/api/accounts", json={"id": args.user, "name": f"Demo {args.user}", "department": "Demo", "role": args.role})
    if not check("account created", r.status_code == 200, f"HTTP {r.status_code} {r.text[:120] if r.status_code != 200 else ''}"):
        return 1
    pw = r.json()["initial_password"]                      # generated; used below, never printed

    print("2. the new user works normally (human pace)")
    gw = httpx.Client(base_url=args.gateway, timeout=10)
    st = gw.post("/auth/login", json={"username": args.user, "password": pw}).status_code
    check("new user signs in at the gateway", st == 200, f"HTTP {st}")
    codes = []
    for path in NORMAL:
        codes.append(gw.get(path).status_code)
        time.sleep(args.normal_pause)
    time.sleep(3)
    dec = adm.get(f"/api/users/{args.user}/decision").json()
    risk = adm.get(f"/api/risk/users?limit=2000").json()["users"]
    me = next((x for x in risk if x["u"] == args.user), {})
    check("normal work is NOT blocked", not dec.get("blocked") and all(c == 200 for c in codes),
          f"{len(codes)} requests {sorted(set(codes))}; S_ind {me.get('s_ind', 0):.3f} tier {me.get('tier')}")

    print(f"3. the same user attacks ({args.attack})")
    t0, n, hit = time.time(), 0, None
    while time.time() - t0 < args.timeout and hit is None:
        path = PROBES[n % len(PROBES)] if args.attack == "probe" else f"/apps/hr/directory/{n + 1}"
        rr = gw.get(path); n += 1
        # Two ways the block shows up, whichever is first: request-time enforcement (403 "suspended" page) or
        # session-time enforcement (the PDP had the gateway end the session -> 401). Confirmed below by the PDP.
        if (rr.status_code == 403 and SUSPENDED in rr.text) or rr.status_code == 401:
            hit = (n - 1, time.time() - t0, "request refused (403 suspended)" if rr.status_code == 403 else "session ended (401)")
        time.sleep(args.attack_pause if args.attack_pause is not None else (0.2 if args.attack == "probe" else 0.05))
    blocked = adm.get(f"/api/users/{args.user}/decision").json().get("blocked")
    check("attack detected and the account blocked", hit is not None and blocked,
          f"after {hit[0]} attack requests, {hit[1]:.1f} s; first sign: {hit[2]}" if hit else f"not blocked after {n} requests")

    print("4. enforcement and analyst view")
    time.sleep(3)
    rr = gw.get("/apps/hr/directory")
    check("open session ended", rr.status_code == 401, f"old cookie -> HTTP {rr.status_code}")
    st2 = httpx.post(f"{args.gateway}/auth/login", json={"username": args.user, "password": pw}, timeout=10).status_code
    check("new login refused while blocked", st2 == 403, f"HTTP {st2}")
    dec = adm.get(f"/api/users/{args.user}/decision").json()
    basis = dec.get("basis")
    if isinstance(basis, str):                              # stored as JSON text in the decision cache
        try:
            basis = json.loads(basis)
        except ValueError:
            basis = {}
    s_ind = (basis or {}).get("s_ind")
    check("PDP decision", dec.get("blocked"),
          f"reason={dec.get('reason')}" + (f", S_ind={s_ind:.2f} >= theta_high {(basis or {}).get('theta_high')}" if s_ind else ""))
    inc = None
    for _ in range(10):
        inc = next((i for i in adm.get("/api/incidents?status=open").json()["incidents"] if args.user in i["users"]), None)
        if inc:
            break
        time.sleep(2)
    check("incident opened for the analyst", inc is not None,
          f"{inc['title']} (severity {inc['severity']})" if inc else "none")
    print(f"{sum(results)}/{len(results)} checks passed. Dashboard: http://localhost:8080/#/risk/{args.user}")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())

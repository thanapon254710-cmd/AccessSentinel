"""Phase 8 live checks: enforcement through the real gateway. Run INSIDE Docker with the PDP in enforce mode:

    docker compose run --rm -T tools python scripts/smoke_pdp.py individual   # ivan: browse, then probe -> blocked
    docker compose run --rm -T tools python scripts/smoke_pdp.py expiry       # henry: manual block that expires
    docker compose run --rm -T tools python scripts/smoke_pdp.py policy       # dave: policy block, release refused
    docker compose run --rm -T tools python scripts/smoke_pdp.py group        # last TGN group above theta -> all blocked

The demo user's monitoring state is reset first (scripts/reset_demo_state.py) so earlier test traffic does not
decide the outcome.
"""
import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from accesssentinel.settings import get_settings  # noqa: E402

SUSPENDED = "suspended by the continuous-monitoring risk policy"


def sh(*args) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, *args], capture_output=True, text=True, timeout=120)


def admin(*args):
    return sh("-m", "accesssentinel.pdp_admin", *args)


def client(gw):
    import httpx
    return httpx.Client(base_url=gw, timeout=10)


def login(gw, user, pw):
    c = client(gw)
    r = c.post("/auth/login", json={"username": user, "password": pw})
    return c, r.status_code


def suspended(r) -> bool:
    return r.status_code == 403 and SUSPENDED in r.text


def report(results):
    for ok, what, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {what}  [{detail}]")
    failed = sum(1 for ok, _, _ in results if not ok)
    print(f"{len(results) - failed}/{len(results)} checks passed")
    return 1 if failed else 0


def audit(user, limit=10):
    out = admin("audit", user, "--limit", str(limit))
    return json.loads(out.stdout) if out.returncode == 0 else []


def individual(s, args):
    import redis as redis_lib
    pw, gw, u = s.secret("demo_user_password"), args.gateway, "ivan"
    sh("scripts/reset_demo_state.py", u)
    admin("release", u, "--by", "smoke-test", "--note", "reset before test", "--grace", "0")
    res = []
    c, st = login(gw, u, pw)
    res.append((st == 200, "ivan (staff) logs in", f"HTTP {st}"))
    normal = []
    for path in ["/apps/hr/directory", "/apps/docs/handbook", "/apps/docs/policies", "/apps/hr/directory/4", "/apps/docs/handbook/2"]:
        normal.append(c.get(path).status_code); time.sleep(args.pause)
    time.sleep(3)
    pre = json.loads(admin("status", u).stdout)
    res.append((not pre.get("blocked"), "normal browsing at human pace is not blocked",
                f"statuses {normal}; PDP blocked={pre.get('blocked')} tier={redis_lib.Redis(**s.redis_kwargs(), decode_responses=True).hget('risk:' + u, 'tier')}"))
    probes = ["/apps/hr/payroll", "/apps/finance/payments", "/apps/hr/admin", "/apps/finance/backup", "/apps/docs/contracts",
              "/apps/hr/reviews", "/apps/docs/board-minutes", "/apps/finance/vendor-banking", "/apps/docs/config", "/apps/hr/export-all"]
    t0, n, hit, statuses = time.time(), 0, None, []
    while time.time() - t0 < args.timeout and hit is None:
        r = c.get(probes[n % len(probes)]); n += 1; statuses.append(r.status_code)
        if suspended(r):
            hit = (n, time.time() - t0)
        time.sleep(0.2)
    res.append((hit is not None, "probing is suspended by the PDP (request-time enforcement)",
                f"after {hit[0] - 1} attack requests, {hit[1]:.1f} s (exposure window)" if hit else f"not blocked after {n} requests"))
    time.sleep(3)
    st = json.loads(admin("status", u).stdout)
    res.append((st.get("blocked") and st.get("reason") in ("individual", "collective"), "decision cached for the gateway",
                f"reason={st.get('reason')} expires_at={st.get('expires_at')}"))
    r = c.get("/apps/hr/directory")
    res.append((r.status_code == 401, "active session terminated (session-time enforcement)", f"old cookie -> HTTP {r.status_code}"))
    _, st2 = login(gw, u, pw)
    res.append((st2 == 403, "new login refused while blocked", f"HTTP {st2}"))
    time.sleep(20)
    res.append((json.loads(admin("status", u).stdout).get("blocked"), "still blocked 20 s later (sticky)", "no release, no expiry"))
    a = audit(u)
    res.append((any(x["decision"] == "Block" and x["actor"] == "pdp" for x in a), "Block in the PostgreSQL audit trail",
                str([(x["transition"], x["reason"]) for x in a[:3]])))
    rel = admin("release", u, "--by", "analyst-demo", "--note", "smoke test: confirmed test traffic")
    c3, st3 = login(gw, u, pw)
    ok = st3 == 200 and c3.get("/apps/hr/directory").status_code == 200
    res.append((rel.returncode == 0 and ok, "analyst release restores access", f"login HTTP {st3}"))
    return report(res)


def expiry(s, args):
    pw, gw, u = s.secret("demo_user_password"), args.gateway, "henry"
    sh("scripts/reset_demo_state.py", u)
    c, _ = login(gw, u, pw)
    before = c.get("/apps/docs/policies").status_code
    admin("block", u, "--by", "analyst-demo", "--note", "smoke test: 20 s block", "--ttl", "20")
    time.sleep(2)
    during = c.get("/apps/docs/policies")
    c2, st = login(gw, u, pw)
    time.sleep(25)
    c3, st3 = login(gw, u, pw)
    after = c3.get("/apps/docs/policies").status_code
    time.sleep(5)
    a = audit(u)
    return report([(before == 200, "henry allowed before", f"HTTP {before}"),
                   (suspended(during) or during.status_code == 401, "manual block enforced", f"HTTP {during.status_code}; login HTTP {st}"),
                   (st3 == 200 and after == 200, "access returns after the 20 s expiry", f"login {st3}, request {after}"),
                   (any(x["transition"] == "expire" for x in a), "expiry recorded by the PDP", str([(x["transition"], x["reason"]) for x in a[:3]]))])


def policy(s, args):
    pw, gw, u = s.secret("demo_user_password"), args.gateway, "dave"
    sh("scripts/reset_demo_state.py", u)
    pb = admin("policy-block", u, "--by", "hr-admin", "--reason", "smoke test: offboarding")
    time.sleep(2)
    c, st = login(gw, u, pw)
    rel = admin("release", u, "--by", "analyst-demo")
    still = json.loads(admin("status", u).stdout)
    un = admin("policy-unblock", u, "--by", "hr-admin")
    c2, st2 = login(gw, u, pw)
    ok_after = st2 == 200 and c2.get("/apps/hr/directory").status_code == 200
    return report([(pb.returncode == 0 and st == 403, "policy block: login refused", f"HTTP {st}"),
                   (rel.returncode == 3 and still.get("blocked") and still.get("reason") == "policy",
                    "analyst release refused for a policy block", rel.stderr.strip()[:120]),
                   (un.returncode == 0 and ok_after, "lifting the policy restores access", f"login HTTP {st2}")])


def group(s, args):
    import redis as redis_lib
    r = redis_lib.Redis(**s.redis_kwargs(), decode_responses=True)
    out = sh("scripts/smoke_tgn.py", "collusion", "--index", str(args.index))
    flagged = []
    for k in r.scan_iter("coll:syn-tgn-*"):
        h = r.hgetall(k)
        if h.get("above_theta_coll") == "1":
            flagged.append((float(h["t"]), h["group_id"], json.loads(h["members"])))
    if not flagged:
        print(out.stdout[-1500:]); return report([(False, "a TGN group above theta_coll exists", "none")])
    t, gid, members = max(flagged)
    time.sleep(5)
    st = {m: json.loads(admin("status", m).stdout) for m in members}
    blocked = all(v.get("blocked") for v in st.values())
    coll = {m: v.get("reason") == "collective" or "collective" in json.loads(v.get("also") or "[]") for m, v in st.items()}
    return report([(True, "TGN flagged a group", f"group {gid}, {len(members)} members"),
                   (blocked, "every member of g(u) blocked (group-wide enforcement)",
                    ", ".join(f"{m.split('-', 3)[-1]}:{v.get('reason')}" for m, v in st.items())),
                   (all(coll.values()), "collective evidence recorded for every member (block or evidence transition)",
                    ", ".join(f"{m.split('-', 3)[-1]}:{'yes' if ok else 'no'}" for m, ok in coll.items()))])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario", choices=["individual", "expiry", "policy", "group"])
    ap.add_argument("--gateway", default=os.environ.get("AS_GATEWAY_URL", "http://gateway:8000"))
    ap.add_argument("--timeout", type=float, default=90)
    ap.add_argument("--pause", type=float, default=4.0)
    ap.add_argument("--index", type=int, default=5)
    args = ap.parse_args(argv)
    return {"individual": individual, "expiry": expiry, "policy": policy, "group": group}[args.scenario](get_settings(), args)


if __name__ == "__main__":
    sys.exit(main())

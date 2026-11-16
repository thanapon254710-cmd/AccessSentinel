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


def sh(*args, timeout: float = 120) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, *args], capture_output=True, text=True, timeout=timeout)


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
        # the block shows up as request-time enforcement (403 "suspended") or, if the PDP's session revocation
        # wins the race, as session-time enforcement (401): either counts, the PDP decision is checked below
        if suspended(r) or r.status_code == 401:
            hit = (n, time.time() - t0)
        time.sleep(0.2)
    res.append((hit is not None, "probing is stopped by the PDP (403 suspended or session ended)",
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
    res.append((any(x["decision"] in ("Terminate", "Block") and x["actor"] == "pdp" for x in a),
                "Terminate / Block in the PostgreSQL audit trail",
                str([(x["decision"], x["transition"], x["reason"]) for x in a[:3]])))
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
    from accesssentinel.pdp_engine import RANK, collective_action
    since = time.time()
    out = sh("scripts/smoke_tgn.py", "collusion", "--index", str(args.index), "--timeout", "1800", timeout=2400)
    flagged = []
    for k in r.scan_iter("coll:syn-tgn-*"):
        h = r.hgetall(k)
        if h.get("above_theta_coll") == "1" and float(h["t"]) >= since:
            flagged.append((float(h["t"]), h["group_id"], json.loads(h["members"])))
    if not flagged:
        print(out.stdout[-1500:]); return report([(False, "a TGN group above theta_C exists", "none")])
    t, gid, members = max(flagged)
    time.sleep(5)
    st = {m: json.loads(admin("status", m).stdout) for m in members}
    exp, got = {}, {}
    for m in members:                                    # Eq. 21 per member from its own S_coll_u and A_u
        h = r.hgetall(f"coll:{m}")
        f = lambda k: float(h[k]) if h.get(k) not in (None, "") else None
        exp[m] = collective_action(f("s_coll"), f("A_u"), f("theta_coll") or 1.0, f("theta_A")) or "Allow"
        got[m] = st[m].get("action", "Allow")
    # A member already under a MORE severe action (e.g. Terminate) keeps it: the collective evidence is recorded.
    ok = all(got[m] == exp[m] or RANK.get(got[m], 0) > RANK[exp[m]] for m in members)
    acted = [m for m in members if exp[m] != "Allow"]
    coll = {m: st[m].get("reason") == "collective" or "collective" in json.loads(st[m].get("also") or "[]") for m in acted}
    short = lambda m: m.split("-", 3)[-1]
    return report([(True, "TGN flagged a group", f"group {gid}, {len(members)} members"),
                   (ok and bool(acted), "every member gets its Eq. 21 action (Restrict if A_u >= theta_A, else StepUp)",
                    ", ".join(f"{short(m)}:{got[m]} (expected {exp[m]})" for m in members)),
                   (all(coll.values()), "collective evidence recorded for every acted-on member",
                    ", ".join(f"{short(m)}:{'yes' if v else 'no'}" for m, v in coll.items()))])


# ============================================================================================================
# Revised v3 Phase 7: the six-action PDP end to end (score on Kafka -> PDP -> decision:<u> -> gateway)
#     docker compose run --rm -T tools python scripts/smoke_pdp.py actions
#     docker compose stop pdp && sleep 35 && docker compose run --rm -T tools python scripts/smoke_pdp.py fallback
#     docker compose start pdp
# ============================================================================================================
def _accounts(s, args, n, role="hr_manager"):
    """n fresh synthetic accounts (syn-act-*) through the admin API; passwords are generated, never printed."""
    import httpx
    adm = httpx.Client(base_url=args.api, headers={"X-Requested-With": "accesssentinel"}, timeout=30)
    if adm.post("/api/auth/login", json={"username": "secadmin", "password": s.secret("analyst_password")}).status_code != 200:
        raise SystemExit("cannot sign in as secadmin (AS_ANALYST_PASSWORD)")
    tag, out = os.urandom(3).hex(), []
    for i in range(n):
        u = f"syn-act-{tag}-{i}"
        r = adm.post("/api/accounts", json={"id": u, "name": f"Phase 7 {u}", "department": "Test", "role": role})
        if r.status_code != 200:
            raise SystemExit(f"account {u}: HTTP {r.status_code}")
        out.append((u, r.json()["initial_password"]))
    return out


def _produce(s, topic, rec):
    from confluent_kafka import Producer
    from accesssentinel.topics import topic_name
    pr = Producer({"bootstrap.servers": s.kafka_bootstrap, "enable.idempotence": True, "acks": "all"})
    t0 = time.time()                                       # before produce: the PDP may consume before flush returns
    pr.produce(topic_name(topic, s.kafka_topic_prefix), key=rec["u"].encode(), value=json.dumps(rec).encode())
    assert pr.flush(10) == 0
    return t0


def _wait(fn, timeout=30.0, step=0.05):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = fn()
        if v:
            return v, time.time()
        time.sleep(step)
    return None, time.time()


def actions(s, args):
    import uuid
    import redis as redis_lib
    from accesssentinel.topics import GROUP_SCORES, RISK_SCORES
    r = redis_lib.Redis(**s.redis_kwargs(), decode_responses=True)
    gw = args.gateway
    (ua, pa), (ub, pb), (uc, pc) = _accounts(s, args, 3)
    res = []
    ca, _ = login(gw, ua, pa); cb, _ = login(gw, ub, pb); cc, _ = login(gw, uc, pc)
    base = [ca.get("/apps/hr/payroll").status_code, cb.get("/apps/hr/directory").status_code, cc.get("/apps/hr/directory").status_code]
    res.append((base == [200, 200, 200], "three fresh hr_manager accounts work normally", f"{base}"))

    # --- collective: Restrict (A_u >= theta_A) and StepUp (A_u < theta_A) from one v3 group score
    now = time.time()
    g = {"score_id": f"coll:smoke-{uuid.uuid4().hex[:8]}", "schema_version": 2, "u": ua, "t": now, "anchor_t": now,
         "window": [now - 3600, now], "s_coll": 0.95, "above_theta_coll": True, "theta_coll": 0.8609, "theta_A": 0.3643,
         "group_id": uuid.uuid4().hex[:16], "members": [ua, ub], "model_kind": "tgn-v3", "model_version": "smoke-phase7",
         "member_scores": {ua: {"s_coll": 0.95, "A_u": 0.80}, ub: {"s_coll": 0.92, "A_u": 0.10}}, "source": "synthetic-replay"}
    t_g = _produce(s, GROUP_SCORES, g)
    # Poll the decision cache the gateway reads (no gateway traffic: repeated denied requests are themselves
    # anomalous and the live TCN would rightly score them), then check enforcement with ONE request.
    hit, t_hit = _wait(lambda: r.hget(f"decision:{ua}", "decision") == "Restrict", 30, 0.005)
    res.append((bool(hit), "group score -> Restrict for the core member (A_u >= theta_A)",
                f"{(t_hit - t_g) * 1000:.0f} ms from group score to the gateway's decision cache" if hit else "not decided in 30 s"))
    r0 = ca.get("/apps/hr/directory")                                     # non-sensitive first ...
    r1 = ca.get("/apps/hr/payroll")                                       # ... then ONE sensitive attempt
    res.append((r0.status_code == 200 and r1.status_code == 403 and "restricted" in r1.text,
                "Restrict at the gateway: s = 0 allowed, s = 1 refused", f"directory {r0.status_code}, payroll {r1.status_code}"))
    time.sleep(3)
    st_a = json.loads(admin("status", ua).stdout)
    res.append((True, "observed: live TCN reaction to the refused sensitive attempt (reported, not asserted)",
                f"action now {st_a.get('action')} (reason {st_a.get('reason')})"))
    st_b, _ = _wait(lambda: r.hget(f"decision:{ub}", "decision") == "StepUp", 15, 0.02)
    rb = cb.get("/apps/hr/directory")
    step = rb.status_code == 401 and rb.json().get("step_up") is True
    res.append((bool(st_b) and step, "StepUp for the peripheral member (A_u < theta_A): request needs re-authentication",
                f"HTTP {rb.status_code}"))
    cb2, stl = login(gw, ub, pb)
    rb2 = cb2.get("/apps/hr/directory").status_code
    res.append((stl == 200 and rb2 == 200, "signing in again satisfies the StepUp", f"login {stl}, directory {rb2}"))

    # --- psi: an older decision written later is rejected by the gateway (no sensitive traffic needed)
    key = f"decision:{ub}"
    orig = r.hgetall(key)
    r.hset(key, mapping={"decision": "Terminate", "reason": "stale-writer", "psi": int(orig["psi"]) - 1,
                         "decided_at": time.time(), "expires_at": ""})
    stale = cb2.get("/apps/hr/directory").status_code
    r.delete(key); r.hset(key, mapping=orig)                              # restore the PDP's state
    res.append((stale == 200, "stale decision (psi <= psi_cur) rejected: an older Terminate is not applied",
                f"directory HTTP {stale}"))

    # --- individual: Throttle band, then Terminate (on a user whose own traffic stays light)
    def score(u, v):
        t_ = time.time()
        return {"score_id": f"smoke-{uuid.uuid4().hex[:8]}", "u": u, "s_ind": v, "theta_high": 0.0482, "theta_B": 0.0849,
                "theta_low": 0.0284, "scored_at": t_, "t": t_, "trigger": "event", "model_version": "smoke-phase7",
                "model_kind": "tcn-v3", "source": "synthetic-replay"}
    t_s = _produce(s, RISK_SCORES, score(uc, 0.07))
    st_c, t_c = _wait(lambda: r.hget(f"decision:{uc}", "decision") == "Throttle", 15, 0.005)
    res.append((bool(st_c) and cc.get("/apps/hr/directory").status_code == 200, "theta_high <= S_ind < theta_B -> Throttle (requests within the rate pass)",
                f"{(t_c - t_s) * 1000:.0f} ms to the decision cache"))
    t_s2 = _produce(s, RISK_SCORES, score(uc, 0.40))
    hit, t_hit = _wait(lambda: r.hget(f"decision:{uc}", "decision") == "Terminate", 15, 0.005)
    rr = cc.get("/apps/hr/directory").status_code
    res.append((bool(hit) and rr in (401, 403), "S_ind >= theta_B escalates Throttle -> Terminate",
                f"{(t_hit - t_s2) * 1000:.0f} ms to the decision cache; next request HTTP {rr}"))
    _, st_l = login(gw, uc, pc)
    res.append((st_l == 403, "Terminate refuses new logins", f"HTTP {st_l}"))
    a = audit(uc)
    blocks = [x["decision"] for x in a if x["transition"] == "block"]   # later evidence (e.g. the TGN) may follow
    res.append((blocks[:2] == ["Terminate", "Throttle"], "audit trail: Throttle then Terminate (psi ordered)",
                str([(x["decision"], x["transition"], x["reason"]) for x in a[:4]])))
    # --- Throttle rate limit (separate user: a burst of requests is itself anomalous for the TCN)
    (ud, pd), = _accounts(s, args, 1)
    cd, _ = login(gw, ud, pd)
    _produce(s, RISK_SCORES, score(ud, 0.07))
    _wait(lambda: r.hget(f"decision:{ud}", "decision") == "Throttle", 15, 0.005)
    codes = [cd.get("/apps/hr/directory").status_code for _ in range(s.throttle_per_minute + 5)]
    res.append((429 in codes and codes.count(200) <= s.throttle_per_minute, "Throttle: requests above the per-minute rate get HTTP 429",
                f"{codes.count(200)} x 200, {codes.count(429)} x 429, other {[c for c in codes if c not in (200, 429)]} "
                f"(limit {s.throttle_per_minute}/min)"))
    for u in (ua, ub, uc, ud):
        admin("release", u, "--by", "smoke-test", "--note", "Phase 7 smoke cleanup", "--grace", "0")
    return report(res)


def fallback(s, args):
    """Run while the PDP is STOPPED for longer than the gateway's decision TTL."""
    import httpx
    (u, pw), = _accounts(s, args, 1)
    rd = httpx.get(f"{args.gateway}/readyz", timeout=10).json()
    res = [(str(rd["checks"].get("pdp", "")).startswith("unavailable"), "gateway sees the PDP as unavailable (heartbeat > TTL)",
            str(rd["checks"].get("pdp")))]
    c, st = login(gw := args.gateway, u, pw)
    r1, r0 = c.get("/apps/hr/payroll"), c.get("/apps/hr/directory")
    res.append((st == 200, "login still works (authentication does not need the PDP)", f"HTTP {st}"))
    res.append((r1.status_code == 403 and "risk decision service is unavailable" in r1.text, "s = 1 fails closed (Restrict)",
                f"payroll HTTP {r1.status_code}"))
    res.append((r0.status_code == 200, "s = 0 fails open (Allow, first-line ACL still applies)", f"directory HTTP {r0.status_code}"))
    return report(res)



def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario", choices=["individual", "expiry", "policy", "group", "actions", "fallback"])
    ap.add_argument("--gateway", default=os.environ.get("AS_GATEWAY_URL", "http://gateway:8000"))
    ap.add_argument("--timeout", type=float, default=90)
    ap.add_argument("--pause", type=float, default=4.0)
    ap.add_argument("--index", type=int, default=5)
    ap.add_argument("--api", default=os.environ.get("AS_API_URL", "http://api:8080"))
    args = ap.parse_args(argv)
    return {"individual": individual, "expiry": expiry, "policy": policy, "group": group, "actions": actions,
            "fallback": fallback}[args.scenario](get_settings(), args)


if __name__ == "__main__":
    sys.exit(main())

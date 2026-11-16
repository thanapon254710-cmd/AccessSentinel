"""Experiment G - enforcement (Deliverable 4 Section V-G) on the LIVE stack.

    docker compose run --rm -T tools python -m evaluation.exp_g eq21 --n 200
    docker compose run --rm -T tools python -m evaluation.exp_g psi --n 20
    docker compose run --rm -T tools python -m evaluation.exp_g ttl
    docker compose run --rm -T tools python -m evaluation.exp_g policy --runs 3
    # fallback: the host stops / starts the PDP while the probe runs (see evaluation/README.md):
    docker compose run --rm -T tools python -m evaluation.exp_g fallback-probe --seconds 120

eq21      N random cases (S_ind, S_coll_u, A_u around the calibrated thresholds) injected on Kafka for fresh synthetic
          users (risk score and/or v3 group score); the live decision:<u> is compared with the pure Eq. 21 mapping
          (pdp_engine.decide) and the propagation delay produce -> decision cache is recorded.
psi       stale-overwrite checks on a live account: a decision with psi <= psi_cur written into the cache is never
          applied by the gateway (Table IV "no stale overwrite").
ttl       Terminate with the runtime setting pdp.block_ttl = 60 s: time from expires_at to the Allow (expire) decision
          and to the gateway letting the user log in again; the setting is restored afterwards.
policy    policy precedence: a policy Block replaces an ML Terminate, an analyst release is refused, lifting the
          policy restores access.
fallback  gateway behaviour per 0.5 s while the PDP is stopped and restarted: onset of the sensitivity fallback
          relative to the stop (decision TTL), recovery after the restart.
Raw output: results/raw/exp_g/<cmd>[-run<k>].json
"""
import argparse
import json
import os
import random
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

from . import env_info
from .live import SUSPENDED, accounts, admin_client, pct, produce, settings, write

TH = {"theta_high": 0.0482, "theta_B": 0.0849, "theta_low": 0.0284}       # promoted TCN tcn3-...-0c7a4e (Eq. 8)
TC = {"theta_coll": 0.8609, "theta_A": 0.3643}                              # promoted TGN tgn3-...-ccc61d


def _redis(s):
    import redis as redis_lib
    return redis_lib.Redis(**s.redis_kwargs(), decode_responses=True)


def _admin(*a):
    return subprocess.run([sys.executable, "-m", "accesssentinel.pdp_admin", *a], capture_output=True, text=True, timeout=120)


def _wait(fn, timeout=20.0, step=0.005):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = fn()
        if v:
            return v, time.time()
        time.sleep(step)
    return None, time.time()


def _score(u, v):
    t = time.time()
    return {"score_id": f"expg-{uuid.uuid4().hex[:10]}", "u": u, "s_ind": v, **TH, "scored_at": t, "t": t,
            "trigger": "event", "model_version": "exp-g", "model_kind": "tcn-v3", "source": "synthetic-replay"}


def _group(u, s_coll, a_u):
    t = time.time()
    return {"score_id": f"coll:expg-{uuid.uuid4().hex[:10]}", "schema_version": 2, "u": u, "t": t, "anchor_t": t,
            "window": [t - 3600, t], "s_coll": s_coll, "above_theta_coll": s_coll >= TC["theta_coll"], **TC,
            "group_id": uuid.uuid4().hex[:16], "members": [u], "model_kind": "tgn-v3", "model_version": "exp-g",
            "member_scores": {u: {"s_coll": s_coll, "A_u": a_u}}, "source": "synthetic-replay"}


def cmd_eq21(s, args):
    from accesssentinel.pdp_engine import decide
    r, rng = _redis(s), random.Random(args.seed)
    rows = []
    for i in range(args.n):
        u = f"syn-eq21-{uuid.uuid4().hex[:8]}"
        s_ind = rng.choice([None, rng.uniform(0, 0.04), rng.uniform(0.0482, 0.0849), rng.uniform(0.0849, 0.6),
                            rng.choice([0.0482, 0.0849])])
        coll = rng.random() < 0.5
        s_coll = rng.choice([rng.uniform(0.5, 0.86), rng.uniform(0.8609, 1.0), 0.8609]) if coll else None
        a_u = rng.choice([rng.uniform(0, 0.3643), rng.uniform(0.3643, 1.0), 0.3643]) if coll else None
        if s_ind is None and not coll:
            s_ind = rng.uniform(0, 0.6)
        exp = decide(False, s_ind, TH["theta_high"], TH["theta_B"], s_coll, a_u, TC["theta_coll"], TC["theta_A"])
        t0 = time.time()
        if s_ind is not None:
            produce(s, "risk-scores", _score(u, s_ind))
        if coll:
            produce(s, "group-scores", _group(u, s_coll, a_u))
        want = None if exp == "Allow" else exp
        got, t1 = _wait(lambda: r.hget(f"decision:{u}", "decision") == want if want else None, 10 if want else 1.5)
        act = r.hget(f"decision:{u}", "decision") or "Allow"
        rows.append({"u": u, "s_ind": s_ind, "s_coll": s_coll, "A_u": a_u, "expected": exp, "live": act,
                     "match": act == exp, "propagation_ms": (t1 - t0) * 1000 if want and got else None})
    ok = sum(x["match"] for x in rows)
    by = {}
    for x in rows:
        d = by.setdefault(x["expected"], {"cases": 0, "match": 0}); d["cases"] += 1; d["match"] += int(x["match"])
    out = {"cases": len(rows), "match": ok, "by_expected_action": by,
           "propagation_ms": pct([x["propagation_ms"] for x in rows]), "rows": rows}
    print(f"Eq. 21 live vs pure mapping: {ok}/{len(rows)} match; propagation {out['propagation_ms']}")
    for u in {x["u"] for x in rows if x["live"] != "Allow"}:
        _admin("release", u, "--by", "exp-g", "--note", "experiment cleanup", "--grace", "0")
    return out


def cmd_psi(s, args):
    r = _redis(s)
    (u, pw), = accounts(s, args.api, 1, "staff", "psi")
    import httpx
    c = httpx.Client(base_url=args.gateway, timeout=10)
    c.post("/auth/login", json={"username": u, "password": pw})
    produce(s, "group-scores", _group(u, 0.95, 0.10))                        # StepUp ...
    _wait(lambda: r.hget(f"decision:{u}", "decision") == "StepUp", 15)
    c.get("/apps/hr/directory")                                              # ... ends this session
    c = httpx.Client(base_url=args.gateway, timeout=10)
    c.post("/auth/login", json={"username": u, "password": pw})              # re-authenticated: StepUp satisfied
    base = c.get("/apps/hr/directory").status_code
    rows = []
    for i in range(args.n):
        key = f"decision:{u}"
        orig = r.hgetall(key)
        stale = {"decision": random.choice(["Terminate", "Block"]), "reason": "stale-writer",
                 "psi": int(orig["psi"]) - random.randint(1, 1000), "decided_at": time.time(), "expires_at": ""}
        r.hset(key, mapping=stale)
        st = c.get("/apps/hr/directory").status_code
        r.delete(key); r.hset(key, mapping=orig)
        rows.append({"stale_action": stale["decision"], "status": st, "rejected": st == 200})
    stats = c.get("/internal/stats", headers={"X-AS-Internal-Token": s.secret("internal_token")})
    _admin("release", u, "--by", "exp-g", "--note", "experiment cleanup", "--grace", "0")
    out = {"baseline_status": base, "trials": len(rows), "rejected": sum(x["rejected"] for x in rows), "rows": rows,
           "gateway_stats": stats.json() if stats.status_code == 200 else None}
    print(f"psi: {out['rejected']}/{out['trials']} stale decisions rejected")
    return out


def cmd_ttl(s, args):
    r = _redis(s)
    adm = admin_client(s, args.api)
    before = {x["key"]: x for x in adm.get("/api/settings").json()["settings"]}["pdp.block_ttl"]
    assert adm.put("/api/settings", json={"pdp.block_ttl": 60}).status_code == 200
    time.sleep(20)                                                           # applied within 15 s
    (u, pw), = accounts(s, args.api, 1, "staff", "ttl")
    produce(s, "risk-scores", _score(u, 0.5))
    got, t_dec = _wait(lambda: r.hget(f"decision:{u}", "decision") == "Terminate", 20)
    if not got:
        raise RuntimeError(f"ttl: no Terminate for {u} within 20 s (PDP lagging or down) - rerun")
    exp = float(r.hget(f"decision:{u}", "expires_at"))
    import httpx
    st_during = httpx.post(f"{args.gateway}/auth/login", json={"username": u, "password": pw}, timeout=10).status_code
    _, t_allow = _wait(lambda: r.hget(f"decision:{u}", "decision") == "Allow", 180, 0.05)
    t_login = None
    while time.time() < exp + 120:
        if httpx.post(f"{args.gateway}/auth/login", json={"username": u, "password": pw}, timeout=10).status_code == 200:
            t_login = time.time(); break
        time.sleep(0.1)
    adm.put("/api/settings", json={"pdp.block_ttl": before["value"]})
    out = {"ttl_s": 60, "decision_to_expiry_s": exp - t_dec, "login_during": st_during,
           "expire_decision_after_expiry_s": t_allow - exp, "gateway_login_after_expiry_s": (t_login - exp) if t_login else None,
           "restored_block_ttl": before["value"]}
    print(f"ttl: {out}")
    return out


def cmd_policy(s, args):
    import httpx
    r = _redis(s)
    rows = []
    for k in range(args.runs):
        (u, pw), = accounts(s, args.api, 1, "staff", "pol")
        produce(s, "risk-scores", _score(u, 0.5))
        _wait(lambda: r.hget(f"decision:{u}", "decision") == "Terminate", 20)
        t0 = time.time()
        pb = _admin("policy-block", u, "--by", "hr-admin", "--reason", "experiment G: offboarding")
        got, t1 = _wait(lambda: r.hget(f"decision:{u}", "reason") == "policy", 60, 0.05)
        rel = _admin("release", u, "--by", "analyst-exp")
        still = r.hget(f"decision:{u}", "decision")
        produce(s, "risk-scores", _score(u, 0.01))                           # low score: must not lift a policy Block
        time.sleep(2)
        still2 = r.hget(f"decision:{u}", "decision")
        _admin("policy-unblock", u, "--by", "hr-admin")
        lifted, t2 = _wait(lambda: r.hget(f"decision:{u}", "decision") == "Allow", 60, 0.05)
        st = httpx.post(f"{args.gateway}/auth/login", json={"username": u, "password": pw}, timeout=10).status_code
        rows.append({"policy_block_rc": pb.returncode, "policy_replaced_terminate": bool(got),
                     "policy_apply_s": t1 - t0, "analyst_release_refused": rel.returncode == 3 and still == "Block",
                     "low_score_kept_block": still2 == "Block", "lift_restores": bool(lifted) and st == 200,
                     "login_after_lift": st})
    out = {"runs": rows, "all_ok": all(x["policy_replaced_terminate"] and x["analyst_release_refused"]
                                       and x["low_score_kept_block"] and x["lift_restores"] for x in rows)}
    print(f"policy precedence: {out['all_ok']} ({len(rows)} runs)")
    return out


def cmd_fallback_probe(s, args):
    import httpx
    (u, pw), = accounts(s, args.api, 1, "hr_manager", "fb")
    # The live TCN acts on a fresh hr_manager reading payroll (cold start, Phase 7), which hid the fallback in the
    # first probe (401 from second 1). An analyst release opens a grace period in which no ML evidence can act on
    # the user (pdp_engine._overridden), so the probe sees only the PDP-availability behaviour.
    r = _redis(s)
    produce(s, "risk-scores", _score(u, 0.5))
    got, _ = _wait(lambda: r.hget(f"decision:{u}", "decision") == "Terminate", 30)
    rel = _admin("release", u, "--by", "exp-g", "--note", "fallback probe: grace against ML actions", "--grace",
                 str(args.seconds + 600))
    if not got or rel.returncode != 0:
        raise RuntimeError(f"fallback probe set-up failed (terminate={bool(got)}, release rc={rel.returncode})")
    time.sleep(2)
    c = httpx.Client(base_url=args.gateway, timeout=10)
    c.post("/auth/login", json={"username": u, "password": pw})
    rows, t_end = [], time.time() + args.seconds
    while time.time() < t_end:
        t = time.time()
        try:
            ready = httpx.get(f"{args.gateway}/readyz", timeout=5).json()["checks"].get("pdp")
        except Exception:  # noqa: BLE001
            ready = None
        r1 = c.get("/apps/hr/payroll"); r0 = c.get("/apps/hr/directory")
        rows.append({"t": t, "pdp": ready, "s1": r1.status_code, "s1_fallback": "risk decision service is unavailable" in r1.text,
                     "s0": r0.status_code})
        time.sleep(0.5)
    print(f"fallback probe: {len(rows)} samples; fallback samples {sum(x['s1_fallback'] for x in rows)}")
    return {"user_role": "hr_manager", "rows": rows}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["eq21", "psi", "ttl", "policy", "fallback-probe"])
    ap.add_argument("--n", type=int, default=200)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--seconds", type=float, default=120)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--tag", default="")
    ap.add_argument("--out", default="evaluation/results/raw/exp_g")
    ap.add_argument("--gateway", default=os.environ.get("AS_GATEWAY_URL", "http://gateway:8000"))
    ap.add_argument("--api", default=os.environ.get("AS_API_URL", "http://api:8080"))
    args = ap.parse_args(argv)
    s = settings()
    started = datetime.now(timezone.utc).isoformat()
    fn = {"eq21": cmd_eq21, "psi": cmd_psi, "ttl": cmd_ttl, "policy": cmd_policy, "fallback-probe": cmd_fallback_probe}[args.cmd]
    res = fn(s, args)
    write(f"{args.out}/{args.cmd}{('-' + args.tag) if args.tag else ''}.json",
          {"experiment": "G", "cmd": args.cmd, "started_at": started, "finished_at": datetime.now(timezone.utc).isoformat(),
           "thresholds": {**TH, **TC}, "result": res, "environment": env_info.collect()})
    return 0


if __name__ == "__main__":
    sys.exit(main())

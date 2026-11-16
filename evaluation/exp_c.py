"""Experiments C (latency, Eqs. 31-34) and D (throughput / lag) on the LIVE stack, generated load.

    docker compose run --rm -T tools python -m evaluation.exp_c --rates 5 20 50 --runs 3 --duration 120

Per (rate R, run k): n_bg fresh staff accounts send background requests through the gateway at a total Poisson rate
R (permitted, non-sensitive resources); n_att fresh accounts start probing at random instants at lambda_u = 1 /
attack_pause requests/s. Afterwards the run's records are read from Kafka (risk-scores with per-stage trace,
pdp-decisions, escalations.realtime, group-scores) from offsets taken before the run and joined per event:

  Eq. 31 stages  Lat_gw = ts_gw_pub - ts_gw_in; Lat_kafka = ts_seq_in - ts_gw_pub; Lat_reorder = reorder_wait;
                 Lat_TCN-q = ts_tcn_dequeue - ts_seq_out; Lat_TCN = ts_tcn_scored - ts_tcn_dequeue;
                 Lat_PDP = decision.t - scored_at; Lat_push = 0 for request-time enforcement (the gateway reads the
                 decision cache on every request; session revocation is measured separately); Lat_enf = first
                 refused request - decision.t (client side, same host clock)
  Lat_ind        decision-ready: decision.t - ts_gw_in(trigger event)   (end-to-end from ONE trace, never a sum
                 of stage percentiles); observed: first refused request - ts_gw_in(trigger)
  Eq. 32         Lat_esc = escalation.t - scored_at(trigger); TGN = group_score.t - escalation.t (incl. graph-state
                 reads and queueing); Lat_coll(to group score) = group_score.t - ts_gw_in(trigger)
  Eq. 33         N_exposed = attacker requests sent after the trigger request and before the first PDP refusal
  Eq. 34         empirical P(Lat_ind <= x) for x in 0.5 / 1 / 2 / 5 s (v3 gives no Lat_SLO value)
  Exp. D         processed score rate, gateway request latency and outcome mix, consumer lag sampled every 5 s.
All timestamps come from one host (Docker Desktop VM), so no cross-host clock skew is involved (not measured).
Raw output: results/raw/exp_c/rate<R>-run<k>.json
"""
import argparse
import random
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

from . import env_info
from .live import SUSPENDED, accounts, end_offsets, lag_total, pct, read_from, settings, write

BG_PATHS = ["/apps/docs/handbook", "/apps/docs/policies", "/apps/hr/directory"]
PROBES = ["/apps/hr/payroll", "/apps/finance/payments", "/apps/hr/admin", "/apps/finance/backup", "/apps/docs/contracts",
          "/apps/hr/reviews", "/apps/docs/board-minutes", "/apps/finance/vendor-banking", "/apps/docs/config",
          "/apps/hr/export-all"]
TOPICS = ["risk-scores", "pdp-decisions", "escalations.realtime", "group-scores"]


def _client(gw):
    import httpx
    return httpx.Client(base_url=gw, timeout=15)


def _ready(gw, timeout=0.0) -> bool:
    import httpx
    t_end = time.time() + timeout
    while True:
        try:
            if httpx.get(f"{gw}/readyz", timeout=5).status_code == 200:
                return True
        except Exception:  # noqa: BLE001
            pass
        if time.time() >= t_end:
            return False
        time.sleep(2)


def background(gw, users, rate, t_end, log_rows, seed):
    rng = random.Random(seed)
    per_user = rate / max(len(users), 1)

    def worker(u, pw, r):
        c = _client(gw)
        try:
            c.post("/auth/login", json={"username": u, "password": pw})
        except Exception:  # noqa: BLE001 - counted below as a failed request; the run is then marked invalid
            log_rows.append((u, time.time(), time.time(), -1))
        while time.time() < t_end:
            time.sleep(r.expovariate(per_user))
            p = r.choice(BG_PATHS) + (f"/{r.randint(1, 5)}" if r.random() < 0.4 else "")
            t0 = time.time()
            try:
                st = c.get(p).status_code
            except Exception:  # noqa: BLE001
                st = -1
            log_rows.append((u, t0, time.time(), st))
    ths = [threading.Thread(target=worker, args=(u, pw, random.Random(rng.random())), daemon=True) for u, pw in users]
    for t in ths:
        t.start()
    return ths


def attacker(gw, u, pw, start_at, pause, timeout, rows):
    c = _client(gw)
    try:
        c.post("/auth/login", json={"username": u, "password": pw})
    except Exception:  # noqa: BLE001
        rows.append({"u": u, "t_send": time.time(), "t_recv": time.time(), "status": -1, "pdp_refused": False, "n": -1})
    while time.time() < start_at:
        time.sleep(0.05)
    n, refused_at, after = 0, None, 0
    t_stop = time.time() + timeout
    while time.time() < t_stop:
        t0 = time.time()
        try:
            r = c.get(PROBES[n % len(PROBES)]); st, txt = r.status_code, r.text
        except Exception:  # noqa: BLE001
            st, txt = -1, ""
        pdp = st == 401 or (st == 403 and SUSPENDED in txt)
        rows.append({"u": u, "t_send": t0, "t_recv": time.time(), "status": st, "pdp_refused": pdp, "n": n})
        n += 1
        if pdp:
            refused_at = refused_at or t0
            after += 1
            if after >= 2:
                break
        time.sleep(pause)


def analyse(recs, bg_rows, att_rows, attackers, t0, t1):
    scores = [d for t, d in recs if t.endswith("risk-scores") and d.get("trace") and d["trace"].get("ts_gw_in")]
    by_id = {d["score_id"]: d for t, d in recs if t.endswith("risk-scores")}
    stage = {k: [] for k in ("gw", "kafka", "reorder", "tcn_q", "tcn", "gw_to_score")}
    for d in scores:
        tr = d["trace"]
        g = lambda a, b: (tr[b] - tr[a]) * 1000 if tr.get(a) is not None and tr.get(b) is not None else None
        stage["gw"].append(g("ts_gw_in", "ts_gw_pub")); stage["kafka"].append(g("ts_gw_pub", "ts_seq_in"))
        stage["reorder"].append((tr.get("reorder_wait") or 0.0) * 1000)
        stage["tcn_q"].append(g("ts_seq_out", "ts_tcn_dequeue")); stage["tcn"].append(g("ts_tcn_dequeue", "ts_tcn_scored"))
        stage["gw_to_score"].append(g("ts_gw_in", "ts_tcn_scored"))
    decisions = [d for t, d in recs if t.endswith("pdp-decisions") and d.get("u") in attackers and d.get("transition") == "block"]
    per_att = {}
    for u in attackers:
        ds_ = sorted((d for d in decisions if d["u"] == u), key=lambda d: d["t"])
        rows = sorted((r for r in att_rows if r["u"] == u), key=lambda r: r["t_send"])
        first_any = next((d for d in ds_ if (d.get("basis") or {}).get("score_id") in by_id), None)
        # Lat_ind is measured to the first ENFORCING decision (Terminate / Block: requests refused); a milder
        # earlier action (e.g. Throttle) is recorded separately
        first = next((d for d in ds_ if d["decision"] in ("Terminate", "Block") and (d.get("basis") or {}).get("score_id") in by_id), None)
        if first is None:
            per_att[u] = {"decided": False, "requests": len(rows), "first_action": first_any and first_any["decision"]}
            continue
        sc = by_id[first["basis"]["score_id"]]
        tr = sc.get("trace") or {}
        trig = tr.get("ts_gw_in") or sc.get("t")
        refused = next((r for r in rows if r["pdp_refused"]), None)
        exposed = sum(1 for r in rows if r["t_send"] > trig + 1e-6 and not r["pdp_refused"] and (refused is None or r["t_send"] < refused["t_send"]))
        esc = next((d for t, d in recs if t.endswith("escalations.realtime") and d.get("u") == u), None)
        gsc = next((d for t, d in recs if t.endswith("group-scores") and esc and d.get("escalation_id") == esc.get("escalation_id")), None)
        # the escalation is timed from the score that triggered IT (evidence.high.score_id), which can be an
        # earlier score than the first enforcing one - pairing it with the latter gave negative latencies
        esc_sc = by_id.get((((esc or {}).get("evidence") or {}).get("high") or {}).get("score_id"))
        esc_trig = (((esc_sc or {}).get("trace") or {}).get("ts_gw_in") or (esc_sc or {}).get("t")) if esc_sc else None
        per_att[u] = {"decided": True, "action": first["decision"], "s_ind": sc.get("s_ind"),
                      "first_action": first_any["decision"],
                      "first_action_after_ms": (first_any["t"] - trig) * 1000,
                      "requests_before_trigger": sum(1 for r in rows if r["t_send"] <= trig + 1e-6),
                      "lat_pdp_ms": (first["t"] - sc["scored_at"]) * 1000,
                      "lat_ind_decision_ms": (first["t"] - trig) * 1000,
                      "lat_enf_ms": (refused["t_send"] - first["t"]) * 1000 if refused else None,
                      "lat_ind_observed_ms": (refused["t_send"] - trig) * 1000 if refused else None,
                      "n_exposed": exposed,
                      "escalation_paths": esc.get("paths") if esc else None,
                      "lat_esc_ms": (esc["t"] - esc_sc["scored_at"]) * 1000 if esc and esc_sc else None,
                      "lat_tgn_total_ms": (gsc["t"] - esc["t"]) * 1000 if gsc and esc else None,
                      "tgn_inference_ms": gsc.get("inference_ms") if gsc else None,
                      "lat_coll_to_group_score_ms": (gsc["t"] - esc_trig) * 1000 if gsc and esc_trig else None,
                      "trigger_stages_ms": {k: (tr[b] - tr[a]) * 1000 for k, a, b in
                                            (("gw", "ts_gw_in", "ts_gw_pub"), ("kafka", "ts_gw_pub", "ts_seq_in"),
                                             ("tcn_q", "ts_seq_out", "ts_tcn_dequeue"), ("tcn", "ts_tcn_dequeue", "ts_tcn_scored"))
                                            if tr.get(a) is not None and tr.get(b) is not None}}
    lat = [v["lat_ind_decision_ms"] for v in per_att.values() if v.get("decided")]
    dur = t1 - t0
    resp = [(b - a) * 1000 for _, a, b, st in bg_rows if st > 0]
    st_mix = {}
    for *_, st in bg_rows:
        st_mix[str(st)] = st_mix.get(str(st), 0) + 1
    return {"stages_ms": {k: pct(v) for k, v in stage.items()}, "per_attacker": per_att,
            "lat_ind_decision_ms": pct(lat), "lat_ind_observed_ms": pct([v.get("lat_ind_observed_ms") for v in per_att.values()]),
            "slo_fraction": {f"le_{x}s": (sum(1 for v in lat if v <= x * 1000) / len(lat)) if lat else None for x in (0.5, 1, 2, 5)},
            "n_exposed": pct([v.get("n_exposed") for v in per_att.values() if v.get("decided")]),
            "throughput": {"scores_per_s": sum(1 for d in scores if t0 <= d["scored_at"] <= t1) / dur,
                           "gateway_requests_per_s": len(bg_rows) / dur, "gateway_status_mix": st_mix,
                           "gateway_response_ms": pct(resp)}}


def run_one(s, args, rate, k, log):
    started = datetime.now(timezone.utc).isoformat()
    bg = accounts(s, args.api, args.bg_users, "staff", "lat")
    att = accounts(s, args.api, args.attackers, "staff", "att")
    start = end_offsets(s, TOPICS)
    t0 = time.time(); t_end = t0 + args.duration
    bg_rows, att_rows, lags = [], [], []
    ths = background(args.gateway, bg, rate, t_end, bg_rows, seed=1000 * rate + k)
    rng = random.Random(k)
    ath = [threading.Thread(target=attacker, args=(args.gateway, u, pw, t0 + rng.uniform(0.15, 0.6) * args.duration,
                                                   args.attack_pause, args.attack_timeout, att_rows), daemon=True)
           for u, pw in att]
    for t in ath:
        t.start()
    while time.time() < t_end:
        lags.append((time.time(), lag_total(s)))
        time.sleep(5)
    for t in ths + ath:
        t.join(timeout=args.attack_timeout + 30)
    t1 = time.time()
    time.sleep(args.settle)
    users = {u for u, _ in bg} | {u for u, _ in att}
    recs = read_from(s, start, lambda topic, d: d.get("u") in users)
    failed = sum(1 for *_, st in bg_rows if st < 0) + sum(1 for x in att_rows if x["status"] < 0)
    total = len(bg_rows) + len(att_rows)
    # a run is VALID only if the gateway answered (almost) every request: a stopped gateway once produced
    # silent zero-score runs (Exp. C, 2026-11-15); invalid runs are kept for the record but not tabulated
    valid = total > 0 and failed / total <= 0.01 and _ready(args.gateway)
    out = {"experiment": "C/D", "rate": rate, "run": k, "started_at": started, "duration_s": args.duration,
           "valid": valid, "failed_requests": failed, "requests": total,
           "bg_users": args.bg_users, "attackers": args.attackers, "attack_rate_per_s": 1.0 / args.attack_pause,
           "records": len(recs), "result": analyse(recs, bg_rows, att_rows, {u for u, _ in att}, t0, t1),
           "lag_samples": lags, "environment": env_info.collect()}
    for u in users:                                                       # leave no action behind
        subprocess.run([sys.executable, "-m", "accesssentinel.pdp_admin", "release", u, "--by", "exp-c",
                        "--note", "experiment cleanup", "--grace", "0"], capture_output=True, timeout=60)
    write(f"{args.out}/rate{rate:g}-run{k}{'' if valid else '-INVALID'}.json", out)
    r = out["result"]
    log(f"{'' if valid else 'INVALID (gateway unreachable for ' + str(failed) + '/' + str(total) + ' requests) '}rate {rate} run {k}: scores/s {r['throughput']['scores_per_s']:.1f}, gw->score p50/p99 "
        f"{(r['stages_ms']['gw_to_score'] or {}).get('p50', float('nan')):.1f}/{(r['stages_ms']['gw_to_score'] or {}).get('p99', float('nan')):.1f} ms, "
        f"Lat_ind(decision) {r['lat_ind_decision_ms']}, decided {sum(1 for v in r['per_attacker'].values() if v.get('decided'))}/{args.attackers}")
    return out


def main(argv=None) -> int:
    import os
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rates", type=float, nargs="+", default=[5, 20, 50])
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--duration", type=float, default=120)
    ap.add_argument("--bg-users", type=int, default=40)
    ap.add_argument("--attackers", type=int, default=5)
    ap.add_argument("--attack-pause", type=float, default=0.2)
    ap.add_argument("--attack-timeout", type=float, default=60)
    ap.add_argument("--settle", type=float, default=20, help="seconds after the run before reading Kafka")
    ap.add_argument("--out", default="evaluation/results/raw/exp_c")
    ap.add_argument("--skip-existing", action="store_true", help="skip (rate, run) pairs with a valid result file")
    ap.add_argument("--gateway", default=os.environ.get("AS_GATEWAY_URL", "http://gateway:8000"))
    ap.add_argument("--api", default=os.environ.get("AS_API_URL", "http://api:8080"))
    args = ap.parse_args(argv)
    s = settings()
    log = lambda *a: print(*a, flush=True)
    for rate in args.rates:
        for k in range(args.runs):
            if args.skip_existing and os.path.exists(f"{args.out}/rate{rate:g}-run{k}.json"):
                log(f"rate {rate} run {k}: valid result exists, skipped"); continue
            if not _ready(args.gateway, timeout=120):
                log(f"ABORT: gateway {args.gateway} not ready before rate {rate} run {k}")
                return 2
            run_one(s, args, rate, k, log)
            time.sleep(15)
    return 0


if __name__ == "__main__":
    sys.exit(main())

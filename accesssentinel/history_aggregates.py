"""Hourly feature aggregates and resource-participant summaries on HDFS (Revised v3 Step 3, Table II "periodic
sweep input": compacted feature tables / materialised aggregates).

The periodic-sweep escalation path (Step 6, Phase 5) reads these instead of scanning raw events; HDFS is never
queried synchronously by the real-time path.

  agg-user-hourly             one row per (user, hour): events, sensitive, denied, exports, distinct resources /
                              applications, off-hours events, first/last event time
  agg-resource-participants   one row per (r*, hour): participants (distinct users), events, denied, sensitivity

An hour is aggregated once, after it is closed for `grace` seconds (late events and the HDFS sink's flush
interval), and recorded in a done-marker set so the job is idempotent. Re-running an hour (e.g. after a
backfill) writes rows with the same record_id; readers keep the newest.

    python -m accesssentinel.history_aggregates [--hours 48] [--grace 3600]
"""
import argparse
import logging
import time
from collections import defaultdict
from typing import Dict, Iterable, List, Tuple

from .events import AccessEvent

log = logging.getLogger("accesssentinel.aggregates")

AGG_USER = "agg-user-hourly"
AGG_RES = "agg-resource-participants"
HOUR = 3600
WORK_HOURS = (7, 19)                     # [IMPL] UTC hours counted as "on hours" for the off-hours count


def aggregate_hour(events: Iterable[AccessEvent], hour_start: float) -> Tuple[List[dict], List[dict]]:
    users: Dict[str, dict] = {}
    res: Dict[str, dict] = {}
    for e in events:
        if not (hour_start <= e.t < hour_start + HOUR):
            continue
        u = users.setdefault(e.u, {"u": e.u, "hour": hour_start, "events": 0, "sensitive": 0, "denied": 0,
                                   "exports": 0, "off_hours": 0, "resources": set(), "apps": set(),
                                   "first_t": e.t, "last_t": e.t})
        u["events"] += 1
        u["sensitive"] += int(e.s)
        u["denied"] += int(e.y == 0)
        u["exports"] += int(e.o == "export")
        h = int((e.t % 86400) // 3600)
        u["off_hours"] += int(not (WORK_HOURS[0] <= h < WORK_HOURS[1]))
        u["resources"].add(e.rstar)
        u["apps"].add(e.a)
        u["first_t"], u["last_t"] = min(u["first_t"], e.t), max(u["last_t"], e.t)
        r = res.setdefault(e.rstar, {"rstar": e.rstar, "hour": hour_start, "s": int(e.s), "events": 0, "denied": 0,
                                     "users": set()})
        r["events"] += 1
        r["denied"] += int(e.y == 0)
        r["users"].add(e.u)
    urows = []
    for u in users.values():
        urows.append({**{k: v for k, v in u.items() if k not in ("resources", "apps")},
                      "distinct_resources": len(u["resources"]), "distinct_apps": len(u["apps"])})
    rrows = [{**{k: v for k, v in r.items() if k != "users"}, "participants": sorted(r["users"]),
              "distinct_users": len(r["users"])} for r in res.values()]
    return urows, rrows


class HourlyAggregator:
    def __init__(self, lake, redis=None, marker_key: str = "hdfs:agg:done", grace: float = HOUR):
        self.lake, self.r, self.key, self.grace = lake, redis, marker_key, grace
        self._done = set()

    def _is_done(self, h: int) -> bool:
        if self.r is not None:
            return bool(self.r.sismember(self.key, h))
        return h in self._done

    def _mark(self, h: int) -> None:
        if self.r is not None:
            self.r.sadd(self.key, h)
        self._done.add(h)

    def run_hour(self, h: int) -> Tuple[int, int]:
        evs = self.lake.read_events(h - 1e-6, h + HOUR - 1e-6)          # read_events is (t0, t1]
        urows, rrows = aggregate_hour(evs, float(h))
        if urows:
            self.lake.write_records(AGG_USER, [{"record_id": f"{r['u']}|{h}", "ts": float(h), "payload": r} for r in urows])
        if rrows:
            self.lake.write_records(AGG_RES, [{"record_id": f"{r['rstar']}|{h}", "ts": float(h), "payload": r} for r in rrows])
        self._mark(h)
        return len(urows), len(rrows)

    def run_pending(self, now: float, lookback_hours: int = 48) -> List[Tuple[int, int, int]]:
        last_closed = int((now - self.grace) // HOUR) * HOUR - HOUR
        out = []
        for h in range(last_closed - (lookback_hours - 1) * HOUR, last_closed + HOUR, HOUR):
            if h < 0 or self._is_done(h):
                continue
            nu, nr = self.run_hour(h)
            out.append((h, nu, nr))
        return out


def read_user_aggregates(lake, t0: float, t1: float) -> List[dict]:
    newest = {}
    for r in lake.read_records(AGG_USER, t0 - HOUR, t1, dedup=False):
        newest[r["record_id"]] = r["payload"]
    return sorted(newest.values(), key=lambda p: (p["hour"], p["u"]))


def read_resource_participants(lake, t0: float, t1: float) -> List[dict]:
    newest = {}
    for r in lake.read_records(AGG_RES, t0 - HOUR, t1, dedup=False):
        newest[r["record_id"]] = r["payload"]
    return sorted(newest.values(), key=lambda p: (p["hour"], p["rstar"]))


def main(argv=None) -> int:
    import redis as redis_lib
    from .adapters.hdfs_lake import make_lake
    from .settings import get_settings
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hours", type=int, default=48)
    ap.add_argument("--grace", type=float, default=HOUR)
    args = ap.parse_args(argv)
    s = get_settings()
    logging.basicConfig(level=s.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    agg = HourlyAggregator(make_lake(s), redis_lib.Redis(**s.redis_kwargs()), grace=args.grace)
    for h, nu, nr in agg.run_pending(time.time(), args.hours):
        print(f"hour {time.strftime('%Y-%m-%d %H:00', time.gmtime(h))} UTC: {nu} user rows, {nr} resource rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

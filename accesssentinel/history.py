"""Query and maintain the HDFS history (D3 Step 3: historical analysis, audit/investigation, training data).

    python -m accesssentinel.history partitions [--dataset events]
    python -m accesssentinel.history user alice --hours 24
    python -m accesssentinel.history range --from 2026-11-10T00:00 --to 2026-11-11T00:00 [--app hr]
    python -m accesssentinel.history records pdp-decisions --hours 24
    python -m accesssentinel.history compact --date 2026-11-10 [--app hr] [--dataset events]
    python -m accesssentinel.history export --from ... --to ... --out events.parquet

Times are UTC (ISO 8601) or epoch seconds. Run inside Docker (`docker compose run --rm tools python -m
accesssentinel.history ...`) unless the host can resolve the HDFS DataNode (see docs/SETUP_WINDOWS.md).
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone

from .adapters.hdfs_lake import EVENTS, make_lake


def _ts(v: str) -> float:
    try:
        return float(v)
    except ValueError:
        dt = datetime.fromisoformat(v)
        return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()


def _print_events(evs, limit: int) -> None:
    for e in evs[:limit]:
        print(json.dumps({"t": datetime.fromtimestamp(e.t, tz=timezone.utc).isoformat(timespec="seconds"),
                          "u": e.u, "a": e.a, "r": e.r, "s": e.s, "o": e.o, "p": e.p, "y": e.y,
                          "reason": e.meta.get("reason"), "event_id": e.event_id}))
    print(f"# {len(evs)} event(s){' (showing %d)' % limit if len(evs) > limit else ''}", file=sys.stderr)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("partitions"); p.add_argument("--dataset", default=EVENTS)
    p = sub.add_parser("user"); p.add_argument("user"); p.add_argument("--hours", type=float, default=24)
    p.add_argument("--limit", type=int, default=200)
    p = sub.add_parser("range"); p.add_argument("--from", dest="t0", required=True); p.add_argument("--to", dest="t1", required=True)
    p.add_argument("--app", action="append"); p.add_argument("--limit", type=int, default=200)
    p = sub.add_parser("records"); p.add_argument("dataset"); p.add_argument("--hours", type=float, default=24)
    p = sub.add_parser("compact"); p.add_argument("--date", required=True); p.add_argument("--app")
    p.add_argument("--dataset", default=EVENTS)
    p = sub.add_parser("export"); p.add_argument("--from", dest="t0", required=True); p.add_argument("--to", dest="t1", required=True)
    p.add_argument("--out", required=True)
    args = ap.parse_args(argv)

    from .settings import get_settings
    lake = make_lake(get_settings())
    now = time.time()
    if args.cmd == "partitions":
        for row in lake.partitions(args.dataset):
            print(json.dumps(row))
    elif args.cmd == "user":
        _print_events(lake.user_history(args.user, now - args.hours * 3600, now), args.limit)
    elif args.cmd == "range":
        _print_events(lake.read_events(_ts(args.t0), _ts(args.t1), apps=args.app), args.limit)
    elif args.cmd == "records":
        for r in lake.read_records(args.dataset, now - args.hours * 3600, now):
            print(json.dumps({"record_id": r["record_id"], "ts": r["ts"], "payload": r["payload"]}))
    elif args.cmd == "compact":
        apps = [args.app] if args.app else [p["app"] for p in lake.partitions(args.dataset) if p["date"] == args.date]
        for a in apps or [None]:
            print(json.dumps({"date": args.date, "app": a, **lake.compact(args.dataset, args.date, a)}))
    elif args.cmd == "export":
        import pyarrow as pa
        import pyarrow.parquet as pq
        evs = lake.read_events(_ts(args.t0), _ts(args.t1))
        rows = [{**e.__dict__, "c": json.dumps(e.c), "meta": json.dumps(e.meta)} for e in evs]
        pq.write_table(pa.Table.from_pylist(rows) if rows else pa.table({}), args.out)
        print(f"wrote {len(rows)} deduplicated events to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

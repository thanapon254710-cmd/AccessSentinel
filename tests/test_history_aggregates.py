"""Revised v3 Phase 2: hourly HDFS feature aggregates and resource-participant summaries (Step 3)."""
import fakeredis

from accesssentinel.adapters.hdfs_lake import HistoryLake, LocalFsClient
from accesssentinel.events import AccessEvent
from accesssentinel.history_aggregates import (HourlyAggregator, aggregate_hour, read_resource_participants,
                                               read_user_aggregates)

H = 1_794_009_600.0          # an exact UTC hour (2026-11-07 00:00)


def ev(u, t, r="payroll", s=1, y=1, o="view", i=0):
    return AccessEvent(u=u, t=t, a="hr", r=r, s=s, p="read", o=o, y=y, event_id=f"{u}{t}{i}", schema_version=2,
                       tenant="acme")


def test_aggregate_hour_counts_per_user_and_resource():
    evs = [ev("a", H + 10), ev("a", H + 20, r="directory", s=0, y=0), ev("a", H + 30, o="export", i=1),
           ev("b", H + 40), ev("b", H + 3600)]                          # last one belongs to the next hour
    urows, rrows = aggregate_hour(evs, H)
    a = next(r for r in urows if r["u"] == "a")
    assert (a["events"], a["sensitive"], a["denied"], a["exports"], a["distinct_resources"]) == (3, 2, 1, 1, 2)
    assert a["off_hours"] == 3                                          # 00:xx UTC is outside 07-19
    pay = next(r for r in rrows if r["rstar"] == "acme/hr:payroll")
    assert pay["participants"] == ["a", "b"] and pay["distinct_users"] == 2 and pay["events"] == 3


def test_aggregator_runs_closed_hours_once_and_readers_return_them(tmp_path):
    lake = HistoryLake(LocalFsClient(str(tmp_path / "lake")), "/as")
    lake.write_events([(ev("a", H + 5), {}), (ev("b", H + 50), {}), (ev("a", H + 3700), {})])
    r = fakeredis.FakeRedis()
    agg = HourlyAggregator(lake, r, grace=3600)
    done = agg.run_pending(now=H + 3 * 3600 + 10, lookback_hours=3)    # hours H and H+1 are closed + grace
    assert [(h, nu) for h, nu, _ in done if nu] == [(int(H), 2), (int(H + 3600), 1)]
    assert agg.run_pending(now=H + 3 * 3600 + 10, lookback_hours=3) == []   # idempotent
    users = read_user_aggregates(lake, H, H + 7200)
    assert [(u["u"], u["hour"]) for u in users] == [("a", H), ("b", H), ("a", H + 3600)]
    parts = read_resource_participants(lake, H, H + 3600)
    assert parts[0]["participants"] == ["a", "b"]

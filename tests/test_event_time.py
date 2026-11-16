"""Revised v3 Phase 2: per-user event-time ordering (Eq. 2), sequence fast path, idle bound, late policies."""
import random

import pytest

from accesssentinel.streaming.event_time import EventTimeOrderer, Item


def it(te, seq=-1, stream="", eid=None, s=0, arrived=0.0):
    return Item(te=te, seq=seq, stream=stream, eid=eid or f"e{te}-{seq}-{stream}", s=s, arrived=arrived, payload={})


def test_single_stream_in_sequence_is_released_immediately():
    o = EventTimeOrderer(delta_late=2.0)
    out = []
    for i in range(1, 6):
        out += o.add("u", it(100 + i, seq=i, stream="gw1:a", arrived=100 + i))
    assert [r.item.te for r in out] == [101, 102, 103, 104, 105]
    assert all(r.reason == "fast_path" and r.wait == 0.0 and r.late is None for r in out)


def test_out_of_order_events_are_released_in_event_time_order_by_the_watermark():
    o = EventTimeOrderer(delta_late=2.0)
    assert o.add("u", it(5.0, arrived=1)) == []          # no sequence info: buffered (W = 3)
    assert o.add("u", it(3.5, arrived=2)) == []          # 3.5 > W: waits
    assert o.add("u", it(4.0, arrived=3)) == []
    out = o.add("u", it(10.0, arrived=4))                # W = 8 -> 3.5, 4, 5 released in te order, 10 waits
    assert [r.item.te for r in out] == [3.5, 4.0, 5.0] and {r.reason for r in out} == {"watermark"}
    (r,) = EventTimeOrderer(2.0).add("v", it(1.0, arrived=0)) or [None]  # te <= W of its own key? no: W = -1
    assert r is None
    assert [r.item.te for _, r in o.tick(now=6.1)] == [10.0]   # idle bound: waited 2.1 s >= delta_late


def test_idle_bound_releases_a_users_last_event_after_delta_late():
    o = EventTimeOrderer(delta_late=2.0)
    o.add("u", it(50.0, seq=3, stream="s", arrived=10.0))       # seq gap: buffered
    assert o.tick(now=11.9) == []
    (u, r), = o.tick(now=12.0)
    assert u == "u" and r.reason == "idle" and r.wait == pytest.approx(2.0)


def test_late_events_get_the_policy_of_their_sensitivity():
    o = EventTimeOrderer(delta_late=1.0, policy_s0="history_only", policy_s1="retro_alert")
    o.add("u", it(100.0, seq=1, stream="s", arrived=0))
    (r,) = o.add("u", it(90.0, s=0, arrived=1))                  # behind the watermark (99)
    assert (r.late, r.reason) == ("history_only", "behind_watermark")
    (r,) = o.add("u", it(99.5, s=1, arrived=2))                  # within lateness but older than a released event
    assert (r.late, r.reason) == ("retro_alert", "behind_released")
    assert o.stats["late"] == 2


def test_duplicates_in_the_buffer_are_ignored_and_bad_policy_rejected():
    o = EventTimeOrderer(delta_late=5.0)
    o.add("u", it(10.0, eid="x", arrived=0))
    assert o.add("u", it(10.0, eid="x", arrived=1)) == [] and o.stats["duplicates"] == 1
    with pytest.raises(ValueError):
        EventTimeOrderer(1.0, policy_s1="ignore")


def test_state_round_trip_keeps_buffer_and_stream_positions():
    o = EventTimeOrderer(delta_late=2.0)
    o.add("u", it(1.0, seq=1, stream="s", arrived=0))
    o.add("u", it(5.0, seq=5, stream="s", arrived=1))           # gap -> buffered
    o2 = EventTimeOrderer(delta_late=2.0)
    o2.restore_user("u", o.user_state("u"))
    assert [r.item.te for _, r in o2.tick(now=10)] == [5.0]
    assert o2.users["u"].streams == {"s": 5}
    o2.restore_user("u", None)
    assert "u" not in o2.users


@pytest.mark.parametrize("seed", range(5))
def test_property_order_completeness_and_bounded_wait(seed):
    """Random arrival jitter up to 1.5 x delta_late on 2 interleaved streams of 3 users: every event is
    released exactly once or reported late (never lost or duplicated); released te is non-decreasing per user;
    no event waits longer than delta_late (+ the tick step)."""
    rng = random.Random(seed)
    delta, step = 2.0, 0.1
    o = EventTimeOrderer(delta_late=delta)
    events = []
    for u in ("a", "b", "c"):
        seq = {"s1": 0, "s2": 0}
        t = 0.0
        for _ in range(200):
            t += rng.expovariate(2.0)
            st = rng.choice(["s1", "s2"])
            seq[st] += 1
            arrive = t + rng.uniform(0, 1.5 * delta)
            events.append((arrive, u, it(t, seq=seq[st], stream=f"{u}:{st}", eid=f"{u}{st}{seq[st]}", arrived=arrive)))
    events.sort(key=lambda x: x[0])
    released, late = {}, set()
    now = 0.0
    for arrive, u, item in events:
        while now + step <= arrive:
            now += step
            for uu, r in o.tick(now):
                released.setdefault(uu, []).append(r)
        for r in o.add(u, item):
            (late.add(r.item.eid) if r.late else released.setdefault(u, []).append(r))
    for _ in range(100):
        now += step
        for uu, r in o.tick(now):
            released.setdefault(uu, []).append(r)
    ids = [r.item.eid for rs in released.values() for r in rs]
    assert len(ids) == len(set(ids)) and set(ids) | late == {e[2].eid for e in events} and not set(ids) & late
    for rs in released.values():
        tes = [r.item.te for r in rs]
        assert tes == sorted(tes)
        assert max(r.wait for r in rs) <= delta + step + 1e-9
    assert o.stats["fast_path"] > 0


def test_concurrent_sessions_use_the_watermark_not_the_fast_path():
    """Regression (live check, Phase 2): two interleaved sessions must not overtake each other."""
    o = EventTimeOrderer(delta_late=2.0)
    assert [r.reason for r in o.add("u", it(10.0, seq=1, stream="a", arrived=10.0))] == ["fast_path"]
    assert o.add("u", it(10.5, seq=1, stream="b", arrived=10.6)) == []      # second session -> buffered
    assert o.add("u", it(10.3, seq=2, stream="a", arrived=10.7)) == []      # no fast path while b is active
    out = [r for _, r in o.tick(now=13.0)]
    assert [r.item.te for r in out] == [10.3, 10.5] and o.stats["late"] == 0

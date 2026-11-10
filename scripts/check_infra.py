"""Live infrastructure check for the Phase 2 stack: Kafka, HDFS, PostgreSQL, Redis.

Each check performs a real round trip through the same adapters the pipeline uses and cleans up after
itself (temporary topics / HDFS directory / temp table / Redis key). Exit code 0 only if all pass.

    # inside the Docker network (recommended; no hosts-file edit needed for HDFS):
    docker compose run --rm tools python scripts/check_infra.py
    # from the Windows host (HDFS needs `127.0.0.1 datanode` in the hosts file, see SETUP_WINDOWS.md):
    python scripts/check_infra.py --only kafka,postgres,redis
"""
import argparse
import os
import sys
import time
import traceback
import uuid
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from accesssentinel.config import StreamConfig          # noqa: E402
from accesssentinel.events import AccessEvent           # noqa: E402
from accesssentinel.settings import Settings, get_settings  # noqa: E402
from accesssentinel.topics import all_topic_names       # noqa: E402

CHECKS = ("kafka", "hdfs", "postgres", "redis")


def _events(n=40, users=5, t0=1_000_000.0, step=200.0):
    """Synthetic probe events (not monitoring data); spans several hours so HDFS hour partitions differ."""
    return [AccessEvent(f"infracheck-u{i % users}", t0 + i * step, "infracheck-app", f"res{i % 3:02d}", i % 2,
                        "read", "view", {"ip": "10.0.0.1", "device": "probe", "session": "active"}, 1, i)
            for i in range(n)]


def _retry(fn, wait: float, what: str):
    deadline = time.time() + wait
    while True:
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            if time.time() > deadline:
                raise RuntimeError(f"{what}: {e}") from e
            time.sleep(2)


# ------------------------------------------------------------------------------------------- Kafka
def check_kafka(s: Settings, wait: float) -> str:
    from confluent_kafka.admin import AdminClient
    from accesssentinel.adapters.kafka_broker import KafkaDualViewBroker

    admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap})
    md = _retry(lambda: admin.list_topics(timeout=5), wait, f"Kafka at {s.kafka_bootstrap} unreachable")

    # 1) deployment topics provisioned by kafka-init
    expected = all_topic_names(s.kafka_topic_prefix)
    missing = [t for t in expected if t not in md.topics]
    if missing:
        raise RuntimeError(f"topics not provisioned (run kafka-init): {missing}")
    wrong = {t: len(md.topics[t].partitions) for t in expected if len(md.topics[t].partitions) != s.kafka_partitions}
    if wrong:
        raise RuntimeError(f"unexpected partition counts: {wrong}")

    # 2) round trip through the real KafkaDualViewBroker on throw-away topics
    prefix = f"infracheck-{uuid.uuid4().hex[:8]}"
    cfg = StreamConfig(n_partitions=s.kafka_partitions)
    broker = KafkaDualViewBroker(s.kafka_bootstrap, cfg, prefix=prefix)
    groups = []
    try:
        evs = _events()
        for e in evs:
            broker.publish_event(e)
        broker.flush()
        got_u, got_r = [], []
        gu, gr = broker.user_topic.group("check"), broker.resource_topic.group("check")
        groups += [gu, gr]
        deadline = time.time() + 60
        while (len(got_u) < len(evs) or len(got_r) < len(evs)) and time.time() < deadline:
            got_u += gu.poll(timeout=1.0)
            got_r += gr.poll(timeout=1.0)
        if len(got_u) != len(evs) or len(got_r) != len(evs):
            raise RuntimeError(f"consumed {len(got_u)}/{len(evs)} user-view and {len(got_r)}/{len(evs)} "
                               "resource-view messages within 60 s")
        if sorted(e.eid for _, _, e in got_u) != [e.eid for e in evs]:
            raise RuntimeError("user-view payloads do not match what was produced")
        part_of_user, seq = defaultdict(set), defaultdict(list)
        for p, off, e in sorted(got_u, key=lambda x: (x[0], x[1])):
            part_of_user[e.u].add(p)
            seq[e.u].append(e.t)
        split = {u: ps for u, ps in part_of_user.items() if len(ps) > 1}
        if split:
            raise RuntimeError(f"user key spread over several partitions: {split}")
        unordered = [u for u, ts in seq.items() if ts != sorted(ts)]
        if unordered:
            raise RuntimeError(f"per-user order not preserved for {unordered}")
        part_of_rkey = defaultdict(set)
        for p, _, e in got_r:
            part_of_rkey[broker.resource_key(e)].add(p)
        if any(len(ps) > 1 for ps in part_of_rkey.values()):
            raise RuntimeError("resource-view key spread over several partitions")
        used = len({p for ps in part_of_user.values() for p in ps})
        return (f"{len(expected)} topics provisioned x {s.kafka_partitions} partitions; round trip "
                f"{len(evs)} events: per-user partition+order OK ({len(seq)} users on {used} partitions), "
                f"resource-view co-location OK ({len(part_of_rkey)} keys)")
    finally:
        for g in groups:
            try:
                g.close()
            except Exception:  # noqa: BLE001
                pass
        tmp = [f"{prefix}.access-events", f"{prefix}.access-events-by-resource"]
        for f in admin.delete_topics(tmp, operation_timeout=30).values():
            try:
                f.result()
            except Exception:  # noqa: BLE001
                pass


# -------------------------------------------------------------------------------------------- HDFS
def check_hdfs(s: Settings, wait: float) -> str:
    from accesssentinel.adapters.hdfs_store import WebHdfsStore, make_client

    client = make_client(s.hdfs_url, user=s.hdfs_user)
    base = f"{s.hdfs_base.rstrip('/')}-infracheck/{uuid.uuid4().hex[:8]}"
    # NameNode leaves safe mode only after the DataNode registers: retry the first write.
    store = _retry(lambda: WebHdfsStore(client, base=base, flush_every=10_000), wait, "HDFS not writable")
    try:
        evs = _events()
        for e in evs:
            store.append(e)
        try:
            store.flush()
        except Exception as e:  # noqa: BLE001
            if "datanode" in str(e).lower():
                raise RuntimeError("WebHDFS redirected the write to host 'datanode', which this machine cannot "
                                   "resolve. Run the check inside Docker (`docker compose run --rm tools ...`) "
                                   "or add `127.0.0.1 datanode` to the hosts file.") from e
            raise
        back = store.read_range(evs[0].t - 1, evs[-1].t)            # (t0, t1] semantics
        if [(e.u, e.t, e.eid, e.c) for e in back] != [(e.u, e.t, e.eid, e.c) for e in evs]:
            raise RuntimeError("events read back from HDFS differ from those written")
        hours = sorted({int(e.t // 3600) for e in evs})
        store.append(evs[0]); store.flush()                     # second part file in the first hour
        store.compact(hours[0])
        if len(client.list(f"{base}/events/hour={hours[0]:06d}")) != 1:
            raise RuntimeError("compaction did not merge part files")
        store.write_feedback([{"probe": True}])
        return (f"Parquet write/read of {len(evs)} events across {len(hours)} hour partitions OK, "
                f"compaction OK, feedback write OK (WebHDFS {s.hdfs_url})")
    finally:
        try:
            client.delete(base, recursive=True)
        except Exception:  # noqa: BLE001
            pass


# ---------------------------------------------------------------------------------------- Postgres
def check_postgres(s: Settings, wait: float) -> str:
    import psycopg

    conn = _retry(lambda: psycopg.connect(s.postgres_dsn(), connect_timeout=5), wait,
                  f"PostgreSQL at {s.postgres_host}:{s.postgres_port} unreachable")
    with conn:
        with conn.cursor() as cur:
            cur.execute("SHOW server_version")
            ver = cur.fetchone()[0]
            cur.execute("CREATE TEMP TABLE infracheck (id int PRIMARY KEY, note text)")
            cur.execute("INSERT INTO infracheck VALUES (%s, %s)", (1, "probe"))
            cur.execute("SELECT note FROM infracheck WHERE id = %s", (1,))
            if cur.fetchone()[0] != "probe":
                raise RuntimeError("round trip mismatch")
    return f"PostgreSQL {ver} at {s.postgres_host}:{s.postgres_port}/{s.postgres_db}: connect + temp-table round trip OK"


# ------------------------------------------------------------------------------------------- Redis
def check_redis(s: Settings, wait: float) -> str:
    import redis

    r = redis.Redis(**s.redis_kwargs(), socket_connect_timeout=5, decode_responses=True)
    _retry(r.ping, wait, f"Redis at {s.redis_host}:{s.redis_port} unreachable")
    key = f"infracheck:{uuid.uuid4().hex}"
    r.set(key, "probe", px=10_000)
    if r.get(key) != "probe" or r.pttl(key) <= 0:
        raise RuntimeError("round trip / TTL mismatch")
    r.delete(key)
    aof = r.info("persistence").get("aof_enabled")
    policy = r.config_get("maxmemory-policy").get("maxmemory-policy")
    if aof != 1 or policy != "noeviction":
        raise RuntimeError(f"expected AOF on and noeviction, got aof_enabled={aof} policy={policy}")
    ver = r.info("server")["redis_version"]
    return f"Redis {ver}: auth + set/get/TTL OK, AOF on, maxmemory-policy=noeviction"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", default=",".join(CHECKS), help="comma-separated subset of " + ",".join(CHECKS))
    ap.add_argument("--wait", type=float, default=90.0, help="seconds to keep retrying connections")
    args = ap.parse_args(argv)
    only = [c.strip() for c in args.only.split(",") if c.strip()]
    bad = set(only) - set(CHECKS)
    if bad:
        ap.error(f"unknown checks: {sorted(bad)}")
    s = get_settings()
    print(f"AccessSentinel infrastructure check (environment={s.environment})")
    fns = {"kafka": check_kafka, "hdfs": check_hdfs, "postgres": check_postgres, "redis": check_redis}
    failed = 0
    for name in only:
        t0 = time.time()
        try:
            detail = fns[name](s, args.wait)
            print(f"  PASS  {name:9s} {time.time() - t0:5.1f}s  {detail}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  FAIL  {name:9s} {time.time() - t0:5.1f}s  {e}")
            if os.environ.get("AS_CHECK_TRACEBACK"):
                traceback.print_exc()
    print(f"{len(only) - failed}/{len(only)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

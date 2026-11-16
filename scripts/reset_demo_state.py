"""Reset the monitoring state of demo users (TEST ENVIRONMENTS ONLY; refused when AS_ENVIRONMENT=prod).

    docker compose run --rm -T tools python scripts/reset_demo_state.py ivan carol
    docker compose run --rm -T tools python scripts/reset_demo_state.py --all-demo

Earlier phases drove the demo accounts with machine-speed test scripts, so their TCN windows look bot-like and
would be blocked as soon as the PDP enforces. This clears, for the named users only: TCN profile and risk state,
PDP decision / override / blocked-set entries, escalation cooldowns and tiers, collective scores. It does NOT
touch the audit trail (PostgreSQL, Kafka, HDFS), so the history of what happened remains.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from accesssentinel.db.demo_catalog import USERS  # noqa: E402
from accesssentinel.settings import get_settings  # noqa: E402


def main(argv=None) -> int:
    import redis as redis_lib
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("users", nargs="*")
    ap.add_argument("--all-demo", action="store_true")
    args = ap.parse_args(argv)
    s = get_settings()
    if s.environment == "prod":
        print("refusing to reset monitoring state when AS_ENVIRONMENT=prod", file=sys.stderr)
        return 2
    users = [u for u, *_ in USERS] if args.all_demo else args.users
    if not users:
        ap.error("name users or pass --all-demo")
    r = redis_lib.Redis(**s.redis_kwargs())
    for u in users:
        keys = [f"tcn:profile:{u}", f"tcn3:state:{u}", f"risk:{u}", f"decision:{u}", f"shadow:decision:{u}", f"override:{u}", f"coll:{u}",
                f"esc:cool:rt:{u}", f"esc:cool:bg:{u}"]
        n = r.delete(*keys)
        r.zrem("tcn:recheck", u); r.zrem("pdp:blocked", u); r.zrem("shadow:pdp:blocked", u); r.zrem("esc:active", u)
        r.hdel("esc:tier", u); r.hdel("esc:pending", u); r.hdel("pdp:revoke_pending", u)
        print(f"reset {u}: {n} keys removed")
    return 0


if __name__ == "__main__":
    sys.exit(main())

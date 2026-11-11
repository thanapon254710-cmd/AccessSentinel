"""Operator / analyst actions on PDP decisions (the Phase 9 dashboard calls the same engine).

    python -m accesssentinel.pdp_admin status <user>
    python -m accesssentinel.pdp_admin list
    python -m accesssentinel.pdp_admin release <user> --by <analyst> --note "..." [--grace 3600]
    python -m accesssentinel.pdp_admin block <user> --by <analyst> --note "..." [--ttl 3600]
    python -m accesssentinel.pdp_admin policy-block <user> --by <admin> --reason "..." [--hours 24]
    python -m accesssentinel.pdp_admin policy-unblock <user> --by <admin>
    python -m accesssentinel.pdp_admin audit <user> [--limit 20]

Release/block go through the engine (decision cache + outbox); the PDP service publishes and audits them and
revokes sessions. Policy changes are written to PostgreSQL and applied immediately.
"""
import argparse
import json
import sys
from datetime import datetime, timedelta, timezone


def main(argv=None) -> int:
    import redis as redis_lib
    from sqlalchemy import select, update
    from sqlalchemy.orm import Session
    from .db.engine import make_engine
    from .db.models import PdpDecision, PolicyBlock
    from .pdp_engine import PolicyLocked
    from .settings import get_settings
    from .streaming.pdp_service import SqlPolicySource, build_pdp

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("status"); p.add_argument("user")
    sub.add_parser("list")
    p = sub.add_parser("release"); p.add_argument("user"); p.add_argument("--by", required=True); p.add_argument("--note", default="")
    p.add_argument("--grace", type=float, default=None)
    p = sub.add_parser("block"); p.add_argument("user"); p.add_argument("--by", required=True); p.add_argument("--note", default="")
    p.add_argument("--ttl", type=float, default=None)
    p = sub.add_parser("policy-block"); p.add_argument("user"); p.add_argument("--by", required=True)
    p.add_argument("--reason", required=True); p.add_argument("--hours", type=float, default=None)
    p = sub.add_parser("policy-unblock"); p.add_argument("user"); p.add_argument("--by", required=True)
    p = sub.add_parser("audit"); p.add_argument("user"); p.add_argument("--limit", type=int, default=20)
    args = ap.parse_args(argv)

    s = get_settings()
    pdp = build_pdp(s, redis_lib.Redis(**s.redis_kwargs()))
    engine = make_engine(s)
    policy = SqlPolicySource(engine)
    out = None
    if args.cmd == "status":
        out = pdp.status(args.user)
    elif args.cmd == "list":
        out = [pdp.status(u) for u in pdp.blocked_users()]
    elif args.cmd == "release":
        try:
            out = pdp.release(args.user, args.by, args.note, args.grace) or {"note": "not blocked; nothing to release"}
        except PolicyLocked as e:
            print(str(e), file=sys.stderr); return 3
    elif args.cmd == "block":
        out = pdp.block_manual(args.user, args.by, args.note, args.ttl)
    elif args.cmd in ("policy-block", "policy-unblock"):
        with Session(engine) as db, db.begin():
            if args.cmd == "policy-block":
                exp = datetime.now(timezone.utc) + timedelta(hours=args.hours) if args.hours else None
                db.add(PolicyBlock(user_id=args.user, reason=args.reason, created_by=args.by, expires_at=exp))
            else:
                db.execute(update(PolicyBlock).where(PolicyBlock.user_id == args.user, PolicyBlock.active.is_(True))
                           .values(active=False))
        out = pdp.sync_policy(policy.denied())
    elif args.cmd == "audit":
        with Session(engine) as db:
            rows = db.scalars(select(PdpDecision).where(PdpDecision.user_id == args.user)
                              .order_by(PdpDecision.t.desc()).limit(args.limit)).all()
            out = [{"t": datetime.fromtimestamp(x.t, tz=timezone.utc).isoformat(timespec="seconds"), "decision": x.decision,
                    "transition": x.transition, "reason": x.reason, "actor": x.actor, "mode": x.mode, "s_ind": x.s_ind,
                    "s_coll": x.s_coll, "group_id": x.group_id} for x in rows]
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

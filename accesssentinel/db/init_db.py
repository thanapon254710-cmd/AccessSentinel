"""Create the PostgreSQL schema and (optionally) load the demonstration catalog. Idempotent.

    python -m accesssentinel.db.init_db                 # tables only
    python -m accesssentinel.db.init_db --seed-demo     # + demo apps/resources/ACL/users

Demo users get the password from AS_DEMO_USER_PASSWORD. Existing users keep their password unless
--reset-demo-passwords is given. Demo seeding is refused when AS_ENVIRONMENT=prod.
"""
import argparse
import sys
import time

from sqlalchemy import delete, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from ..security import hash_password
from ..settings import get_settings
from . import demo_catalog as demo
from .engine import make_engine
from .models import SCHEMA_VERSION, AclRule, Application, Base, Resource, SchemaInfo, User


def create_schema(engine: Engine) -> None:
    Base.metadata.create_all(engine)
    with Session(engine) as db, db.begin():
        info = db.get(SchemaInfo, 1)
        if info is None:
            db.add(SchemaInfo(id=1, version=SCHEMA_VERSION))
        elif info.version > SCHEMA_VERSION:
            raise RuntimeError(f"database schema v{info.version} is newer than this code (v{SCHEMA_VERSION})")
        else:
            info.version = SCHEMA_VERSION


def seed_security_users(engine: Engine, password: str, reset_passwords: bool = False) -> int:
    """Dashboard accounts (security_analyst / security_admin); created once, password from AS_ANALYST_PASSWORD."""
    n = 0
    with Session(engine) as db, db.begin():
        for uid, name, role, dept in demo.SECURITY_USERS:
            user = db.get(User, uid)
            if user is None:
                db.add(User(id=uid, display_name=name, role=role, department=dept, password_hash=hash_password(password)))
                n += 1
            elif reset_passwords:
                user.password_hash = hash_password(password)
    return n


def seed_demo(engine: Engine, password: str, reset_passwords: bool = False) -> dict:
    """Upsert demo apps/resources, replace demo-role ACL rows, create demo users. Returns counts."""
    counts = dict(apps=0, resources=0, acl=0, users_created=0, users_reset=0)
    with Session(engine) as db, db.begin():
        for app_id, (name, url, desc) in demo.APPS.items():
            app = db.get(Application, app_id)
            if app is None:
                db.add(Application(id=app_id, name=name, base_url=url, description=desc))
            else:
                app.name, app.base_url, app.description = name, url, desc
            counts["apps"] += 1
        db.flush()
        for app_id, items in demo.RESOURCES.items():
            for key, s, desc in items:
                res = db.scalar(select(Resource).where(Resource.app_id == app_id, Resource.resource_key == key))
                if res is None:
                    db.add(Resource(app_id=app_id, resource_key=key, sensitivity=s, description=desc))
                else:
                    res.sensitivity, res.description = s, desc
                counts["resources"] += 1
        db.flush()
        db.execute(delete(AclRule).where(AclRule.role.in_(demo.ROLES)))     # demo roles are fully managed here
        for a, r, role, p in demo.acl_rows():
            db.add(AclRule(app_id=a, resource_key=r, role=role, permission=p))
            counts["acl"] += 1
        for uid, name, role, dept in demo.USERS:
            user = db.get(User, uid)
            pw_hash = hash_password(password) if (user is None or reset_passwords) else None   # per-user salt
            if user is None:
                db.add(User(id=uid, display_name=name, role=role, department=dept, password_hash=pw_hash))
                counts["users_created"] += 1
            else:
                user.display_name, user.role, user.department = name, role, dept
                if reset_passwords:
                    user.password_hash = pw_hash
                    counts["users_reset"] += 1
    return counts


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed-demo", action="store_true")
    ap.add_argument("--reset-demo-passwords", action="store_true")
    ap.add_argument("--wait", type=float, default=60.0, help="seconds to retry the first connection")
    args = ap.parse_args(argv)
    s = get_settings()
    engine = make_engine(s)
    deadline = time.time() + args.wait
    while True:
        try:
            with engine.connect():
                break
        except Exception as e:  # noqa: BLE001
            if time.time() > deadline:
                print(f"PostgreSQL unreachable: {type(e).__name__}", file=sys.stderr)
                return 1
            time.sleep(2)
    create_schema(engine)
    print(f"schema v{SCHEMA_VERSION} ready")
    if args.seed_demo:
        if s.environment == "prod":
            print("refusing to seed demo users when AS_ENVIRONMENT=prod", file=sys.stderr)
            return 2
        counts = seed_demo(engine, s.secret("demo_user_password"), args.reset_demo_passwords)
        print("demo catalog:", ", ".join(f"{k}={v}" for k, v in counts.items()))
        if s.analyst_password is not None:
            print("security users created:", seed_security_users(engine, s.secret("analyst_password"), args.reset_demo_passwords))
    return 0


if __name__ == "__main__":
    sys.exit(main())

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ..settings import Settings


def make_engine(s: Settings, **kw) -> Engine:
    dsn = s.postgres_dsn().replace("postgresql://", "postgresql+psycopg://", 1)
    return create_engine(dsn, pool_pre_ping=True, pool_size=5, max_overflow=5, **kw)

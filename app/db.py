import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, make_url


def get_engine() -> Engine:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")

    url = make_url(db_url)
    # Normalize to psycopg driver for SQLAlchemy (safe even if already present)
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+psycopg")

    return create_engine(url, pool_pre_ping=True, future=True)

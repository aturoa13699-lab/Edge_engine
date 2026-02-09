import os
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url


def get_engine():
    url = make_url(os.getenv("DATABASE_URL"))
    # Force psycopg driver for SQLAlchemy if not present
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+psycopg")
    return create_engine(url, pool_pre_ping=True)

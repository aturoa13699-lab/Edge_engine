import os

from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.engine import Engine, make_url

_DB_URL_ALIASES = (
    "DATABASE_URL",
    "DATABASE_PRIVATE_URL",
    "POSTGRES_URL",
    "POSTGRESQL_URL",
)


def _resolve_database_url() -> str:
    for key in _DB_URL_ALIASES:
        value = os.getenv(key)
        if value:
            return value
    raise RuntimeError(
        "DATABASE_URL is not set (checked aliases: DATABASE_URL, DATABASE_PRIVATE_URL, POSTGRES_URL, POSTGRESQL_URL)"
    )


def get_engine() -> Engine:
    db_url = _resolve_database_url()

    url = make_url(db_url)
    # Normalize to psycopg driver for SQLAlchemy (safe even if already present)
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+psycopg")

    query = dict(url.query)
    sslmode = query.get("sslmode")
    explicit_sslmode = os.getenv("DB_SSLMODE", "").strip()
    require_ssl = os.getenv("REQUIRE_DB_SSL", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not sslmode and explicit_sslmode:
        query["sslmode"] = explicit_sslmode
    elif not sslmode and require_ssl and url.drivername.startswith("postgresql"):
        query["sslmode"] = "require"
    if query != dict(url.query):
        url = url.set(query=query)

    return create_engine(url, pool_pre_ping=True, future=True)


def check_db_connectivity(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(sql_text("SELECT 1"))

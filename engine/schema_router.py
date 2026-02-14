from __future__ import annotations

import os

from sqlalchemy.engine import Engine


def truth_schema() -> str:
    return os.getenv("NRL_SCHEMA", "nrl_clean").strip() or "nrl_clean"


def ops_schema() -> str:
    return os.getenv("NRL_OPS_SCHEMA", "nrl").strip() or "nrl"


def _qualify(engine: Engine, schema: str, relation: str) -> str:
    if engine.dialect.name.startswith("postgres"):
        return f"{schema}.{relation}"
    return relation


def truth_table(engine: Engine, table: str) -> str:
    return _qualify(engine, truth_schema(), table)


def truth_view(engine: Engine, view: str) -> str:
    return _qualify(engine, truth_schema(), view)


def ops_table(engine: Engine, table: str) -> str:
    return _qualify(engine, ops_schema(), table)
def truth_table(engine: Engine, table: str) -> str:
    if engine.dialect.name.startswith("postgres"):
        return f"{truth_schema()}.{table}"
    return table


def truth_view(engine: Engine, view: str) -> str:
    if engine.dialect.name.startswith("postgres"):
        return f"{truth_schema()}.{view}"
    return view

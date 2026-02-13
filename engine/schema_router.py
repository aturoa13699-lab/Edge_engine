from __future__ import annotations

import os

from sqlalchemy.engine import Engine


def truth_schema() -> str:
    return os.getenv("NRL_SCHEMA", "nrl_clean").strip() or "nrl_clean"


def truth_table(engine: Engine, table: str) -> str:
    if engine.dialect.name.startswith("postgres"):
        return f"{truth_schema()}.{table}"
    return table


def truth_view(engine: Engine, view: str) -> str:
    if engine.dialect.name.startswith("postgres"):
        return f"{truth_schema()}.{view}"
    return view

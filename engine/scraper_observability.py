from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

logger = logging.getLogger("nrl-pillar1")


def scraper_dry_run_enabled() -> bool:
    return os.getenv("SCRAPER_DRY_RUN", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def new_run_id() -> str:
    return str(uuid.uuid4())


def log_event(event: str, **payload: Any) -> None:
    logger.info(
        "SCRAPER_%s %s", event, json.dumps(payload, default=str, sort_keys=True)
    )


def _ensure_runs_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            sql_text(
                """
                CREATE TABLE IF NOT EXISTS nrl.scraper_runs (
                  run_id text NOT NULL,
                  scraper text NOT NULL,
                  season integer,
                  started_at timestamptz NOT NULL DEFAULT now(),
                  finished_at timestamptz,
                  status text NOT NULL,
                  dry_run boolean NOT NULL DEFAULT false,
                  rows_inserted integer NOT NULL DEFAULT 0,
                  rows_updated integer NOT NULL DEFAULT 0,
                  fetch_count integer NOT NULL DEFAULT 0,
                  last_error text,
                  details_json jsonb NOT NULL DEFAULT '{}'::jsonb,
                  PRIMARY KEY (run_id, scraper)
                )
                """
            )
        )


def upsert_run(
    engine: Engine,
    *,
    run_id: str,
    scraper: str,
    season: int,
    status: str,
    dry_run: bool,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    fetch_count: int = 0,
    last_error: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    _ensure_runs_table(engine)
    with engine.begin() as conn:
        conn.execute(
            sql_text(
                """
                INSERT INTO nrl.scraper_runs
                (run_id, scraper, season, status, dry_run, rows_inserted, rows_updated, fetch_count, last_error, details_json)
                VALUES (:run_id, :scraper, :season, :status, :dry_run, :rows_inserted, :rows_updated, :fetch_count, :last_error, CAST(:details AS jsonb))
                ON CONFLICT (run_id, scraper) DO UPDATE SET
                  finished_at = CASE WHEN EXCLUDED.status IN ('success', 'failed', 'skipped') THEN now() ELSE nrl.scraper_runs.finished_at END,
                  status = EXCLUDED.status,
                  dry_run = EXCLUDED.dry_run,
                  rows_inserted = EXCLUDED.rows_inserted,
                  rows_updated = EXCLUDED.rows_updated,
                  fetch_count = EXCLUDED.fetch_count,
                  last_error = EXCLUDED.last_error,
                  details_json = EXCLUDED.details_json
                """
            ),
            {
                "run_id": run_id,
                "scraper": scraper,
                "season": season,
                "status": status,
                "dry_run": dry_run,
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "fetch_count": fetch_count,
                "last_error": last_error,
                "details": json.dumps(details or {}, default=str),
            },
        )


def latest_status(engine: Engine) -> list[dict[str, Any]]:
    _ensure_runs_table(engine)
    with engine.begin() as conn:
        rows = (
            conn.execute(
                sql_text(
                    """
                SELECT DISTINCT ON (scraper)
                  scraper,
                  run_id,
                  season,
                  started_at,
                  finished_at,
                  status,
                  dry_run,
                  rows_inserted,
                  rows_updated,
                  fetch_count,
                  last_error,
                  details_json
                FROM nrl.scraper_runs
                ORDER BY scraper, started_at DESC
                """
                )
            )
            .mappings()
            .all()
        )
    out: list[dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        data["last_success"] = (
            data["finished_at"] if data["status"] == "success" else None
        )
        out.append(data)
    return out


class StepTimer:
    def __init__(self) -> None:
        self.started = time.perf_counter()

    def elapsed_ms(self) -> int:
        return int((time.perf_counter() - self.started) * 1000)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

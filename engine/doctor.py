from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

logger = logging.getLogger("nrl-pillar1")


@dataclass
class DoctorReport:
    ok: bool
    failures: list[str]
    warnings: list[str]


def _check_env() -> tuple[list[str], list[str]]:
    failures: list[str] = []
    warnings: list[str] = []
    required_any = [
        "DATABASE_URL",
        "DATABASE_PRIVATE_URL",
        "POSTGRES_URL",
        "POSTGRESQL_URL",
    ]
    if not any(os.getenv(k) for k in required_any):
        failures.append("Missing database URL env (DATABASE_URL or supported aliases)")

    if not os.getenv("TZ"):
        warnings.append("TZ is not set")

    if not os.getenv("REFEREE_URL"):
        warnings.append("REFEREE_URL not set (referee scraper will skip)")

    return failures, warnings


def _check_playwright() -> list[str]:
    warnings: list[str] = []
    try:
        import playwright  # noqa: F401
    except Exception:
        warnings.append("Playwright package not installed")
    return warnings


def _check_required_tables(engine: Engine) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    warnings: list[str] = []
    required_tables = [
        ("nrl", "matches_raw"),
        ("nrl", "odds"),
        ("nrl", "model_prediction"),
        ("nrl", "slips"),
        ("nrl", "scraper_runs"),
        ("nrl_clean", "matches_raw"),
        ("nrl_clean", "odds"),
    ]
    with engine.begin() as conn:
        for schema, table in required_tables:
            exists = conn.execute(
                sql_text(
                    """
                    SELECT EXISTS(
                      SELECT 1
                      FROM information_schema.tables
                      WHERE table_schema = :schema AND table_name = :table
                    )
                    """
                ),
                {"schema": schema, "table": table},
            ).scalar_one()
            if not exists:
                failures.append(f"Missing required table: {schema}.{table}")

        freshness_rows = (
            conn.execute(
                sql_text(
                    """
                SELECT scraper, MAX(started_at) AS last_run
                FROM nrl.scraper_runs
                GROUP BY scraper
                """
                )
            )
            .mappings()
            .all()
        )

        if not freshness_rows:
            warnings.append("No scraper_runs history found")
        else:
            stale_rows = (
                conn.execute(
                    sql_text(
                        """
                    SELECT scraper
                    FROM (
                      SELECT scraper, MAX(started_at) AS last_run
                      FROM nrl.scraper_runs
                      GROUP BY scraper
                    ) x
                    WHERE last_run < (now() - interval '24 hours')
                    """
                    )
                )
                .scalars()
                .all()
            )
            for scraper in stale_rows:
                warnings.append(f"Scraper stale >24h: {scraper}")

    return failures, warnings


def run_doctor(engine: Engine) -> DoctorReport:
    failures, warnings = _check_env()

    try:
        with engine.begin() as conn:
            conn.execute(sql_text("SELECT 1"))
    except Exception as exc:
        failures.append(f"DB connectivity failed: {type(exc).__name__}: {exc}")
        report = DoctorReport(ok=False, failures=failures, warnings=warnings)
        _log_report(report)
        return report

    table_failures, table_warnings = _check_required_tables(engine)
    failures.extend(table_failures)
    warnings.extend(table_warnings)
    warnings.extend(_check_playwright())

    require_ssl = os.getenv("REQUIRE_DB_SSL", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    sslmode = os.getenv("DB_SSLMODE", "").strip()
    if require_ssl and not sslmode:
        warnings.append(
            "REQUIRE_DB_SSL=1 set without DB_SSLMODE; defaulting to sslmode=require"
        )

    report = DoctorReport(ok=not failures, failures=failures, warnings=warnings)
    _log_report(report)
    return report


def _log_report(report: DoctorReport) -> None:
    if report.ok:
        logger.info("Doctor OK")
    else:
        logger.error("Doctor FAIL")

    for item in report.failures:
        logger.error("DOCTOR_FAIL %s", item)
    for item in report.warnings:
        logger.warning("DOCTOR_WARN %s", item)

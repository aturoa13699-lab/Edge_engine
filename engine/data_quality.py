from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .schema_router import ops_schema, truth_schema
from .seed_data import HOME_VENUES, NRL_TEAMS


class DataQualityError(RuntimeError):
    """Raised when fail-closed data quality checks fail."""


@dataclass
class DataQualityReport:
    ok: bool
    checked_at: str
    seasons: list[int]
    checks: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    metrics: dict[str, int | str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "checked_at": self.checked_at,
            "seasons": self.seasons,
            "checks": self.checks,
            "errors": self.errors,
            "metrics": self.metrics,
        }


def _table_name(engine: Engine, table: str) -> str:
    if engine.dialect.name.startswith("postgres"):
        return f"{truth_schema()}.{table}"
        return f"nrl.{table}"
    return table


def _report_table_name(engine: Engine) -> str:
    if engine.dialect.name.startswith("postgres"):
        return f"{ops_schema()}.data_quality_reports"
    return "data_quality_reports"


def _ensure_report_table(engine: Engine) -> None:
    table_name = _report_table_name(engine)
    if engine.dialect.name.startswith("postgres"):
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id bigserial PRIMARY KEY,
            checked_at timestamptz NOT NULL,
            seasons text NOT NULL,
            ok boolean NOT NULL,
            report_json jsonb NOT NULL
        )
        """
    else:
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            checked_at TEXT NOT NULL,
            seasons TEXT NOT NULL,
            ok INTEGER NOT NULL,
            report_json TEXT NOT NULL
        )
        """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def persist_data_quality_report(engine: Engine, report: DataQualityReport) -> None:
    _ensure_report_table(engine)
    table_name = _report_table_name(engine)
    insert_sql = (
        f"""
        INSERT INTO {table_name} (checked_at, seasons, ok, report_json)
        VALUES (:checked_at, :seasons, :ok, CAST(:report_json AS jsonb))
        """
        if engine.dialect.name.startswith("postgres")
        else f"""
        INSERT INTO {table_name} (checked_at, seasons, ok, report_json)
        VALUES (:checked_at, :seasons, :ok, :report_json)
        """
    )
    with engine.begin() as conn:
        conn.execute(
            text(insert_sql),
            {
                "checked_at": report.checked_at,
                "seasons": ",".join(str(s) for s in report.seasons),
                "ok": report.ok
                if engine.dialect.name.startswith("postgres")
                else (1 if report.ok else 0),
                "report_json": json.dumps(report.to_dict(), sort_keys=True),
            },
        )


def _parse_gate_seasons(raw: str | None) -> list[int]:
    if not raw:
        return [2022, 2023, 2024, 2025]
    return [int(s.strip()) for s in raw.split(",") if s.strip()]


def run_data_quality_gate(
    engine: Engine,
    seasons: list[int] | None = None,
    *,
    expected_matches_per_round: int = 8,
    max_score: int = 80,
) -> DataQualityReport:
    checked_at = datetime.now(timezone.utc).isoformat()
    target_seasons = seasons or _parse_gate_seasons(os.getenv("QUALITY_GATE_SEASONS"))
    report = DataQualityReport(ok=True, checked_at=checked_at, seasons=target_seasons)
    matches_table = _table_name(engine, "matches_raw")

    with engine.begin() as conn:
        for season in target_seasons:
            report.checks.append(f"season:{season}:presence")
            row = (
                conn.execute(
                    text(
                        f"SELECT count(*) AS n FROM {matches_table} WHERE season = :season"
                    ),
                    {"season": season},
                )
                .mappings()
                .first()
            )
            match_count = int(row["n"]) if row else 0
            report.metrics[f"season_{season}_matches"] = match_count
            if match_count == 0:
                report.errors.append(f"season {season}: no matches found")
                continue

            report.checks.append(f"season:{season}:duplicate_match_id")
            duplicate_matches = (
                conn.execute(
                    text(
                        f"""
                    SELECT count(*) AS n
                    FROM (
                        SELECT match_id
                        FROM {matches_table}
                        WHERE season = :season
                        GROUP BY match_id
                        HAVING count(*) > 1
                    ) d
                    """
                    ),
                    {"season": season},
                )
                .mappings()
                .first()
            )
            duplicate_count = int(duplicate_matches["n"]) if duplicate_matches else 0
            report.metrics[f"season_{season}_duplicate_match_ids"] = duplicate_count
            if duplicate_count > 0:
                report.errors.append(
                    f"season {season}: duplicate match_id rows detected"
                )

            report.checks.append(f"season:{season}:home_away_distinct")
            same_team_rows = (
                conn.execute(
                    text(
                        f"""
                    SELECT count(*) AS n
                    FROM {matches_table}
                    WHERE season = :season
                      AND home_team = away_team
                    """
                    ),
                    {"season": season},
                )
                .mappings()
                .first()
            )
            same_team_count = int(same_team_rows["n"]) if same_team_rows else 0
            report.metrics[f"season_{season}_home_equals_away_rows"] = same_team_count
            if same_team_count > 0:
                report.errors.append(
                    f"season {season}: rows found where home_team == away_team"
                )

            report.checks.append(f"season:{season}:round_integrity")
            rounds = (
                conn.execute(
                    text(
                        f"""
                    SELECT round_num, count(*) AS n
                    FROM {matches_table}
                    WHERE season = :season
                    GROUP BY round_num
                    ORDER BY round_num
                    """
                    ),
                    {"season": season},
                )
                .mappings()
                .all()
            )
            round_nums = [int(r["round_num"]) for r in rounds]
            if not round_nums:
                report.errors.append(f"season {season}: no rounds found")
            else:
                missing = sorted(
                    set(range(min(round_nums), max(round_nums) + 1)) - set(round_nums)
                )
                if missing:
                    report.errors.append(f"season {season}: missing rounds {missing}")
                for round_row in rounds:
                    if int(round_row["n"]) != expected_matches_per_round:
                        report.errors.append(
                            f"season {season} round {int(round_row['round_num'])}: expected "
                            f"{expected_matches_per_round} matches, found {int(round_row['n'])}"
                        )

            report.checks.append(f"season:{season}:score_bounds")
            invalid_scores = (
                conn.execute(
                    text(
                        f"""
                    SELECT count(*) AS n
                    FROM {matches_table}
                    WHERE season = :season
                      AND (
                        home_score IS NULL OR away_score IS NULL OR
                        home_score < 0 OR away_score < 0 OR
                        home_score > :max_score OR away_score > :max_score
                      )
                    """
                    ),
                    {"season": season, "max_score": max_score},
                )
                .mappings()
                .first()
            )
            bad_score_count = int(invalid_scores["n"]) if invalid_scores else 0
            report.metrics[f"season_{season}_bad_score_rows"] = bad_score_count
            if bad_score_count > 0:
                report.errors.append(
                    f"season {season}: {bad_score_count} rows have null/implausible scores"
                )

            report.checks.append(f"season:{season}:team_canonical")
            team_rows = (
                conn.execute(
                    text(
                        f"""
                    SELECT home_team AS team FROM {matches_table} WHERE season = :season
                    UNION ALL
                    SELECT away_team AS team FROM {matches_table} WHERE season = :season
                    """
                    ),
                    {"season": season},
                )
                .mappings()
                .all()
            )
            unknown_teams = sorted(
                {str(r["team"]) for r in team_rows if str(r["team"]) not in NRL_TEAMS}
            )
            if unknown_teams:
                report.errors.append(f"season {season}: unknown teams {unknown_teams}")

            report.checks.append(f"season:{season}:venue_canonical")
            venue_rows = (
                conn.execute(
                    text(
                        f"SELECT DISTINCT venue FROM {matches_table} WHERE season = :season"
                    ),
                    {"season": season},
                )
                .mappings()
                .all()
            )
            known_venues = set(HOME_VENUES.values())
            unknown_venues = sorted(
                {
                    str(r["venue"])
                    for r in venue_rows
                    if r["venue"] is None
                    or str(r["venue"]).strip() == ""
                    or str(r["venue"]) not in known_venues
                }
            )
            if unknown_venues:
                report.errors.append(
                    f"season {season}: non-canonical venues {unknown_venues}"
                )

            report.checks.append(f"season:{season}:checksum")
            digest_source = (
                conn.execute(
                    text(
                        f"""
                    SELECT match_id, home_team, away_team, home_score, away_score
                    FROM {matches_table}
                    WHERE season = :season
                    ORDER BY match_id
                    """
                    ),
                    {"season": season},
                )
                .mappings()
                .all()
            )
            payload = "|".join(
                f"{r['match_id']}:{r['home_team']}:{r['away_team']}:{r['home_score']}:{r['away_score']}"
                for r in digest_source
            )
            checksum = hashlib.sha256(payload.encode("utf-8")).hexdigest()
            report.metrics[f"season_{season}_checksum"] = checksum
            expected_checksum = os.getenv(f"QUALITY_GATE_CHECKSUM_{season}")
            if expected_checksum and checksum != expected_checksum:
                report.errors.append(
                    f"season {season}: checksum mismatch expected={expected_checksum} actual={checksum}"
                )

    report.ok = len(report.errors) == 0
    return report


def enforce_data_quality_gate(
    engine: Engine, seasons: list[int] | None = None
) -> DataQualityReport:
    report = run_data_quality_gate(engine, seasons=seasons)
    persist_data_quality_report(engine, report)
    if not report.ok:
        joined = "; ".join(report.errors)
        raise DataQualityError(f"Data quality gate failed: {joined}")
    return report

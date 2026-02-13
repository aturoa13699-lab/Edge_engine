from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from .model_trainer import FEATURE_COLS, build_features
from .schema_router import truth_schema, truth_table


@dataclass
class SchemaParityReport:
    ok: bool
    schema: str
    checked_objects: list[str] = field(default_factory=list)
    missing_objects: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "schema": self.schema,
            "checked_objects": self.checked_objects,
            "missing_objects": self.missing_objects,
            "errors": self.errors,
        }


def _exists(engine: Engine, schema: str, relation: str) -> bool:
    if engine.dialect.name.startswith("postgres"):
        with engine.begin() as conn:
            row = (
                conn.execute(
                    sql_text(
                        """
                        SELECT EXISTS(
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_schema = :schema_name AND table_name = :rel
                        )
                        OR EXISTS(
                            SELECT 1
                            FROM information_schema.views
                            WHERE table_schema = :schema_name AND table_name = :rel
                        ) AS ok
                        """
                    ),
                    {"schema_name": schema, "rel": relation},
                )
                .mappings()
                .first()
            )
        return bool(row and row["ok"])

    # sqlite/local fallback: relation names are unqualified
    with engine.begin() as conn:
        row = (
            conn.execute(
                sql_text(
                    "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name=:rel"
                ),
                {"rel": relation},
            )
            .mappings()
            .first()
        )
    return row is not None


def run_truth_schema_parity_smoke(engine: Engine) -> SchemaParityReport:
    schema = truth_schema()
    report = SchemaParityReport(ok=True, schema=schema)

    required_relations = [
        "matches_raw",
        "odds",
        "team_rest_v",
        "team_form_v",
        "coach_profile",
        "injuries_current",
        "team_ratings",
        "weather_daily",
    ]

    for rel in required_relations:
        report.checked_objects.append(rel)
        if not _exists(engine, schema, rel):
            report.missing_objects.append(rel)

    if report.missing_objects:
        report.ok = False
        report.errors.append(
            f"missing relations in {schema}: {', '.join(sorted(report.missing_objects))}"
        )
        return report

    if not engine.dialect.name.startswith("postgres"):
        # sqlite test/dev fallback: only validate object presence.
        return report

    # End-to-end feature plan smoke: ensure feature builder executes
    try:
        seasons_query = sql_text(
            f"SELECT DISTINCT season FROM {truth_table(engine, 'matches_raw')} ORDER BY season"
        )
        with engine.begin() as conn:
            seasons_rows = conn.execute(seasons_query).mappings().all()
        seasons = [int(r["season"]) for r in seasons_rows[:1]] or [2025]

        df = build_features(engine, seasons=seasons)
        missing_cols = [c for c in FEATURE_COLS if c not in df.columns]
        if missing_cols:
            report.ok = False
            report.errors.append(
                f"feature query missing columns: {', '.join(sorted(missing_cols))}"
            )
    except Exception as exc:
        report.ok = False
        report.errors.append(f"feature query plan failed: {exc}")

    # Deploy feature path smoke: query should compile/run even for a non-existent match
    try:
        from .deploy_engine import _fetch_live_feature_row

        _fetch_live_feature_row(engine, match_id="__parity_smoke_missing__")
    except Exception as exc:
        report.ok = False
        report.errors.append(f"deploy feature fetch failed: {exc}")

    return report


def enforce_truth_schema_parity_smoke(engine: Engine) -> SchemaParityReport:
    report = run_truth_schema_parity_smoke(engine)
    if not report.ok:
        raise RuntimeError(
            "Truth schema parity smoke failed: " + "; ".join(report.errors)
        )
    return report

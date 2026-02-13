from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from .schema_router import ops_schema, ops_table


@dataclass
class OpsParityReport:
    ok: bool
    schema: str
    checked_objects: list[str] = field(default_factory=list)
    missing_objects: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    write_roundtrip_ok: bool = False

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "schema": self.schema,
            "checked_objects": self.checked_objects,
            "missing_objects": self.missing_objects,
            "errors": self.errors,
            "write_roundtrip_ok": self.write_roundtrip_ok,
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
                        ) AS ok
                        """
                    ),
                    {"schema_name": schema, "rel": relation},
                )
                .mappings()
                .first()
            )
        return bool(row and row["ok"])

    with engine.begin() as conn:
        row = (
            conn.execute(
                sql_text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=:rel"
                ),
                {"rel": relation},
            )
            .mappings()
            .first()
        )
    return row is not None


def _ops_write_roundtrip(engine: Engine, report: OpsParityReport) -> bool:
    # Postgres-only write test to avoid sqlite schema/type differences.
    if not engine.dialect.name.startswith("postgres"):
        return True

    slips_table = ops_table(engine, "slips")
    portfolio_id = "ops_parity_probe"

    try:
        with engine.begin() as conn:
            conn.execute(
                sql_text(
                    f"""
                    INSERT INTO {slips_table} (portfolio_id, season, round_num, slip_json, status)
                    VALUES (:pid, :s, :r, CAST(:sj AS jsonb), :st)
                    ON CONFLICT (portfolio_id) DO UPDATE
                    SET status = EXCLUDED.status
                    """
                ),
                {
                    "pid": portfolio_id,
                    "s": 2099,
                    "r": 1,
                    "sj": '{"probe":true}',
                    "st": "ops_parity_probe",
                },
            )

            row = (
                conn.execute(
                    sql_text(
                        f"SELECT status FROM {slips_table} WHERE portfolio_id = :pid"
                    ),
                    {"pid": portfolio_id},
                )
                .mappings()
                .first()
            )
            conn.execute(
                sql_text(f"DELETE FROM {slips_table} WHERE portfolio_id = :pid"),
                {"pid": portfolio_id},
            )

        return bool(row and row["status"] == "ops_parity_probe")
    except Exception as exc:
        report.errors.append(f"ops write/read roundtrip failed: {exc}")
        return False


def run_ops_schema_parity_smoke(engine: Engine) -> OpsParityReport:
    schema = ops_schema()
    report = OpsParityReport(ok=True, schema=schema)

    required_relations = [
        "slips",
        "model_prediction",
        "model_registry",
        "calibration_params",
        "data_quality_reports",
    ]

    for rel in required_relations:
        report.checked_objects.append(rel)
        if not _exists(engine, schema, rel):
            report.missing_objects.append(rel)

    if report.missing_objects:
        report.ok = False
        report.errors.append(
            f"missing ops relations in {schema}: {', '.join(sorted(report.missing_objects))}"
        )
        return report

    report.write_roundtrip_ok = _ops_write_roundtrip(engine, report)
    if not report.write_roundtrip_ok:
        report.ok = False

    return report


def enforce_ops_schema_parity_smoke(engine: Engine) -> OpsParityReport:
    report = run_ops_schema_parity_smoke(engine)
    if not report.ok:
        raise RuntimeError(
            "Ops schema parity smoke failed: " + "; ".join(report.errors)
        )
    return report

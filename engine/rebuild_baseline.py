from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from .backfill import backfill_predictions, label_outcomes
from .backtester import run_backtest
from .calibration import fit_beta_calibrator
from .model_trainer import train_model
from .ops_parity import enforce_ops_schema_parity_smoke
from .schema_parity import enforce_truth_schema_parity_smoke
from .schema_router import ops_table, ops_schema, truth_schema


@dataclass
class RebuildResult:
    seasons: list[int]
    calibration_season: int
    backtest_season: int
    backfilled: int
    labelled: int
    backtest_summary: dict
    manifest_id: int | None

    def to_dict(self) -> dict:
        return {
            "seasons": self.seasons,
            "calibration_season": self.calibration_season,
            "backtest_season": self.backtest_season,
            "backfilled": self.backfilled,
            "labelled": self.labelled,
            "backtest_summary": self.backtest_summary,
            "manifest_id": self.manifest_id,
        }


def _ensure_manifest_table(engine: Engine) -> None:
    table = ops_table(engine, "run_manifest")
    if engine.dialect.name.startswith("postgres"):
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
          id bigserial PRIMARY KEY,
          run_type text NOT NULL,
          run_started_at timestamptz NOT NULL,
          truth_schema text NOT NULL,
          ops_schema text NOT NULL,
          seasons text NOT NULL,
          payload jsonb NOT NULL
        )
        """
    else:
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_type text NOT NULL,
          run_started_at text NOT NULL,
          truth_schema text NOT NULL,
          ops_schema text NOT NULL,
          seasons text NOT NULL,
          payload text NOT NULL
        )
        """
    with engine.begin() as conn:
        conn.execute(sql_text(ddl))


def _insert_manifest(engine: Engine, payload: dict, seasons: list[int]) -> int | None:
    _ensure_manifest_table(engine)
    table = ops_table(engine, "run_manifest")
    started = datetime.now(timezone.utc).isoformat()
    serialized = json.dumps(payload, sort_keys=True)
    with engine.begin() as conn:
        if engine.dialect.name.startswith("postgres"):
            row = (
                conn.execute(
                    sql_text(
                        f"""
                        INSERT INTO {table} (run_type, run_started_at, truth_schema, ops_schema, seasons, payload)
                        VALUES (:rt, :started, :truth, :ops, :seasons, CAST(:payload AS jsonb))
                        RETURNING id
                        """
                    ),
                    {
                        "rt": "p0_rebuild_clean_baseline",
                        "started": started,
                        "truth": truth_schema(),
                        "ops": ops_schema(),
                        "seasons": ",".join(str(s) for s in seasons),
                        "payload": serialized,
                    },
                )
                .mappings()
                .first()
            )
            return int(row["id"]) if row else None

        conn.execute(
            sql_text(
                f"""
                INSERT INTO {table} (run_type, run_started_at, truth_schema, ops_schema, seasons, payload)
                VALUES (:rt, :started, :truth, :ops, :seasons, :payload)
                """
            ),
            {
                "rt": "p0_rebuild_clean_baseline",
                "started": started,
                "truth": truth_schema(),
                "ops": ops_schema(),
                "seasons": ",".join(str(s) for s in seasons),
                "payload": serialized,
            },
        )
        row = (
            conn.execute(sql_text("SELECT last_insert_rowid() AS id"))
            .mappings()
            .first()
        )
        return int(row["id"]) if row else None


def run_rebuild_clean_baseline(
    engine: Engine,
    seasons: list[int],
    calibration_season: int,
    backtest_season: int,
) -> RebuildResult:
    enforce_truth_schema_parity_smoke(engine)
    enforce_ops_schema_parity_smoke(engine)

    train_out = train_model(engine, seasons=seasons)
    if not train_out:
        raise RuntimeError("training failed or insufficient data")

    fit_params = fit_beta_calibrator(engine, calibration_season)
    if not fit_params:
        raise RuntimeError("calibration fit failed or insufficient samples")

    backfill_out = backfill_predictions(engine, season=backtest_season)
    label_out = label_outcomes(engine, season=backtest_season)
    bt = run_backtest(engine, season=backtest_season)
    summary = bt.summary()

    result_payload = {
        "train": train_out,
        "calibration": fit_params,
        "backfill": backfill_out,
        "label": label_out,
        "backtest": summary,
        "env": {
            "NRL_SCHEMA": os.getenv("NRL_SCHEMA", "nrl_clean"),
            "NRL_OPS_SCHEMA": os.getenv("NRL_OPS_SCHEMA", "nrl"),
        },
    }

    manifest_id = _insert_manifest(engine, result_payload, seasons=seasons)

    return RebuildResult(
        seasons=seasons,
        calibration_season=calibration_season,
        backtest_season=backtest_season,
        backfilled=int(backfill_out.get("backfilled", 0)),
        labelled=int(label_out.get("labelled", 0)),
        backtest_summary=summary,
        manifest_id=manifest_id,
    )

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine


def get_champion(engine: Engine, model_key: str) -> Optional[Dict[str, Any]]:
    with engine.begin() as conn:
        row = conn.execute(
            sql_text(
                """
                SELECT model_key, version, artifact_path, metrics, created_at
                FROM nrl.model_registry
                WHERE model_key=:k AND is_champion=true
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            dict(k=model_key),
        ).mappings().first()

    if not row:
        return None

    m = row["metrics"]
    if isinstance(m, str):
        m = json.loads(m)
    return {
        "model_key": row["model_key"],
        "version": row["version"],
        "artifact_path": row["artifact_path"],
        "metrics": m,
        "created_at": str(row["created_at"]),
    }


def register_model(engine: Engine, model_key: str, version: str, artifact_path: str, metrics: Dict[str, Any]) -> None:
    with engine.begin() as conn:
        conn.execute(
            sql_text(
                """
                INSERT INTO nrl.model_registry (model_key, version, artifact_path, metrics, is_champion)
                VALUES (:k, :v, :p, :m::jsonb, false)
                ON CONFLICT (model_key, version) DO UPDATE
                SET artifact_path=EXCLUDED.artifact_path,
                    metrics=EXCLUDED.metrics
                """
            ),
            dict(k=model_key, v=version, p=artifact_path, m=json.dumps(metrics)),
        )


def promote_champion(engine: Engine, model_key: str, version: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            sql_text("UPDATE nrl.model_registry SET is_champion=false WHERE model_key=:k"),
            dict(k=model_key),
        )
        conn.execute(
            sql_text("UPDATE nrl.model_registry SET is_champion=true WHERE model_key=:k AND version=:v"),
            dict(k=model_key, v=version),
        )


def maybe_promote_by_brier(engine: Engine, model_key: str, version: str, new_brier: float) -> bool:
    champ = get_champion(engine, model_key)
    if not champ:
        promote_champion(engine, model_key, version)
        return True

    champ_brier = None
    try:
        champ_brier = float(champ["metrics"].get("cv_brier_mean"))
    except Exception:
        champ_brier = None

    if champ_brier is None or new_brier < champ_brier:
        promote_champion(engine, model_key, version)
        return True

    return False

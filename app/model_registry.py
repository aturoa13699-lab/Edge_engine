import os
import json
import logging
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

logger = logging.getLogger("nrl-pillar1")


def register_model(engine: Engine, model_id: str, metrics: dict, artifact_path: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            sql_text(
                """
            INSERT INTO nrl.model_registry (model_id, metrics, artifact_path)
            VALUES (:mid, :m::jsonb, :p)
            ON CONFLICT (model_id) DO UPDATE
            SET metrics = EXCLUDED.metrics, artifact_path = EXCLUDED.artifact_path, updated_at = now()
        """
            ),
            dict(mid=model_id, m=json.dumps(metrics), p=artifact_path),
        )
    logger.info(f"Registered model {model_id} → {artifact_path}")


def get_champion(engine: Engine) -> dict | None:
    with engine.begin() as conn:
        row = conn.execute(
            sql_text(
                """
            SELECT model_id, metrics, artifact_path
            FROM nrl.model_registry
            ORDER BY (metrics->>'cv_brier')::numeric ASC, updated_at DESC
            LIMIT 1
        """
            )
        ).mappings().first()
    return dict(row) if row else None


def champion_path(engine: Engine) -> str | None:
    champ = get_champion(engine)
    if champ:
        return champ.get("artifact_path")
    # fallback default path
    path = os.path.join("models", "nrl_xgboost_v1.joblib")
    return path if os.path.exists(path) else None

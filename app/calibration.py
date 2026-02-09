from __future__ import annotations

import json
import logging
from typing import Dict, Optional

import numpy as np
from scipy.optimize import minimize
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

logger = logging.getLogger("nrl-pillar1")


def _is_postgres(engine: Engine) -> bool:
    try:
        return engine.dialect.name.lower().startswith("postgres")
    except Exception:
        return False


def _tbl(engine: Engine, name: str) -> str:
    # SQLite (tests) doesn't support schema prefixes; Postgres does.
    return f"nrl.{name}" if _is_postgres(engine) else name


def _beta_transform(p: np.ndarray, a: float, b: float) -> np.ndarray:
    p = np.clip(p, 1e-6, 1.0 - 1e-6)
    return (p**a) / (p**a + (1.0 - p) ** b)


def load_latest_calibrator(engine: Engine, season: int) -> Optional[Dict]:
    table = _tbl(engine, "calibration_params")
    with engine.begin() as conn:
        row = conn.execute(
            sql_text(f"SELECT params FROM {table} WHERE season=:s ORDER BY fitted_at DESC LIMIT 1"),
            dict(s=season),
        ).mappings().first()

    if row and row.get("params") is not None:
        params = row["params"]
        if isinstance(params, str):
            return json.loads(params)
        return dict(params)
    return None


def apply_calibration(p_fair: float, params: Optional[Dict]) -> float:
    if not params or "a" not in params or "b" not in params:
        return float(p_fair)

    a = float(params["a"])
    b = float(params["b"])
    p = float(np.clip(float(p_fair), 1e-6, 1.0 - 1e-6))
    calibrated = (p**a) / (p**a + (1.0 - p) ** b)
    return float(np.clip(calibrated, 0.0, 1.0))


def fit_beta_calibrator(engine: Engine, season: int, min_samples: int = 80) -> Optional[Dict]:
    pred_table = _tbl(engine, "model_prediction")
    cal_table = _tbl(engine, "calibration_params")

    with engine.begin() as conn:
        rows = conn.execute(
            sql_text(
                f"""
                SELECT p_fair, outcome_home_win
                FROM {pred_table}
                WHERE season=:s AND outcome_known=true AND p_fair IS NOT NULL
                """
            ),
            dict(s=season),
        ).mappings().all()

    if len(rows) < min_samples:
        logger.info("Calibration skipped for S%s — only %s samples (need %s)", season, len(rows), min_samples)
        return None

    p = np.array([float(r["p_fair"]) for r in rows], dtype=float)
    y = np.array([1.0 if r["outcome_home_win"] else 0.0 for r in rows], dtype=float)

    def brier_loss(params):
        a, b = params
        p_cal = _beta_transform(p, float(a), float(b))
        return float(np.mean((p_cal - y) ** 2))

    res = minimize(
        brier_loss,
        x0=[1.0, 1.0],
        bounds=[(0.01, 10.0), (0.01, 10.0)],
        method="L-BFGS-B",
    )

<<<<<<< HEAD
    a, b = float(res.x[0]), float(res.x[1])
    params = {"a": a, "b": b, "brier_loss": float(res.fun), "fitted_on": season}

    with engine.begin() as conn:
        conn.execute(
            sql_text(
                """
            INSERT INTO nrl.calibration_params (season, params)
            VALUES (:s, :p::jsonb)
            ON CONFLICT (season) DO UPDATE
            SET params = EXCLUDED.params, fitted_at = now()
        """
            ),
            dict(s=season, p=json.dumps(params)),
        )

    logger.info(f"✓ Beta calibration fitted for S{season}: a={a:.3f}, b={b:.3f} (Brier={res.fun:.4f})")
    return params


def load_latest_calibrator(engine: Engine, season: int) -> Optional[Dict]:
    """Load the most recent calibration parameters for a season."""
    with engine.begin() as conn:
        row = conn.execute(
            sql_text(
                """
            SELECT params FROM nrl.calibration_params
            WHERE season = :s ORDER BY fitted_at DESC LIMIT 1
        """
            ),
            dict(s=season),
        ).mappings().first()

    if row and row.get("params"):
        return dict(row["params"])
    return None


def apply_calibration(p_fair: float, params: Optional[Dict]) -> float:
    """Apply fitted beta calibration (fallback to raw p_fair)."""
    if not params or "a" not in params:
        return float(p_fair)

    a = params["a"]
    b = params["b"]
    p = np.clip(float(p_fair), 1e-6, 1.0 - 1e-6)
    calibrated = (p ** a) / (p ** a + (1.0 - p) ** b)
    return float(np.clip(calibrated, 0.0, 1.0))
=======
    a = float(res.x[0])
    b = float(res.x[1])
    params = {"a": a, "b": b, "brier_loss": float(res.fun), "fitted_on": season}

    payload = json.dumps(params)

    with engine.begin() as conn:
        if _is_postgres(engine):
            conn.execute(
                sql_text(
                    f"""
                    INSERT INTO {cal_table} (season, params)
                    VALUES (:s, :p::jsonb)
                    ON CONFLICT (season)
                    DO UPDATE SET params=EXCLUDED.params, fitted_at=now()
                    """
                ),
                dict(s=season, p=payload),
            )
        else:
            # SQLite/test path
            conn.execute(
                sql_text(
                    f"""
                    INSERT INTO {cal_table} (season, params, fitted_at)
                    VALUES (:s, :p, CURRENT_TIMESTAMP)
                    ON CONFLICT(season) DO UPDATE SET params=excluded.params, fitted_at=CURRENT_TIMESTAMP
                    """
                ),
                dict(s=season, p=payload),
            )

    logger.info("✓ Beta calibration fitted for S%s: a=%.3f b=%.3f (Brier=%.4f)", season, a, b, float(res.fun))
    return params
>>>>>>> origin/codex/2026-02-09-bootstrap-and-verify-nrl-edge-engine-v1.1

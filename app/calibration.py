import json
import logging
from typing import Dict, Optional
import numpy as np
from scipy.optimize import minimize
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

logger = logging.getLogger("nrl-pillar1")


def _beta_transform(p: np.ndarray, a: float, b: float) -> np.ndarray:
    """Beta calibration transform (handles over/under-confidence in tails)."""
    p = np.clip(p, 1e-6, 1.0 - 1e-6)
    return (p ** a) / (p ** a + (1.0 - p) ** b)


def fit_beta_calibrator(engine: Engine, season: int, min_samples: int = 80) -> Optional[Dict]:
    """Fit beta calibration parameters on historical outcomes for a season."""
    with engine.begin() as conn:
        rows = conn.execute(
            sql_text(
                """
            SELECT p_fair, outcome_home_win
            FROM nrl.model_prediction
            WHERE season = :s AND outcome_known = true AND p_fair IS NOT NULL
        """
            ),
            dict(s=season),
        ).mappings().all()

    if len(rows) < min_samples:
        logger.info(f"Calibration skipped for S{season} — only {len(rows)} samples")
        return None

    p = np.array([float(r["p_fair"]) for r in rows])
    y = np.array([1.0 if r["outcome_home_win"] else 0.0 for r in rows])

    def brier_loss(params):
        a, b = params
        p_cal = _beta_transform(p, a, b)
        return np.mean((p_cal - y) ** 2)

    res = minimize(
        brier_loss,
        x0=[1.0, 1.0],
        bounds=[(0.01, 10.0), (0.01, 10.0)],
        method="L-BFGS-B",
    )

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

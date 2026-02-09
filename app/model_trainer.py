from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import brier_score_loss, log_loss
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from .model_registry import maybe_promote_by_brier, register_model

logger = logging.getLogger("nrl-pillar1")


FEATURE_COLS = [
    "home_rest_days",
    "away_rest_days",
    "home_form",
    "away_form",
    "home_coach_style",
    "away_coach_style",
    "home_injuries",
    "away_injuries",
    "market_implied_prob",
    "rating_diff",
    "is_wet",
    "temp_c",
    "wind_speed_kmh",
]


def _safe_float(x, default=0.0) -> float:
    try:
        if x is None:
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def build_features(engine: Engine, seasons: List[int]) -> pd.DataFrame:
    # All features come from tables/views that exist in schema_pg.sql.
    query = """
    WITH base AS (
      SELECT
        m.match_id, m.season, m.round_num, m.match_date, m.venue,
        m.home_team, m.away_team,
        (m.home_score > m.away_score) AS home_win
      FROM nrl.matches_raw m
      WHERE m.season = ANY(:seasons)
        AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
    )
    SELECT
      b.match_id, b.season, b.round_num, b.match_date,
      b.home_team, b.away_team,
      b.home_win::int AS home_win,

      COALESCE(rh.rest_days, 7) AS home_rest_days,
      COALESCE(ra.rest_days, 7) AS away_rest_days,

      COALESCE(fh.win_pct_last5, 0.5) AS home_form,
      COALESCE(fa.win_pct_last5, 0.5) AS away_form,

      COALESCE(ch.style_score, 0.0) AS home_coach_style,
      COALESCE(ca.style_score, 0.0) AS away_coach_style,

      COALESCE(ih.injury_count, 0) AS home_injuries,
      COALESCE(ia.injury_count, 0) AS away_injuries,

      COALESCE(1.0 / NULLIF(oh.close_price,0), 0.5) AS market_implied_prob,

      COALESCE(ph.rating, 1500) - COALESCE(pa.rating, 1500) AS rating_diff,

      COALESCE(w.is_wet, 0) AS is_wet,
      COALESCE(w.temp_c, 20.0) AS temp_c,
      COALESCE(w.wind_speed_kmh, 10.0) AS wind_speed_kmh

    FROM base b
    LEFT JOIN nrl.team_rest_v rh ON rh.match_id=b.match_id AND rh.team=b.home_team
    LEFT JOIN nrl.team_rest_v ra ON ra.match_id=b.match_id AND ra.team=b.away_team

    LEFT JOIN nrl.team_form_v fh ON fh.match_id=b.match_id AND fh.team=b.home_team
    LEFT JOIN nrl.team_form_v fa ON fa.match_id=b.match_id AND fa.team=b.away_team

    LEFT JOIN nrl.coach_profile ch ON ch.season=b.season AND ch.team=b.home_team
    LEFT JOIN nrl.coach_profile ca ON ca.season=b.season AND ca.team=b.away_team

    LEFT JOIN nrl.injuries_current ih ON ih.season=b.season AND ih.team=b.home_team
    LEFT JOIN nrl.injuries_current ia ON ia.season=b.season AND ia.team=b.away_team

    LEFT JOIN nrl.odds oh ON oh.match_id=b.match_id AND oh.team=b.home_team

    LEFT JOIN nrl.team_ratings ph ON ph.season=b.season AND ph.team=b.home_team
    LEFT JOIN nrl.team_ratings pa ON pa.season=b.season AND pa.team=b.away_team

    LEFT JOIN nrl.weather_daily w ON w.match_date=b.match_date AND w.venue=b.venue
    ORDER BY b.season, b.round_num, b.match_date
    """

    df = pd.read_sql(sql_text(query), engine, params={"seasons": seasons})
    return df


def _time_series_cv_metrics(model, X: pd.DataFrame, y: pd.Series, n_splits: int = 5) -> Dict[str, float]:
    tscv = TimeSeriesSplit(n_splits=n_splits)
    briers = []
    loglosses = []

    Xv = X.values
    yv = y.values

    for train_idx, test_idx in tscv.split(Xv):
        X_train, X_test = Xv[train_idx], Xv[test_idx]
        y_train, y_test = yv[train_idx], yv[test_idx]

        model.fit(X_train, y_train)
        p = model.predict_proba(X_test)[:, 1]
        briers.append(brier_score_loss(y_test, p))
        loglosses.append(log_loss(y_test, p, eps=1e-15))

    return {
        "cv_brier_mean": float(np.mean(briers)),
        "cv_brier_std": float(np.std(briers)),
        "cv_logloss_mean": float(np.mean(loglosses)),
        "cv_logloss_std": float(np.std(loglosses)),
    }


def train_model(engine: Engine, seasons: List[int]) -> Optional[Dict]:
    df = build_features(engine, seasons)
    if df.empty or len(df) < 120:
        logger.error("Not enough resolved matches to train (need ~120+, got %s).", len(df))
        return None

    for c in FEATURE_COLS:
        if c not in df.columns:
            df[c] = 0.0

    X = df[FEATURE_COLS].astype(float)
    y = df["home_win"].astype(int)

    model = xgb.XGBClassifier(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_lambda=1.0,
        eval_metric="logloss",
        random_state=42,
        n_jobs=max(1, os.cpu_count() or 2),
    )

    metrics = _time_series_cv_metrics(model, X, y, n_splits=5)

    # Fit final model on all data
    model.fit(X.values, y.values)

    version = f"xgb_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    artifact_dir = "models"
    os.makedirs(artifact_dir, exist_ok=True)
    artifact_path = os.path.join(artifact_dir, f"nrl_h2h_{version}.joblib")

    bundle = {"model": model, "feature_cols": FEATURE_COLS, "version": version, "metrics": metrics}
    joblib.dump(bundle, artifact_path)

    register_model(engine, model_key="nrl_h2h_xgb", version=version, artifact_path=artifact_path, metrics=metrics)

    promoted = maybe_promote_by_brier(engine, model_key="nrl_h2h_xgb", version=version, new_brier=float(metrics["cv_brier_mean"]))

    # Log feature importance
    try:
        imp = pd.Series(model.feature_importances_, index=FEATURE_COLS).sort_values(ascending=False)
        logger.info("Top features:\n%s", imp.head(12).to_string())
    except Exception:
        pass

    out = {"version": version, "artifact_path": artifact_path, "metrics": metrics, "promoted_to_champion": promoted}
    logger.info("✓ Train complete: %s", out)
    return out


if __name__ == "__main__":
    from .db import get_engine

    eng = get_engine()
    train_model(eng, seasons=[2022, 2023, 2024, 2025])

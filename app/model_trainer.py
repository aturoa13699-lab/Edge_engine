import os
import joblib
import logging
import pandas as pd
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.metrics import brier_score_loss, log_loss

from .db import get_engine
from .model_registry import register_model

logger = logging.getLogger("nrl-pillar1")


def build_features(engine: Engine, seasons: list[int]) -> pd.DataFrame:
    """Build training features from existing views + tables."""
    query = """
        SELECT
            m.match_id, m.season, m.round_num, m.home_team, m.away_team,
            (m.home_score > m.away_score)::int AS home_win,

            COALESCE(tr_home.rest_days, 7) AS home_rest_days,
            COALESCE(tr_away.rest_days, 7) AS away_rest_days,

            COALESCE(form_home.win_pct_last5, 0.5) AS home_form,
            COALESCE(form_away.win_pct_last5, 0.5) AS away_form,

            COALESCE(cp_home.style_score, 7.0) AS home_coach_style,
            COALESCE(cp_away.style_score, 7.0) AS away_coach_style,

            COALESCE(inj_home.injury_count, 0) AS home_injuries,
            COALESCE(inj_away.injury_count, 0) AS away_injuries,

            COALESCE(o.close_price, 1.90) AS home_price,
            (1.0 / NULLIF(COALESCE(o.close_price, 1.90), 0)) AS home_implied_prob,

            COALESCE(m.f_diff, 0) AS f_diff

        FROM nrl.matches m
        LEFT JOIN nrl.team_rest_v tr_home ON tr_home.match_id = m.match_id AND tr_home.team = m.home_team
        LEFT JOIN nrl.team_rest_v tr_away ON tr_away.match_id = m.match_id AND tr_away.team = m.away_team
        LEFT JOIN nrl.team_form_v form_home ON form_home.match_id = m.match_id AND form_home.team = m.home_team
        LEFT JOIN nrl.team_form_v form_away ON form_away.match_id = m.match_id AND form_away.team = m.away_team
        LEFT JOIN nrl.coach_profile cp_home ON cp_home.season = m.season AND cp_home.team = m.home_team
        LEFT JOIN nrl.coach_profile cp_away ON cp_away.season = m.season AND cp_away.team = m.away_team
        LEFT JOIN (
            SELECT team, COUNT(*) AS injury_count
            FROM nrl.injuries_current
            GROUP BY team
        ) inj_home ON inj_home.team = m.home_team
        LEFT JOIN (
            SELECT team, COUNT(*) AS injury_count
            FROM nrl.injuries_current
            GROUP BY team
        ) inj_away ON inj_away.team = m.away_team
        LEFT JOIN nrl.odds o ON o.match_id = m.match_id AND o.team = m.home_team

        WHERE m.season = ANY(:seasons)
          AND m.home_score IS NOT NULL
          AND m.away_score IS NOT NULL
        ORDER BY m.season, m.round_num, m.match_id
    """
    df = pd.read_sql(sql_text(query), engine, params={"seasons": seasons})
    return df


def train_model(engine: Engine, seasons: list[int] | None = None):
    if seasons is None:
        seasons = [2022, 2023, 2024, 2025]

    df = build_features(engine, seasons)
    if len(df) < 100:
        logger.error("Not enough data to train")
        return None

    feature_cols = [
        c
        for c in df.columns
        if c
        not in [
            "match_id",
            "season",
            "round_num",
            "home_team",
            "away_team",
            "home_win",
            "home_price",
        ]
    ]

    X = df[feature_cols]
    y = df["home_win"].astype(int)

    model = xgb.XGBClassifier(
        n_estimators=400,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="logloss",
        random_state=42,
    )

    # Time-series CV (Brier)
    tscv = TimeSeriesSplit(n_splits=5)
    scores = cross_val_score(model, X, y, cv=tscv, scoring="neg_brier_score")
    cv_brier = float(-scores.mean())
    logger.info(f"CV Brier score: {cv_brier:.4f}")

    model.fit(X, y)

    # In-sample diagnostics (lightweight)
    p_hat = model.predict_proba(X)[:, 1]
    ins_brier = float(brier_score_loss(y, p_hat))
    ins_logloss = float(log_loss(y, p_hat))

    # Save artifact bundle
    os.makedirs("models", exist_ok=True)
    model_id = os.getenv("MODEL_VERSION", "nrl_xgboost_v1")
    model_path = os.path.join("models", f"{model_id}.joblib")
    joblib.dump({"model": model, "feature_cols": feature_cols}, model_path)
    logger.info(f"✓ XGBoost model trained and saved to {model_path}")

    # Feature importances
    imp = pd.Series(model.feature_importances_, index=feature_cols).sort_values(ascending=False)
    logger.info("Top features:\n%s", imp.head(12))

    metrics = {"cv_brier": cv_brier, "ins_brier": ins_brier, "ins_logloss": ins_logloss}
    register_model(engine, model_id=model_id, metrics=metrics, artifact_path=model_path)

    return model


if __name__ == "__main__":
    eng = get_engine()
    train_model(eng)

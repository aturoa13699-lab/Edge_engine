"""Backfill historical predictions and label outcomes."""

from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional

from sqlalchemy import bindparam, text as sql_text
from sqlalchemy.engine import Engine

from .schema_router import truth_table

from .calibration import apply_calibration, load_latest_calibrator
from .deploy_engine import _fetch_live_feature_row, _heuristic_p, _ml_p
from .schema_router import ops_table, truth_table

logger = logging.getLogger("nrl-pillar1")


def backfill_predictions(
    engine: Engine,
    season: int,
    rounds: Optional[List[int]] = None,
    label_outcomes: bool = True,
) -> Dict:
    matches_table = truth_table(engine, "matches_raw")
    pred_table = ops_table(engine, "model_prediction")

    - Generates predictions (heuristic + ML blend) for each match
    - Labels outcomes from actual scores if label_outcomes=True
    - Skips matches that already have predictions (idempotent)
    """
    matches_table = truth_table(engine, "matches_raw")
    base_sql = f"""
        SELECT m.match_id, m.season, m.round_num, m.match_date,
               m.home_team, m.away_team, m.home_score, m.away_score
        FROM {matches_table} m
        WHERE m.season = :s
          AND m.home_score IS NOT NULL
          AND m.away_score IS NOT NULL
    """
    params: dict = {"s": season}

    if rounds:
        base_sql += "  AND m.round_num IN :rounds"
        query = sql_text(base_sql + " ORDER BY m.round_num, m.match_date").bindparams(
            bindparam("rounds", expanding=True)
        )
        params["rounds"] = rounds
    else:
        query = sql_text(base_sql + " ORDER BY m.round_num, m.match_date")

    with engine.begin() as conn:
        matches = conn.execute(query, params).mappings().all()

    if not matches:
        logger.warning("No resolved matches found for season=%s", season)
        return {"season": season, "backfilled": 0, "skipped": 0}

    model_version = os.getenv("MODEL_VERSION", "v2026-02-poisson-v1")
    alpha = float(os.getenv("ML_BLEND_ALPHA", "0.65"))
    calibrator = load_latest_calibrator(engine, season)

    backfilled = 0
    skipped = 0

    for m in matches:
        match_id = m["match_id"]

        with engine.begin() as conn:
            existing = conn.execute(
                sql_text(
                    f"SELECT 1 FROM {pred_table} WHERE match_id = :mid AND season = :s LIMIT 1"
                ),
                dict(mid=match_id, s=season),
            ).first()

        if existing:
            skipped += 1
            continue

        feature_row = _fetch_live_feature_row(engine, match_id)
        p_h = _heuristic_p(feature_row)
        p_ml = _ml_p(engine, feature_row)
        p_blend = p_h if p_ml is None else float(alpha * p_ml + (1.0 - alpha) * p_h)
        p_cal = apply_calibration(p_blend, calibrator)

        home_win = bool(m["home_score"] > m["away_score"])
        close_price = float(feature_row.get("close_price", 1.90))
        odds_taken = float(feature_row.get("odds_taken", 1.90))
        clv_diff = float(close_price - odds_taken)

        with engine.begin() as conn:
            conn.execute(
                sql_text(
                    f"""
                    INSERT INTO {pred_table}
                    (season, round_num, match_id, home_team, away_team,
                     p_fair, calibrated_p, model_version, clv_diff,
                     outcome_known, outcome_home_win)
                    VALUES (:s, :r, :mid, :h, :a, :pf, :cp, :ver, :clv, :ok, :ohw)
                    """
                ),
                dict(
                    s=season,
                    r=m["round_num"],
                    mid=match_id,
                    h=m["home_team"],
                    a=m["away_team"],
                    pf=p_blend,
                    cp=p_cal,
                    ver=model_version,
                    clv=clv_diff,
                    ok=label_outcomes,
                    ohw=home_win,
                ),
            )

        backfilled += 1

    result = {"season": season, "backfilled": backfilled, "skipped": skipped}
    logger.info("Backfill complete: %s", result)
    return result


def label_outcomes(engine: Engine, season: int) -> Dict:
    """
    Label already-existing predictions with outcomes from resolved matches.
    Updates model_prediction rows where outcome_known is false but scores exist.
    """
    matches_table = truth_table(engine, "matches_raw")
    with engine.begin() as conn:
        result = conn.execute(
            sql_text(f"""
                UPDATE nrl.model_prediction mp
                SET outcome_known = true,
                    outcome_home_win = (mr.home_score > mr.away_score)
                FROM {matches_table} mr
                WHERE mp.match_id = mr.match_id
                  AND mp.season = :s
                  AND mp.outcome_known = false
                  AND mr.home_score IS NOT NULL
                  AND mr.away_score IS NOT NULL
                """
            ),
            dict(s=season),
        )
        updated = result.rowcount

    logger.info("Labelled %s prediction outcomes for season %s", updated, season)
    return {"season": season, "labelled": updated}

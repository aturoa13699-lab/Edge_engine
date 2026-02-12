from __future__ import annotations

import json
import logging
import math
import os
import uuid
from dataclasses import asdict
from typing import Any, Dict, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from .calibration import apply_calibration, load_latest_calibrator
from .model_registry import get_champion
from .risk import size_stake
from .types import Slip

logger = logging.getLogger("nrl-pillar1")


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _fetch_match(engine: Engine, match_id: str) -> Optional[Dict[str, Any]]:
    with engine.begin() as conn:
        row = conn.execute(
            sql_text(
                """
                SELECT match_id, season, round_num, match_date, venue, home_team, away_team
                FROM nrl.matches_raw
                WHERE match_id=:mid
                """
            ),
            dict(mid=match_id),
        ).mappings().first()
    return dict(row) if row else None


def _fetch_live_feature_row(engine: Engine, match_id: str) -> Dict[str, float]:
    # All features are defined in schema_pg.sql (tables/views).
    with engine.begin() as conn:
        row = conn.execute(
            sql_text(
                """
                SELECT
                  m.season, m.match_id, m.match_date, m.venue, m.home_team, m.away_team,

                  COALESCE(rh.rest_days, 7) AS home_rest_days,
                  COALESCE(ra.rest_days, 7) AS away_rest_days,

                  COALESCE(fh.win_pct_last5, 0.5) AS home_form,
                  COALESCE(fa.win_pct_last5, 0.5) AS away_form,

                  COALESCE(ch.style_score, 0.0) AS home_coach_style,
                  COALESCE(ca.style_score, 0.0) AS away_coach_style,

                  COALESCE(ih.injury_count, 0) AS home_injuries,
                  COALESCE(ia.injury_count, 0) AS away_injuries,

                  COALESCE(oh.last_price, oh.close_price, oh.opening_price, 1.90) AS odds_taken,
                  COALESCE(oh.close_price, oh.last_price, 1.90) AS close_price,

                  COALESCE(ph.rating, 1500) AS home_rating,
                  COALESCE(pa.rating, 1500) AS away_rating,

                  COALESCE(w.is_wet, 0) AS is_wet,
                  COALESCE(w.temp_c, 20.0) AS temp_c,
                  COALESCE(w.wind_speed_kmh, 10.0) AS wind_speed_kmh

                FROM nrl.matches_raw m
                LEFT JOIN nrl.team_rest_v rh ON rh.match_id=m.match_id AND rh.team=m.home_team
                LEFT JOIN nrl.team_rest_v ra ON ra.match_id=m.match_id AND ra.team=m.away_team

                LEFT JOIN nrl.team_form_v fh ON fh.match_id=m.match_id AND fh.team=m.home_team
                LEFT JOIN nrl.team_form_v fa ON fa.match_id=m.match_id AND fa.team=m.away_team

                LEFT JOIN nrl.coach_profile ch ON ch.season=m.season AND ch.team=m.home_team
                LEFT JOIN nrl.coach_profile ca ON ca.season=m.season AND ca.team=m.away_team

                LEFT JOIN nrl.injuries_current ih ON ih.season=m.season AND ih.team=m.home_team
                LEFT JOIN nrl.injuries_current ia ON ia.season=m.season AND ia.team=m.away_team

                LEFT JOIN nrl.odds oh ON oh.match_id=m.match_id AND oh.team=m.home_team

                LEFT JOIN nrl.team_ratings ph ON ph.season=m.season AND ph.team=m.home_team
                LEFT JOIN nrl.team_ratings pa ON pa.season=m.season AND pa.team=m.away_team

                LEFT JOIN nrl.weather_daily w ON w.match_date=m.match_date AND w.venue=m.venue

                WHERE m.match_id=:mid
                """
            ),
            dict(mid=match_id),
        ).mappings().first()

    if not row:
        return {
            "home_rest_days": 7,
            "away_rest_days": 7,
            "home_form": 0.5,
            "away_form": 0.5,
            "home_coach_style": 0.0,
            "away_coach_style": 0.0,
            "home_injuries": 0.0,
            "away_injuries": 0.0,
            "market_implied_prob": 0.5,
            "rating_diff": 0.0,
            "is_wet": 0.0,
            "temp_c": 20.0,
            "wind_speed_kmh": 10.0,
            "odds_taken": 1.90,
            "close_price": 1.90,
        }

    home_rating = float(row["home_rating"])
    away_rating = float(row["away_rating"])
    rating_diff = home_rating - away_rating

    close_price = float(row["close_price"]) if row["close_price"] else 1.90
    market_implied_prob = float(1.0 / close_price) if close_price > 0 else 0.5

    return {
        "home_rest_days": float(row["home_rest_days"]),
        "away_rest_days": float(row["away_rest_days"]),
        "home_form": float(row["home_form"]),
        "away_form": float(row["away_form"]),
        "home_coach_style": float(row["home_coach_style"]),
        "away_coach_style": float(row["away_coach_style"]),
        "home_injuries": float(row["home_injuries"]),
        "away_injuries": float(row["away_injuries"]),
        "market_implied_prob": float(market_implied_prob),
        "rating_diff": float(rating_diff),
        "is_wet": float(row["is_wet"]),
        "temp_c": float(row["temp_c"]),
        "wind_speed_kmh": float(row["wind_speed_kmh"]),
        "odds_taken": float(row["odds_taken"] or 1.90),
        "close_price": float(close_price),
    }


def _heuristic_p(feature_row: Dict[str, float]) -> float:
    # Logistic baseline on rating diff + modest adjustments
    rd = feature_row["rating_diff"]
    injuries = feature_row["home_injuries"] - feature_row["away_injuries"]
    rest = feature_row["home_rest_days"] - feature_row["away_rest_days"]
    form = feature_row["home_form"] - feature_row["away_form"]

    x = (rd / 200.0) + (-0.08 * injuries) + (0.04 * rest) + (0.9 * form)
    return float(np.clip(_sigmoid(x), 0.01, 0.99))


def _ml_p(engine: Engine, feature_row: Dict[str, float]) -> Optional[float]:
    champ = get_champion(engine, model_key="nrl_h2h_xgb")
    if not champ:
        return None

    path = champ.get("artifact_path")
    if not path or not os.path.exists(path):
        return None

    bundle = joblib.load(path)
    model = bundle["model"]
    cols = bundle["feature_cols"]

    X = pd.DataFrame([{c: float(feature_row.get(c, 0.0)) for c in cols}])
    p = float(model.predict_proba(X.values)[:, 1][0])
    return float(np.clip(p, 0.01, 0.99))


def evaluate_match_and_decide(engine: Engine, season: int, round_num: int, match_id: str, dry_run: bool) -> Tuple[Slip, Dict[str, Any]]:
    feature_row = _fetch_live_feature_row(engine, match_id)

    p_h = _heuristic_p(feature_row)
    p_ml = _ml_p(engine, feature_row)

    alpha = float(os.getenv("ML_BLEND_ALPHA", "0.65"))
    if p_ml is None:
        p_blend = p_h
    else:
        p_blend = float(alpha * p_ml + (1.0 - alpha) * p_h)

    # Calibration (beta)
    calibrator = load_latest_calibrator(engine, season)
    p_cal = apply_calibration(p_blend, calibrator)

    odds_taken = float(feature_row.get("odds_taken", 1.90))
    close_price = float(feature_row.get("close_price", odds_taken))

    # EV in decimal odds space: E[profit] per $1 stake
    ev = (p_cal * odds_taken) - 1.0

    bankroll = float(os.getenv("BANKROLL", "1000"))
    sizing = size_stake(bankroll=bankroll, p=p_cal, odds=odds_taken, max_frac=0.05)
    stake = float(sizing.stake)

    status = "dry_run" if dry_run else "pending"

    # Build slip
    match = _fetch_match(engine, match_id) or {}
    home_team = match.get("home_team", "HOME")
    away_team = match.get("away_team", "AWAY")

    portfolio_id = str(uuid.uuid4())
    model_version = os.getenv("MODEL_VERSION", "v2026-02-poisson-v1")

    slip = Slip(
        portfolio_id=portfolio_id,
        season=season,
        round_num=round_num,
        match_id=match_id,
        home_team=home_team,
        away_team=away_team,
        market="H2H",
        selection=f"{home_team} H2H",
        odds=odds_taken,
        stake=stake,
        ev=ev,
        status=status,
        model_version=model_version,
        reason=f"p_h={p_h:.3f} p_ml={(p_ml if p_ml is not None else float('nan')):.3f} p_blend={p_blend:.3f} p_cal={p_cal:.3f} capped={sizing.capped}",
    )

    # CLV diff (odds space): close - taken (positive is good if taken earlier at better odds)
    clv_diff = float(close_price - odds_taken)

    # Persist prediction + slip ALWAYS (even in dry-run)
    with engine.begin() as conn:
        conn.execute(
            sql_text(
                """
                INSERT INTO nrl.model_prediction
                (season, round_num, match_id, home_team, away_team, p_fair, calibrated_p, model_version, clv_diff)
                VALUES (:s,:r,:mid,:h,:a,:pf,:cp,:ver,:clv)
                """
            ),
            dict(
                s=season,
                r=round_num,
                mid=match_id,
                h=home_team,
                a=away_team,
                pf=p_blend,
                cp=p_cal,
                ver=model_version,
                clv=clv_diff,
            ),
        )

        conn.execute(
            sql_text(
                """
                INSERT INTO nrl.slips (portfolio_id, season, round_num, slip_json, status)
                VALUES (:pid, :s, :r, CAST(:sj AS jsonb), :st)
                ON CONFLICT (portfolio_id) DO NOTHING
                """
            ),
            dict(pid=portfolio_id, s=season, r=round_num, sj=json.dumps(asdict(slip)), st=status),
        )

    debug = {
        "p_heuristic": p_h,
        "p_ml": p_ml,
        "p_blend": p_blend,
        "p_cal": p_cal,
        "odds_taken": odds_taken,
        "close_price": close_price,
        "clv_diff": clv_diff,
        "stake": stake,
        "ev": ev,
    }
    return slip, debug


def evaluate_round(engine: Engine, season: int, round_num: int, dry_run: bool) -> None:
    with engine.begin() as conn:
        matches = conn.execute(
            sql_text(
                """
                SELECT match_id
                FROM nrl.matches_raw
                WHERE season=:s AND round_num=:r
                ORDER BY match_date NULLS LAST, match_id
                """
            ),
            dict(s=season, r=round_num),
        ).mappings().all()

    if not matches:
        logger.warning("No matches found for season=%s round=%s", season, round_num)
        return

    for m in matches:
        slip, debug = evaluate_match_and_decide(engine, season, round_num, m["match_id"], dry_run=dry_run)
        logger.info("Slip %s: %s (stake=%.2f ev=%.4f clv=%.3f)", slip.portfolio_id[:8], slip.selection, slip.stake, slip.ev, debug["clv_diff"])

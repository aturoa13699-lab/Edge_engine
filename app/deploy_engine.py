import os
import logging
from dataclasses import asdict
from datetime import datetime

import pandas as pd
import joblib
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from .types import Slip, SlipLeg
from .risk import size_and_guard
from .calibration import load_latest_calibrator, apply_calibration

logger = logging.getLogger("nrl-pillar1")


def _implied_prob_from_odds(price: float) -> float:
    try:
        if price and price > 0:
            return 1.0 / float(price)
    except Exception:
        pass
    return 0.5


def _fetch_match_context(engine: Engine, season: int, round_num: int) -> list[dict]:
    with engine.begin() as conn:
        rows = conn.execute(
            sql_text(
                """
            SELECT match_id, season, round_num, home_team, away_team
            FROM nrl.matches
            WHERE season = :s AND round_num = :r
            ORDER BY match_id
        """
            ),
            dict(s=season, r=round_num),
        ).mappings().all()
    return [dict(r) for r in rows]


def _fetch_market_price(engine: Engine, match_id: str, team: str) -> float | None:
    with engine.begin() as conn:
        row = conn.execute(
            sql_text(
                """
            SELECT close_price
            FROM nrl.odds
            WHERE match_id = :mid AND team = :t
            ORDER BY captured_at DESC
            LIMIT 1
        """
            ),
            dict(mid=match_id, t=team),
        ).mappings().first()
    if row and row.get("close_price") is not None:
        return float(row["close_price"])
    return None


def _heuristic_p_fair(row: dict) -> float:
    # Minimal baseline heuristic: 0.50 and adjust via f_diff if available
    f_diff = float(row.get("f_diff") or 0.0)
    p = 0.50 + (0.02 * max(-5.0, min(5.0, f_diff)))
    return float(max(0.01, min(0.99, p)))


def _build_live_features(engine: Engine, row: dict) -> pd.DataFrame:
    # NOTE: This is a minimal live feature vector aligned to trainer.
    # In production, you would join the same views used in training.
    match_id = row["match_id"]
    home_team = row["home_team"]

    home_price = _fetch_market_price(engine, match_id, home_team) or 1.90
    home_implied_prob = _implied_prob_from_odds(home_price)

    feat = {
        "home_rest_days": float(row.get("home_rest_days") or 7),
        "away_rest_days": float(row.get("away_rest_days") or 7),
        "home_form": float(row.get("home_form") or 0.5),
        "away_form": float(row.get("away_form") or 0.5),
        "home_coach_style": float(row.get("home_coach_style") or 7.0),
        "away_coach_style": float(row.get("away_coach_style") or 7.0),
        "home_injuries": float(row.get("home_injuries") or 0),
        "away_injuries": float(row.get("away_injuries") or 0),
        "home_implied_prob": float(home_implied_prob),
        "f_diff": float(row.get("f_diff") or 0.0),
    }
    return pd.DataFrame([feat])


def evaluate_match_and_decide(engine: Engine, season: int, round_num: int, row: dict) -> tuple[float, Slip | None]:
    home_team = row["home_team"]
    away_team = row["away_team"]
    match_id = row["match_id"]

    # === Baseline heuristic ===
    p_fair = _heuristic_p_fair(row)

    # === ML prediction (if available) ===
    try:
        model_path = os.path.join("models", "nrl_xgboost_v1.joblib")
        if os.path.exists(model_path):
            bundle = joblib.load(model_path)
            ml_model = bundle["model"]
            feature_cols = bundle.get("feature_cols")
            X_live = _build_live_features(engine, row)
            if feature_cols:
                X_live = X_live[feature_cols]
            ml_raw_p = float(ml_model.predict_proba(X_live)[:, 1][0])
            # Blend ML with heuristic baseline
            p_fair = 0.65 * ml_raw_p + 0.35 * p_fair
            logger.debug(f"ML blend {home_team} vs {away_team}: ml={ml_raw_p:.3f}, blend={p_fair:.3f}")
    except Exception as e:
        logger.warning(f"ML model unavailable, falling back to heuristic: {e}")

    p_fair = float(max(0.01, min(0.99, p_fair)))

    # === Calibration ===
    calibrator = load_latest_calibrator(engine, season)
    p_calibrated = apply_calibration(p_fair, calibrator)
    if abs(p_calibrated - p_fair) > 0.02:
        logger.debug(f"Calibration shift {home_team} vs {away_team}: {p_fair:.3f} → {p_calibrated:.3f}")

    # Market odds
    home_price = _fetch_market_price(engine, match_id, home_team) or 1.90
    # Choose side if edge exists
    p_book = _implied_prob_from_odds(home_price)
    edge = p_calibrated - p_book

    slip = None
    if edge > 0.02:
        leg = SlipLeg(
            match_id=match_id,
            market="H2H",
            selection=home_team,
            price=float(home_price),
            p_model=float(p_calibrated),
        )
        slip = Slip(
            portfolio_id=f"{season}-{round_num}-{match_id}-{home_team}",
            season=season,
            round_num=round_num,
            match_id=match_id,
            market="H2H",
            legs=[leg],
            stake_units=0.0,  # filled after sizing
            status="pending",
            created_at=datetime.utcnow().isoformat(),
        )

    # Persist prediction row
    model_version = os.getenv("MODEL_VERSION", "v2026-02-poisson-v1")
    with engine.begin() as conn:
        conn.execute(
            sql_text(
                """
            INSERT INTO nrl.model_prediction
            (season, round_num, match_id, home_team, away_team, p_fair, calibrated_p, model_version)
            VALUES (:s,:r,:mid,:h,:a,:pf,:cp,:ver)
            ON CONFLICT (season, round_num, match_id) DO UPDATE
            SET p_fair=EXCLUDED.p_fair,
                calibrated_p=EXCLUDED.calibrated_p,
                model_version=EXCLUDED.model_version,
                updated_at=now()
        """
            ),
            dict(
                s=season,
                r=round_num,
                mid=match_id,
                h=home_team,
                a=away_team,
                pf=float(p_fair),
                cp=float(p_calibrated),
                ver=model_version,
            ),
        )

    return float(p_calibrated), slip


def deploy_round(engine: Engine, season: int, round_num: int, dry_run: bool = True) -> list[Slip]:
    matches = _fetch_match_context(engine, season, round_num)
    slips: list[Slip] = []

    bankroll_units = float(os.getenv("BANKROLL_UNITS", "10"))

    for row in matches:
        p_cal, slip = evaluate_match_and_decide(engine, season, round_num, row)
        if not slip:
            continue

        # Size stake with risk controls (uses calibrated prob embedded in leg)
        stake_units = size_and_guard(
            bankroll_units=bankroll_units,
            p=float(slip.legs[0].p_model),
            price=float(slip.legs[0].price),
        )
        slip.stake_units = float(stake_units)

        slips.append(slip)

        # Persist slip (even in dry run, store as pending)
        with engine.begin() as conn:
            conn.execute(
                sql_text(
                    """
                INSERT INTO nrl.slips
                (portfolio_id, season, round_num, match_id, market, slip_json, stake_units, status)
                VALUES (:pid,:s,:r,:mid,:mkt,:js::jsonb,:stk,:st)
                ON CONFLICT (portfolio_id) DO UPDATE
                SET slip_json=EXCLUDED.slip_json,
                    stake_units=EXCLUDED.stake_units,
                    status=EXCLUDED.status,
                    updated_at=now()
            """
                ),
                dict(
                    pid=slip.portfolio_id,
                    s=season,
                    r=round_num,
                    mid=slip.match_id,
                    mkt=slip.market,
                    js=str(asdict(slip)).replace("'", '"'),
                    stk=float(slip.stake_units),
                    st=slip.status,
                ),
            )

        logger.info(
            f"{'[DRY]' if dry_run else '[LIVE]'} {slip.market} {slip.legs[0].selection} "
            f"@{slip.legs[0].price:.2f} p={slip.legs[0].p_model:.3f} stake={slip.stake_units:.2f}u"
        )

    return slips

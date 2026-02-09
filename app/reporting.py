import json
from typing import Any, Dict, List, Optional

from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine


def fetch_round_slips(engine: Engine, season: int, round_num: int, status: str = "pending") -> List[Dict[str, Any]]:
    """Fetch slips for a specific season/round filtered by status."""
    with engine.begin() as conn:
        rows = conn.execute(
            sql_text(
                """
                SELECT portfolio_id, slip_json, status, created_at
                FROM nrl.slips
                WHERE season = :s AND round_num = :r AND status = :st
                ORDER BY created_at DESC
                """
            ),
            dict(s=season, r=round_num, st=status),
        ).mappings().all()

    slips: List[Dict[str, Any]] = []
    for row in rows:
        sj = row["slip_json"]
        if isinstance(sj, str):
            sj = json.loads(sj)
        slips.append(sj)
    return slips


def fetch_recent_slips(engine: Engine, limit: int = 25) -> List[Dict[str, Any]]:
    """Fetch recent slip JSON blobs (any status)."""
    with engine.begin() as conn:
        rows = conn.execute(
            sql_text(
                """
                SELECT slip_json
                FROM nrl.slips
                ORDER BY created_at DESC
                LIMIT :n
                """
            ),
            dict(n=limit),
        ).mappings().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        sj = r["slip_json"]
        if isinstance(sj, str):
            sj = json.loads(sj)
        out.append(sj)
    return out


def fetch_recent_predictions(engine: Engine, limit: int = 50) -> List[Dict[str, Any]]:
    """Fetch recent model predictions for reporting/audit."""
    with engine.begin() as conn:
        rows = conn.execute(
            sql_text(
                """
                SELECT
                  season, round_num, match_id, home_team, away_team,
                  p_fair, calibrated_p, model_version, clv_diff,
                  outcome_known, outcome_home_win, created_at
                FROM nrl.model_prediction
                ORDER BY created_at DESC
                LIMIT :n
                """
            ),
            dict(n=limit),
        ).mappings().all()
    return [dict(r) for r in rows]


def fetch_calibration_for_season(engine: Engine, season: int) -> Optional[Dict[str, Any]]:
    """Fetch calibration params for a season (latest row)."""
    with engine.begin() as conn:
        row = conn.execute(
            sql_text(
                """
                SELECT params
                FROM nrl.calibration_params
                WHERE season = :s
                ORDER BY fitted_at DESC
                LIMIT 1
                """
            ),
            dict(s=season),
        ).mappings().first()

    if not row:
        return None
    params = row["params"]
    if isinstance(params, str):
        return json.loads(params)
    return dict(params)

from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from .types import Slip


def fetch_round_slips(engine: Engine, season: int, round_num: int, status: str = "pending") -> list[Slip]:
    with engine.begin() as conn:
        rows = conn.execute(
            sql_text(
                """
            SELECT portfolio_id, season, round_num, match_id, market, slip_json, stake_units, status
            FROM nrl.slips
            WHERE season = :s AND round_num = :r AND status = :st
            ORDER BY created_at DESC
        """
            ),
            dict(s=season, r=round_num, st=status),
        ).mappings().all()

    slips: list[Slip] = []
    for r in rows:
        # slip_json stored as json-ish string; keep minimal usage
        # For now, we rebuild Slip from columns where possible.
        slips.append(
            Slip(
                portfolio_id=r["portfolio_id"],
                season=int(r["season"]),
                round_num=int(r["round_num"]),
                match_id=r["match_id"],
                market=r["market"],
                legs=[],
                stake_units=float(r["stake_units"] or 0.0),
                status=r["status"],
                created_at="",
            )
        )
    return slips


def fetch_recent_predictions(engine: Engine, limit: int = 50) -> list[dict]:
    with engine.begin() as conn:
        rows = conn.execute(
            sql_text(
                """
            SELECT season, round_num, match_id, p_fair, calibrated_p, model_version, created_at
            FROM nrl.model_prediction
            ORDER BY created_at DESC
            LIMIT :lim
        """
            ),
            dict(lim=limit),
        ).mappings().all()
    return [dict(r) for r in rows]


def fetch_recent_slips(engine: Engine, limit: int = 25) -> list[dict]:
    with engine.begin() as conn:
        rows = conn.execute(
            sql_text(
                """
            SELECT portfolio_id, market, stake_units, status, slip_json, created_at
            FROM nrl.slips
            ORDER BY created_at DESC
            LIMIT :lim
        """
            ),
            dict(lim=limit),
        ).mappings().all()
    return [dict(r) for r in rows]

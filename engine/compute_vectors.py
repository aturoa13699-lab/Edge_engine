"""Compute player and team vectors from raw stats and write to Postgres.

Usage (via CLI):
    python -m engine.run compute-vectors --season 2025 --rounds 1,2,3

Pipeline:
    1. Read rows from nrl_clean.player_match_stats + nrl.match_context
    2. Compute atomic + hybrid metrics per player-match
    3. Write to nrl.player_vectors
    4. Aggregate by positional unit and write to nrl.team_vectors
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from .schema_router import ops_table, truth_table
from .vector_metrics import (
    compute_atomic_metrics,
    compute_context_drivers,
    compute_hybrid_metrics,
)
from .vector_registry import atomic_vector_names, hybrid_vector_names

logger = logging.getLogger("nrl-pillar1")

# Positional units for team-level aggregation
UNITS = ("spine", "middles", "edges", "bench")


def _fetch_player_stats(
    engine: Engine,
    season: int,
    rounds: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """Fetch raw player-match stats from nrl_clean.player_match_stats."""
    pms = truth_table(engine, "player_match_stats")
    where = "WHERE pms.season = :season"
    params: Dict[str, Any] = {"season": season}
    if rounds:
        placeholders = ",".join(str(int(r)) for r in rounds)
        where += f" AND pms.round_num IN ({placeholders})"

    q = f"""
    SELECT pms.*
    FROM {pms} pms
    {where}
    ORDER BY pms.match_id, pms.player_name
    """
    with engine.begin() as conn:
        rows = conn.execute(sql_text(q), params).mappings().all()
    return [dict(r) for r in rows]


def _fetch_match_context(
    engine: Engine,
    season: int,
    rounds: Optional[List[int]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Fetch match context rows keyed by match_id."""
    mc = ops_table(engine, "match_context")
    where = "WHERE mc.season = :season"
    params: Dict[str, Any] = {"season": season}
    if rounds:
        placeholders = ",".join(str(int(r)) for r in rounds)
        where += f" AND mc.round_num IN ({placeholders})"

    q = f"""
    SELECT mc.*
    FROM {mc} mc
    {where}
    """
    with engine.begin() as conn:
        rows = conn.execute(sql_text(q), params).mappings().all()
    return {r["match_id"]: dict(r) for r in rows}


def _upsert_player_vectors(
    engine: Engine,
    rows: List[Dict[str, Any]],
) -> int:
    """Write computed player vectors to nrl.player_vectors."""
    pv = ops_table(engine, "player_vectors")
    count = 0
    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                sql_text(f"""
                    INSERT INTO {pv}
                    (match_id, player_name, team, unit, season, round_num,
                     minutes, atomics_json, hybrids_json, context_json)
                    VALUES (:match_id, :player_name, :team, :unit, :season,
                            :round_num, :minutes, CAST(:atomics AS jsonb),
                            CAST(:hybrids AS jsonb), CAST(:context AS jsonb))
                    ON CONFLICT (match_id, player_name) DO UPDATE SET
                        team = EXCLUDED.team,
                        unit = EXCLUDED.unit,
                        minutes = EXCLUDED.minutes,
                        atomics_json = EXCLUDED.atomics_json,
                        hybrids_json = EXCLUDED.hybrids_json,
                        context_json = EXCLUDED.context_json,
                        updated_at = now()
                """),
                {
                    "match_id": row["match_id"],
                    "player_name": row["player_name"],
                    "team": row["team"],
                    "unit": row.get("unit", "bench"),
                    "season": row["season"],
                    "round_num": row["round_num"],
                    "minutes": row["minutes"],
                    "atomics": json.dumps(row["atomics"]),
                    "hybrids": json.dumps(row["hybrids"]),
                    "context": json.dumps(row["context"]),
                },
            )
            count += 1
    return count


def _aggregate_team_vectors(
    player_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Aggregate player vectors by team + unit for each match."""
    # Group by (match_id, team, unit)
    groups: Dict[tuple, List[Dict]] = {}
    for row in player_rows:
        key = (row["match_id"], row["team"], row.get("unit", "bench"))
        groups.setdefault(key, []).append(row)

    team_rows: List[Dict[str, Any]] = []
    atomic_names = atomic_vector_names()
    hybrid_names = hybrid_vector_names()

    for (match_id, team, unit), members in groups.items():
        # Minutes-weighted average of vector components
        total_minutes = sum(m["minutes"] for m in members)
        if total_minutes <= 0:
            continue

        avg_atomics: Dict[str, Optional[float]] = {}
        for name in atomic_names:
            vals = [
                (m["atomics"].get(name), m["minutes"])
                for m in members
                if m["atomics"].get(name) is not None
            ]
            if vals:
                weighted = sum(v * w for v, w in vals)
                avg_atomics[name] = weighted / sum(w for _, w in vals)
            else:
                avg_atomics[name] = None

        avg_hybrids: Dict[str, Optional[float]] = {}
        for name in hybrid_names:
            vals = [
                (m["hybrids"].get(name), m["minutes"])
                for m in members
                if m["hybrids"].get(name) is not None
            ]
            if vals:
                weighted = sum(v * w for v, w in vals)
                avg_hybrids[name] = weighted / sum(w for _, w in vals)
            else:
                avg_hybrids[name] = None

        first = members[0]
        team_rows.append(
            {
                "match_id": match_id,
                "team": team,
                "unit": unit,
                "season": first["season"],
                "round_num": first["round_num"],
                "player_count": len(members),
                "total_minutes": total_minutes,
                "atomics": avg_atomics,
                "hybrids": avg_hybrids,
                "context": first.get("context", {}),
            }
        )

    return team_rows


def _upsert_team_vectors(
    engine: Engine,
    rows: List[Dict[str, Any]],
) -> int:
    """Write aggregated team vectors to nrl.team_vectors."""
    tv = ops_table(engine, "team_vectors")
    count = 0
    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                sql_text(f"""
                    INSERT INTO {tv}
                    (match_id, team, unit, season, round_num,
                     player_count, total_minutes,
                     atomics_json, hybrids_json, context_json)
                    VALUES (:match_id, :team, :unit, :season, :round_num,
                            :player_count, :total_minutes,
                            CAST(:atomics AS jsonb),
                            CAST(:hybrids AS jsonb),
                            CAST(:context AS jsonb))
                    ON CONFLICT (match_id, team, unit) DO UPDATE SET
                        player_count = EXCLUDED.player_count,
                        total_minutes = EXCLUDED.total_minutes,
                        atomics_json = EXCLUDED.atomics_json,
                        hybrids_json = EXCLUDED.hybrids_json,
                        context_json = EXCLUDED.context_json,
                        updated_at = now()
                """),
                {
                    "match_id": row["match_id"],
                    "team": row["team"],
                    "unit": row["unit"],
                    "season": row["season"],
                    "round_num": row["round_num"],
                    "player_count": row["player_count"],
                    "total_minutes": row["total_minutes"],
                    "atomics": json.dumps(row["atomics"]),
                    "hybrids": json.dumps(row["hybrids"]),
                    "context": json.dumps(row["context"]),
                },
            )
            count += 1
    return count


def run(
    engine: Engine,
    season: int,
    rounds: Optional[List[int]] = None,
) -> Dict[str, int]:
    """Main entry point: compute and persist player + team vectors."""
    stats = _fetch_player_stats(engine, season, rounds)
    if not stats:
        logger.warning(
            "No player_match_stats found for season=%s rounds=%s", season, rounds
        )
        return {"player_vectors": 0, "team_vectors": 0}

    contexts = _fetch_match_context(engine, season, rounds)

    player_rows: List[Dict[str, Any]] = []
    for raw in stats:
        minutes = float(raw.get("minutes") or 0)
        if minutes < 1:
            continue

        atomics = compute_atomic_metrics(raw)
        hybrids = compute_hybrid_metrics(raw, atomics)

        # Attach context if available
        ctx_raw = contexts.get(raw["match_id"], {})
        context = compute_context_drivers(ctx_raw) if ctx_raw else {}

        player_rows.append(
            {
                "match_id": raw["match_id"],
                "player_name": raw["player_name"],
                "team": raw.get("team", ""),
                "unit": raw.get("unit", "bench"),
                "season": raw["season"],
                "round_num": raw["round_num"],
                "minutes": minutes,
                "atomics": atomics,
                "hybrids": hybrids,
                "context": context,
            }
        )

    pv_count = _upsert_player_vectors(engine, player_rows)

    team_rows = _aggregate_team_vectors(player_rows)
    tv_count = _upsert_team_vectors(engine, team_rows)

    logger.info(
        "Computed vectors: %d player, %d team (season=%s rounds=%s)",
        pv_count,
        tv_count,
        season,
        rounds,
    )
    return {"player_vectors": pv_count, "team_vectors": tv_count}

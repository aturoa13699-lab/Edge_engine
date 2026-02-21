"""Compute atomic and hybrid metrics from raw player-match stats.

All per-80 metrics use ``(stat / minutes) * 80`` normalisation.  Rows
with ``minutes < 1`` should be excluded by the caller before invoking
these functions.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


# ── Helpers ─────────────────────────────────────────────────────────────────


def _safe(raw: Dict[str, Any], key: str, default: float = 0.0) -> float:
    v = raw.get(key)
    if v is None:
        return default
    return float(v)


def _per80(raw: Dict[str, Any], stat: str) -> float:
    minutes = _safe(raw, "minutes", 0.0)
    if minutes < 1:
        return 0.0
    return (_safe(raw, stat) / minutes) * 80.0


def _safe_ratio(numerator: float, denominator: float) -> Optional[float]:
    if denominator <= 0:
        return None
    return numerator / denominator


# ── Atomic metrics ──────────────────────────────────────────────────────────


def compute_atomic_metrics(raw: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """Compute all 14 atomic vectors from a raw player-match stats row.

    *raw* must contain at least ``minutes`` and the stat fields referenced
    by the metric dictionary.  Missing fields default to 0.
    """
    tackles_made = _safe(raw, "tackles_made")
    missed_tackles = _safe(raw, "missed_tackles")
    effective_tackles = _safe(raw, "effective_tackles")
    ineffective_tackles = _safe(raw, "ineffective_tackles")
    one_on_one_steals = _safe(raw, "one_on_one_steals")
    one_on_one_attempts = _safe(raw, "one_on_one_attempts")
    offloads = _safe(raw, "offloads")
    carries = _safe(raw, "carries")

    return {
        "line_breaks_per80": _per80(raw, "line_breaks"),
        "post_contact_meters_per80": _per80(raw, "post_contact_meters"),
        "tackle_efficiency": _safe_ratio(tackles_made, tackles_made + missed_tackles),
        "offload_rate": _safe_ratio(offloads, carries),
        "errors_per80": _per80(raw, "errors"),
        "kick_meters_per80": _per80(raw, "kick_meters"),
        "run_meters_per80": _per80(raw, "run_meters"),
        "dummy_half_runs_per80": _per80(raw, "dummy_half_runs"),
        "tackle_breaks_per80": _per80(raw, "tackle_breaks"),
        "try_assists_per80": _per80(raw, "try_assists"),
        "tries_per80": _per80(raw, "tries"),
        "one_on_one_steal_rate": _safe_ratio(one_on_one_steals, one_on_one_attempts),
        "effective_tackle_pct": _safe_ratio(
            effective_tackles,
            effective_tackles + ineffective_tackles + missed_tackles,
        ),
        "involvement_rate": (
            _per80(raw, "runs") + _per80(raw, "passes") + _per80(raw, "kicks")
            if _safe(raw, "minutes") >= 1
            else 0.0
        ),
    }


# ── Hybrid metrics ──────────────────────────────────────────────────────────


def compute_hybrid_metrics(
    raw: Dict[str, Any],
    atomics: Dict[str, Optional[float]],
) -> Dict[str, Optional[float]]:
    """Compute all 10 hybrid vectors from raw stats + pre-computed atomics.

    Uses the formulas locked in the metric dictionary.  Nulls in atomics
    propagate through to the hybrid (the hybrid becomes ``None``).
    """

    def _a(name: str) -> Optional[float]:
        return atomics.get(name)

    def _null_guard(*vals: Optional[float]) -> bool:
        return all(v is not None for v in vals)

    # carry_dominance
    rm = _a("run_meters_per80")
    pcm = _a("post_contact_meters_per80")
    tb = _a("tackle_breaks_per80")
    carry_dominance: Optional[float] = None
    if _null_guard(rm, pcm, tb):
        carry_dominance = (
            0.4 * rm / 100.0  # type: ignore[operator]
            + 0.35 * pcm / 50.0  # type: ignore[operator]
            + 0.25 * tb / 5.0  # type: ignore[operator]
        )

    # defensive_pressure
    te = _a("tackle_efficiency")
    etp = _a("effective_tackle_pct")
    oos = _a("one_on_one_steal_rate")
    defensive_pressure: Optional[float] = None
    if _null_guard(te, etp, oos):
        defensive_pressure = (
            0.45 * te + 0.25 * etp + 0.30 * oos  # type: ignore[operator]
        )

    # playmaking_index
    ta = _a("try_assists_per80")
    ofr = _a("offload_rate")
    dhr = _a("dummy_half_runs_per80")
    playmaking_index: Optional[float] = None
    if _null_guard(ta, ofr, dhr):
        playmaking_index = (
            0.45 * ta / 2.0  # type: ignore[operator]
            + 0.30 * ofr  # type: ignore[operator]
            + 0.25 * dhr / 5.0  # type: ignore[operator]
        )

    # error_discipline
    ep = _a("errors_per80")
    error_discipline: Optional[float] = None
    if ep is not None:
        error_discipline = 1.0 / (1.0 + ep)

    # kicking_game
    km = _a("kick_meters_per80")
    kicking_game: Optional[float] = None
    if km is not None:
        kicking_game = km / 200.0

    # yardage_efficiency (raw meters / carries, not per-80)
    run_meters = _safe(raw, "run_meters")
    carries = _safe(raw, "carries")
    yardage_efficiency = _safe_ratio(run_meters, max(carries, 1))

    # fatigue_resilience
    fh_inv = _safe(raw, "first_half_involvements")
    sh_inv = _safe(raw, "second_half_involvements")
    fatigue_resilience = _safe_ratio(sh_inv, max(fh_inv, 1))

    # momentum_contribution
    lb = _a("line_breaks_per80")
    momentum_contribution: Optional[float] = None
    if _null_guard(lb, tb):
        momentum_contribution = (
            0.55 * lb / 3.0 + 0.45 * tb / 5.0  # type: ignore[operator]
        )

    # set_completion_impact
    inv = _a("involvement_rate")
    set_completion_impact: Optional[float] = None
    if _null_guard(inv) and error_discipline is not None:
        set_completion_impact = (inv / 40.0) * error_discipline  # type: ignore[operator]

    # field_position_impact
    field_position_impact: Optional[float] = None
    if _null_guard(km, rm):
        field_position_impact = (
            0.5 * km / 200.0 + 0.5 * rm / 100.0  # type: ignore[operator]
        )

    return {
        "carry_dominance": carry_dominance,
        "defensive_pressure": defensive_pressure,
        "playmaking_index": playmaking_index,
        "error_discipline": error_discipline,
        "kicking_game": kicking_game,
        "yardage_efficiency": yardage_efficiency,
        "fatigue_resilience": fatigue_resilience,
        "momentum_contribution": momentum_contribution,
        "set_completion_impact": set_completion_impact,
        "field_position_impact": field_position_impact,
    }


# ── Context drivers ─────────────────────────────────────────────────────────


def compute_context_drivers(ctx: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """Compute 7 context drivers from a match-context row.

    *ctx* should have keys like ``home_rating``, ``away_rating``,
    ``is_wet``, ``wind_speed_kmh``, ``temp_c``, ``injury_count``,
    ``rest_days``, ``is_home``, ``venue``.
    """
    home_rating = _safe(ctx, "home_rating", 1500.0)
    away_rating = _safe(ctx, "away_rating", 1500.0)
    matchup_score = (home_rating - away_rating) / 400.0

    is_wet = _safe(ctx, "is_wet")
    wind = _safe(ctx, "wind_speed_kmh", 10.0)
    temp = _safe(ctx, "temp_c", 20.0)
    weather_score = (
        0.4 * is_wet
        + 0.3 * max(0.0, (wind - 15.0) / 30.0)
        + 0.3 * max(0.0, (35.0 - temp) / 20.0)
    )

    is_home = _safe(ctx, "is_home", 0.5)
    venue_score = float(is_home)

    script_score = 0.5 * abs(matchup_score) + 0.3 * weather_score + 0.2 * venue_score

    injury_count = _safe(ctx, "injury_count")
    role_uncertainty = min(1.0, injury_count / 5.0)

    travel_km = _safe(ctx, "travel_distance_km")
    turnaround = _safe(ctx, "rest_days", 7.0)

    return {
        "matchup_score": matchup_score,
        "script_score": script_score,
        "weather_score": weather_score,
        "venue_score": venue_score,
        "role_uncertainty": role_uncertainty,
        "travel_distance_km": travel_km,
        "turnaround_days": turnaround,
    }

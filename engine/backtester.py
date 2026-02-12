"""Walk-forward backtesting engine."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
from sqlalchemy import bindparam, text as sql_text
from sqlalchemy.engine import Engine

from .calibration import apply_calibration, load_latest_calibrator
from .deploy_engine import _fetch_live_feature_row, _heuristic_p, _ml_p
from .guardrails import RoundExposureTracker, passes_edge_floor, passes_entropy_gate
from .risk import apply_fractional_kelly, kelly_fraction

logger = logging.getLogger("nrl-pillar1")


@dataclass
class BacktestResult:
    total_bets: int = 0
    wins: int = 0
    losses: int = 0
    no_edge_skipped: int = 0
    entropy_skipped: int = 0
    edge_floor_skipped: int = 0
    exposure_capped: int = 0
    initial_bankroll: float = 1000.0
    final_bankroll: float = 1000.0
    peak_bankroll: float = 1000.0
    max_drawdown: float = 0.0
    total_staked: float = 0.0
    total_pnl: float = 0.0
    brier_scores: list = field(default_factory=list)
    round_results: list = field(default_factory=list)

    @property
    def roi(self) -> float:
        return (self.total_pnl / self.total_staked * 100) if self.total_staked > 0 else 0.0

    @property
    def hit_rate(self) -> float:
        return (self.wins / self.total_bets * 100) if self.total_bets > 0 else 0.0

    @property
    def avg_brier(self) -> float:
        return float(np.mean(self.brier_scores)) if self.brier_scores else 0.0

    def summary(self) -> Dict:
        return {
            "total_bets": self.total_bets,
            "wins": self.wins,
            "losses": self.losses,
            "no_edge_skipped": self.no_edge_skipped,
            "entropy_skipped": self.entropy_skipped,
            "edge_floor_skipped": self.edge_floor_skipped,
            "exposure_capped": self.exposure_capped,
            "hit_rate_pct": round(self.hit_rate, 2),
            "initial_bankroll": round(self.initial_bankroll, 2),
            "final_bankroll": round(self.final_bankroll, 2),
            "total_pnl": round(self.total_pnl, 2),
            "roi_pct": round(self.roi, 2),
            "peak_bankroll": round(self.peak_bankroll, 2),
            "max_drawdown_pct": round(self.max_drawdown * 100, 2),
            "avg_brier_score": round(self.avg_brier, 5),
        }


def run_backtest(
    engine: Engine,
    season: int,
    initial_bankroll: float = 1000.0,
    max_stake_frac: float = 0.05,
    rounds: Optional[List[int]] = None,
) -> BacktestResult:
    """
    Walk-forward backtest over resolved matches in a season.

    For each match:
    1. Generate prediction using features available at match time
    2. Size bet using Kelly criterion (home team H2H only, matching deploy logic)
    3. Resolve against actual outcome
    4. Update bankroll
    """
    base_sql = """
        SELECT m.match_id, m.season, m.round_num, m.match_date,
               m.home_team, m.away_team, m.home_score, m.away_score
        FROM nrl.matches_raw m
        WHERE m.season = :s
          AND m.home_score IS NOT NULL
          AND m.away_score IS NOT NULL
    """
    params: dict = {"s": season}

    if rounds:
        base_sql += "  AND m.round_num IN :rounds"
        query = sql_text(base_sql + " ORDER BY m.round_num, m.match_date, m.match_id").bindparams(
            bindparam("rounds", expanding=True)
        )
        params["rounds"] = rounds
    else:
        query = sql_text(base_sql + " ORDER BY m.round_num, m.match_date, m.match_id")

    with engine.begin() as conn:
        matches = conn.execute(query, params).mappings().all()

    if not matches:
        logger.warning("No resolved matches for backtest season=%s", season)
        return BacktestResult(initial_bankroll=initial_bankroll, final_bankroll=initial_bankroll)

    alpha = float(os.getenv("ML_BLEND_ALPHA", "0.65"))
    calibrator = load_latest_calibrator(engine, season)

    result = BacktestResult(
        initial_bankroll=initial_bankroll,
        final_bankroll=initial_bankroll,
        peak_bankroll=initial_bankroll,
    )
    bankroll = initial_bankroll
    tracker = RoundExposureTracker(bankroll=bankroll)

    for m in matches:
        match_id = m["match_id"]
        round_num = m["round_num"]
        home_win = bool(m["home_score"] > m["away_score"])

        # Generate prediction
        feature_row = _fetch_live_feature_row(engine, match_id)
        p_h = _heuristic_p(feature_row)
        p_ml = _ml_p(engine, feature_row)

        if p_ml is None:
            p_blend = p_h
        else:
            p_blend = float(alpha * p_ml + (1.0 - alpha) * p_h)

        p_cal = apply_calibration(p_blend, calibrator)

        # Brier score (always tracked regardless of bet decision)
        outcome_val = 1.0 if home_win else 0.0
        brier = (p_cal - outcome_val) ** 2
        result.brier_scores.append(float(brier))

        # Stake sizing (home team H2H only, matching deploy_engine logic)
        odds_taken = float(feature_row.get("odds_taken", 1.90))

        if odds_taken <= 1.0:
            result.no_edge_skipped += 1
            continue

        # EV check
        ev = (p_cal * odds_taken) - 1.0

        # --- Guardrails ---
        if not passes_entropy_gate(p_cal):
            result.entropy_skipped += 1
            result.no_edge_skipped += 1
            continue

        if not passes_edge_floor(ev):
            result.edge_floor_skipped += 1
            result.no_edge_skipped += 1
            continue

        raw_f = kelly_fraction(p_cal, odds_taken)
        f = apply_fractional_kelly(raw_f)

        if f <= 0.0:
            result.no_edge_skipped += 1
            continue

        if f > max_stake_frac:
            f = max_stake_frac

        stake = bankroll * f
        if stake <= 0:
            result.no_edge_skipped += 1
            continue

        # Round exposure cap
        stake = tracker.clamp_stake(round_num, stake)
        if stake <= 0:
            result.exposure_capped += 1
            result.no_edge_skipped += 1
            continue
        tracker.record(round_num, stake)

        result.total_bets += 1
        result.total_staked += stake

        # Resolve bet
        if home_win:
            profit = stake * (odds_taken - 1.0)
            bankroll += profit
            result.wins += 1
            result.total_pnl += profit
        else:
            bankroll -= stake
            result.losses += 1
            result.total_pnl -= stake

        # Track peak/drawdown
        if bankroll > result.peak_bankroll:
            result.peak_bankroll = bankroll
        if result.peak_bankroll > 0:
            drawdown = (result.peak_bankroll - bankroll) / result.peak_bankroll
            if drawdown > result.max_drawdown:
                result.max_drawdown = drawdown

        result.round_results.append({
            "match_id": match_id,
            "round_num": m["round_num"],
            "home_team": m["home_team"],
            "away_team": m["away_team"],
            "p_cal": round(p_cal, 4),
            "odds": odds_taken,
            "stake": round(stake, 2),
            "outcome": "win" if home_win else "loss",
            "pnl": round(profit if home_win else -stake, 2),
            "bankroll": round(bankroll, 2),
        })

        logger.debug(
            "BT %s R%s: %s vs %s | p=%.3f odds=%.2f stake=%.2f => %s (bank=%.2f)",
            match_id[:8],
            m["round_num"],
            m["home_team"],
            m["away_team"],
            p_cal,
            odds_taken,
            stake,
            "WIN" if home_win else "LOSS",
            bankroll,
        )

    result.final_bankroll = bankroll

    summary = result.summary()
    logger.info("Backtest complete for season %s:", season)
    logger.info("  Bets: %s (W:%s L:%s) | Hit rate: %.1f%%", summary["total_bets"], summary["wins"], summary["losses"], summary["hit_rate_pct"])
    logger.info("  P&L: $%.2f | ROI: %.2f%%", summary["total_pnl"], summary["roi_pct"])
    logger.info("  Bankroll: $%.2f -> $%.2f | Peak: $%.2f | Max DD: %.1f%%", summary["initial_bankroll"], summary["final_bankroll"], summary["peak_bankroll"], summary["max_drawdown_pct"])
    logger.info("  Avg Brier: %.5f | Skipped (no edge): %s", summary["avg_brier_score"], summary["no_edge_skipped"])
    logger.info("  Guardrails — entropy: %s, edge_floor: %s, exposure_cap: %s", summary["entropy_skipped"], summary["edge_floor_skipped"], summary["exposure_capped"])

    return result

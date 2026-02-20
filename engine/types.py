from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# Decision states for slip lifecycle
DECISION_RECO = "RECO"
DECISION_DECLINED = "DECLINED"

# ML status values
ML_STATUS_ML = "ml"
ML_STATUS_HEURISTIC = "heuristic"
ML_STATUS_BLEND = "blend"


# Stake ladder: predefined levels that map EV tiers to descriptive labels
STAKE_LADDER: list[dict[str, str | float]] = [
    {
        "level": "pass",
        "label": "Pass",
        "min_ev": float("-inf"),
        "max_ev": 0.03,
        "frac": 0.0,
    },
    {
        "level": "unit_half",
        "label": "0.5 Unit",
        "min_ev": 0.03,
        "max_ev": 0.06,
        "frac": 0.5,
    },
    {"level": "unit_1", "label": "1 Unit", "min_ev": 0.06, "max_ev": 0.10, "frac": 1.0},
    {
        "level": "unit_2",
        "label": "2 Units",
        "min_ev": 0.10,
        "max_ev": 0.15,
        "frac": 2.0,
    },
    {
        "level": "unit_3",
        "label": "3 Units",
        "min_ev": 0.15,
        "max_ev": float("inf"),
        "frac": 3.0,
    },
]


def resolve_stake_ladder_level(ev: float) -> dict:
    """Return the stake ladder entry matching the given EV."""
    for entry in STAKE_LADDER:
        if float(entry["min_ev"]) <= ev < float(entry["max_ev"]):
            return entry
    return STAKE_LADDER[0]


@dataclass
class Slip:
    portfolio_id: str
    season: int
    round_num: int
    match_id: str
    home_team: str
    away_team: str
    market: str
    selection: str
    odds: float
    stake: float
    ev: float
    status: str = "pending"
    model_version: str = "v2026-02-poisson-v1"
    reason: Optional[str] = None
    ml_status: str = ML_STATUS_HEURISTIC
    decision: str = DECISION_RECO
    decline_reason: Optional[str] = None
    stake_ladder_level: Optional[str] = None

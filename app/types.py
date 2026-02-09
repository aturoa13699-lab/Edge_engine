<<<<<<< HEAD
from dataclasses import dataclass
from typing import List


@dataclass
class SlipLeg:
    match_id: str
    market: str
    selection: str
    price: float
    p_model: float
=======
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
>>>>>>> origin/codex/2026-02-09-bootstrap-and-verify-nrl-edge-engine-v1.1


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

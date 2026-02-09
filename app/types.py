from dataclasses import dataclass
from typing import List


@dataclass
class SlipLeg:
    match_id: str
    market: str
    selection: str
    price: float
    p_model: float


@dataclass
class Slip:
    portfolio_id: str
    season: int
    round_num: int
    match_id: str
    market: str
    legs: List[SlipLeg]
    stake_units: float
    status: str
    created_at: str

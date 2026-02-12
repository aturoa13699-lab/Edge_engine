"""Betting guardrails: entropy gate, edge floor, round exposure cap."""
from __future__ import annotations

import logging
import math
import os
from typing import Dict

logger = logging.getLogger("nrl-pillar1")

# Binary entropy: H(p) = -p*ln(p) - (1-p)*ln(1-p), max = ln(2) ~ 0.693
_LN2 = math.log(2.0)


def binary_entropy(p: float) -> float:
    """Binary entropy in nats. Max is ln(2) ~ 0.693 at p=0.5."""
    if p <= 0.0 or p >= 1.0:
        return 0.0
    return -(p * math.log(p) + (1.0 - p) * math.log(1.0 - p))


def passes_entropy_gate(p: float, max_entropy: float | None = None) -> bool:
    """Return True if the prediction is confident enough (low entropy)."""
    if max_entropy is None:
        max_entropy = float(os.getenv("ENTROPY_MAX", "0.65"))
    h = binary_entropy(p)
    return h <= max_entropy


def passes_edge_floor(ev: float, min_edge: float | None = None) -> bool:
    """Return True if expected value exceeds the minimum edge threshold."""
    if min_edge is None:
        min_edge = float(os.getenv("EDGE_MIN", "0.05"))
    return ev >= min_edge


class RoundExposureTracker:
    """Track cumulative stake exposure within a single round."""

    def __init__(self, bankroll: float, max_frac: float | None = None):
        self.bankroll = bankroll
        if max_frac is None:
            max_frac = float(os.getenv("MAX_ROUND_EXPOSURE_FRAC", "0.06"))
        self.max_frac = max_frac
        self._round_stakes: Dict[int, float] = {}

    def remaining(self, round_num: int) -> float:
        """Return the remaining stake budget for a round."""
        used = self._round_stakes.get(round_num, 0.0)
        cap = self.bankroll * self.max_frac
        return max(0.0, cap - used)

    def can_stake(self, round_num: int, stake: float) -> bool:
        return stake <= self.remaining(round_num)

    def record(self, round_num: int, stake: float) -> None:
        self._round_stakes[round_num] = self._round_stakes.get(round_num, 0.0) + stake

    def clamp_stake(self, round_num: int, stake: float) -> float:
        """Return the smaller of stake and remaining round budget."""
        rem = self.remaining(round_num)
        return min(stake, rem)

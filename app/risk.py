import os
from dataclasses import dataclass


def kelly_fraction(p: float, odds: float) -> float:
    """
    Kelly fraction for decimal odds.
    f* = (b*p - q)/b, where b = odds-1, q=1-p
    """
    if odds <= 1.0:
        return 0.0
    b = odds - 1.0
    q = 1.0 - p
    f = (b * p - q) / b
    return max(0.0, f)


def apply_fractional_kelly(f: float) -> float:
    frac = float(os.getenv("FRACTIONAL_KELLY", "1.0"))
    if frac <= 0.0:
        frac = 1.0
    return f * frac


@dataclass
class SizingDecision:
    stake: float
    kelly_f: float
    capped: bool
    reason: str


def size_stake(bankroll: float, p: float, odds: float, max_frac: float = 0.05) -> SizingDecision:
    raw_f = kelly_fraction(p, odds)
    f = apply_fractional_kelly(raw_f)

    if f <= 0.0:
        return SizingDecision(stake=0.0, kelly_f=0.0, capped=False, reason="no edge")

    capped = False
    if f > max_frac:
        f = max_frac
        capped = True

    stake = bankroll * f
    return SizingDecision(stake=stake, kelly_f=f, capped=capped, reason="kelly")

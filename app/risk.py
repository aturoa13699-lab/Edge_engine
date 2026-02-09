import os
import random


def _kelly_fraction(p: float, price: float) -> float:
    """
    Standard Kelly for binary bet with decimal odds.
    f* = (bp - q) / b, where b = price - 1, q = 1-p.
    """
    b = float(price) - 1.0
    if b <= 0:
        return 0.0
    q = 1.0 - float(p)
    return max(0.0, (b * float(p) - q) / b)


def _mc_drawdown_penalty(stake_units: float, bankroll_units: float, n: int = 200) -> float:
    """
    Lightweight Monte Carlo to penalize stakes that produce large drawdowns.
    This is not a full portfolio simulator; it's a guardrail heuristic.
    """
    if bankroll_units <= 0:
        return 1.0
    if stake_units <= 0:
        return 0.0

    worst = 0.0
    for _ in range(n):
        br = bankroll_units
        # simulate a streaky run: 20 trials
        for __ in range(20):
            if random.random() < 0.52:
                br += stake_units * 0.9
            else:
                br -= stake_units
            if br <= 0:
                br = 0
                break
        dd = (bankroll_units - br) / bankroll_units
        worst = max(worst, dd)

    # penalty scale
    if worst > 0.50:
        return 0.0
    if worst > 0.35:
        return 0.5
    return 1.0


def size_and_guard(
    bankroll_units: float,
    p: float,
    price: float,
) -> float:
    """
    Compute stake size with fractional Kelly + caps + heat + MC drawdown guard.
    """
    p = float(max(0.01, min(0.99, p)))
    price = float(price)

    full_k = _kelly_fraction(p, price)
    frac_k = float(os.getenv("FRACTIONAL_KELLY", "1.0"))
    k = full_k * frac_k

    # caps
    cap = float(os.getenv("KELLY_FRACTION_CAP", "1.0"))
    k = max(0.0, min(cap, k))

    # baseline stake (units)
    stake = k * float(bankroll_units)

    # heat cap
    heat_cap = float(os.getenv("HEAT_CAP", "0.30"))
    stake = min(stake, heat_cap * float(bankroll_units))

    # MC drawdown guard
    penalty = _mc_drawdown_penalty(stake, bankroll_units)
    stake *= penalty

    # minimum visibility stake (optional)
    if stake < 0.0:
        stake = 0.0
    return float(stake)

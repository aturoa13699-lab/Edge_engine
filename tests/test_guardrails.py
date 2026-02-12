import math

from engine.guardrails import (
    RoundExposureTracker,
    binary_entropy,
    passes_edge_floor,
    passes_entropy_gate,
)


def test_binary_entropy_at_half():
    """Entropy peaks at p=0.5 with H = ln(2) ~ 0.693."""
    assert abs(binary_entropy(0.5) - math.log(2)) < 1e-9


def test_binary_entropy_at_extremes():
    assert binary_entropy(0.0) == 0.0
    assert binary_entropy(1.0) == 0.0


def test_binary_entropy_symmetric():
    assert abs(binary_entropy(0.3) - binary_entropy(0.7)) < 1e-9


def test_passes_entropy_gate_confident():
    # p=0.8 -> H ~ 0.50, well below 0.65
    assert passes_entropy_gate(0.8, max_entropy=0.65) is True


def test_passes_entropy_gate_uncertain():
    # p=0.5 -> H ~ 0.693, above 0.65
    assert passes_entropy_gate(0.5, max_entropy=0.65) is False


def test_passes_entropy_gate_borderline():
    # p=0.55 -> H ~ 0.688, above 0.65
    assert passes_entropy_gate(0.55, max_entropy=0.65) is False


def test_passes_edge_floor_above():
    assert passes_edge_floor(0.10, min_edge=0.05) is True


def test_passes_edge_floor_below():
    assert passes_edge_floor(0.03, min_edge=0.05) is False


def test_passes_edge_floor_exact():
    assert passes_edge_floor(0.05, min_edge=0.05) is True


def test_round_exposure_tracker_basic():
    tracker = RoundExposureTracker(bankroll=1000.0, max_frac=0.06)
    # Budget for round 1 = 1000 * 0.06 = 60
    assert tracker.remaining(1) == 60.0
    assert tracker.can_stake(1, 30.0) is True
    tracker.record(1, 30.0)
    assert tracker.remaining(1) == 30.0


def test_round_exposure_tracker_clamp():
    tracker = RoundExposureTracker(bankroll=1000.0, max_frac=0.06)
    tracker.record(1, 50.0)
    # Only 10 left
    assert tracker.clamp_stake(1, 20.0) == 10.0


def test_round_exposure_tracker_separate_rounds():
    tracker = RoundExposureTracker(bankroll=1000.0, max_frac=0.06)
    tracker.record(1, 60.0)
    # Round 2 is independent
    assert tracker.remaining(2) == 60.0
    assert tracker.can_stake(2, 60.0) is True

import pytest

from engine.vector_metrics import (
    compute_atomic_metrics,
    compute_context_drivers,
    compute_hybrid_metrics,
)


def _full_raw_row():
    """A complete player-match stats row with realistic values."""
    return {
        "minutes": 80,
        "runs": 15,
        "run_meters": 120,
        "carries": 18,
        "post_contact_meters": 45,
        "line_breaks": 2,
        "tackle_breaks": 3,
        "offloads": 2,
        "tackles_made": 30,
        "missed_tackles": 3,
        "effective_tackles": 28,
        "ineffective_tackles": 2,
        "errors": 1,
        "tries": 1,
        "try_assists": 1,
        "kicks": 4,
        "kick_meters": 150,
        "passes": 10,
        "dummy_half_runs": 3,
        "one_on_one_steals": 1,
        "one_on_one_attempts": 3,
        "first_half_involvements": 15,
        "second_half_involvements": 14,
    }


# ── Atomic metrics ──────────────────────────────────────────────────────────


class TestAtomicMetrics:
    def test_per80_at_80_minutes_equals_raw(self):
        raw = _full_raw_row()
        a = compute_atomic_metrics(raw)
        # At exactly 80 minutes, per-80 = raw count
        assert a["line_breaks_per80"] == pytest.approx(2.0)
        assert a["errors_per80"] == pytest.approx(1.0)
        assert a["tries_per80"] == pytest.approx(1.0)

    def test_per80_scales_with_minutes(self):
        raw = _full_raw_row()
        raw["minutes"] = 40  # half a game
        a = compute_atomic_metrics(raw)
        # line_breaks=2 in 40 min → 4.0 per 80
        assert a["line_breaks_per80"] == pytest.approx(4.0)

    def test_tackle_efficiency(self):
        raw = _full_raw_row()
        a = compute_atomic_metrics(raw)
        assert a["tackle_efficiency"] == pytest.approx(30 / 33)

    def test_offload_rate(self):
        raw = _full_raw_row()
        a = compute_atomic_metrics(raw)
        assert a["offload_rate"] == pytest.approx(2 / 18)

    def test_zero_denominator_returns_none(self):
        raw = _full_raw_row()
        raw["carries"] = 0
        raw["one_on_one_attempts"] = 0
        a = compute_atomic_metrics(raw)
        assert a["offload_rate"] is None
        assert a["one_on_one_steal_rate"] is None

    def test_sub_minute_returns_zero(self):
        raw = _full_raw_row()
        raw["minutes"] = 0.5
        a = compute_atomic_metrics(raw)
        assert a["line_breaks_per80"] == 0.0
        assert a["run_meters_per80"] == 0.0

    def test_all_14_keys_present(self):
        a = compute_atomic_metrics(_full_raw_row())
        assert len(a) == 14

    def test_involvement_rate(self):
        raw = _full_raw_row()
        a = compute_atomic_metrics(raw)
        # (15 + 10 + 4) / 80 * 80 = 29
        assert a["involvement_rate"] == pytest.approx(29.0)


# ── Hybrid metrics ──────────────────────────────────────────────────────────


class TestHybridMetrics:
    def test_all_10_keys_present(self):
        raw = _full_raw_row()
        a = compute_atomic_metrics(raw)
        h = compute_hybrid_metrics(raw, a)
        assert len(h) == 10

    def test_error_discipline_inverse(self):
        raw = _full_raw_row()
        a = compute_atomic_metrics(raw)
        h = compute_hybrid_metrics(raw, a)
        # errors_per80 = 1.0 → error_discipline = 1/(1+1) = 0.5
        assert h["error_discipline"] == pytest.approx(0.5)

    def test_carry_dominance_positive(self):
        raw = _full_raw_row()
        a = compute_atomic_metrics(raw)
        h = compute_hybrid_metrics(raw, a)
        assert h["carry_dominance"] is not None
        assert h["carry_dominance"] > 0

    def test_fatigue_resilience_near_one(self):
        raw = _full_raw_row()
        a = compute_atomic_metrics(raw)
        h = compute_hybrid_metrics(raw, a)
        # 14/15 ≈ 0.93
        assert h["fatigue_resilience"] == pytest.approx(14 / 15)

    def test_null_propagation_from_atomics(self):
        raw = _full_raw_row()
        raw["carries"] = 0
        raw["one_on_one_attempts"] = 0
        raw["tackles_made"] = 0
        raw["missed_tackles"] = 0
        raw["effective_tackles"] = 0
        raw["ineffective_tackles"] = 0
        a = compute_atomic_metrics(raw)
        h = compute_hybrid_metrics(raw, a)
        # defensive_pressure requires tackle_efficiency, effective_tackle_pct,
        # one_on_one_steal_rate — all None when denominators are 0
        assert h["defensive_pressure"] is None


# ── Context drivers ─────────────────────────────────────────────────────────


class TestContextDrivers:
    def test_all_7_keys_present(self):
        ctx = {
            "home_rating": 1600,
            "away_rating": 1400,
            "is_wet": 0,
            "wind_speed_kmh": 10,
            "temp_c": 22,
            "is_home": 1,
            "injury_count": 2,
            "travel_distance_km": 500,
            "rest_days": 6,
        }
        c = compute_context_drivers(ctx)
        assert len(c) == 7

    def test_matchup_score(self):
        ctx = {"home_rating": 1600, "away_rating": 1400}
        c = compute_context_drivers(ctx)
        assert c["matchup_score"] == pytest.approx(200 / 400)

    def test_venue_score_home(self):
        ctx = {"is_home": 1}
        c = compute_context_drivers(ctx)
        assert c["venue_score"] == 1.0

    def test_role_uncertainty_capped(self):
        ctx = {"injury_count": 10}
        c = compute_context_drivers(ctx)
        assert c["role_uncertainty"] == 1.0

    def test_weather_score_dry_calm(self):
        ctx = {"is_wet": 0, "wind_speed_kmh": 5, "temp_c": 22}
        c = compute_context_drivers(ctx)
        # All weather components near zero for ideal conditions
        assert c["weather_score"] < 0.3

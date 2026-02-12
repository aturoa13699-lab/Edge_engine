import random

from engine.seed_data import (
    NRL_TEAMS,
    _generate_fixtures,
    _generate_odds,
    _generate_scores,
    _season_ratings,
    _win_prob,
)


def test_win_prob_range():
    assert 0.0 < _win_prob(1600, 1400) < 1.0
    assert _win_prob(1500, 1500) > 0.5  # home advantage


def test_win_prob_stronger_team():
    assert _win_prob(1700, 1300) > _win_prob(1500, 1500)


def test_season_ratings_all_teams():
    rng = random.Random(42)
    ratings = _season_ratings(2025, rng)
    assert len(ratings) == len(NRL_TEAMS)
    for team in NRL_TEAMS:
        assert team in ratings
        assert 1200 < ratings[team] < 1900


def test_generate_fixtures_count():
    rng = random.Random(2025)
    fixtures = _generate_fixtures(2025, num_rounds=27, rng=rng)
    # 17 teams, 8 games per round, 27 rounds = 216 games
    assert len(fixtures) == 8 * 27


def test_generate_fixtures_3_rounds():
    rng = random.Random(2026)
    fixtures = _generate_fixtures(2026, num_rounds=3, rng=rng)
    assert len(fixtures) == 8 * 3


def test_generate_scores():
    rng = random.Random(2025)
    ratings = _season_ratings(2025, rng)
    rng2 = random.Random(2025)
    fixtures = _generate_fixtures(2025, num_rounds=5, rng=rng2)
    _generate_scores(fixtures, ratings, rng2)
    for f in fixtures:
        assert f["home_score"] is not None
        assert f["away_score"] is not None
        assert 0 <= f["home_score"] <= 56
        assert 0 <= f["away_score"] <= 56
        assert f["home_score"] != f["away_score"]


def test_generate_odds():
    rng = random.Random(2025)
    ratings = _season_ratings(2025, rng)
    rng2 = random.Random(2025)
    fixtures = _generate_fixtures(2025, num_rounds=2, rng=rng2)
    odds = _generate_odds(fixtures, ratings, rng2)
    # 2 odds rows per match (home + away)
    assert len(odds) == len(fixtures) * 2
    for o in odds:
        assert o["opening_price"] >= 1.05
        assert o["close_price"] >= 1.0
        assert o["last_price"] >= 1.0

"""Seed historical NRL data for pipeline development and backtesting.

Generates realistic synthetic data for all core tables so the full
pipeline (train -> backfill -> calibrate -> backtest -> deploy) works
end-to-end.
"""
from __future__ import annotations

import logging
import math
import random
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

logger = logging.getLogger("nrl-pillar1")

NRL_TEAMS = [
    "Brisbane Broncos",
    "Canberra Raiders",
    "Canterbury-Bankstown Bulldogs",
    "Cronulla-Sutherland Sharks",
    "Dolphins",
    "Gold Coast Titans",
    "Manly Warringah Sea Eagles",
    "Melbourne Storm",
    "Newcastle Knights",
    "New Zealand Warriors",
    "North Queensland Cowboys",
    "Parramatta Eels",
    "Penrith Panthers",
    "South Sydney Rabbitohs",
    "St. George Illawarra Dragons",
    "Sydney Roosters",
    "Wests Tigers",
]

HOME_VENUES = {
    "Brisbane Broncos": "Suncorp Stadium",
    "Canberra Raiders": "GIO Stadium",
    "Canterbury-Bankstown Bulldogs": "Accor Stadium",
    "Cronulla-Sutherland Sharks": "PointsBet Stadium",
    "Dolphins": "Suncorp Stadium",
    "Gold Coast Titans": "Cbus Super Stadium",
    "Manly Warringah Sea Eagles": "4 Pines Park",
    "Melbourne Storm": "AAMI Park",
    "Newcastle Knights": "McDonald Jones Stadium",
    "New Zealand Warriors": "Go Media Stadium",
    "North Queensland Cowboys": "Qld Country Bank Stadium",
    "Parramatta Eels": "CommBank Stadium",
    "Penrith Panthers": "BlueBet Stadium",
    "South Sydney Rabbitohs": "Accor Stadium",
    "St. George Illawarra Dragons": "WIN Stadium",
    "Sydney Roosters": "Allianz Stadium",
    "Wests Tigers": "Campbelltown Stadium",
}

BASE_RATINGS = {
    "Penrith Panthers": 1650,
    "Melbourne Storm": 1600,
    "Sydney Roosters": 1580,
    "Cronulla-Sutherland Sharks": 1560,
    "Brisbane Broncos": 1540,
    "North Queensland Cowboys": 1530,
    "Canterbury-Bankstown Bulldogs": 1520,
    "New Zealand Warriors": 1510,
    "South Sydney Rabbitohs": 1500,
    "Dolphins": 1490,
    "Manly Warringah Sea Eagles": 1480,
    "Canberra Raiders": 1470,
    "Newcastle Knights": 1460,
    "Gold Coast Titans": 1450,
    "Parramatta Eels": 1440,
    "St. George Illawarra Dragons": 1430,
    "Wests Tigers": 1400,
}

COACH_STYLE_SCORES = {
    "Penrith Panthers": 0.85,
    "Melbourne Storm": 0.82,
    "Sydney Roosters": 0.72,
    "Cronulla-Sutherland Sharks": 0.68,
    "Brisbane Broncos": 0.65,
    "North Queensland Cowboys": 0.60,
    "Canterbury-Bankstown Bulldogs": 0.58,
    "New Zealand Warriors": 0.55,
    "South Sydney Rabbitohs": 0.50,
    "Dolphins": 0.48,
    "Manly Warringah Sea Eagles": 0.45,
    "Canberra Raiders": 0.42,
    "Newcastle Knights": 0.40,
    "Gold Coast Titans": 0.35,
    "Parramatta Eels": 0.32,
    "St. George Illawarra Dragons": 0.28,
    "Wests Tigers": 0.20,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _win_prob(home_rating: float, away_rating: float, home_advantage: float = 50.0) -> float:
    diff = home_rating - away_rating + home_advantage
    return 1.0 / (1.0 + math.pow(10.0, -diff / 400.0))


def _season_ratings(season: int, rng: random.Random) -> Dict[str, float]:
    return {t: r + rng.gauss(0, 30) for t, r in BASE_RATINGS.items()}


def _generate_fixtures(season: int, num_rounds: int, rng: random.Random) -> List[Dict[str, Any]]:
    fixtures: List[Dict[str, Any]] = []
    season_start = date(season, 3, 7)

    for round_num in range(1, num_rounds + 1):
        round_date = season_start + timedelta(weeks=round_num - 1)
        shuffled = list(NRL_TEAMS)
        rng.shuffle(shuffled)

        # 17 teams -> 8 games, 1 bye
        for i in range(0, len(shuffled) - 1, 2):
            home = shuffled[i]
            away = shuffled[i + 1]
            game_idx = i // 2
            game_day_offset = game_idx % 4  # Thu-Sun spread
            fixtures.append({
                "match_id": f"NRL_{season}_R{round_num:02d}_M{game_idx + 1:02d}",
                "season": season,
                "round_num": round_num,
                "match_date": round_date + timedelta(days=game_day_offset),
                "venue": HOME_VENUES.get(home, "Neutral Venue"),
                "home_team": home,
                "away_team": away,
                "home_score": None,
                "away_score": None,
            })
    return fixtures


def _generate_scores(fixtures: List[Dict], ratings: Dict[str, float], rng: random.Random) -> None:
    for f in fixtures:
        hr = ratings.get(f["home_team"], 1500)
        ar = ratings.get(f["away_team"], 1500)
        p_home = _win_prob(hr, ar)

        home_base = 18 + (hr - 1500) / 80 + 2
        away_base = 18 + (ar - 1500) / 80
        hs = max(0, round(home_base + rng.gauss(0, 8)))
        aws = max(0, round(away_base + rng.gauss(0, 8)))

        if hs == aws:
            if rng.random() < p_home:
                hs += 2
            else:
                aws += 2

        f["home_score"] = min(hs, 56)
        f["away_score"] = min(aws, 56)


def _generate_odds(fixtures: List[Dict], ratings: Dict[str, float], rng: random.Random) -> List[Dict]:
    rows: List[Dict] = []
    overround = 1.05
    for f in fixtures:
        hr = ratings.get(f["home_team"], 1500)
        ar = ratings.get(f["away_team"], 1500)
        p_h = _win_prob(hr, ar)
        p_a = 1.0 - p_h

        h_odds = round(overround / max(p_h, 0.05), 2)
        a_odds = round(overround / max(p_a, 0.05), 2)
        h_odds = max(1.05, min(h_odds, 15.0))
        a_odds = max(1.05, min(a_odds, 15.0))

        for team, opening in [(f["home_team"], h_odds), (f["away_team"], a_odds)]:
            rows.append({
                "match_id": f["match_id"],
                "team": team,
                "opening_price": opening,
                "close_price": round(opening * (0.97 + 0.06 * rng.random()), 2),
                "last_price": round(opening * (0.96 + 0.08 * rng.random()), 2),
            })
    return rows


# ---------------------------------------------------------------------------
# Insert helpers
# ---------------------------------------------------------------------------

def _insert_matches(engine: Engine, fixtures: List[Dict]) -> int:
    affected = 0
    with engine.begin() as conn:
        for f in fixtures:
            result = conn.execute(
                sql_text("""
                    INSERT INTO nrl.matches_raw
                    (match_id, season, round_num, match_date, venue, home_team, away_team, home_score, away_score)
                    VALUES (:mid, :s, :r, :d, :v, :h, :a, :hs, :as_)
                    ON CONFLICT (match_id) DO UPDATE
                    SET home_score = COALESCE(EXCLUDED.home_score, nrl.matches_raw.home_score),
                        away_score = COALESCE(EXCLUDED.away_score, nrl.matches_raw.away_score),
                        updated_at = now()
                """),
                dict(
                    mid=f["match_id"], s=f["season"], r=f["round_num"],
                    d=f["match_date"], v=f["venue"],
                    h=f["home_team"], a=f["away_team"],
                    hs=f["home_score"], as_=f["away_score"],
                ),
            )
            affected += result.rowcount
    return affected


def _insert_odds(engine: Engine, odds_rows: List[Dict]) -> int:
    affected = 0
    with engine.begin() as conn:
        for o in odds_rows:
            result = conn.execute(
                sql_text("""
                    INSERT INTO nrl.odds (match_id, team, opening_price, close_price, last_price)
                    VALUES (:mid, :t, :op, :cp, :lp)
                    ON CONFLICT (match_id, team) DO UPDATE
                    SET opening_price = COALESCE(EXCLUDED.opening_price, nrl.odds.opening_price),
                        close_price = COALESCE(EXCLUDED.close_price, nrl.odds.close_price),
                        last_price = COALESCE(EXCLUDED.last_price, nrl.odds.last_price),
                        updated_at = now()
                """),
                dict(
                    mid=o["match_id"], t=o["team"],
                    op=o["opening_price"], cp=o["close_price"], lp=o["last_price"],
                ),
            )
            affected += result.rowcount
    return affected


def _insert_team_ratings(engine: Engine, season: int, ratings: Dict[str, float]) -> int:
    with engine.begin() as conn:
        for team, rating in ratings.items():
            conn.execute(
                sql_text("""
                    INSERT INTO nrl.team_ratings (season, team, rating)
                    VALUES (:s, :t, :r)
                    ON CONFLICT (season, team) DO UPDATE SET rating = EXCLUDED.rating
                """),
                dict(s=season, t=team, r=round(rating, 2)),
            )
    return len(ratings)


def _insert_coach_profiles(engine: Engine, season: int) -> int:
    rng = random.Random(season * 13 + 5)
    with engine.begin() as conn:
        for team, style in COACH_STYLE_SCORES.items():
            conn.execute(
                sql_text("""
                    INSERT INTO nrl.coach_profile (season, team, style_score)
                    VALUES (:s, :t, :ss)
                    ON CONFLICT (season, team) DO UPDATE SET style_score = EXCLUDED.style_score
                """),
                dict(s=season, t=team, ss=round(style + rng.gauss(0, 0.05), 3)),
            )
    return len(COACH_STYLE_SCORES)


def _insert_injuries(engine: Engine, season: int, rng: random.Random) -> int:
    with engine.begin() as conn:
        for team in NRL_TEAMS:
            conn.execute(
                sql_text("""
                    INSERT INTO nrl.injuries_current (season, team, injury_count)
                    VALUES (:s, :t, :c)
                    ON CONFLICT (season, team) DO UPDATE SET injury_count = EXCLUDED.injury_count
                """),
                dict(s=season, t=team, c=rng.randint(0, 5)),
            )
    return len(NRL_TEAMS)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_table_counts(engine: Engine) -> Dict[str, int]:
    """Return row counts for every core NRL table."""
    tables = [
        "matches_raw", "odds", "team_ratings", "coach_profile",
        "injuries_current", "weather_daily", "model_prediction",
        "slips", "calibration_params", "model_registry",
    ]
    counts: Dict[str, int] = {}
    for t in tables:
        try:
            with engine.begin() as conn:
                row = conn.execute(sql_text(f"SELECT count(*) AS n FROM nrl.{t}")).mappings().first()
                counts[t] = int(row["n"]) if row else 0
        except Exception:
            counts[t] = -1  # table may not exist
    return counts


def seed_all(
    engine: Engine,
    historical_seasons: Optional[List[int]] = None,
    current_season: int = 2026,
) -> Dict[str, int]:
    """
    Seed all core tables with synthetic NRL data.

    historical_seasons get full 27-round seasons with scores.
    current_season gets full 27 rounds of fixtures (no scores) for deployment.
    """
    if historical_seasons is None:
        historical_seasons = [2022, 2023, 2024, 2025]

    totals: Dict[str, int] = {
        "matches": 0, "odds": 0, "team_ratings": 0,
        "coach_profiles": 0, "injuries": 0,
    }

    # --- Historical seasons (with scores) ---
    for season in historical_seasons:
        rng = random.Random(season * 31 + 7)
        ratings = _season_ratings(season, rng)
        fixtures = _generate_fixtures(season, num_rounds=27, rng=rng)
        _generate_scores(fixtures, ratings, rng)
        odds = _generate_odds(fixtures, ratings, rng)

        totals["matches"] += _insert_matches(engine, fixtures)
        totals["odds"] += _insert_odds(engine, odds)
        totals["team_ratings"] += _insert_team_ratings(engine, season, ratings)
        totals["coach_profiles"] += _insert_coach_profiles(engine, season)
        totals["injuries"] += _insert_injuries(engine, season, rng)
        logger.info("Seeded season %s: %s matches, %s odds", season, len(fixtures), len(odds))

    # --- Current season (fixtures only, no scores) ---
    rng = random.Random(current_season * 31 + 7)
    ratings = _season_ratings(current_season, rng)
    fixtures = _generate_fixtures(current_season, num_rounds=27, rng=rng)
    odds = _generate_odds(fixtures, ratings, rng)

    totals["matches"] += _insert_matches(engine, fixtures)
    totals["odds"] += _insert_odds(engine, odds)
    totals["team_ratings"] += _insert_team_ratings(engine, current_season, ratings)
    totals["coach_profiles"] += _insert_coach_profiles(engine, current_season)
    totals["injuries"] += _insert_injuries(engine, current_season, rng)
    logger.info("Seeded season %s (current): %s fixtures (no scores)", current_season, len(fixtures))

    logger.info("Seed complete: %s", totals)
    return totals

import os
from sqlalchemy import text
from .db import get_engine


def seed_player_ratings():
    engine = get_engine()

    players = [
        ("James Tedesco", "Sydney Roosters", 2215.0, 88.6, "19 Line Breaks", True, "Highest ceiling player"),
        ("Payne Haas", "Brisbane Broncos", 2064.0, 86.0, "3589 Run Metres", False, "Engine room dominant"),
        ("Nicho Hynes", "Cronulla-Sutherland Sharks", 2047.0, 75.8, "5969 Kick Metres", False, "Possession king"),
        ("Reece Walsh", "Brisbane Broncos", 1905.0, 90.7, "14 Tries, 42 LBAs", True, "High-ceiling volatility"),
        ("Scott Drinkwater", "North Queensland Cowboys", 1947.0, 81.1, "30 Try Assists", True, "Creative hub"),
        ("Nathan Cleary", "Penrith Panthers", 1865.0, 84.8, "79 Goals", False, "Playmaker king"),
        ("Addin Fonua-Blake", "Cronulla-Sutherland Sharks", 1743.0, 64.5, "3906 Run Metres", False, "Workhorse prop gain"),
        ("Dominic Young", "Newcastle Knights", 992.0, 52.0, "10 Tries, 18 LB", True, "Return to Newcastle"),
        ("Josh Addo-Carr", "Parramatta Eels", 1330.0, 54.0, "25 Line Breaks", True, "Speed king transfer"),
        ("Damien Cook", "St. George Illawarra Dragons", 1292.0, 54.0, "962 Tackles", False, "Defensive volume"),
        ("Lachlan Galvin", "Canterbury-Bankstown Bulldogs", 1375.0, 60.0, "16 Try Assists", False, "Creative half gain"),
        ("Jason Saab", "Manly Warringah Sea Eagles", 950.0, 47.0, "19 LB, 10 Tries", True, "Aerial + speed"),
        ("Tyrell Sloan", "St. George Illawarra Dragons", 1100.0, 46.0, "22 LB, 17 Tries", True, "High-variance speed"),
        ("Ronaldo Mulitalo", "Cronulla-Sutherland Sharks", 1150.0, 48.0, "22 LB + 116 TB", True, "Tackle-breaking wing"),
        ("Paul Alamoti", "Penrith Panthers", 1050.0, 52.0, "21 LB in 19 games", True, "Centre speed threat"),
    ]

    with engine.begin() as conn:
        for player, team, rating, avg_score, key_stat, is_speed, note in players:
            conn.execute(
                text(
                    """
                    INSERT INTO nrl.player_ratings
                    (season, player_name, team, rating, avg_score, key_stat, is_speed_player, note, last_updated)
                    VALUES (2026, :p, :t, :r, :a, :k, :speed, :note, NOW())
                    ON CONFLICT (season, player_name, team)
                    DO UPDATE SET
                        rating=EXCLUDED.rating,
                        avg_score=EXCLUDED.avg_score,
                        key_stat=EXCLUDED.key_stat,
                        is_speed_player=EXCLUDED.is_speed_player,
                        note=EXCLUDED.note,
                        last_updated=NOW()
                    """
                ),
                dict(p=player, t=team, r=rating, a=avg_score, k=key_stat, speed=is_speed, note=note),
            )

    print("✅ Seed complete.")


if __name__ == "__main__":
    if not os.getenv("DATABASE_URL"):
        raise SystemExit("DATABASE_URL must be set")
    seed_player_ratings()

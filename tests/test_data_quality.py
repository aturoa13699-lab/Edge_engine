from sqlalchemy import create_engine, text

from engine.data_quality import (
    DataQualityError,
    enforce_data_quality_gate,
    run_data_quality_gate,
)


def _seed_valid(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE matches_raw (
                  match_id text PRIMARY KEY,
                  season integer NOT NULL,
                  round_num integer NOT NULL,
                  match_date text,
                  venue text,
                  home_team text NOT NULL,
                  away_team text NOT NULL,
                  home_score integer,
                  away_score integer
                )
                """
            )
        )
        teams = [
            ("Brisbane Broncos", "Suncorp Stadium"),
            ("Canberra Raiders", "GIO Stadium"),
            ("Canterbury-Bankstown Bulldogs", "Accor Stadium"),
            ("Cronulla-Sutherland Sharks", "PointsBet Stadium"),
            ("Dolphins", "Suncorp Stadium"),
            ("Gold Coast Titans", "Cbus Super Stadium"),
            ("Manly Warringah Sea Eagles", "4 Pines Park"),
            ("Melbourne Storm", "AAMI Park"),
            ("Newcastle Knights", "McDonald Jones Stadium"),
            ("New Zealand Warriors", "Go Media Stadium"),
            ("North Queensland Cowboys", "Qld Country Bank Stadium"),
            ("Parramatta Eels", "CommBank Stadium"),
            ("Penrith Panthers", "BlueBet Stadium"),
            ("South Sydney Rabbitohs", "Accor Stadium"),
            ("St. George Illawarra Dragons", "WIN Stadium"),
            ("Sydney Roosters", "Allianz Stadium"),
        ]
        for round_num in (1, 2):
            for match_idx in range(8):
                home_team, venue = teams[match_idx]
                away_team, _ = teams[(match_idx + 8) % 16]
                conn.execute(
                    text(
                        """
                        INSERT INTO matches_raw (
                          match_id, season, round_num, match_date, venue,
                          home_team, away_team, home_score, away_score
                        ) VALUES (
                          :match_id, 2025, :round_num, '2025-03-01', :venue,
                          :home_team, :away_team, 20, 14
                        )
                        """
                    ),
                    {
                        "match_id": f"M{round_num}_{match_idx}",
                        "round_num": round_num,
                        "venue": venue,
                        "home_team": home_team,
                        "away_team": away_team,
                    },
                )


def test_data_quality_gate_passes_on_valid_data():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_valid(engine)

    report = run_data_quality_gate(engine, seasons=[2025])

    assert report.ok is True
    assert report.errors == []
    assert report.metrics["season_2025_matches"] == 16


def test_data_quality_gate_fails_closed_on_invalid_data():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_valid(engine)
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM matches_raw WHERE round_num = 2 AND match_id = 'M2_0'")
        )

    report = run_data_quality_gate(engine, seasons=[2025])

    assert report.ok is False
    assert any("expected 8 matches" in error for error in report.errors)


def test_enforce_data_quality_gate_raises():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_valid(engine)
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE matches_raw SET home_score = NULL WHERE match_id = 'M1_0'")
        )

    try:
        enforce_data_quality_gate(engine, seasons=[2025])
    except DataQualityError as exc:
        assert "Data quality gate failed" in str(exc)
    else:
        raise AssertionError("Expected DataQualityError")


def test_data_quality_gate_detects_home_equals_away():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_valid(engine)
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE matches_raw SET away_team = home_team WHERE match_id = 'M1_1'")
        )

    report = run_data_quality_gate(engine, seasons=[2025])

    assert report.ok is False
    assert any("home_team == away_team" in error for error in report.errors)


def test_data_quality_report_is_persisted_on_enforce():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_valid(engine)

    report = enforce_data_quality_gate(engine, seasons=[2025])

    assert report.ok is True
    with engine.begin() as conn:
        row = (
            conn.execute(text("SELECT count(*) AS n FROM data_quality_reports"))
            .mappings()
            .first()
        )
    assert row is not None
    assert int(row["n"]) == 1


def test_data_quality_gate_detects_duplicate_match_ids():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE matches_raw (
                  match_id text,
                  season integer NOT NULL,
                  round_num integer NOT NULL,
                  match_date text,
                  venue text,
                  home_team text NOT NULL,
                  away_team text NOT NULL,
                  home_score integer,
                  away_score integer
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO matches_raw
                (match_id, season, round_num, match_date, venue, home_team, away_team, home_score, away_score)
                VALUES
                ('dup_1', 2025, 1, '2025-03-01', 'Suncorp Stadium', 'Brisbane Broncos', 'Canberra Raiders', 20, 10),
                ('dup_1', 2025, 1, '2025-03-01', 'Suncorp Stadium', 'Brisbane Broncos', 'Canberra Raiders', 18, 12)
                """
            )
        )

    report = run_data_quality_gate(engine, seasons=[2025], expected_matches_per_round=2)

    assert report.ok is False
    assert any("duplicate match_id" in error for error in report.errors)

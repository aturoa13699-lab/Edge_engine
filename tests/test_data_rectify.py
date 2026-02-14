import json
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from engine.data_rectify import rectify_historical_partitions


def _seed_raw(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE nrl_matches_raw (
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
        conn.execute(
            text(
                """
                CREATE TABLE nrl_odds (
                  match_id text NOT NULL,
                  team text NOT NULL,
                  opening_price real,
                  close_price real,
                  last_price real,
                  steam_factor real,
                  PRIMARY KEY (match_id, team)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO nrl_matches_raw
                (match_id, season, round_num, match_date, venue, home_team, away_team, home_score, away_score)
                VALUES
                ('M1', 2025, 1, '2025-03-01', 'Suncorp Stadium', 'Brisbane Broncos', 'Canberra Raiders', 20, 12),
                ('M2', 2025, 1, '2025-03-02', 'Accor Stadium', 'Canterbury-Bankstown Bulldogs', 'Cronulla-Sutherland Sharks', 18, 14)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO nrl_odds (match_id, team, opening_price, close_price, last_price, steam_factor)
                VALUES
                ('M1', 'Brisbane Broncos', 1.9, 1.8, 1.82, 0.1),
                ('M1', 'Canberra Raiders', 2.0, 2.05, 2.01, -0.1),
                ('M2', 'Canterbury-Bankstown Bulldogs', 1.95, 1.97, 1.96, 0.02),
                ('M2', 'Cronulla-Sutherland Sharks', 1.92, 1.9, 1.91, -0.02)
                """
            )
        )


def test_rectify_copies_to_clean_and_records_provenance():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_raw(engine)

    artifacts_dir = Path("artifacts/test_inputs")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    canary_name = f"canary_{uuid.uuid4().hex}.json"
    canary_file = artifacts_dir / canary_name
    canary_file.write_text(
        json.dumps(
            [
                {
                    "match_id": "M1",
                    "home_team": "Brisbane Broncos",
                    "away_team": "Canberra Raiders",
                    "home_score": 20,
                    "away_score": 12,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = rectify_historical_partitions(
        engine,
        seasons=[2025],
        source_name="trusted_nrl_api",
        source_url_or_id="https://example.test/nrl",
        canary_path=f"test_inputs/{canary_name}",
    )

    assert result.copied_matches == 2
    assert result.copied_odds == 4
    assert result.provenance_rows == 2
    assert result.canary_checked == 1

    with engine.begin() as conn:
        rows = (
            conn.execute(text("SELECT count(*) AS n FROM nrl_clean_matches_raw"))
            .mappings()
            .first()
        )
        assert rows is not None
        assert int(rows["n"]) == 2

        prov = (
            conn.execute(
                text(
                    "SELECT source_name, source_url_or_id, checksum FROM nrl_clean_ingestion_provenance ORDER BY match_id"
                )
            )
            .mappings()
            .all()
        )
        assert len(prov) == 2
        assert prov[0]["source_name"] == "trusted_nrl_api"
        assert prov[0]["source_url_or_id"] == "https://example.test/nrl"
        assert prov[0]["checksum"]


def test_rectify_from_authoritative_payload():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_raw(engine)

    payload = {
        "matches": [
            {
                "match_id": "AUTH1",
                "season": 2025,
                "round_num": 1,
                "match_date": "2025-03-07",
                "venue": "Suncorp Stadium",
                "home_team": "Brisbane Broncos",
                "away_team": "Canberra Raiders",
                "home_score": 22,
                "away_score": 10,
            }
        ],
        "odds": [
            {
                "match_id": "AUTH1",
                "team": "Brisbane Broncos",
                "opening_price": 1.8,
                "close_price": 1.75,
                "last_price": 1.77,
                "steam_factor": 0.05,
            },
            {
                "match_id": "AUTH1",
                "team": "Canberra Raiders",
                "opening_price": 2.1,
                "close_price": 2.15,
                "last_price": 2.13,
                "steam_factor": -0.05,
            },
        ],
    }
    artifacts_dir = Path("artifacts/test_inputs")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    payload_name = f"authoritative_payload_{uuid.uuid4().hex}.json"
    payload_file = artifacts_dir / payload_name
    payload_file.write_text(json.dumps(payload), encoding="utf-8")

    result = rectify_historical_partitions(
        engine,
        seasons=[2025],
        source_name="official_feed",
        source_url_or_id="authoritative://feed",
        authoritative_payload_path=f"test_inputs/{payload_name}",
    )

    assert result.copied_matches == 1
    assert result.copied_odds == 2

    with engine.begin() as conn:
        row = (
            conn.execute(
                text(
                    "SELECT match_id, home_score, away_score FROM nrl_clean_matches_raw"
                )
            )
            .mappings()
            .first()
        )
        assert row is not None
        assert row["match_id"] == "AUTH1"
        assert int(row["home_score"]) == 22
        assert int(row["away_score"]) == 10


def test_rectify_rejects_disallowed_absolute_path():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_raw(engine)

    with pytest.raises(
        ValueError, match="absolute path must be under artifacts/ or data/"
    ):
        rectify_historical_partitions(
            engine,
            seasons=[2025],
            source_name="trusted_nrl_api",
            source_url_or_id="https://example.test/nrl",
            canary_path="/tmp/not_allowed.json",
        )


def test_rectify_accepts_artifacts_prefixed_relative_path():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_raw(engine)

    artifacts_dir = Path("artifacts/test_inputs")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    payload_name = f"authoritative_payload_{uuid.uuid4().hex}.json"
    payload_file = artifacts_dir / payload_name
    payload_file.write_text(json.dumps({"matches": [], "odds": []}), encoding="utf-8")

    # Explicit artifacts/ prefix should resolve safely without double-prefixing
    result = rectify_historical_partitions(
        engine,
        seasons=[2025],
        source_name="official_feed",
        source_url_or_id="authoritative://feed",
        authoritative_payload_path=f"artifacts/test_inputs/{payload_name}",
    )

    assert result.copied_matches == 2
    assert result.copied_odds == 4

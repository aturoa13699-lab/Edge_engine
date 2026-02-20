import json
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from engine.data_rectify import (
    AuthoritativePayloadError,
    rectify_historical_partitions,
    validate_authoritative_payload,
)


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


def _valid_payload():
    """Minimal valid authoritative payload matching the engine schema."""
    return {
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


def _write_payload(payload: dict) -> str:
    """Write payload to artifacts/test_inputs and return the relative path."""
    artifacts_dir = Path("artifacts/test_inputs")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    name = f"authoritative_payload_{uuid.uuid4().hex}.json"
    (artifacts_dir / name).write_text(json.dumps(payload), encoding="utf-8")
    return f"test_inputs/{name}"


# ── Existing behaviour (fallback to nrl schema, opt-in bypass) ──────────────


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
        allow_empty_authoritative=True,
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

    payload_path = _write_payload(_valid_payload())

    result = rectify_historical_partitions(
        engine,
        seasons=[2025],
        source_name="official_feed",
        source_url_or_id="authoritative://feed",
        authoritative_payload_path=payload_path,
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
            allow_empty_authoritative=True,
        )


# ── Fail-closed tests ──────────────────────────────────────────────────────


def test_rectify_fails_closed_when_no_payload_and_seasons_provided():
    """Default behaviour: seasons + no payload = deterministic error."""
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_raw(engine)

    with pytest.raises(
        AuthoritativePayloadError, match="required but was not provided"
    ):
        rectify_historical_partitions(
            engine,
            seasons=[2025],
            source_name="test",
            source_url_or_id="test://x",
        )


def test_rectify_allows_empty_with_explicit_bypass():
    """allow_empty_authoritative=True skips the requirement."""
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_raw(engine)

    result = rectify_historical_partitions(
        engine,
        seasons=[2025],
        source_name="test",
        source_url_or_id="test://x",
        allow_empty_authoritative=True,
    )
    assert result.copied_matches == 2


def test_rectify_empty_seasons_does_not_require_payload():
    """No seasons = nothing to rectify, so no payload needed."""
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_raw(engine)

    result = rectify_historical_partitions(
        engine,
        seasons=[],
        source_name="test",
        source_url_or_id="test://x",
    )
    assert result.copied_matches == 0


# ── Schema validation tests ────────────────────────────────────────────────


def test_validate_payload_accepts_valid():
    validate_authoritative_payload(_valid_payload())


def test_validate_payload_rejects_missing_opening_price():
    payload = _valid_payload()
    del payload["odds"][0]["opening_price"]
    with pytest.raises(AuthoritativePayloadError, match="opening_price"):
        validate_authoritative_payload(payload)


def test_validate_payload_rejects_empty_matches():
    payload = _valid_payload()
    payload["matches"] = []
    with pytest.raises(AuthoritativePayloadError):
        validate_authoritative_payload(payload)


def test_validate_payload_rejects_empty_odds():
    payload = _valid_payload()
    payload["odds"] = []
    with pytest.raises(AuthoritativePayloadError):
        validate_authoritative_payload(payload)


def test_validate_payload_rejects_orphan_odds():
    payload = _valid_payload()
    payload["odds"].append(
        {
            "match_id": "GHOST_MATCH",
            "team": "Nobody",
            "opening_price": 2.0,
        }
    )
    with pytest.raises(AuthoritativePayloadError, match="undeclared match_ids"):
        validate_authoritative_payload(payload)


def test_validate_payload_rejects_opening_price_lte_one():
    payload = _valid_payload()
    payload["odds"][0]["opening_price"] = 1.0
    with pytest.raises(AuthoritativePayloadError):
        validate_authoritative_payload(payload)


def test_validate_payload_rejects_extra_fields():
    payload = _valid_payload()
    payload["matches"][0]["surprise_field"] = "oops"
    with pytest.raises(AuthoritativePayloadError):
        validate_authoritative_payload(payload)


def test_rectify_rejects_invalid_payload_on_disk():
    """A payload on disk that fails schema validation should raise."""
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _seed_raw(engine)

    bad_payload = _valid_payload()
    del bad_payload["odds"][0]["opening_price"]
    payload_path = _write_payload(bad_payload)

    with pytest.raises(AuthoritativePayloadError, match="opening_price"):
        rectify_historical_partitions(
            engine,
            seasons=[2025],
            source_name="test",
            source_url_or_id="test://x",
            authoritative_payload_path=payload_path,
        )

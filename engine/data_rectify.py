from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


from sqlalchemy import text
from sqlalchemy.engine import Engine

ALLOWED_PATH_BASES = (Path("artifacts").resolve(), Path("data").resolve())


@dataclass
class RectifySummary:
    seasons: list[int]
    copied_matches: int
    copied_odds: int
    provenance_rows: int
    canary_checked: int

    def to_dict(self) -> dict[str, int | list[int]]:
        return {
            "seasons": self.seasons,
            "copied_matches": self.copied_matches,
            "copied_odds": self.copied_odds,
            "provenance_rows": self.provenance_rows,
            "canary_checked": self.canary_checked,
        }


def _qname(engine: Engine, schema: str, table: str) -> str:
    if engine.dialect.name.startswith("postgres"):
        return f"{schema}.{table}"
    return f"{schema}_{table}"


def _ensure_clean_tables(engine: Engine) -> None:
    if engine.dialect.name.startswith("postgres"):
        ddl = [
            "CREATE SCHEMA IF NOT EXISTS nrl_clean",
            """
            CREATE TABLE IF NOT EXISTS nrl_clean.matches_raw (
              match_id text PRIMARY KEY,
              season integer NOT NULL,
              round_num integer NOT NULL,
              match_date date,
              venue text,
              home_team text NOT NULL,
              away_team text NOT NULL,
              home_score integer,
              away_score integer,
              created_at timestamptz DEFAULT now(),
              updated_at timestamptz DEFAULT now()
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS nrl_clean.odds (
              match_id text NOT NULL,
              team text NOT NULL,
              opening_price numeric(7,3),
              close_price numeric(7,3),
              last_price numeric(7,3),
              steam_factor numeric(7,4),
              updated_at timestamptz DEFAULT now(),
              PRIMARY KEY (match_id, team)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS nrl_clean.ingestion_provenance (
              id bigserial PRIMARY KEY,
              season integer NOT NULL,
              match_id text NOT NULL,
              source_name text NOT NULL,
              source_url_or_id text NOT NULL,
              fetched_at timestamptz NOT NULL,
              checksum text NOT NULL,
              created_at timestamptz DEFAULT now()
            )
            """,
            "CREATE INDEX IF NOT EXISTS ix_nrl_clean_prov_season_match ON nrl_clean.ingestion_provenance(season, match_id)",
        ]
    else:
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS nrl_clean_matches_raw (
              match_id text PRIMARY KEY,
              season integer NOT NULL,
              round_num integer NOT NULL,
              match_date text,
              venue text,
              home_team text NOT NULL,
              away_team text NOT NULL,
              home_score integer,
              away_score integer,
              created_at text,
              updated_at text
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS nrl_clean_odds (
              match_id text NOT NULL,
              team text NOT NULL,
              opening_price real,
              close_price real,
              last_price real,
              steam_factor real,
              updated_at text,
              PRIMARY KEY (match_id, team)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS nrl_clean_ingestion_provenance (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              season integer NOT NULL,
              match_id text NOT NULL,
              source_name text NOT NULL,
              source_url_or_id text NOT NULL,
              fetched_at text NOT NULL,
              checksum text NOT NULL,
              created_at text
            )
            """,
        ]

    with engine.begin() as conn:
        for stmt in ddl:
            conn.execute(text(stmt))


def _resolve_allowed_path(path: str | None) -> Path | None:
    if not path:
        return None
    raw = path.strip()
    if "://" in raw:
        raise ValueError("path must be a local filesystem path")

    requested = Path(raw)

    def _is_within(base: Path, candidate: Path) -> bool:
        return candidate == base or base in candidate.parents

    if requested.is_absolute():
        resolved = requested.resolve()
        for base in ALLOWED_PATH_BASES:
            if _is_within(base, resolved):
                return resolved
        raise ValueError("absolute path must be under artifacts/ or data/")

    for base in ALLOWED_PATH_BASES:
        candidate = (base / requested).resolve()
        if _is_within(base, candidate):
            return candidate

    raise ValueError("path must be under artifacts/ or data/")


def _season_checksum(row: dict) -> str:
    payload = (
        f"{row['match_id']}:{row['season']}:{row['round_num']}:{row['home_team']}:"
        f"{row['away_team']}:{row['home_score']}:{row['away_score']}"
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _load_authoritative_payload(path: str | None) -> tuple[list[dict], list[dict]]:
    if not path:
        return [], []
    safe_path = _resolve_allowed_path(path)
    if safe_path is None:
        return [], []
    payload = json.loads(safe_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("authoritative payload must be a JSON object")
    matches = payload.get("matches", [])
    odds = payload.get("odds", [])
    if not isinstance(matches, list) or not isinstance(odds, list):
        raise ValueError("authoritative payload requires list fields: matches and odds")
    return matches, odds


def _load_authoritative_sample(path: str | None) -> list[dict]:
    if not path:
        return []
    safe_path = _resolve_allowed_path(path)
    if safe_path is None:
        return []
    data = json.loads(safe_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("authoritative sample must be a JSON list")
    return data


def rectify_historical_partitions(
    engine: Engine,
    seasons: list[int],
    *,
    source_name: str,
    source_url_or_id: str,
    canary_path: str | None = None,
    canary_sample_size: int = 25,
    authoritative_payload_path: str | None = None,
) -> RectifySummary:
    _ensure_clean_tables(engine)
    source_matches = _qname(engine, "nrl", "matches_raw")
    source_odds = _qname(engine, "nrl", "odds")
    target_matches = _qname(engine, "nrl_clean", "matches_raw")
    target_odds = _qname(engine, "nrl_clean", "odds")
    target_prov = _qname(engine, "nrl_clean", "ingestion_provenance")

    copied_matches = 0
    copied_odds = 0
    provenance_rows = 0

    fetched_at = datetime.now(timezone.utc).isoformat()
    authoritative_matches, authoritative_odds = _load_authoritative_payload(
        authoritative_payload_path
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                f"DELETE FROM {target_prov} WHERE season IN ({','.join(str(int(s)) for s in seasons)})"
            )
        )
        conn.execute(
            text(
                f"DELETE FROM {target_odds} WHERE match_id IN (SELECT match_id FROM {target_matches} WHERE season IN ({','.join(str(int(s)) for s in seasons)}))"
            )
        )
        conn.execute(
            text(
                f"DELETE FROM {target_matches} WHERE season IN ({','.join(str(int(s)) for s in seasons)})"
            )
        )

        if authoritative_matches:
            rows = [
                r for r in authoritative_matches if int(r.get("season", 0)) in seasons
            ]
        else:
            rows = [
                dict(r)
                for r in (
                    conn.execute(
                        text(
                            f"""
                            SELECT match_id, season, round_num, match_date, venue,
                                   home_team, away_team, home_score, away_score
                            FROM {source_matches}
                            WHERE season IN ({",".join(str(int(s)) for s in seasons)})
                            ORDER BY season, round_num, match_id
                            """
                        )
                    )
                    .mappings()
                    .all()
                )
            ]

        for row in rows:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {target_matches}
                    (match_id, season, round_num, match_date, venue, home_team, away_team, home_score, away_score)
                    VALUES (:match_id, :season, :round_num, :match_date, :venue, :home_team, :away_team, :home_score, :away_score)
                    """
                ),
                dict(row),
            )
            copied_matches += 1

            conn.execute(
                text(
                    f"""
                    INSERT INTO {target_prov}
                    (season, match_id, source_name, source_url_or_id, fetched_at, checksum)
                    VALUES (:season, :match_id, :source_name, :source_url_or_id, :fetched_at, :checksum)
                    """
                ),
                {
                    "season": row["season"],
                    "match_id": row["match_id"],
                    "source_name": source_name,
                    "source_url_or_id": source_url_or_id,
                    "fetched_at": fetched_at,
                    "checksum": _season_checksum(dict(row)),
                },
            )
            provenance_rows += 1

        if authoritative_odds:
            match_ids = {r["match_id"] for r in rows}
            odds_rows = [
                o for o in authoritative_odds if o.get("match_id") in match_ids
            ]
        else:
            odds_rows = [
                dict(r)
                for r in (
                    conn.execute(
                        text(
                            f"""
                            SELECT o.match_id, o.team, o.opening_price, o.close_price, o.last_price, o.steam_factor
                            FROM {source_odds} o
                            JOIN {target_matches} m ON m.match_id = o.match_id
                            WHERE m.season IN ({",".join(str(int(s)) for s in seasons)})
                            """
                        )
                    )
                    .mappings()
                    .all()
                )
            ]

        for row in odds_rows:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {target_odds}
                    (match_id, team, opening_price, close_price, last_price, steam_factor)
                    VALUES (:match_id, :team, :opening_price, :close_price, :last_price, :steam_factor)
                    """
                ),
                dict(row),
            )
            copied_odds += 1

    authoritative = _load_authoritative_sample(canary_path)
    canary_checked = verify_authoritative_canary(
        engine,
        seasons=seasons,
        authoritative_sample=authoritative,
        sample_size=canary_sample_size,
    )

    return RectifySummary(
        seasons=seasons,
        copied_matches=copied_matches,
        copied_odds=copied_odds,
        provenance_rows=provenance_rows,
        canary_checked=canary_checked,
    )


def verify_authoritative_canary(
    engine: Engine,
    *,
    seasons: list[int],
    authoritative_sample: list[dict],
    sample_size: int = 25,
) -> int:
    target_matches = _qname(engine, "nrl_clean", "matches_raw")

    if authoritative_sample:
        check_rows = authoritative_sample
    else:
        with engine.begin() as conn:
            rows = (
                conn.execute(
                    text(
                        f"""
                        SELECT match_id, home_team, away_team, home_score, away_score
                        FROM {target_matches}
                        WHERE season IN ({",".join(str(int(s)) for s in seasons)})
                        ORDER BY match_id
                        """
                    )
                )
                .mappings()
                .all()
            )
        rng = random.Random(20260213)
        check_rows = [dict(r) for r in rows]
        rng.shuffle(check_rows)
        check_rows = check_rows[:sample_size]

    with engine.begin() as conn:
        for row in check_rows:
            actual = (
                conn.execute(
                    text(
                        f"""
                        SELECT home_team, away_team, home_score, away_score
                        FROM {target_matches}
                        WHERE match_id = :match_id
                        """
                    ),
                    {"match_id": row["match_id"]},
                )
                .mappings()
                .first()
            )
            if not actual:
                raise ValueError(f"canary missing match_id={row['match_id']}")
            for col in ("home_team", "away_team", "home_score", "away_score"):
                if col in row and row[col] != actual[col]:
                    raise ValueError(
                        f"canary mismatch match_id={row['match_id']} col={col} expected={row[col]} actual={actual[col]}"
                    )

    return len(check_rows)

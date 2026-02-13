from sqlalchemy import create_engine, text

from engine.schema_parity import run_truth_schema_parity_smoke


def _create_required_relations(engine):
    stmts = [
        "CREATE TABLE matches_raw (match_id text, season integer)",
        "CREATE TABLE odds (match_id text, team text)",
        "CREATE TABLE coach_profile (season integer, team text)",
        "CREATE TABLE injuries_current (season integer, team text)",
        "CREATE TABLE team_ratings (season integer, team text)",
        "CREATE TABLE weather_daily (match_date text, venue text)",
        "CREATE VIEW team_rest_v AS SELECT 'm1' AS match_id, 2025 AS season, 'A' AS team, 7 AS rest_days",
        "CREATE VIEW team_form_v AS SELECT 'm1' AS match_id, 2025 AS season, 'A' AS team, 0.5 AS win_pct_last5",
    ]
    with engine.begin() as conn:
        for stmt in stmts:
            conn.execute(text(stmt))


def test_schema_parity_smoke_passes_with_required_relations():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _create_required_relations(engine)

    report = run_truth_schema_parity_smoke(engine)

    assert report.ok is True
    assert report.missing_objects == []


def test_schema_parity_smoke_fails_on_missing_relation():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE matches_raw (match_id text, season integer)"))

    report = run_truth_schema_parity_smoke(engine)

    assert report.ok is False
    assert "odds" in report.missing_objects

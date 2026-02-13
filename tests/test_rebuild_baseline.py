from sqlalchemy import create_engine, text

from engine.rebuild_baseline import run_rebuild_clean_baseline


class _DummyBacktest:
    def summary(self):
        return {"total_bets": 3, "roi_pct": 4.2, "total_pnl": 12.0}


def test_rebuild_clean_baseline_records_manifest(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    # minimal parity tables for sqlite parity checks
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE matches_raw (match_id text, season integer)"))
        conn.execute(text("CREATE TABLE odds (match_id text, team text)"))
        conn.execute(
            text(
                "CREATE VIEW team_rest_v AS SELECT 'm' AS match_id, 2025 AS season, 'A' AS team, 7 AS rest_days"
            )
        )
        conn.execute(
            text(
                "CREATE VIEW team_form_v AS SELECT 'm' AS match_id, 2025 AS season, 'A' AS team, 0.5 AS win_pct_last5"
            )
        )
        conn.execute(text("CREATE TABLE coach_profile (season integer, team text)"))
        conn.execute(text("CREATE TABLE injuries_current (season integer, team text)"))
        conn.execute(text("CREATE TABLE team_ratings (season integer, team text)"))
        conn.execute(text("CREATE TABLE weather_daily (match_date text, venue text)"))
        conn.execute(
            text(
                "CREATE TABLE slips (portfolio_id text PRIMARY KEY, season integer, round_num integer, slip_json text, status text, created_at text)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE model_prediction (id integer, season integer, created_at text)"
            )
        )
        conn.execute(text("CREATE TABLE model_registry (model_key text)"))
        conn.execute(text("CREATE TABLE calibration_params (season integer)"))
        conn.execute(text("CREATE TABLE data_quality_reports (id integer)"))

    monkeypatch.setattr(
        "engine.rebuild_baseline.train_model", lambda e, seasons: {"ok": True}
    )
    monkeypatch.setattr(
        "engine.rebuild_baseline.fit_beta_calibrator", lambda e, s: {"a": 1.0, "b": 1.0}
    )
    monkeypatch.setattr(
        "engine.rebuild_baseline.backfill_predictions",
        lambda e, season: {"backfilled": 10},
    )
    monkeypatch.setattr(
        "engine.rebuild_baseline.label_outcomes", lambda e, season: {"labelled": 8}
    )
    monkeypatch.setattr(
        "engine.rebuild_baseline.run_backtest", lambda e, season: _DummyBacktest()
    )

    result = run_rebuild_clean_baseline(
        engine,
        seasons=[2022, 2023, 2024, 2025],
        calibration_season=2025,
        backtest_season=2025,
    )

    assert result.manifest_id is not None
    assert result.backfilled == 10
    assert result.labelled == 8

    with engine.begin() as conn:
        row = (
            conn.execute(text("SELECT count(*) AS n FROM run_manifest"))
            .mappings()
            .first()
        )
    assert row is not None
    assert int(row["n"]) == 1

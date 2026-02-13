import os

import pytest


def test_pg_lifecycle_smoke(monkeypatch):
    db_url = os.getenv("DATABASE_URL", "")
    integration_required = os.getenv("INTEGRATION_TEST", "0") == "1"

    if "postgresql" not in db_url:
        if integration_required:
            pytest.fail(
                "INTEGRATION_TEST=1 requires DATABASE_URL to point to PostgreSQL"
            )
        pytest.skip("DATABASE_URL is not configured for PostgreSQL")

    from engine.db import get_engine
    from engine.run import (
        cmd_backfill,
        cmd_backtest,
        cmd_deploy,
        cmd_fit_calibration,
        cmd_init,
        cmd_label_outcomes,
        cmd_seed,
        cmd_train,
    )

    monkeypatch.setenv("QUALITY_GATE_SEASONS", "2022,2023,2024,2025")
    engine = get_engine()

    cmd_init(engine)
    cmd_seed(engine, season=2026)
    cmd_train(engine, seasons=[2022, 2023, 2024, 2025])
    cmd_fit_calibration(engine, season=2025)
    cmd_deploy(engine, season=2026, round_num=1, dry_run=True)
    cmd_backfill(engine, season=2025, rounds=[1])
    cmd_label_outcomes(engine, season=2025)
    result = cmd_backtest(engine, season=2025, rounds=[1], bankroll=1000.0)

    assert result is not None

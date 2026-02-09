from sqlalchemy import text
from app.calibration import fit_beta_calibrator, load_latest_calibrator, apply_calibration


def test_apply_calibration_identity_when_missing():
    assert apply_calibration(0.42, None) == 0.42


def test_fit_and_load_sqlite(sqlite_engine):
    # Seed synthetic prediction outcomes
    with sqlite_engine.begin() as conn:
        for i in range(120):
            p = 0.7 if i % 2 == 0 else 0.3
            y = 1 if i % 2 == 0 else 0
            conn.execute(
                text("INSERT INTO model_prediction (season, p_fair, outcome_known, outcome_home_win) VALUES (2026, :p, 1, :y)"),
                dict(p=p, y=y),
            )

    params = fit_beta_calibrator(sqlite_engine, 2026, min_samples=80)
    assert params is not None
    loaded = load_latest_calibrator(sqlite_engine, 2026)
    assert loaded is not None
    out = apply_calibration(0.6, loaded)
    assert 0.0 <= out <= 1.0

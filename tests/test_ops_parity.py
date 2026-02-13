from sqlalchemy import create_engine, text

from engine.ops_parity import run_ops_schema_parity_smoke


def _create_ops_tables(engine):
    stmts = [
        "CREATE TABLE slips (portfolio_id text PRIMARY KEY, season integer, round_num integer, slip_json text, status text)",
        "CREATE TABLE model_prediction (id integer)",
        "CREATE TABLE model_registry (model_key text)",
        "CREATE TABLE calibration_params (season integer)",
        "CREATE TABLE data_quality_reports (id integer)",
    ]
    with engine.begin() as conn:
        for stmt in stmts:
            conn.execute(text(stmt))


def test_ops_parity_smoke_passes_with_required_relations():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _create_ops_tables(engine)

    report = run_ops_schema_parity_smoke(engine)

    assert report.ok is True
    assert report.missing_objects == []
    assert report.write_roundtrip_ok is True


def test_ops_parity_smoke_fails_with_missing_relation():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE slips (portfolio_id text PRIMARY KEY)"))

    report = run_ops_schema_parity_smoke(engine)

    assert report.ok is False
    assert "model_prediction" in report.missing_objects

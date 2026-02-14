from sqlalchemy import create_engine

from engine.schema_router import ops_table, truth_table


def test_schema_router_import_contract() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    assert ops_table(engine, "slips") == "slips"
    assert truth_table(engine, "matches_raw") == "matches_raw"

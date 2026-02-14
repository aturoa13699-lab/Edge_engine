#!/usr/bin/env bash
set -euo pipefail

python -m engine.run --help
python -m compileall -q engine
python -c "from sqlalchemy import create_engine; from engine.schema_router import ops_table, truth_table; e=create_engine('sqlite://'); print('OK', ops_table(e,'slips'), truth_table(e,'matches_raw'))"
mypy .
pytest -q
pytest -q tests/test_integration_pg.py
INTEGRATION_TEST=1 pytest -q tests/test_integration_pg.py
ruff format . --check
ruff check .

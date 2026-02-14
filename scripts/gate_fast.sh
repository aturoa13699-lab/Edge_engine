#!/usr/bin/env bash
set -euo pipefail

python -m engine.run --help
python -m compileall -q engine
python -c "from sqlalchemy import create_engine; from engine.schema_router import ops_table, truth_table; e=create_engine('sqlite://'); print('OK', ops_table(e,'slips'), truth_table(e,'matches_raw'))"
mypy .
pytest -q
ruff format . --check
ruff check .

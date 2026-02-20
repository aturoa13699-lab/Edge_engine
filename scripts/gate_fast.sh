#!/usr/bin/env bash
set -euo pipefail

python -m pip check
python -m engine.run --help
python -m compileall -q engine tests streamlit_app
python - <<'PY'
from sqlalchemy import create_engine
from engine.schema_router import ops_table, truth_table

e = create_engine("sqlite://")
ops_table(e, "slips")
truth_table(e, "matches_raw")
print("import-smoke-ok")
PY
python -m pytest -q
ruff format . --check
ruff check .

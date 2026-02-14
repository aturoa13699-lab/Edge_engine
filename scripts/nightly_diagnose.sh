#!/usr/bin/env bash
set -u

ARTIFACT_DIR="${1:-artifacts/nightly}"
mkdir -p "$ARTIFACT_DIR"

STATUS=0
SUMMARY_FILE="$ARTIFACT_DIR/summary.json"

run_step() {
  local name="$1"
  local cmd="$2"
  local logfile="$ARTIFACT_DIR/${name}.log"

  echo "[nightly] running: $name"
  if bash -lc "$cmd" >"$logfile" 2>&1; then
    echo "[nightly] PASS: $name"
    echo "\"$name\":\"pass\"" >>"$ARTIFACT_DIR/.status.tmp"
  else
    local rc=$?
    STATUS=1
    echo "[nightly] FAIL: $name (rc=$rc)"
    echo "\"$name\":\"fail\"" >>"$ARTIFACT_DIR/.status.tmp"
  fi
}

: >"$ARTIFACT_DIR/.status.tmp"
run_step cli_help "python -m engine.run --help"
run_step compileall "python -m compileall -q engine"
run_step import_contract "python -c \"from sqlalchemy import create_engine; from engine.schema_router import ops_table, truth_table; e=create_engine('sqlite://'); print('OK', ops_table(e,'slips'), truth_table(e,'matches_raw'))\""
run_step mypy "mypy ."
run_step pytest_unit "pytest -q"
run_step pytest_integration "pytest -q tests/test_integration_pg.py"
run_step pytest_integration_strict "INTEGRATION_TEST=1 pytest -q tests/test_integration_pg.py"
run_step ruff_format_check "ruff format . --check"
run_step ruff_check "ruff check ."

python - <<'PY' "$ARTIFACT_DIR/.status.tmp" "$SUMMARY_FILE"
import json
import sys
from pathlib import Path

status_lines = [line.strip() for line in Path(sys.argv[1]).read_text().splitlines() if line.strip()]
status = {}
for item in status_lines:
    k, v = item.split(':', 1)
    status[k.strip('"')] = v.strip('"')

failed = sorted([k for k, v in status.items() if v == 'fail'])
summary = {
    'overall': 'fail' if failed else 'pass',
    'failed_steps': failed,
    'steps': status,
}
Path(sys.argv[2]).write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary))
PY

rm -f "$ARTIFACT_DIR/.status.tmp"
exit "$STATUS"

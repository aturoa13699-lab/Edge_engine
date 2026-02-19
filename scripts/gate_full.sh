#!/usr/bin/env bash
set -euo pipefail

python -m pip check
bash scripts/gate_fast.sh
mypy .
pytest -q tests/test_integration_pg.py
ruff format . --check
ruff check .

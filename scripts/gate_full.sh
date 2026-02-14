#!/usr/bin/env bash
set -euo pipefail

bash scripts/gate_fast.sh
mypy .
pytest -q tests/test_integration_pg.py

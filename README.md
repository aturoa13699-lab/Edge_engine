# NRL Edge Engine v1.1 — Goldmaster CML

A hardened NRL quant pipeline with:
- ML (XGBoost) + heuristic blend
- Beta calibration (DB-versioned)
- Risk controls (fractional Kelly + guardrails)
- Discord slip cards (PNG attachments)
- PDF audit reporting (styled cards + reliability plot)

## Quick Start

```bash
pip install -r requirements.txt

# 1) Set env (copy .env.example to .env and edit)
# DATABASE_URL=postgresql://user:pass@localhost:5432/nrl_edge

# 2) Apply schema (no `psql` required)
python -m engine.run init

# If you *do* have psql installed, this also works:
# psql "$DATABASE_URL" -f app/sql/schema_pg.sql

# 3) Seed player ratings (optional)
python -m engine.seed_player_ratings

# 4) Train
python -m engine.run train --seasons 2022,2023,2024,2025

# 5) Daily run (scrape + deploy + (optional) notify)
python -m engine.run daily --season 2026 --round 1

# 6) Dry run (still persists artifacts as status=dry_run; no notify unless DRY_NOTIFY=1)
python -m engine.run daily --season 2026 --round 1 --dry-run

Commands
	•	python -m engine.run init — apply schema
	•	python -m engine.run scrapers --season 2026 — scrape-only
	•	python -m engine.run deploy --season 2026 --round 1 — deploy-only
	•	python -m engine.run daily --season 2026 --round 1 — scrapers + deploy (+ notify)
	•	python -m engine.run train --seasons 2022,2023,2024,2025 — train + registry update
	•	python -m engine.run report --season 2026 --round 1 — generate PDF audit report
	•	python -m engine.run fit-calibration --season 2026 — fit beta calibrator (needs resolved outcomes)
	•	python -m engine.run schema-parity-smoke — fail-closed truth schema parity check
	•	python -m engine.run ops-parity-smoke — fail-closed ops schema parity check
	•	python -m engine.run rebuild-clean-baseline --seasons 2022,2023,2024,2025 --calibration-season 2025 --backtest-season 2025 — run first truthful rebuild pipeline

CI
	•	ruff check .
	•	pytest -q


## Schema policy (truth vs ops)

- **Truth schema** (`NRL_SCHEMA`, default `nrl_clean`): source-of-truth reads for fixtures/odds/features and data-quality gates.
- **Ops schema** (`NRL_OPS_SCHEMA`, default `nrl`): operational artifacts and outputs (`model_prediction`, `slips`, `calibration_params`, `model_registry`, `data_quality_reports`).
- Legacy behavior can be re-enabled explicitly by setting `NRL_SCHEMA=nrl`.


## Run Tests + Lint (End-of-TODO Merge Gate — MUST RUN LAST)

Rule: Merge gate is only **PASS** if the final two commands are `ruff format . --check` then `ruff check .` and both succeed.

1. Install
	•	python -m pip install -r requirements.txt
	•	python -m pip install -r requirements-dev.txt

2. Sanity
	•	python -m engine.run --help

3. Syntax / import tripwires (fail fast)
	•	python -m compileall -q engine
	•	python -c "from sqlalchemy import create_engine; from engine.schema_router import ops_table, truth_table; e=create_engine('sqlite://'); print('OK', ops_table(e, 'slips'), truth_table(e, 'matches_raw'))"

4. Typecheck
	•	mypy .

5. Unit tests
	•	pytest -q

6. Integration
	•	pytest -q tests/test_integration_pg.py
	•	CI: `INTEGRATION_TEST=1 pytest -q tests/test_integration_pg.py` (Postgres provisioned runner)
	•	Local: `INTEGRATION_TEST=1` must fail when `DATABASE_URL` is not PostgreSQL (fail-closed)

7. FINAL LINT END-CAP (RUN LAST, NO EXCEPTIONS)
	•	ruff format . --check
	•	ruff check .

Done when: the last output is a successful `ruff check .` run.

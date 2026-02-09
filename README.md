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
python -m app.run init

# If you *do* have psql installed, this also works:
# psql "$DATABASE_URL" -f app/sql/schema_pg.sql

# 3) Seed player ratings (optional)
python -m app.seed_player_ratings

# 4) Train
python -m app.run train --seasons 2022,2023,2024,2025

# 5) Daily run (scrape + deploy + (optional) notify)
python -m app.run daily --season 2026 --round 1

# 6) Dry run (still persists artifacts as status=dry_run; no notify unless DRY_NOTIFY=1)
python -m app.run daily --season 2026 --round 1 --dry-run

Commands
	•	python -m app.run init — apply schema
	•	python -m app.run scrapers --season 2026 — scrape-only
	•	python -m app.run deploy --season 2026 --round 1 — deploy-only
	•	python -m app.run daily --season 2026 --round 1 — scrapers + deploy (+ notify)
	•	python -m app.run train --seasons 2022,2023,2024,2025 — train + registry update
	•	python -m app.run report --season 2026 --round 1 — generate PDF audit report
	•	python -m app.run fit-calibration --season 2026 — fit beta calibrator (needs resolved outcomes)

CI
	•	ruff check .
	•	pytest -q

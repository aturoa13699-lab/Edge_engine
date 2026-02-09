# NRL Edge Engine v1.1 – Goldmaster CML

**Self-improving quantitative betting system for NRL.**  
Includes drift-triggered retraining, champion promotion, calibration, Discord/PDF/Streamlit outputs.

## Quick Start

```bash
pip install -r requirements.txt
playwright install chromium

# Database schema
psql "$DATABASE_URL" -f app/sql/schema_pg.sql

> Note: `.env.example` uses a plain `postgresql://...` URL so `psql "$DATABASE_URL"` works.
> The app upgrades it internally for SQLAlchemy (`postgresql+psycopg`).

# Seed intelligence
python -m app.seed_player_ratings

# Full pipeline
python -m app.run full

Commands
	•	python -m app.run full → schema + scrapers + train + calibrate + deploy + report
	•	python -m app.run daily → scrapers + deploy + notify + report
	•	python -m app.run train → train and promote champion
	•	python -m app.run scrapers → weather + referee scrape
	•	python -m app.run report → weekly PDF audit
	•	python -m app.run calibrate → fit beta calibrator for season

Dry-run semantics

Set:
	•	DRY_RUN=1 → predictions + slips still persist (status=dry_run) for HUD/PDF visibility
	•	DRY_NOTIFY=1 → still posts Discord during dry-run

Production

Railway cron jobs run scrapers + retrain + weekly reports.
Engine tracks:
	•	model versions and champion artifacts
	•	drift metrics (Brier/LogLoss/PSI)
	•	CLV tracking where available
	•	calibrated probabilities for better sizing

Built with discipline.

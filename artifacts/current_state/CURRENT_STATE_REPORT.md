# CURRENT_STATE_REPORT

## A) System Topology

- **Railway (repo-defined):** Dockerfile service starts Streamlit HUD; runs schema init before app startup (fail-closed).
- **GitHub Actions:**
  - `ci.yml` runs fast gate
  - `integration.yml` runs full gate with Postgres service
  - `scrapers.yml` runs scheduled daily command
  - `nightly-self-check.yml` runs gate + parity + data-quality
- **Local:** direct CLI (`python -m engine.run ...`) for init/seed/scrapers/deploy/backfill.

## B) Data Flow

- Scrape path:
  - weather/referee scrapers -> `nrl.weather_daily` / `nrl.referee_tendencies`
- Seed path:
  - `seed_all` -> `nrl.matches_raw`, `nrl.odds`, `nrl.team_ratings`, `nrl.coach_profile`, `nrl.injuries_current`
  - `rectify_historical_partitions` -> `nrl_clean.matches_raw`, `nrl_clean.odds`, `nrl_clean.ingestion_provenance`
- Model/deploy path:
  - features from truth tables/views + ops refs -> prediction in `nrl.model_prediction` and slips in `nrl.slips`
- Reporting/notify path:
  - slip rows -> Discord cards (`notify_slips`) and PDF reporting command.

## C) Seeding Status Diagnosis

Diagnosis: **most likely "not triggered in Railway runtime"**.

Evidence:
1. Railway runtime command in Dockerfile does not call `seed` or `daily`.
2. Scraper/daily scheduling is present in GitHub Actions, not Railway config.
3. Seeding command works when executed manually against Postgres (see seed repro log).

Secondary risk (mitigated in code):
- Docker init failure now fails startup instead of being swallowed.

## D) Actionable Fixes

Implemented in this PR:
1. Added scraper observability events + run ledger table (`nrl.scraper_runs`).
2. Added scraper dry-run mode (`SCRAPER_DRY_RUN`) for safe behavior proofing.
3. Added `scraper-status` CLI command to inspect latest scraper outcomes.

Recommended operational follow-ups (dashboard/config):
1. Add Railway cron or scheduled trigger for desired commands (`seed`, `daily`, or both).
2. Ensure service has `DATABASE_URL` (or supported alias) set to reachable Postgres URL with required SSL settings.
3. Run `python -m engine.run doctor` in deploy checks and on-call playbooks.

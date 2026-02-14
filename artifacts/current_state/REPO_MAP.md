# REPO_MAP

## What runs where

```text
Local CLI / CI / Railway

python -m engine.run <command>
 ├─ init                -> apply engine/sql/schema_pg.sql
 ├─ seed                -> engine.seed_data.seed_all + engine.data_rectify.rectify_historical_partitions
 ├─ scrapers            -> BOM weather + Playwright referee scrapers
 ├─ deploy / daily      -> model eval, slip write, optional Discord notify
 ├─ data-quality        -> quality gate write to nrl.data_quality_reports
 ├─ scraper-status      -> latest rows from nrl.scraper_runs
 └─ doctor              -> env/db/schema/scraper diagnostics

GitHub Actions
 ├─ ci.yml              -> scripts/gate_fast.sh
 ├─ integration.yml     -> scripts/gate_full.sh with postgres service
 ├─ scrapers.yml        -> python -m engine.run daily --season 2026 --round 1
 └─ nightly-self-check  -> gate_full + schema parity + ops parity + data-quality

Railway
 ├─ Build               -> railway.json => Dockerfile
 ├─ Runtime command     -> Docker CMD: init (fail-closed) + streamlit app
 └─ No repo-defined cron/scheduler for daily/seed
```

## Entrypoints and runtime surfaces

- `engine/run.py`: primary CLI command router (`init`, `daily`, `seed`, `scrapers`, `data-quality`, `scraper-status`, etc.).
- `engine/admin_api.py`: HTTP admin endpoints for schema apply, train, seed, backfill, parity, rectify.
- `Dockerfile`: runtime starts Streamlit HUD, not `daily` and not `seed`.
- `railway.json`: Dockerfile builder, restart policy only.
- `.github/workflows/*.yml`: CI/integration/nightly/scraper scheduling in GitHub Actions.

## Import graph notes (scrapers + DB)

- `engine.run.cmd_scrapers`
  - imports `engine.scrapers.bom_weather_scraper.run`
  - imports `engine.scrapers.referee_scraper_playwright.run`
  - uses `engine.scraper_observability` for run_id and dry-run.
- Both scrapers write to DB through SQLAlchemy engine transactions (`engine.begin`).
- DB engine creation is centralized in `engine.db.get_engine` and requires `DATABASE_URL`.
- Schema qualification uses `engine.schema_router` (`NRL_SCHEMA` + `NRL_OPS_SCHEMA`).

## Seeding surfaces

- `engine.seed_data.seed_all`: synthetic historical + current fixtures and feature tables in `nrl.*`.
- `engine.run.cmd_seed`: executes `seed_all` then `rectify_historical_partitions` to copy to `nrl_clean.*`.
- `engine.backfill.backfill_predictions`: writes predictions from resolved results.
- `engine.backfill.label_outcomes`: updates outcomes for existing predictions.
- `engine.rebuild_baseline.run_rebuild_clean_baseline`: orchestrates end-to-end rebuild path.

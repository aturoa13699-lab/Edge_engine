# RAILWAY_STATE

## Railway config found in repo

| Source | Key | Value |
|---|---|---|
| `railway.json` | build.builder | `DOCKERFILE` |
| `railway.json` | build.dockerfilePath | `Dockerfile` |
| `railway.json` | deploy.restartPolicyType | `ON_FAILURE` |
| `railway.json` | deploy.restartPolicyMaxRetries | `10` |
| `Dockerfile` | Base image | `python:3.11-slim` |
| `Dockerfile` | Runtime CMD | `python -m engine.run init || echo ...; streamlit run streamlit_app/hud.py ...` |

## Expected Railway env vars from code

| Env var | Required | Default | Where read |
|---|---:|---|---|
| `DATABASE_URL` | YES (or alias) | none | `engine/db.py` |
| `DATABASE_PRIVATE_URL` | alias | none | `engine/db.py` |
| `POSTGRES_URL` | alias | none | `engine/db.py` |
| `POSTGRESQL_URL` | alias | none | `engine/db.py` |
| `DB_SSLMODE` | no | empty | `engine/db.py` |
| `REQUIRE_DB_SSL` | no | `0` | `engine/db.py` |
| `DEPLOY_SEASON` | no | `2026` | `engine/run.py` |
| `DEPLOY_ROUND` | no | `1` | `engine/run.py` |
| `DRY_NOTIFY` | no | `0` | `engine/run.py` |
| `QUALITY_GATE_SEASONS` | no | `2022,2023,2024,2025` | `engine/run.py`, `engine/data_quality.py` |
| `NRL_SCHEMA` | no | `nrl_clean` | `engine/schema_router.py` |
| `NRL_OPS_SCHEMA` | no | `nrl` | `engine/schema_router.py` |
| `DISCORD_WEBHOOK_URL` | optional (notify path) | none | `engine/notify.py` |
| `DISCORD_USERNAME` | no | `Edge Engine` | `engine/notify_slips.py` |
| `MODEL_VERSION` | no | `v2026-02-poisson-v1` | deploy/backfill/notify |
| `ML_BLEND_ALPHA` | no | `0.65` | deploy/backfill/backtest |
| `BANKROLL` | no | `1000` | run/deploy |
| `MAX_STAKE_FRAC` | no | `0.03` | deploy/backtest |
| `FRACTIONAL_KELLY` | no | `0.33` | risk |
| `SCRAPER_DRY_RUN` | no | `0` | `engine/scraper_observability.py` |
| `REFEREE_URL` | optional | empty | `engine/scrapers/referee_scraper_playwright.py` |

## Mismatch risks

1. Railway runtime does not invoke `daily`, `scrapers`, or `seed`; it starts Streamlit only.
2. `python -m engine.run init` errors are swallowed in Docker CMD (`|| echo 'Schema init skipped...'`).
3. No repo-level Railway scheduler/cron is defined; scheduled scraping exists in GitHub Actions (`scrapers.yml`) instead.
4. DB URL alias mismatch risk remains UNKNOWN (`DATABASE_URL` is required in code; if Railway only sets private alias, app fails at startup).
5. SSL behavior can now be forced via env (`REQUIRE_DB_SSL=1` or `DB_SSLMODE=<mode>`), but dashboard values remain UNKNOWN.

## UNKNOWN from dashboard (paste-back required)

Please paste/screenshot:
1. Railway Variables for the running service (`DATABASE_*`, `DEPLOY_*`, `TZ`, `DISCORD_*`, `SCRAPER_DRY_RUN`, `REFEREE_URL`).
2. Railway service start command shown in Deploy settings.
3. Railway cron/scheduled jobs list (if any).
4. Whether Railway Postgres requires SSL and which URL variable is used by service.

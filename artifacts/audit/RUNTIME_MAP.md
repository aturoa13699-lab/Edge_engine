# RUNTIME_MAP

## Runtime/deploy config discovered
- `Dockerfile`
  - Base: python:3.11-slim
  - CMD: `python -m engine.run init && streamlit run streamlit_app/hud.py ...`
- `railway.json`
  - build via Dockerfile
  - restart policy on failure
- `engine/run.py`
  - primary CLI entrypoint for init/full/daily/scrapers/deploy/train/report/.../doctor
- `engine/admin_api.py`
  - FastAPI operational/admin API endpoints

## Missing files in repo (UNKNOWN from repo-only)
- `.env.example`: not present
- `Procfile`: not present
- `nixpacks.toml`: not present
- `start.sh`: not present

## Runtime env contract in code
- Required DB URL aliases: DATABASE_URL or DATABASE_PRIVATE_URL or POSTGRES_URL or POSTGRESQL_URL
- Optional controls: DB_SSLMODE, REQUIRE_DB_SSL, QUALITY_GATE_SEASONS, DEPLOY_SEASON, DEPLOY_ROUND, DISCORD_WEBHOOK_URL, REFEREE_URL

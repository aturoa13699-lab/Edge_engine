# WORKFLOW_MAP

## .github/workflows/ci.yml
- Triggers: push, pull_request
- Permissions: contents: read
- Jobs:
  - test (ubuntu-latest)
    - setup python 3.12
    - install `requirements.txt` + `requirements-dev.txt`
    - run `bash scripts/gate_fast.sh`

## .github/workflows/integration.yml
- Triggers: push, pull_request
- Permissions: contents: read
- Jobs:
  - integration-pg (ubuntu-latest)
    - service: postgres:16 with health-cmd pg_isready
    - env: DATABASE_URL, INTEGRATION_TEST=1, QUALITY_GATE_SEASONS
    - setup python 3.12
    - install requirements and dev requirements
    - run `bash scripts/gate_full.sh`

## .github/workflows/nightly-self-check.yml
- Triggers: schedule + workflow_dispatch
- Permissions: contents: write, issues: write, pull-requests: write
- Jobs:
  - nightly (ubuntu-latest)
    - service: postgres:16 with health check
    - env: DATABASE_URL, INTEGRATION_TEST=1, QUALITY_GATE_SEASONS
    - install requirements + dev requirements
    - run: gate_full + schema parity + ops parity + data quality
    - uploads artifacts/nightly.log
    - opens GitHub issue on failure

## .github/workflows/scrapers.yml
- Triggers: schedule + workflow_dispatch
- Permissions: contents: read
- Jobs:
  - run (ubuntu-latest)
    - setup python 3.12
    - install requirements
    - env from secrets: DATABASE_URL, DISCORD_WEBHOOK_URL
    - run `python -m engine.run daily --season 2026 --round 1`

## .github/workflows/auto-code-scan.yml
- Triggers: push main, pull_request main, schedule, workflow_dispatch
- Permissions: contents: read
- Jobs:
  - python_scan: installs req+dev req, runs `bash scripts/gate_fast.sh`
  - node_scan (conditional on package manifests): install node deps, optional tsc, run optional lint/typecheck/check/test scripts

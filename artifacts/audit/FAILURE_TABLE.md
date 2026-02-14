# FAILURE_TABLE

## Log source
- GitHub CLI unavailable in this environment (`gh: command not found`), so failed-run logs from GitHub could not be pulled directly.
- Local CI-equivalent reproductions were executed and captured under `artifacts/audit/repro/`.

| Workflow | Job | First failing step | Error excerpt | Classification | Root cause vs Secondary |
|---|---|---|---|---|---|
| integration.yml | integration-pg | `bash scripts/gate_full.sh` -> `pytest -q tests/test_integration_pg.py` | `psycopg.OperationalError: connection to server at "127.0.0.1", port 5432 failed: Connection refused` | Integration(DB) / Environment | Secondary in local repro (no postgres service); GH likely mitigated by service container |
| nightly-self-check.yml | nightly | `bash scripts/gate_full.sh` (first command in nightly checks) | Same `connection refused` failure in integration test path | Integration(DB) / Environment | Secondary in local repro (no postgres service) |
| scrapers.yml | run | `python -m engine.run daily --season 2026 --round 1` | `RuntimeError: DATABASE_URL is not set ...` when secrets absent | Secret/env contract | Root cause for contexts where secrets unavailable (fork PRs or missing repo secrets) |
| ci.yml | test | `bash scripts/gate_fast.sh` | Local repro succeeded | N/A | Not failing in current local repro |
| auto-code-scan.yml | python_scan/node_scan | Not executed on GitHub here | YAML parses; Python path mirrors gate_fast | Config/tooling | UNKNOWN (no GitHub run log available) |

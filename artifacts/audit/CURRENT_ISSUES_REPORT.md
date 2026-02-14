# CURRENT_ISSUES_REPORT

## A) Executive Summary
- Workflows present: 5 (`ci`, `integration`, `nightly-self-check`, `scrapers`, `auto-code-scan`).
- Confirmed local failures in CI-equivalent runs: integration-style jobs when PostgreSQL service is unavailable; scraper run when DB secret is absent.
- Unique root-cause classes observed: 3
  1. Missing GitHub-run visibility in this environment (cannot fetch canonical failed run logs without `gh`).
  2. External runtime contract dependencies (Postgres service, secrets) required by specific jobs.
  3. Potential path-resolution edge case in rectify input handling (fixed in this branch) and validated via tests.

## B) Issue List (Complete)

### ISSUE-001
- Scope: cross-workflow audit evidence collection
- Category: Tooling
- Evidence: `gh` unavailable (`bash: command not found: gh`) in `artifacts/audit/logs/gh_run_list.log`.
- Impact: cannot directly prove first failing step from GitHub-hosted runs in this environment.
- Fix: use paste-back gate to collect GitHub failing-step logs; local reproductions still provided.

### ISSUE-002
- Scope: integration.yml / nightly-self-check.yml
- Category: Integration(DB)
- Evidence: local repro logs show first failure at integration pytest due DB connection refused.
- Impact: integration and nightly checks fail when Postgres service is not reachable.
- Fix: ensure service container health before tests in GitHub Actions (already configured); if still failing in GitHub, capture service logs and connection env.

### ISSUE-003
- Scope: scrapers.yml
- Category: Secret/env contract
- Evidence: running daily command without DB secret fails at `engine.db` URL resolution.
- Impact: scraper workflow fails when `secrets.DATABASE_URL` not present (or inaccessible context).
- Fix: ensure secret exists for scheduled workflow or guard step when missing.

### ISSUE-004
- Scope: workflow security posture
- Category: Permissions
- Evidence: CodeQL alerts referenced missing workflow permissions.
- Impact: policy/security warnings and possible governance gate failures.
- Fix status: addressed by explicit `permissions: contents: read` in `ci`, `integration`, `scrapers`.

### ISSUE-005
- Scope: data_rectify path handling
- Category: Security/validation
- Evidence: CodeQL flagged uncontrolled path expression; raw `Path(path).read_text()` was used.
- Impact: unsafe path usage risk.
- Fix status: resolver allowlists `artifacts/` and `data/`, rejects URL-like inputs, tests added for reject/accept behavior.

## C) Conflict Matrix
- Python runtime mismatch: Docker runtime uses Python 3.11 while Actions workflows use 3.12.
- Scheduler split: Railway runtime hosts app; GitHub Actions schedule executes scrapers/nightly.
- Install strategy mismatch: `scrapers.yml` installs only `requirements.txt`; other CI workflows install both requirements + dev requirements.
- Secret dependency asymmetry: `scrapers.yml` depends on repo secrets, while ci/integration/nightly mostly self-contained in workflow config.

## D) Fix Plan (ordered)
1. Pull canonical GitHub failing-step logs (first failing step per failing job) to replace local-only evidence.
2. Confirm `DATABASE_URL` secret presence/access for `scrapers.yml` execution context.
3. If GitHub integration still fails despite postgres service, inspect service startup/health logs and test env values.
4. Keep workflow permissions explicit and least-privileged (already applied).
5. Keep data-rectify path allowlist and regression tests (applied).

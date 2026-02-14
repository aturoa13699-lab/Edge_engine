# SEEDING_TRACE

## Trigger -> fetch -> transform -> write

1. Trigger surfaces:
   - CLI: `python -m engine.run seed --season <S>`
   - Admin API: `POST /seed/{season}`
   - `full` command also runs `seed`.
2. Seed generation:
   - `seed_all` generates synthetic fixtures/results/odds/ratings/injuries/coach profiles.
   - Historical seasons default to 2022-2025 with resolved scores.
   - Current season default generates fixtures without scores.
3. Writes:
   - `seed_all` writes to `nrl.matches_raw`, `nrl.odds`, `nrl.team_ratings`, `nrl.coach_profile`, `nrl.injuries_current`.
4. Clean schema copy:
   - `cmd_seed` immediately calls `rectify_historical_partitions` to copy `nrl.*` source into `nrl_clean.matches_raw` and `nrl_clean.odds`, with provenance writes.

## Local repro evidence

Log file: `artifacts/current_state/seed_repro.log`

Observed:
- seasons 2022-2025 seeded (216 matches + 432 odds each)
- season 2026 seeded with fixtures only
- totals: matches=1080 odds=2160 team_ratings=85 coach_profiles=85 injuries=85
- backfill season 2025 round 1 wrote 8 predictions
- label_outcomes updated 0 rows (because inserted backfill rows already had outcome flags set)

## Failure points identified

1. **Not auto-triggered in Railway runtime**
   - Docker CMD does not run `seed` and does not run `daily`.
2. **Schema init can silently fail**
   - Docker CMD ignores init failure (`|| echo ...`), so downstream commands may run against missing tables.
3. **Depends on DATABASE_URL**
   - `engine.db.get_engine` hard-fails when absent.
4. **Backfill depends on existing resolved matches + features**
   - if no resolved matches in chosen season/rounds, returns `backfilled=0`.

## Why historical seeding may appear absent

Most likely in production runtime:
- no recurring trigger executes `seed`/`full` on Railway
- only GitHub Actions `scrapers.yml` runs `daily`, and that workflow sets explicit env vars independent of Railway runtime.

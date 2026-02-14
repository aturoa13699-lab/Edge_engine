# SCRAPERS_TRACE

## Scraper inventory

### 1) `bom_weather_scraper`
- Target: BOM JSON endpoints in static `VENUE_BOM` map.
- Network: `requests.get(url, timeout=20)`.
- Auth: none.
- Retry/backoff: none explicit; request exception logged and skipped.
- Output table: `nrl.weather_daily` (upsert by `(match_date, venue)`).

### 2) `referee_scraper_playwright`
- Target: env `REFEREE_URL` HTML page.
- Network: Playwright page goto with 45s timeout.
- Auth: none in code.
- Retry/backoff: none explicit.
- Output table: `nrl.referee_tendencies` (upsert by `(season, referee)`).
- Skip conditions: Playwright not installed OR `REFEREE_URL` missing.

## Trigger model

- `python -m engine.run scrapers --season <S>` (CLI)
- `python -m engine.run daily ...` includes scrapers + deploy
- GitHub Actions `scrapers.yml` runs `daily` on cron at `0 6 * * *`
- Railway repo config contains no schedule entries

## Observability now added

- Structured logs emitted for each scraper event:
  - `SCRAPER_START`
  - `SCRAPER_FETCH`
  - `SCRAPER_PARSE`
  - `SCRAPER_WRITE`
  - `SCRAPER_END`
- Dry-run mode via env `SCRAPER_DRY_RUN=1` prints intended writes without DB mutations.
- Run tracking table `nrl.scraper_runs` added with status + row counters + errors.
- CLI `python -m engine.run scraper-status` shows latest scraper status rows.

## Repro evidence

Log file: `artifacts/current_state/scraper_repro.log`

Observed:
- `bom_weather` attempted 3 fetches; all failed in this environment due to outbound proxy 403.
- `referee_playwright` skipped due to Playwright not installed.
- `scraper-status` reports latest `bom_weather` success and `referee_playwright` skipped with error text.

## Known breakpoints

1. External network restrictions can block BOM hosts.
2. Missing Playwright package/browser causes referee scraper skip.
3. Missing `REFEREE_URL` causes referee scraper skip.

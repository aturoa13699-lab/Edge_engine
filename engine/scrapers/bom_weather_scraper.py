from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict

import requests
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from ..scraper_observability import (
    StepTimer,
    log_event,
    scraper_dry_run_enabled,
    upsert_run,
)

logger = logging.getLogger("nrl-pillar1")

# Minimal mapping: venue -> BOM observation JSON endpoint (example stations)
# You can expand these as needed; scraper is idempotent.
VENUE_BOM = {
    "Accor Stadium": "https://reg.bom.gov.au/fwo/IDN60901/IDN60901.94767.json",  # Sydney (example)
    "CommBank Stadium": "https://reg.bom.gov.au/fwo/IDN60901/IDN60901.94767.json",
    "Suncorp Stadium": "https://reg.bom.gov.au/fwo/IDQ60901/IDQ60901.94578.json",  # Brisbane (example)
}


def _fetch_obs(url: str) -> tuple[Dict[str, Any] | None, int | None, int, int]:
    timer = StepTimer()
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        payload = r.json()
        return payload, r.status_code, len(r.content), timer.elapsed_ms()
    except Exception as e:
        logger.warning("BOM fetch failed: %s", e)
        return None, None, 0, timer.elapsed_ms()


def run(engine: Engine, season: int, run_id: str, dry_run: bool | None = None) -> None:
    # Pull today's conditions per venue and store one row per venue/day.
    dry_run = scraper_dry_run_enabled() if dry_run is None else dry_run
    scraper = "bom_weather"
    today = date.today()
    rows_written = 0
    fetch_count = 0

    log_event("START", scraper=scraper, season=season, run_id=run_id, dry_run=dry_run)
    upsert_run(
        engine,
        run_id=run_id,
        scraper=scraper,
        season=season,
        status="running",
        dry_run=dry_run,
    )

    try:
        for venue, url in VENUE_BOM.items():
            blob, status_code, bytes_read, latency_ms = _fetch_obs(url)
            fetch_count += 1
            log_event(
                "FETCH",
                scraper=scraper,
                run_id=run_id,
                url=url,
                status=status_code,
                bytes=bytes_read,
                latency_ms=latency_ms,
            )
            if not blob:
                continue

            try:
                obs = blob["observations"]["data"][0]
                temp_c = obs.get("air_temp")
                wind_kmh = obs.get("wind_spd_kmh")
                rain_trace = obs.get("rain_trace")
                # crude wet flag
                is_wet = (
                    1
                    if (
                        rain_trace not in (None, "", "0.0", "0")
                        and float(rain_trace) > 0.0
                    )
                    else 0
                )
                desc = obs.get("weather") or ""
            except Exception:
                continue

            log_event(
                "PARSE", scraper=scraper, run_id=run_id, venue=venue, items_found=1
            )

            if dry_run:
                log_event(
                    "WRITE",
                    scraper=scraper,
                    run_id=run_id,
                    table="nrl.weather_daily",
                    rows_inserted=1,
                    rows_updated=0,
                    mode="dry-run",
                )
                rows_written += 1
                continue

            write_timer = StepTimer()
            with engine.begin() as conn:
                conn.execute(
                    sql_text(
                        """
                        INSERT INTO nrl.weather_daily (match_date, venue, is_wet, temp_c, wind_speed_kmh, conditions)
                        VALUES (:d, :v, :wet, :t, :w, :c)
                        ON CONFLICT (match_date, venue)
                        DO UPDATE SET
                          is_wet=EXCLUDED.is_wet,
                          temp_c=EXCLUDED.temp_c,
                          wind_speed_kmh=EXCLUDED.wind_speed_kmh,
                          conditions=EXCLUDED.conditions,
                          updated_at=now()
                        """
                    ),
                    dict(d=today, v=venue, wet=is_wet, t=temp_c, w=wind_kmh, c=desc),
                )
            rows_written += 1
            log_event(
                "WRITE",
                scraper=scraper,
                run_id=run_id,
                table="nrl.weather_daily",
                rows_inserted=1,
                rows_updated=0,
                duration_ms=write_timer.elapsed_ms(),
            )

        upsert_run(
            engine,
            run_id=run_id,
            scraper=scraper,
            season=season,
            status="success",
            dry_run=dry_run,
            rows_inserted=rows_written,
            fetch_count=fetch_count,
            details={"venues": list(VENUE_BOM)},
        )
        log_event(
            "END",
            scraper=scraper,
            season=season,
            run_id=run_id,
            success=True,
            rows_inserted=rows_written,
        )
    except Exception as exc:
        upsert_run(
            engine,
            run_id=run_id,
            scraper=scraper,
            season=season,
            status="failed",
            dry_run=dry_run,
            rows_inserted=rows_written,
            fetch_count=fetch_count,
            last_error=f"{type(exc).__name__}: {exc}",
        )
        log_event(
            "END",
            scraper=scraper,
            season=season,
            run_id=run_id,
            success=False,
            error_type=type(exc).__name__,
        )
        raise

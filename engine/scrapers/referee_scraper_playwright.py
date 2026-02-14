from __future__ import annotations

import logging
import os

from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from ..scraper_observability import (
    StepTimer,
    log_event,
    scraper_dry_run_enabled,
    upsert_run,
)

logger = logging.getLogger("nrl-pillar1")


def _playwright_available() -> bool:
    try:
        import playwright  # noqa: F401

        return True
    except Exception:
        return False


def run(engine: Engine, season: int, run_id: str, dry_run: bool | None = None) -> None:
    """
    Best-effort referee tendency scraper.

    - If Playwright isn't installed, we skip safely (pipeline remains green).
    - If installed, this can scrape a configured page and parse referee names.
    """
    dry_run = scraper_dry_run_enabled() if dry_run is None else dry_run
    scraper = "referee_playwright"
    log_event("START", scraper=scraper, season=season, run_id=run_id, dry_run=dry_run)

    if not _playwright_available():
        upsert_run(
            engine,
            run_id=run_id,
            scraper=scraper,
            season=season,
            status="skipped",
            dry_run=dry_run,
            last_error="Playwright not installed",
        )
        logger.info("Playwright not installed; referee scraper skipped.")
        return

    url = os.getenv("REFEREE_URL", "").strip()
    if not url:
        upsert_run(
            engine,
            run_id=run_id,
            scraper=scraper,
            season=season,
            status="skipped",
            dry_run=dry_run,
            last_error="REFEREE_URL not set",
        )
        logger.info("REFEREE_URL not set; referee scraper skipped.")
        return

    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        upsert_run(
            engine,
            run_id=run_id,
            scraper=scraper,
            season=season,
            status="skipped",
            dry_run=dry_run,
            last_error="Playwright sync API unavailable",
        )
        logger.info("Playwright sync API unavailable; referee scraper skipped.")
        return

    names: list[str] = []
    fetch_timer = StepTimer()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=45000)

        # Generic parse: take all table rows with "Referee" keyword
        text = page.content()
        browser.close()

    log_event(
        "FETCH",
        scraper=scraper,
        run_id=run_id,
        url=url,
        status=200,
        bytes=len(text.encode("utf-8")),
        latency_ms=fetch_timer.elapsed_ms(),
    )

    # Very conservative parsing (avoid fragile selector coupling):
    # You can replace with real selectors for your target site.
    for line in text.splitlines():
        if "Referee" in line and len(line) < 200:
            names.append(line.strip())

    log_event("PARSE", scraper=scraper, run_id=run_id, items_found=len(names))
    if not names:
        upsert_run(
            engine,
            run_id=run_id,
            scraper=scraper,
            season=season,
            status="success",
            dry_run=dry_run,
            fetch_count=1,
            details={"items_found": 0},
        )
        logger.info("Referee scraper ran but found no candidates.")
        return

    if dry_run:
        log_event(
            "WRITE",
            scraper=scraper,
            run_id=run_id,
            table="nrl.referee_tendencies",
            rows_inserted=min(len(names), 50),
            rows_updated=0,
            mode="dry-run",
        )
        upsert_run(
            engine,
            run_id=run_id,
            scraper=scraper,
            season=season,
            status="success",
            dry_run=dry_run,
            rows_inserted=min(len(names), 50),
            fetch_count=1,
            details={"items_found": len(names), "url": url},
        )
        log_event("END", scraper=scraper, season=season, run_id=run_id, success=True)
        return

    rows_written = 0
    write_timer = StepTimer()
    with engine.begin() as conn:
        for n in names[:50]:
            conn.execute(
                sql_text(
                    """
                    INSERT INTO nrl.referee_tendencies (season, referee, notes)
                    VALUES (:s, :r, :n)
                    ON CONFLICT (season, referee)
                    DO UPDATE SET notes=EXCLUDED.notes, updated_at=now()
                    """
                ),
                dict(s=season, r=n[:120], n=n[:500]),
            )
            rows_written += 1

    log_event(
        "WRITE",
        scraper=scraper,
        run_id=run_id,
        table="nrl.referee_tendencies",
        rows_inserted=rows_written,
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
        fetch_count=1,
        details={"items_found": len(names), "url": url},
    )
    logger.info("Referee scraper complete (season=%s, rows=%s)", season, rows_written)
    log_event("END", scraper=scraper, season=season, run_id=run_id, success=True)

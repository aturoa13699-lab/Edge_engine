from __future__ import annotations

import logging
import os

from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

logger = logging.getLogger("nrl-pillar1")


def _playwright_available() -> bool:
    try:
        import playwright  # noqa: F401
        return True
    except Exception:
        return False


def run(engine: Engine, season: int) -> None:
    """
    Best-effort referee tendency scraper.

    - If Playwright isn't installed, we skip safely (pipeline remains green).
    - If installed, this can scrape a configured page and parse referee names.
    """
    if not _playwright_available():
        logger.info("Playwright not installed; referee scraper skipped.")
        return

    url = os.getenv("REFEREE_URL", "").strip()
    if not url:
        logger.info("REFEREE_URL not set; referee scraper skipped.")
        return

    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        logger.info("Playwright sync API unavailable; referee scraper skipped.")
        return

    names = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=45000)

        # Generic parse: take all table rows with "Referee" keyword
        text = page.content()
        browser.close()

    # Very conservative parsing (avoid fragile selector coupling):
    # You can replace with real selectors for your target site.
    for line in text.splitlines():
        if "Referee" in line and len(line) < 200:
            names.append(line.strip())

    if not names:
        logger.info("Referee scraper ran but found no candidates.")
        return

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

    logger.info("Referee scraper stored %s rows.", min(len(names), 50))

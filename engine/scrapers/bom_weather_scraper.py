from __future__ import annotations

import logging
from datetime import date
from typing import Dict, Optional

import requests
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

logger = logging.getLogger("nrl-pillar1")

# Minimal mapping: venue -> BOM observation JSON endpoint (example stations)
# You can expand these as needed; scraper is idempotent.
VENUE_BOM = {
    "Accor Stadium": "https://reg.bom.gov.au/fwo/IDN60901/IDN60901.94767.json",  # Sydney (example)
    "CommBank Stadium": "https://reg.bom.gov.au/fwo/IDN60901/IDN60901.94767.json",
    "Suncorp Stadium": "https://reg.bom.gov.au/fwo/IDQ60901/IDQ60901.94578.json",  # Brisbane (example)
}


def _fetch_obs(url: str) -> Optional[Dict]:
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning("BOM fetch failed: %s", e)
        return None


def run(engine: Engine, season: int) -> None:
    # Pull today's conditions per venue and store one row per venue/day.
    today = date.today()

    for venue, url in VENUE_BOM.items():
        blob = _fetch_obs(url)
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
                    rain_trace not in (None, "", "0.0", "0") and float(rain_trace) > 0.0
                )
                else 0
            )
            desc = obs.get("weather") or ""
        except Exception:
            continue

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

    logger.info("Weather scraper complete (season=%s)", season)

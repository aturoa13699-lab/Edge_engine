import os
import logging
import requests
from typing import Optional, List, Any, Dict, Tuple

logger = logging.getLogger("nrl-pillar1")


def post_discord(
    content: Optional[str] = None,
    embeds: Optional[List[Dict[str, Any]]] = None,
    username: Optional[str] = None,
    files: Optional[List[Tuple[str, Tuple[str, Any, str]]]] = None,
) -> None:
    url = os.getenv("DISCORD_WEBHOOK_URL")
    if not url:
        logger.warning("DISCORD_WEBHOOK_URL not set; skipping")
        return

    payload: Dict[str, Any] = {}
    if content:
        payload["content"] = content
    if embeds:
        payload["embeds"] = embeds
    if username:
        payload["username"] = username

    try:
        r = requests.post(url, json=payload, files=files, timeout=20)
        if r.status_code >= 400:
            logger.error("Discord webhook failed: %s %s", r.status_code, r.text[:300])
    except Exception as e:
        logger.error("Discord post exception: %s", e)

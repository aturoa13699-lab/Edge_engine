import os
from typing import Any, Dict, List, Optional, Tuple

import requests


def post_discord(
    content: Optional[str] = None,
    embeds: Optional[List[Dict[str, Any]]] = None,
    username: Optional[str] = None,
    files: Optional[List[Tuple[str, tuple]]] = None,
) -> None:
    webhook = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook:
        return

    payload: Dict[str, Any] = {}
    if content:
        payload["content"] = content
    if embeds:
        payload["embeds"] = embeds
    if username:
        payload["username"] = username

    requests.post(webhook, json=payload, files=files, timeout=30)

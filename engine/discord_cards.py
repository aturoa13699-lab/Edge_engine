from __future__ import annotations

from typing import Any, Dict, List

from .types import Slip


STATUS_COLORS = {
    "pending": 0x0B63F6,
    "dry_run": 0x999999,
    "win": 0x00B050,
    "loss": 0xFB7185,
    "void": 0x999999,
}


def slip_to_embed(slip: Slip) -> Dict[str, Any]:
    color = STATUS_COLORS.get(slip.status, 0x0B63F6)
    title = f"{slip.home_team} v {slip.away_team}"
    fields = [
        {"name": "Market", "value": slip.market, "inline": True},
        {"name": "Selection", "value": slip.selection, "inline": True},
        {"name": "Odds", "value": f"@ {slip.odds:.2f}", "inline": True},
        {"name": "Stake", "value": f"${slip.stake:.2f}", "inline": True},
        {"name": "EV", "value": f"{slip.ev:.4f}", "inline": True},
        {"name": "Model", "value": slip.model_version, "inline": True},
    ]

    return {
        "title": title,
        "description": slip.reason or "",
        "color": color,
        "fields": fields,
        "footer": {"text": f"portfolio_id={slip.portfolio_id}"},
    }


def chunk_embeds(embeds: List[Dict[str, Any]], chunk_size: int = 10) -> List[List[Dict[str, Any]]]:
    return [embeds[i : i + chunk_size] for i in range(0, len(embeds), chunk_size)]

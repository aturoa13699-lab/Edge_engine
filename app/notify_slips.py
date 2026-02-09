import logging
import os
import tempfile
from typing import List

from sqlalchemy.engine import Engine

from .discord_cards import chunk_embeds, slip_to_embed
from .notify import post_discord
from .reporting import fetch_round_slips
from .stake_summary import generate_styled_summary_image
from .types import Slip

logger = logging.getLogger("nrl-pillar1")


def _dict_to_slip(d) -> Slip:
    return Slip(
        portfolio_id=d["portfolio_id"],
        season=int(d["season"]),
        round_num=int(d["round_num"]),
        match_id=d["match_id"],
        home_team=d["home_team"],
        away_team=d["away_team"],
        market=d["market"],
        selection=d["selection"],
        odds=float(d["odds"]),
        stake=float(d["stake"]),
        ev=float(d["ev"]),
        status=d.get("status", "pending"),
        model_version=d.get("model_version", os.getenv("MODEL_VERSION", "v2026-02-poisson-v1")),
        reason=d.get("reason"),
    )


def send_round_slip_cards(engine: Engine, season: int, round_num: int, status: str = "pending") -> None:
    slips_dicts = fetch_round_slips(engine, season, round_num, status=status)
    if not slips_dicts:
        logger.info("No slips to notify for season=%s round=%s status=%s", season, round_num, status)
        return

    slips: List[Slip] = [_dict_to_slip(s) for s in slips_dicts]

    # Generate PNGs and prepare attachments (Discord max 10 per message)
    attachments = []
    tmp_paths = []
    for slip in slips:
        tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        tmp.close()
        png_path = generate_styled_summary_image(slip, tmp.name)
        tmp_paths.append(png_path)
        attachments.append(
            ("files", (f"slip_{slip.portfolio_id[:8]}.png", open(png_path, "rb"), "image/png"))
        )

    embeds = [slip_to_embed(s) for s in slips]
    for batch in chunk_embeds(embeds):
        post_discord(
            embeds=batch,
            username=os.getenv("DISCORD_USERNAME", "Edge Engine"),
            files=attachments[:10],
        )

    for _, (_, f, _) in attachments:
        f.close()

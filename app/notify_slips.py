import os
import logging
import tempfile
from sqlalchemy.engine import Engine

from .notify import post_discord
from .reporting import fetch_round_slips
from .discord_cards import slip_to_embed, chunk_embeds
from .stake_summary import generate_styled_summary_image

logger = logging.getLogger("nrl-pillar1")


def send_round_slip_cards(engine: Engine, season: int, round_num: int, status: str = "pending") -> None:
    slips = fetch_round_slips(engine, season, round_num, status=status)
    if not slips:
        return

    # Generate PNGs and prepare attachments
    attachments = []
    temp_paths = []
    for slip in slips:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            png_path = generate_styled_summary_image(slip, tmp.name)
            temp_paths.append(png_path)
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

    # Cleanup
    for _, (_, f, _) in attachments:
        try:
            f.close()
        except Exception:
            pass
    for p in temp_paths:
        try:
            os.remove(p)
        except Exception:
            pass

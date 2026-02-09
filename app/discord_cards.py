from .types import Slip


def slip_to_embed(slip: Slip) -> dict:
    leg = slip.legs[0]
    title = f"{leg.selection} ({slip.market})"
    desc = f"Price: {leg.price:.2f}\nP(model): {leg.p_model:.3f}\nStake: {slip.stake_units:.2f}u"
    return {"title": title, "description": desc, "color": 0x0B63F6}


def chunk_embeds(embeds: list[dict], size: int = 10):
    for i in range(0, len(embeds), size):
        yield embeds[i : i + size]

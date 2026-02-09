from __future__ import annotations

from typing import Dict, Any
from reportlab.graphics import renderPM
from reportlab.graphics.shapes import Drawing, Rect, String


STATUS_COLORS = {
    "pending": "#0b63f6",
    "win": "#00B050",
    "loss": "#FB7185",
    "void": "#999999",
}


def generate_styled_summary(slip) -> str:
    """
    Returns an HTML card for Streamlit/Discord previews.
    """
    legs = getattr(slip, "legs", []) or []
    is_multi = len(legs) > 1

    if not is_multi and legs:
        leg = legs[0]
        return f"""
<div style="background:#ffffff; border-left:4px solid #0b63f6; border-radius:8px; box-shadow:0 1px 3px rgba(0,0,0,0.1); padding:12px 16px; margin-bottom:12px; font-family:Roboto, sans-serif;">
  <div style="font-size:12px; color:#0b63f6; font-weight:500;">{slip.match_id}</div>
  <div style="font-size:16px; font-weight:700; color:#000000;">{leg.selection}</div>
  <div style="font-size:13px; color:#555555;">{slip.market}</div>
  <div style="margin-top:8px; display:flex; justify-content:space-between; align-items:center;">
    <div style="font-size:14px;">Stake: <strong>{slip.stake_units:.2f}u</strong></div>
    <div style="font-size:14px;">Odds: <strong>{leg.price:.2f}</strong></div>
  </div>
</div>
""".strip()

    # SGM/multi
    legs_html = []
    for leg in legs:
        legs_html.append(
            f"""
  <div style="border-top:1px solid #eee; padding-top:6px;">
    <div style="font-size:13px;">🏉 {slip.match_id}</div>
    <div style="font-size:14px; font-weight:600;">{leg.selection}</div>
    <div style="font-size:12px; color:#666;">{leg.market}</div>
  </div>
""".rstrip()
        )

    odds_total = 1.0
    for leg in legs:
        odds_total *= float(leg.price)

    return f"""
<div style="background:#ffffff; border-left:4px solid #0b63f6; border-radius:8px; box-shadow:0 1px 3px rgba(0,0,0,0.1); padding:12px 16px; margin-bottom:12px; font-family:Roboto, sans-serif;">
  <div style="display:flex; justify-content:space-between; align-items:center;">
    <div style="font-size:16px; font-weight:700; color:#001460;">Same Game Multi</div>
    <div style="font-size:15px; font-weight:700;">@ {odds_total:.2f}</div>
  </div>
  <div style="font-size:11px; color:#777; margin-bottom:6px;"><span style="border:1px solid #ccc; border-radius:999px; padding:2px 8px;">{len(legs)} Legs</span></div>
  {''.join(legs_html)}
  <div style="margin-top:8px; display:flex; justify-content:space-between;">
    <div style="font-size:13px;">Stake: <strong>{slip.stake_units:.2f}u</strong></div>
    <div style="font-size:13px; color:#00B050;">Returns: <strong>{slip.stake_units * odds_total:.2f}u</strong></div>
  </div>
</div>
""".strip()


def generate_styled_summary_image(slip, out_path: str) -> str:
    """
    Generate a PNG card using reportlab drawing primitives (no browser needed).
    """
    status = (getattr(slip, "status", None) or "pending").lower()
    color = STATUS_COLORS.get(status, STATUS_COLORS["pending"])

    width, height = 700, 320
    d = Drawing(width, height)

    # Card background
    d.add(Rect(0, 0, width, height, fillColor=None, strokeColor=None))
    d.add(Rect(10, 10, width - 20, height - 20, fillColor=None, strokeColor=None))
    d.add(Rect(10, 10, 8, height - 20, fillColor=color, strokeColor=color))
    d.add(Rect(18, 10, width - 28, height - 20, fillColor="#FFFFFF", strokeColor="#DDDDDD"))

    # Text
    title = f"{slip.match_id}"
    d.add(String(40, height - 60, title, fontName="Helvetica-Bold", fontSize=14, fillColor="#0b63f6"))
    d.add(String(40, height - 90, f"Market: {slip.market}", fontName="Helvetica", fontSize=12, fillColor="#555555"))
    d.add(String(40, height - 120, f"Stake: {slip.stake_units:.2f}u", fontName="Helvetica-Bold", fontSize=12))

    y = height - 160
    for leg in getattr(slip, "legs", [])[:4]:
        d.add(String(40, y, f"• {leg.selection} @ {float(leg.price):.2f}", fontName="Helvetica", fontSize=12))
        y -= 22

    # Render to PNG
    renderPM.drawToFile(d, out_path, fmt="PNG")
    return out_path


def slip_obj_from_row(row: Dict[str, Any]):
    """
    Rebuild a minimal Slip-like object from DB row dict (for PDF rendering).
    """
    from .types import Slip, SlipLeg

    # slip_json may be empty / not reliable; rebuild minimal single-leg card
    leg = SlipLeg(
        match_id=row.get("match_id") or "",
        market=row.get("market") or "",
        selection=row.get("portfolio_id", "")[:12],
        price=1.90,
        p_model=0.5,
    )
    return Slip(
        portfolio_id=row.get("portfolio_id", ""),
        season=int(row.get("season") or 0),
        round_num=int(row.get("round_num") or 0),
        match_id=row.get("match_id") or row.get("portfolio_id", "")[:12],
        market=row.get("market") or "",
        legs=[leg],
        stake_units=float(row.get("stake_units") or 0.0),
        status=row.get("status") or "pending",
        created_at="",
    )

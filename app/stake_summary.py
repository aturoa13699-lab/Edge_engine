from __future__ import annotations

<<<<<<< HEAD
from typing import Dict, Any
from reportlab.graphics import renderPM
from reportlab.graphics.shapes import Drawing, Rect, String


STATUS_COLORS = {
    "pending": "#0b63f6",
=======
import os

from reportlab.graphics import renderPM
from reportlab.graphics.shapes import Drawing, Rect, String

from .types import Slip

STATUS_COLORS = {
    "pending": "#0b63f6",
    "dry_run": "#999999",
>>>>>>> origin/codex/2026-02-09-bootstrap-and-verify-nrl-edge-engine-v1.1
    "win": "#00B050",
    "loss": "#FB7185",
    "void": "#999999",
}


<<<<<<< HEAD
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
=======
def generate_styled_summary(slip: Slip) -> str:
    status_color = STATUS_COLORS.get(slip.status, "#0b63f6")

    return f"""
<div style="background:#ffffff; border-left:4px solid {status_color}; border-radius:8px; box-shadow:0 1px 3px rgba(0,0,0,0.1); padding:12px 16px; margin-bottom:12px; font-family:Roboto, sans-serif;">
  <div style="font-size:12px; color:#0b63f6; font-weight:500;">{slip.home_team} v {slip.away_team}</div>
  <div style="font-size:16px; font-weight:700; color:#000000;">{slip.selection}</div>
  <div style="font-size:13px; color:#555555;">{slip.market}</div>
  <div style="margin-top:8px; display:flex; justify-content:space-between; align-items:center;">
    <div style="font-size:14px;">Stake: <strong>${slip.stake:.2f}</strong></div>
    <div style="font-size:14px;">Odds: <strong>{slip.odds:.2f}</strong></div>
  </div>
  <div style="margin-top:6px; display:flex; justify-content:space-between; align-items:center;">
    <div style="font-size:12px; color:#666;">EV: <strong>{slip.ev:.4f}</strong></div>
    <div style="font-size:12px; color:#666;">Model: <strong>{slip.model_version}</strong></div>
>>>>>>> origin/codex/2026-02-09-bootstrap-and-verify-nrl-edge-engine-v1.1
  </div>
</div>
""".strip()


<<<<<<< HEAD
def generate_styled_summary_image(slip, out_path: str) -> str:
    """
    Generate a PNG card using reportlab drawing primitives (no browser needed).
    """
    status = (getattr(slip, "status", None) or "pending").lower()
    color = STATUS_COLORS.get(status, STATUS_COLORS["pending"])

    width, height = 700, 320
=======
def _draw_slip_card(slip: Slip, width: int = 800, height: int = 360) -> Drawing:
    status_color = STATUS_COLORS.get(slip.status, "#0b63f6")

>>>>>>> origin/codex/2026-02-09-bootstrap-and-verify-nrl-edge-engine-v1.1
    d = Drawing(width, height)

    # Card background
    d.add(Rect(0, 0, width, height, fillColor=None, strokeColor=None))
    d.add(Rect(10, 10, width - 20, height - 20, fillColor=None, strokeColor=None))

    # White card
    d.add(Rect(20, 20, width - 40, height - 40, fillColor="#ffffff", strokeColor="#e5e7eb", strokeWidth=1))

    # Status strip
    d.add(Rect(20, 20, 10, height - 40, fillColor=status_color, strokeColor=status_color))

    x = 50
    y = height - 70

    d.add(String(x, y, f"{slip.home_team} v {slip.away_team}", fontSize=18, fillColor="#0b63f6"))
    y -= 40
    d.add(String(x, y, slip.selection, fontSize=26, fillColor="#111827"))
    y -= 34
    d.add(String(x, y, slip.market, fontSize=16, fillColor="#6b7280"))
    y -= 50

    d.add(String(x, y, f"Stake: ${slip.stake:.2f}", fontSize=18, fillColor="#111827"))
    d.add(String(x + 320, y, f"Odds: {slip.odds:.2f}", fontSize=18, fillColor="#111827"))
    y -= 34

    d.add(String(x, y, f"EV: {slip.ev:.4f}", fontSize=14, fillColor="#6b7280"))
    d.add(String(x + 320, y, f"Model: {slip.model_version}", fontSize=14, fillColor="#6b7280"))

    return d


def generate_styled_summary_image(slip: Slip, out_path: str) -> str:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    drawing = _draw_slip_card(slip)
    renderPM.drawToFile(drawing, out_path, fmt="PNG")
    return out_path

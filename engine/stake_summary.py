from __future__ import annotations

import os

from reportlab.graphics import renderPM
from reportlab.graphics.shapes import Drawing, Rect, String

from .types import Slip

STATUS_COLORS = {
    "pending": "#0b63f6",
    "dry_run": "#999999",
    "win": "#00B050",
    "loss": "#FB7185",
    "void": "#999999",
}


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
  </div>
</div>
""".strip()


def _draw_slip_card(slip: Slip, width: int = 800, height: int = 360) -> Drawing:
    status_color = STATUS_COLORS.get(slip.status, "#0b63f6")

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

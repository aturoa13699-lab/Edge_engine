from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, List

import matplotlib.pyplot as plt
import numpy as np
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from reportlab.lib import colors

from sqlalchemy.engine import Engine

from .reporting import fetch_recent_predictions, fetch_recent_slips
from .stake_summary import generate_styled_summary_image
from .types import Slip


def _dict_to_slip(d: Dict[str, Any]) -> Slip:
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
        model_version=d.get("model_version", "v2026-02-poisson-v1"),
        reason=d.get("reason"),
    )


def _reliability_plot(pred_rows: List[Dict[str, Any]], out_path: str) -> bool:
    # needs outcome_known + outcome_home_win + calibrated_p
    rows = [r for r in pred_rows if r.get("outcome_known") and r.get("calibrated_p") is not None]
    if len(rows) < 50:
        return False

    p = np.array([float(r["calibrated_p"]) for r in rows])
    y = np.array([1.0 if r["outcome_home_win"] else 0.0 for r in rows])

    bins = np.linspace(0.0, 1.0, 11)
    idx = np.digitize(p, bins) - 1

    bin_centers = []
    acc = []
    conf = []
    for b in range(10):
        mask = idx == b
        if mask.sum() == 0:
            continue
        bin_centers.append((bins[b] + bins[b + 1]) / 2)
        acc.append(float(y[mask].mean()))
        conf.append(float(p[mask].mean()))

    plt.figure(figsize=(5, 5))
    plt.plot([0, 1], [0, 1])
    plt.scatter(conf, acc)
    plt.title("Reliability (Calibrated)")
    plt.xlabel("Mean predicted probability")
    plt.ylabel("Empirical win rate")
    plt.tight_layout()
    plt.savefig(out_path, dpi=140)
    plt.close()
    return True


def generate_weekly_audit_pdf(engine: Engine, season: int, round_num: int, out_path: str) -> str:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(out_path, pagesize=letter)
    elems = []

    elems.append(Paragraph("NRL Edge Engine — Weekly Audit", styles["Title"]))
    elems.append(Paragraph(f"Season {season} — Round {round_num}", styles["Heading2"]))
    elems.append(Spacer(1, 12))

    preds = fetch_recent_predictions(engine, limit=80)
    slips = fetch_recent_slips(engine, limit=12)

    # Predictions table
    if preds:
        data = [["season", "rnd", "match", "p_fair", "p_cal", "model", "clv"]]
        for r in preds[:20]:
            data.append(
                [
                    r["season"],
                    r["round_num"],
                    str(r["match_id"])[:8],
                    f"{float(r['p_fair'] or 0):.3f}",
                    f"{float(r['calibrated_p'] or 0):.3f}",
                    str(r["model_version"] or "")[:18],
                    f"{float(r['clv_diff'] or 0):.3f}",
                ]
            )
        t = Table(data, hAlign="LEFT")
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        elems.append(Paragraph("Recent Predictions", styles["Heading2"]))
        elems.append(t)
        elems.append(Spacer(1, 12))

    # Reliability plot
    tmp_rel = os.path.join(tempfile.gettempdir(), "reliability.png")
    if _reliability_plot(preds, tmp_rel):
        elems.append(Paragraph("Reliability Diagram", styles["Heading2"]))
        elems.append(Image(tmp_rel, width=320, height=320))
        elems.append(Spacer(1, 12))

    # Styled slip cards
    if slips:
        elems.append(Paragraph("Styled Slip Cards", styles["Heading2"]))
<<<<<<< HEAD
        out_dir = os.path.dirname(out_path) or "."
        for s in slips[:8]:
            slip_obj = slip_obj_from_row(s)
            png_path = os.path.join(out_dir, f"slip_{slip_obj.portfolio_id[:8]}.png")
            generate_styled_summary_image(slip_obj, png_path)
            elems.append(Image(png_path, width=420, height=220))
            elems.append(Spacer(1, 12))

    doc.build(elems)
    return out_path


def default_report_path(season: int, round_num: int) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    os.makedirs("reports", exist_ok=True)
    return os.path.join("reports", f"audit_S{season}_R{round_num}_{ts}.pdf")
=======
        for s in slips[:8]:
            slip = _dict_to_slip(s)
            tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
            tmp.close()
            png = generate_styled_summary_image(slip, tmp.name)
            elems.append(Image(png, width=420, height=190))
            elems.append(Spacer(1, 10))

    doc.build(elems)
    return out_path
>>>>>>> origin/codex/2026-02-09-bootstrap-and-verify-nrl-edge-engine-v1.1

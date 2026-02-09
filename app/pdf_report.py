import os
from datetime import datetime
from reportlab.lib.pagesizes import LETTER
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

from .reporting import fetch_recent_predictions, fetch_recent_slips
from .stake_summary import generate_styled_summary_image, slip_obj_from_row


def generate_weekly_audit_pdf(engine, out_path: str, season: int, round_num: int) -> str:
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(out_path, pagesize=LETTER)
    elems = []

    elems.append(Paragraph(f"NRL Edge Engine — Weekly Audit (S{season} R{round_num})", styles["Title"]))
    elems.append(Spacer(1, 12))

    preds = fetch_recent_predictions(engine, limit=20)
    slips = fetch_recent_slips(engine, limit=10)

    # Predictions table
    elems.append(Paragraph("Recent Predictions", styles["Heading2"]))
    data = [["Season", "Round", "Match", "P(fair)", "P(cal)", "Model"]]
    for r in preds:
        data.append(
            [
                r["season"],
                r["round_num"],
                r["match_id"],
                f"{float(r['p_fair'] or 0):.3f}",
                f"{float(r.get('calibrated_p') or (r['p_fair'] or 0)):.3f}",
                r.get("model_version") or "",
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
    elems.append(t)
    elems.append(Spacer(1, 12))

    # Slips table
    elems.append(Paragraph("Recent Slips", styles["Heading2"]))
    sdata = [["Portfolio", "Market", "Stake(u)", "Status"]]
    for s in slips:
        sdata.append([s["portfolio_id"][:12], s["market"], f"{float(s['stake_units'] or 0):.2f}", s["status"]])
    st = Table(sdata, hAlign="LEFT")
    st.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ]
        )
    )
    elems.append(st)
    elems.append(Spacer(1, 12))

    # Styled slip cards
    if slips:
        elems.append(Paragraph("Styled Slip Cards", styles["Heading2"]))
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

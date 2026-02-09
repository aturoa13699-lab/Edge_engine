import os
import sys

import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

# Allow `from app...` imports when running from streamlit_app/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.stake_summary import generate_styled_summary  # noqa: E402
from app.types import Slip  # noqa: E402


def _engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        st.error("DATABASE_URL not set")
        st.stop()

    url = make_url(db_url)
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+psycopg")
    return create_engine(url, pool_pre_ping=True, future=True)


def _dict_to_slip(d):
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


st.set_page_config(page_title="NRL Edge Engine HUD", layout="wide")
st.title("NRL Edge Engine — HUD")

eng = _engine()

status = st.selectbox("Status", ["pending", "dry_run", "win", "loss", "void"], index=0)
limit = st.slider("Limit", 5, 50, 15)

rows = []
with eng.begin() as conn:
    rs = conn.execute(
        text(
            """
            SELECT slip_json
            FROM nrl.slips
            WHERE status=:st
            ORDER BY created_at DESC
            LIMIT :n
            """
        ),
        dict(st=status, n=limit),
    ).mappings().all()

for r in rs:
    sj = r["slip_json"]
    if isinstance(sj, str):
        import json
        sj = json.loads(sj)
    rows.append(sj)

if not rows:
    st.info("No slips found.")
else:
    for d in rows:
        slip = _dict_to_slip(d)
        html = generate_styled_summary(slip)
        st.markdown(html, unsafe_allow_html=True)

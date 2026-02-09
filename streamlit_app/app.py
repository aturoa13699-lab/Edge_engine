import streamlit as st

from app.db import get_engine
from app.reporting import fetch_recent_slips
from app.stake_summary import generate_styled_summary, slip_obj_from_row

st.set_page_config(page_title="NRL Edge Engine", layout="wide")

st.title("NRL Edge Engine — HUD")

engine = get_engine()

limit = st.slider("Recent slips", 5, 50, 15)
rows = fetch_recent_slips(engine, limit=limit)

if not rows:
    st.info("No slips found yet.")
else:
    for r in rows:
        slip = slip_obj_from_row(r)
        html = generate_styled_summary(slip)
        st.markdown(html, unsafe_allow_html=True)

"""Railpack entry point — delegates to Streamlit HUD via exec."""
import os

os.execvp("streamlit", [
    "streamlit", "run", "streamlit_app/hud.py",
    "--server.address=0.0.0.0",
    "--server.port=" + os.environ.get("PORT", "8501"),
    "--server.headless=true",
])

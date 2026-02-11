"""Thin launcher so Railpack auto-detection (main.py) starts Streamlit."""
import os

os.execvp("streamlit", [
    "streamlit", "run", "streamlit_app/hud.py",
    "--server.address=0.0.0.0",
    "--server.port=" + os.environ.get("PORT", "8501"),
    "--server.headless=true",
])

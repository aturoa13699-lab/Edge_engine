"""Railpack entry point — starts the Streamlit HUD server."""
import os
import sys

if __name__ == "__main__":
    from streamlit.web import cli as stcli

    sys.argv = [
        "streamlit", "run", "streamlit_app/hud.py",
        "--server.address=0.0.0.0",
        "--server.port=" + os.environ.get("PORT", "8501"),
        "--server.headless=true",
    ]
    sys.exit(stcli.main())

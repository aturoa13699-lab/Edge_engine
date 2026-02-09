import os
import logging
from fastapi import FastAPI
from sqlalchemy.engine import Engine

from .db import get_engine
from .calibration import fit_beta_calibrator

logger = logging.getLogger("nrl-pillar1")

app = FastAPI(title="NRL Edge Engine Admin API")


def _engine() -> Engine:
    return get_engine()


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/calibration/fit/{season}")
def fit_calibration(season: int):
    params = fit_beta_calibrator(_engine(), season)
    return {"ok": True, "params": params}


@app.get("/env")
def env():
    # limited debug
    return {
        "MODEL_VERSION": os.getenv("MODEL_VERSION", ""),
        "DEPLOY_SEASON": os.getenv("DEPLOY_SEASON", ""),
        "DEPLOY_ROUND": os.getenv("DEPLOY_ROUND", ""),
        "DRY_RUN": os.getenv("DRY_RUN", ""),
        "DRY_NOTIFY": os.getenv("DRY_NOTIFY", ""),
    }

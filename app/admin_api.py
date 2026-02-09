import logging
import os
from fastapi import FastAPI, HTTPException

from .db import get_engine

logger = logging.getLogger("nrl-pillar1")

app = FastAPI(title="NRL Edge Engine Admin API", version="1.1")


@app.get("/health")
def health():
    return {"ok": True, "version": "1.1"}


@app.post("/schema/apply")
def apply_schema():
    from .run import apply_schema as _apply

    engine = get_engine()
    _apply(engine)
    return {"ok": True}


@app.post("/calibration/fit/{season}")
def fit_calibration(season: int):
    from .calibration import fit_beta_calibrator

    engine = get_engine()
    params = fit_beta_calibrator(engine, season)
    if not params:
        raise HTTPException(status_code=400, detail="Not enough samples to fit calibration")
    return {"ok": True, "params": params}


@app.get("/model/champion")
def champion():
    from .model_registry import get_champion

    engine = get_engine()
    champ = get_champion(engine, model_key="nrl_h2h_xgb")
    return {"ok": True, "champion": champ}


@app.post("/train")
def train():
    from .model_trainer import train_model

    seasons = os.getenv("TRAIN_SEASONS", "2022,2023,2024,2025")
    seasons_list = [int(s.strip()) for s in seasons.split(",") if s.strip()]

    engine = get_engine()
    out = train_model(engine, seasons=seasons_list)
    if not out:
        raise HTTPException(status_code=400, detail="Training failed or insufficient data")
    return {"ok": True, "result": out}

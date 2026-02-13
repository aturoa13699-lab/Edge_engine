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
        raise HTTPException(
            status_code=400, detail="Not enough samples to fit calibration"
        )
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
        raise HTTPException(
            status_code=400, detail="Training failed or insufficient data"
        )
    return {"ok": True, "result": out}


@app.post("/seed/{season}")
def seed(season: int):
    from .seed_data import seed_all

    engine = get_engine()
    result = seed_all(engine, current_season=season)
    return {"ok": True, "result": result}


@app.post("/status")
def status():
    from .seed_data import get_table_counts

    engine = get_engine()
    counts = get_table_counts(engine)
    return {"ok": True, "counts": counts}


@app.get("/data-quality/status")
def data_quality_status():
    from .data_quality import run_data_quality_gate

    engine = get_engine()
    report = run_data_quality_gate(engine)
    return {"ok": report.ok, "report": report.to_dict()}


@app.post("/backfill/{season}")
def backfill(season: int):
    from .backfill import backfill_predictions

    engine = get_engine()
    result = backfill_predictions(engine, season=season)
    return {"ok": True, "result": result}


@app.post("/label-outcomes/{season}")
def label_outcomes_endpoint(season: int):
    from .backfill import label_outcomes

    engine = get_engine()
    result = label_outcomes(engine, season=season)
    return {"ok": True, "result": result}


@app.post("/backtest/{season}")
def backtest(season: int, bankroll: float = 1000.0):
    from .backtester import run_backtest

    engine = get_engine()
    result = run_backtest(engine, season=season, initial_bankroll=bankroll)
    return {"ok": True, "summary": result.summary(), "bets": result.round_results}


@app.post("/data/rectify-clean")
def rectify_clean(
    seasons: str = "2022,2023,2024,2025",
    source_name: str = "trusted_import",
    source_ref: str = "manual://unspecified",
    canary_path: str | None = None,
    authoritative_payload_path: str | None = None,
):
    from .data_rectify import rectify_historical_partitions

    engine = get_engine()
    seasons_list = [int(s.strip()) for s in seasons.split(",") if s.strip()]
    result = rectify_historical_partitions(
        engine,
        seasons=seasons_list,
        source_name=source_name,
        source_url_or_id=source_ref,
        canary_path=canary_path,
        authoritative_payload_path=authoritative_payload_path,
    )
    return {"ok": True, "result": result.to_dict()}

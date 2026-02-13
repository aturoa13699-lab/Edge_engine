import argparse
import logging
import os
from pathlib import Path

from .db import get_engine
from .logging_setup import setup_logging
from .sql_utils import split_sql_statements

logger = logging.getLogger("nrl-pillar1")


def _quality_gate_seasons(default_seasons: list[int] | None = None) -> list[int]:
    raw = os.getenv("QUALITY_GATE_SEASONS")
    if raw:
        return [int(s.strip()) for s in raw.split(",") if s.strip()]
    return default_seasons or [2022, 2023, 2024, 2025]


def _run_quality_gate(engine, seasons: list[int] | None = None):
    from .data_quality import enforce_data_quality_gate

    enforce_data_quality_gate(engine, seasons=seasons or _quality_gate_seasons())


def apply_schema(engine):
    schema_path = Path(__file__).parent / "sql" / "schema_pg.sql"
    sql = schema_path.read_text(encoding="utf-8")
    stmts = split_sql_statements(sql)
    with engine.begin() as conn:
        for stmt in stmts:
            conn.exec_driver_sql(stmt)


def cmd_scrapers(engine, season: int):
    # Example: add/expand to cover all scrapers you actually ship.
    from .scrapers.bom_weather_scraper import run as run_weather
    from .scrapers.referee_scraper_playwright import run as run_ref

    logger.info("Running scrapers for season=%s", season)
    run_weather(engine, season=season)
    run_ref(engine, season=season)


def cmd_train(engine, seasons: list[int]):
    from .model_trainer import train_model

    _run_quality_gate(engine, seasons=_quality_gate_seasons(seasons))
    train_model(engine, seasons=seasons)


def cmd_deploy(engine, season: int, round_num: int, dry_run: bool):
    from .deploy_engine import evaluate_round

    _run_quality_gate(engine)
    evaluate_round(engine, season=season, round_num=round_num, dry_run=dry_run)


def cmd_daily(engine, season: int, round_num: int, dry_run: bool):
    # daily = scrapers + deploy + optional notify
    cmd_scrapers(engine, season=season)
    cmd_deploy(engine, season=season, round_num=round_num, dry_run=dry_run)

    # notify slips (default off in dry-run unless DRY_NOTIFY=1)
    dry_notify = os.getenv("DRY_NOTIFY", "0").strip() == "1"
    if dry_run and not dry_notify:
        logger.info("Dry-run: skipping notifications (set DRY_NOTIFY=1 to force)")
        return

    from .notify_slips import send_round_slip_cards

    status = "dry_run" if dry_run else "pending"
    send_round_slip_cards(engine, season=season, round_num=round_num, status=status)


def cmd_report(engine, season: int, round_num: int, out_path: str):
    from .pdf_report import generate_weekly_audit_pdf

    generate_weekly_audit_pdf(
        engine, season=season, round_num=round_num, out_path=out_path
    )


def cmd_fit_calibration(engine, season: int):
    from .calibration import fit_beta_calibrator

    _run_quality_gate(engine, seasons=_quality_gate_seasons([season]))
    fit_beta_calibrator(engine, season)


def cmd_backfill(engine, season: int, rounds: list[int] | None):
    from .backfill import backfill_predictions

    backfill_predictions(engine, season=season, rounds=rounds)


def cmd_label_outcomes(engine, season: int):
    from .backfill import label_outcomes

    label_outcomes(engine, season=season)


def cmd_backtest(engine, season: int, rounds: list[int] | None, bankroll: float):
    from .backtester import run_backtest

    _run_quality_gate(engine, seasons=_quality_gate_seasons([season]))
    result = run_backtest(
        engine, season=season, rounds=rounds, initial_bankroll=bankroll
    )
    return result


def cmd_seed(engine, season: int):
    from .data_rectify import rectify_historical_partitions
    from .seed_data import seed_all

    historical = [2022, 2023, 2024, 2025]
    seed_all(engine, historical_seasons=historical, current_season=season)
    rectify_historical_partitions(
        engine,
        seasons=historical + [season],
        source_name="seed_all",
        source_url_or_id="internal://seed_all",
    )


def cmd_init(engine):
    apply_schema(engine)


def cmd_schema_parity_smoke(engine):
    from .schema_parity import enforce_truth_schema_parity_smoke

    return enforce_truth_schema_parity_smoke(engine)


def cmd_rectify_clean(
    engine,
    seasons: list[int],
    source_name: str,
    source_url_or_id: str,
    canary_path: str | None,
    authoritative_payload_path: str | None,
):
    from .data_rectify import rectify_historical_partitions

    return rectify_historical_partitions(
        engine,
        seasons=seasons,
        source_name=source_name,
        source_url_or_id=source_url_or_id,
        canary_path=canary_path,
        authoritative_payload_path=authoritative_payload_path,
    )


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "command",
        choices=[
            "init",
            "full",
            "daily",
            "scrapers",
            "deploy",
            "train",
            "report",
            "fit-calibration",
            "backfill",
            "label-outcomes",
            "backtest",
            "seed",
            "rectify-clean",
            "schema-parity-smoke",
        ],
    )
    ap.add_argument(
        "--season", type=int, default=int(os.getenv("DEPLOY_SEASON", "2026"))
    )
    ap.add_argument(
        "--round",
        dest="round_num",
        type=int,
        default=int(os.getenv("DEPLOY_ROUND", "1")),
    )
    ap.add_argument(
        "--rounds",
        type=str,
        default=None,
        help="Comma-separated round numbers for backfill/backtest (default: all)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Persist artifacts but mark slips as dry_run; skip notify by default",
    )
    ap.add_argument("--seasons", type=str, default="2022,2023,2024,2025")
    ap.add_argument(
        "--bankroll",
        type=float,
        default=float(os.getenv("BANKROLL", "1000")),
        help="Initial bankroll for backtesting",
    )
    ap.add_argument("--out", type=str, default="reports/weekly_audit.pdf")
    ap.add_argument(
        "--source-name",
        type=str,
        default=os.getenv("RECTIFY_SOURCE_NAME", "trusted_import"),
    )
    ap.add_argument(
        "--source-ref",
        type=str,
        default=os.getenv("RECTIFY_SOURCE_REF", "manual://unspecified"),
    )
    ap.add_argument(
        "--canary-path",
        type=str,
        default=os.getenv("RECTIFY_CANARY_PATH"),
    )
    ap.add_argument(
        "--authoritative-payload-path",
        type=str,
        default=os.getenv("RECTIFY_AUTHORITATIVE_PAYLOAD_PATH"),
    )
    return ap.parse_args()


def cmd_full(engine, args):
    cmd_init(engine)
    cmd_seed(engine, season=args.season)
    cmd_scrapers(engine, season=args.season)
    cmd_train(
        engine, seasons=[int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    )
    cmd_deploy(
        engine, season=args.season, round_num=args.round_num, dry_run=args.dry_run
    )
    cmd_report(engine, season=args.season, round_num=args.round_num, out_path=args.out)


def main():
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    args = parse_args()
    engine = get_engine()

    rounds_list = None
    if args.rounds:
        rounds_list = [int(r.strip()) for r in args.rounds.split(",") if r.strip()]

    if args.command == "init":
        cmd_init(engine)
    elif args.command == "full":
        cmd_full(engine, args)
    elif args.command == "scrapers":
        cmd_scrapers(engine, season=args.season)
    elif args.command == "deploy":
        cmd_deploy(
            engine, season=args.season, round_num=args.round_num, dry_run=args.dry_run
        )
    elif args.command == "daily":
        cmd_daily(
            engine, season=args.season, round_num=args.round_num, dry_run=args.dry_run
        )
    elif args.command == "train":
        cmd_train(
            engine,
            seasons=[int(s.strip()) for s in args.seasons.split(",") if s.strip()],
        )
    elif args.command == "report":
        cmd_report(
            engine, season=args.season, round_num=args.round_num, out_path=args.out
        )
    elif args.command == "fit-calibration":
        cmd_fit_calibration(engine, season=args.season)
    elif args.command == "backfill":
        cmd_backfill(engine, season=args.season, rounds=rounds_list)
    elif args.command == "label-outcomes":
        cmd_label_outcomes(engine, season=args.season)
    elif args.command == "backtest":
        cmd_backtest(
            engine, season=args.season, rounds=rounds_list, bankroll=args.bankroll
        )
    elif args.command == "seed":
        cmd_seed(engine, season=args.season)
    elif args.command == "schema-parity-smoke":
        cmd_schema_parity_smoke(engine)
    elif args.command == "rectify-clean":
        cmd_rectify_clean(
            engine,
            seasons=[int(s.strip()) for s in args.seasons.split(",") if s.strip()],
            source_name=args.source_name,
            source_url_or_id=args.source_ref,
            canary_path=args.canary_path,
            authoritative_payload_path=args.authoritative_payload_path,
        )


if __name__ == "__main__":
    main()

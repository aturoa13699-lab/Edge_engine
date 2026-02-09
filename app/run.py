import argparse
import logging
import os
from pathlib import Path

from .db import get_engine
from .logging_setup import setup_logging
from .sql_utils import split_sql_statements

logger = logging.getLogger("nrl-pillar1")


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

    train_model(engine, seasons=seasons)


def cmd_deploy(engine, season: int, round_num: int, dry_run: bool):
    from .deploy_engine import evaluate_round

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

    generate_weekly_audit_pdf(engine, season=season, round_num=round_num, out_path=out_path)


def cmd_fit_calibration(engine, season: int):
    from .calibration import fit_beta_calibrator

    fit_beta_calibrator(engine, season)


def cmd_init(engine):
    apply_schema(engine)


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "command",
        choices=["init", "full", "daily", "scrapers", "deploy", "train", "report", "fit-calibration"],
    )
    ap.add_argument("--season", type=int, default=int(os.getenv("DEPLOY_SEASON", "2026")))
    ap.add_argument("--round", dest="round_num", type=int, default=int(os.getenv("DEPLOY_ROUND", "1")))
    ap.add_argument("--dry-run", action="store_true", help="Persist artifacts but mark slips as dry_run; skip notify by default")
    ap.add_argument("--seasons", type=str, default="2022,2023,2024,2025")
    ap.add_argument("--out", type=str, default="reports/weekly_audit.pdf")
    return ap.parse_args()


def cmd_full(engine, args):
    cmd_init(engine)
    cmd_scrapers(engine, season=args.season)
    cmd_train(engine, seasons=[int(s.strip()) for s in args.seasons.split(",") if s.strip()])
    cmd_deploy(engine, season=args.season, round_num=args.round_num, dry_run=args.dry_run)
    cmd_report(engine, season=args.season, round_num=args.round_num, out_path=args.out)


def main():
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    args = parse_args()
    engine = get_engine()

    if args.command == "init":
        cmd_init(engine)
    elif args.command == "full":
        cmd_full(engine, args)
    elif args.command == "scrapers":
        cmd_scrapers(engine, season=args.season)
    elif args.command == "deploy":
        cmd_deploy(engine, season=args.season, round_num=args.round_num, dry_run=args.dry_run)
    elif args.command == "daily":
        cmd_daily(engine, season=args.season, round_num=args.round_num, dry_run=args.dry_run)
    elif args.command == "train":
        cmd_train(engine, seasons=[int(s.strip()) for s in args.seasons.split(",") if s.strip()])
    elif args.command == "report":
        cmd_report(engine, season=args.season, round_num=args.round_num, out_path=args.out)
    elif args.command == "fit-calibration":
        cmd_fit_calibration(engine, season=args.season)


if __name__ == "__main__":
    main()

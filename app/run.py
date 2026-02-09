import os
import logging

from sqlalchemy import text as sql_text

from .logging_setup import setup_logging
from .db import get_engine
from .model_trainer import train_model
from .deploy_engine import deploy_round
from .notify_slips import send_round_slip_cards
from .pdf_report import generate_weekly_audit_pdf, default_report_path

logger = logging.getLogger("nrl-pillar1")


def apply_schema(engine) -> None:
    sql_path = os.path.join(os.path.dirname(__file__), "sql", "schema_pg.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with engine.begin() as conn:
        conn.execute(sql_text(sql))
    logger.info("Schema applied")


def cmd_full():
    engine = get_engine()
    apply_schema(engine)

    season = int(os.getenv("DEPLOY_SEASON", "2026"))
    round_num = int(os.getenv("DEPLOY_ROUND", "1"))
    dry_run = os.getenv("DRY_RUN", "1") == "1"

    train_model(engine)
    slips = deploy_round(engine, season, round_num, dry_run=dry_run)

    send_round_slip_cards(engine, season, round_num, status="pending")

    out_path = default_report_path(season, round_num)
    generate_weekly_audit_pdf(engine, out_path, season, round_num)
    logger.info(f"Weekly audit PDF generated at {out_path}")

    logger.info(f"Full run complete: {len(slips)} slips")


def cmd_scrapers():
    logger.info("scrapers command is currently a stub in this goldmaster bundle.")


def cmd_train():
    engine = get_engine()
    apply_schema(engine)
    train_model(engine)


def cmd_deploy():
    engine = get_engine()
    apply_schema(engine)
    season = int(os.getenv("DEPLOY_SEASON", "2026"))
    round_num = int(os.getenv("DEPLOY_ROUND", "1"))
    dry_run = os.getenv("DRY_RUN", "1") == "1"
    deploy_round(engine, season, round_num, dry_run=dry_run)


def main():
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    cmd = (os.getenv("RUN_MODE") or "full").lower()
    if len(os.sys.argv) > 1:
        cmd = os.sys.argv[1].lower()

    if cmd == "full":
        cmd_full()
    elif cmd == "scrapers":
        cmd_scrapers()
    elif cmd == "train":
        cmd_train()
    elif cmd == "deploy":
        cmd_deploy()
    else:
        raise SystemExit(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()

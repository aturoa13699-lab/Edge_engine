"""NRL Edge Engine — Operational Dashboard."""

import json
import os
import sys
import traceback

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from engine.stake_summary import generate_styled_summary  # noqa: E402
from engine.types import Slip  # noqa: E402

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def _engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        st.error("DATABASE_URL not set — add it in Railway service variables or .env")
        st.stop()
    url = make_url(db_url)
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+psycopg")
    return create_engine(url, pool_pre_ping=True, future=True)


def _safe_scalar(engine, sql, params=None):
    try:
        with engine.begin() as conn:
            row = conn.execute(text(sql), params or {}).scalar()
        return row
    except Exception:
        return None


def _get_table_counts(engine):
    tables = [
        "matches_raw",
        "odds",
        "team_ratings",
        "coach_profile",
        "injuries_current",
        "weather_daily",
        "model_prediction",
        "slips",
        "calibration_params",
        "model_registry",
    ]
    counts = {}
    for t in tables:
        val = _safe_scalar(engine, f"SELECT count(*) FROM nrl.{t}")
        counts[t] = int(val) if val is not None else -1
    return counts


def _dict_to_slip(d):
    return Slip(
        portfolio_id=d["portfolio_id"],
        season=int(d["season"]),
        round_num=int(d["round_num"]),
        match_id=d["match_id"],
        home_team=d["home_team"],
        away_team=d["away_team"],
        market=d["market"],
        selection=d["selection"],
        odds=float(d["odds"]),
        stake=float(d["stake"]),
        ev=float(d["ev"]),
        status=d.get("status", "pending"),
        model_version=d.get("model_version", "v2026-02-poisson-v1"),
        reason=d.get("reason"),
        ml_status=d.get("ml_status", "heuristic"),
        decision=d.get("decision", "RECO"),
        decline_reason=d.get("decline_reason"),
        stake_ladder_level=d.get("stake_ladder_level"),
    )


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(page_title="NRL Edge Engine", layout="wide")

eng = _engine()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("NRL Edge Engine")
    st.caption("v1.1 — Goldmaster CML")
    st.divider()
    season = st.number_input(
        "Season", min_value=2020, max_value=2030, value=2026, step=1
    )
    round_num = st.number_input("Round", min_value=1, max_value=30, value=1, step=1)
    st.divider()

    db_ok = _safe_scalar(eng, "SELECT 1")
    if db_ok:
        st.success("DB Connected")
    else:
        st.error("DB Offline")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_status, tab_pipeline, tab_backtest, tab_slips = st.tabs(
    ["Status", "Pipeline", "Backtest", "Slips"]
)

# ============================== STATUS TAB =================================

with tab_status:
    st.header("System Status")

    counts = _get_table_counts(eng)

    col1, col2, col3, col4, col5 = st.columns(5)

    def _metric(col, label, table_name):
        val = counts.get(table_name, -1)
        if val == -1:
            col.metric(label, "N/A", help="Table may not exist — run Init")
        elif val == 0:
            col.metric(label, "0", delta="Empty", delta_color="off")
        else:
            col.metric(label, f"{val:,}")

    _metric(col1, "Matches", "matches_raw")
    _metric(col2, "Odds", "odds")
    _metric(col3, "Team Ratings", "team_ratings")
    _metric(col4, "Predictions", "model_prediction")
    _metric(col5, "Slips", "slips")

    col6, col7, col8, col9, col10 = st.columns(5)
    _metric(col6, "Coach Profiles", "coach_profile")
    _metric(col7, "Injuries", "injuries_current")
    _metric(col8, "Weather", "weather_daily")
    _metric(col9, "Calibration", "calibration_params")
    _metric(col10, "Model Registry", "model_registry")

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Champion Model")
        try:
            from engine.model_registry import get_champion

            champ = get_champion(eng, "nrl_h2h_xgb")
            if champ:
                st.write(f"**Version:** `{champ['version']}`")
                st.write(f"**Created:** {champ['created_at']}")
                metrics = champ.get("metrics", {})
                st.write(f"**Brier Score:** {metrics.get('cv_brier_mean', 'N/A')}")
                st.write(f"**Log Loss:** {metrics.get('cv_logloss_mean', 'N/A')}")
            else:
                st.info("No champion model — run Train in the Pipeline tab.")
        except Exception:
            st.info("Run Init to set up the database schema first.")

    with c2:
        st.subheader("Calibration")
        try:
            from engine.reporting import fetch_calibration_for_season

            cal = fetch_calibration_for_season(eng, season)
            if cal:
                fitted_on = cal.get("fitted_on", "N/A")
                st.write(f"**Fitted on season:** {fitted_on}")
                st.write(f"**a:** {cal.get('a', 'N/A'):.4f}")
                st.write(f"**b:** {cal.get('b', 'N/A'):.4f}")
                st.write(f"**Brier Loss:** {cal.get('brier_loss', 'N/A'):.5f}")
            else:
                st.info(
                    "No calibration fitted yet. Run the Pipeline to fit calibration."
                )
        except Exception:
            st.info("Run Init to set up the database schema first.")

    st.divider()
    st.subheader(f"Matches — Season {season}")
    try:
        with eng.begin() as conn:
            rows = (
                conn.execute(
                    text("""
                    SELECT round_num, match_date, home_team, away_team,
                           home_score, away_score
                    FROM nrl.matches_raw
                    WHERE season = :s
                    ORDER BY round_num, match_date
                    LIMIT 50
                """),
                    dict(s=season),
                )
                .mappings()
                .all()
            )
        if rows:
            df = pd.DataFrame([dict(r) for r in rows])
            df.columns = ["Round", "Date", "Home", "Away", "H Score", "A Score"]
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No matches found. Seed data in the Pipeline tab.")
    except Exception:
        st.info("Tables not initialised — run Init in Pipeline.")


# ============================== PIPELINE TAB ===============================

with tab_pipeline:
    st.header("Pipeline")

    st.markdown(
        "Run these steps **in order** to set up the engine. "
        "Once pre-season setup is done, use **Match Day** buttons each round."
    )

    # --- Pre-Season Setup ---
    st.subheader("Pre-Season Setup")

    p1, p2, p3 = st.columns(3)

    with p1:
        if st.button("1. Init Database", use_container_width=True):
            with st.spinner("Applying schema..."):
                try:
                    from engine.run import apply_schema

                    apply_schema(eng)
                    st.success("Schema applied.")
                except Exception as e:
                    st.error(f"Init failed: {e}")

    with p2:
        if st.button("2. Seed Data (2022-2025 + 2026)", use_container_width=True):
            with st.spinner("Seeding historical data..."):
                try:
                    from engine.seed_data import seed_all

                    result = seed_all(
                        eng,
                        historical_seasons=[2022, 2023, 2024, 2025],
                        current_season=season,
                    )
                    st.success(f"Seeded: {result}")
                except Exception as e:
                    st.error(f"Seed failed: {e}")

    with p3:
        if st.button("3. Train ML Model", use_container_width=True):
            with st.spinner("Training XGBoost model (may take a minute)..."):
                try:
                    from engine.model_trainer import train_model

                    out = train_model(eng, seasons=[2022, 2023, 2024, 2025])
                    if out:
                        m = out["metrics"]
                        st.success(
                            f"Trained: {out['version']} | "
                            f"Brier: {m['cv_brier_mean']:.5f} | "
                            f"Champion: {out['promoted_to_champion']}"
                        )
                    else:
                        st.warning("Training failed — seed data first.")
                except Exception as e:
                    st.error(f"Train failed: {e}")

    p4, p5, p6 = st.columns(3)

    with p4:
        if st.button("4. Backfill Predictions", use_container_width=True):
            with st.spinner("Backfilling predictions..."):
                try:
                    from engine.backfill import backfill_predictions

                    total_bf = 0
                    for s in [2022, 2023, 2024, 2025]:
                        r = backfill_predictions(eng, season=s)
                        total_bf += r["backfilled"]
                    st.success(f"Backfilled {total_bf} predictions across 2022-2025.")
                except Exception as e:
                    st.error(f"Backfill failed: {e}")

    with p5:
        if st.button("5. Fit Calibration", use_container_width=True):
            with st.spinner("Fitting beta calibrator for all historical seasons..."):
                try:
                    from engine.calibration import fit_beta_calibrator

                    fitted_any = False
                    for cal_season in [2022, 2023, 2024, 2025]:
                        params = fit_beta_calibrator(eng, cal_season)
                        if params:
                            st.success(
                                f"Calibrated S{cal_season}: "
                                f"a={params['a']:.3f} b={params['b']:.3f} "
                                f"(Brier={params['brier_loss']:.5f})"
                            )
                            fitted_any = True
                    if not fitted_any:
                        st.warning(
                            "Need 80+ labelled predictions per season. Backfill first."
                        )
                except Exception as e:
                    st.error(f"Calibration failed: {e}")

    with p6:
        if st.button("6. Validate (Backtest)", use_container_width=True):
            with st.spinner("Running backtest..."):
                try:
                    from engine.backtester import run_backtest

                    bt = run_backtest(eng, season=season - 1, initial_bankroll=1000.0)
                    st.session_state["backtest_result"] = bt.summary()
                    st.session_state["backtest_bets"] = bt.round_results
                    s = bt.summary()
                    st.success(
                        f"Backtest S{season - 1}: {s['total_bets']} bets | "
                        f"ROI: {s['roi_pct']}% | Hit: {s['hit_rate_pct']}% | "
                        f"P&L: ${s['total_pnl']:.2f}"
                    )
                except Exception as e:
                    st.error(f"Backtest failed: {e}")

    st.divider()

    # --- Match Day ---
    st.subheader(f"Match Day — Season {season}, Round {round_num}")

    m1, m2 = st.columns(2)

    with m1:
        if st.button("Run Scrapers", use_container_width=True):
            with st.spinner("Running scrapers..."):
                try:
                    from engine.run import cmd_scrapers

                    cmd_scrapers(eng, season=season)
                    st.success("Scrapers complete.")
                except Exception as e:
                    st.error(f"Scrapers failed: {e}")

    with m2:
        if st.button(f"Deploy Round {round_num}", use_container_width=True):
            with st.spinner(f"Evaluating R{round_num}..."):
                try:
                    from engine.deploy_engine import evaluate_round

                    evaluate_round(
                        eng, season=season, round_num=round_num, dry_run=False
                    )
                    st.success(f"Round {round_num} deployed! Check the Slips tab.")
                except Exception as e:
                    st.error(f"Deploy failed: {e}")

    st.divider()

    # --- One-Click Full Pipeline ---
    st.subheader("Full Pre-Season Pipeline")
    st.caption(
        "Runs: Init > Seed > Train > Backfill > Calibrate "
        "— everything you need before the season starts."
    )

    if st.button("Run Full Pre-Season Setup", type="primary", use_container_width=True):
        status_box = st.status("Running full pipeline...", expanded=True)
        try:
            status_box.write("1/5 — Initializing database...")
            from engine.run import apply_schema

            apply_schema(eng)

            status_box.write("2/5 — Seeding historical data...")
            from engine.seed_data import seed_all

            seed_result = seed_all(
                eng,
                historical_seasons=[2022, 2023, 2024, 2025],
                current_season=season,
            )
            status_box.write(
                f"   Seeded: {seed_result['matches']} matches, {seed_result['odds']} odds"
            )

            status_box.write("3/5 — Training ML model...")
            from engine.model_trainer import train_model

            train_out = train_model(eng, seasons=[2022, 2023, 2024, 2025])
            if train_out:
                status_box.write(
                    f"   Model: {train_out['version']} "
                    f"(Brier: {train_out['metrics']['cv_brier_mean']:.5f})"
                )
            else:
                status_box.write("   Training skipped (insufficient data)")

            status_box.write("4/5 — Backfilling predictions...")
            from engine.backfill import backfill_predictions

            for s in [2022, 2023, 2024, 2025]:
                bf = backfill_predictions(eng, season=s)
                status_box.write(f"   S{s}: {bf['backfilled']} backfilled")

            status_box.write("5/5 — Fitting calibration for all historical seasons...")
            from engine.calibration import fit_beta_calibrator

            cal_fitted = 0
            for cal_season in [2022, 2023, 2024, 2025]:
                cal = fit_beta_calibrator(eng, cal_season)
                if cal:
                    status_box.write(
                        f"   S{cal_season}: a={cal['a']:.3f} b={cal['b']:.3f}"
                    )
                    cal_fitted += 1
                else:
                    status_box.write(f"   S{cal_season}: skipped (insufficient data)")
            if cal_fitted == 0:
                status_box.write("   No calibration fitted (not enough labelled data)")

            status_box.update(label="Pipeline complete!", state="complete")
        except Exception as e:
            status_box.update(label=f"Pipeline failed: {e}", state="error")
            status_box.write(traceback.format_exc())


# ============================== BACKTEST TAB ===============================

with tab_backtest:
    st.header("Backtest")

    bc1, bc2 = st.columns([1, 1])
    with bc1:
        bt_season = st.selectbox("Backtest Season", [2022, 2023, 2024, 2025], index=3)
    with bc2:
        bt_bankroll = st.number_input(
            "Starting Bankroll ($)", value=1000.0, min_value=100.0, step=100.0
        )

    if st.button("Run Backtest", type="primary", use_container_width=True):
        with st.spinner(f"Backtesting season {bt_season}..."):
            try:
                from engine.backtester import run_backtest

                bt = run_backtest(eng, season=bt_season, initial_bankroll=bt_bankroll)
                st.session_state["backtest_result"] = bt.summary()
                st.session_state["backtest_bets"] = bt.round_results
            except Exception as e:
                st.error(f"Backtest failed: {e}")

    if "backtest_result" in st.session_state and st.session_state["backtest_result"]:
        s = st.session_state["backtest_result"]
        st.divider()

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("ROI", f"{s['roi_pct']}%")
        k2.metric("Hit Rate", f"{s['hit_rate_pct']}%")
        k3.metric("P&L", f"${s['total_pnl']:.2f}")
        k4.metric("Avg Brier", f"{s['avg_brier_score']:.5f}")

        k5, k6, k7, k8 = st.columns(4)
        k5.metric("Total Bets", s["total_bets"])
        k6.metric("Wins / Losses", f"{s['wins']} / {s['losses']}")
        k7.metric("Final Bankroll", f"${s['final_bankroll']:.2f}")
        k8.metric("Max Drawdown", f"{s['max_drawdown_pct']}%")

        bets = st.session_state.get("backtest_bets", [])
        if bets:
            st.divider()
            st.subheader("Bet Details")
            df = pd.DataFrame(bets)
            df.columns = [
                "Match",
                "Round",
                "Home",
                "Away",
                "P(cal)",
                "Odds",
                "Stake",
                "Result",
                "P&L",
                "Bankroll",
            ]
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "P&L": st.column_config.NumberColumn(format="$%.2f"),
                    "Stake": st.column_config.NumberColumn(format="$%.2f"),
                    "Bankroll": st.column_config.NumberColumn(format="$%.2f"),
                },
            )
    else:
        st.info(
            "Click **Run Backtest** to see results. "
            "Make sure data is seeded and a model is trained first."
        )


# ============================== SLIPS TAB ==================================

with tab_slips:
    st.header("Betting Slips")

    sc1, sc2, sc3 = st.columns([1, 1, 1])
    with sc1:
        slip_status = st.selectbox(
            "Filter by Status", ["pending", "dry_run", "win", "loss", "void"], index=0
        )
    with sc2:
        slip_decision = st.selectbox(
            "Filter by Decision", ["ALL", "RECO", "DECLINED"], index=0
        )
    with sc3:
        slip_limit = st.slider("Limit", 5, 50, 15)

    rows = []
    try:
        query_parts = [
            "SELECT slip_json, decision, ml_status, stake_ladder_level FROM nrl.slips WHERE status = :st"
        ]
        query_params: dict = {"st": slip_status, "n": slip_limit}
        if slip_decision != "ALL":
            query_parts.append("AND decision = :dec")
            query_params["dec"] = slip_decision
        query_parts.append("ORDER BY created_at DESC LIMIT :n")

        with eng.begin() as conn:
            rs = (
                conn.execute(
                    text(" ".join(query_parts)),
                    query_params,
                )
                .mappings()
                .all()
            )

        for r in rs:
            sj = r["slip_json"]
            if isinstance(sj, str):
                sj = json.loads(sj)
            rows.append(sj)
    except Exception:
        st.info("Tables not initialised — run Init in the Pipeline tab.")
        rows = []

    if not rows:
        st.info("No slips found for this filter. Deploy a round to generate slips.")
    else:
        for d in rows:
            slip = _dict_to_slip(d)
            decision_icon = "RECO" if slip.decision == "RECO" else "DECLINED"
            ladder_label = slip.stake_ladder_level or ""
            label = (
                f"[{decision_icon}] {slip.home_team} v {slip.away_team} | "
                f"{slip.selection} | ${slip.stake:.2f} @ {slip.odds:.2f}"
                f"{(' | ' + ladder_label) if ladder_label else ''}"
            )
            with st.expander(label, expanded=False):
                html = generate_styled_summary(slip)
                st.markdown(html, unsafe_allow_html=True)

                # Decision & ML status badges
                d1, d2, d3 = st.columns(3)
                d1.metric("Decision", slip.decision)
                d2.metric("ML Status", slip.ml_status)
                d3.metric("Stake Ladder", slip.stake_ladder_level or "N/A")

                if slip.decline_reason:
                    st.warning(f"Decline reason: {slip.decline_reason}")

                if slip.reason:
                    st.divider()
                    st.caption("Model Debug")
                    st.code(slip.reason, language=None)

from engine.backtester import BacktestResult


def test_backtest_result_empty():
    r = BacktestResult()
    s = r.summary()
    assert s["total_bets"] == 0
    assert s["roi_pct"] == 0.0
    assert s["hit_rate_pct"] == 0.0
    assert s["avg_brier_score"] == 0.0


def test_backtest_result_with_wins():
    r = BacktestResult(
        total_bets=10,
        wins=6,
        losses=4,
        initial_bankroll=1000.0,
        final_bankroll=1150.0,
        peak_bankroll=1200.0,
        total_staked=500.0,
        total_pnl=150.0,
        brier_scores=[0.1, 0.2, 0.15, 0.25, 0.05],
    )
    s = r.summary()
    assert s["total_bets"] == 10
    assert s["wins"] == 6
    assert s["losses"] == 4
    assert s["hit_rate_pct"] == 60.0
    assert s["roi_pct"] == 30.0
    assert s["total_pnl"] == 150.0
    assert s["final_bankroll"] == 1150.0
    assert s["peak_bankroll"] == 1200.0
    assert 0 < s["avg_brier_score"] < 1.0


def test_backtest_result_drawdown():
    r = BacktestResult(
        peak_bankroll=1200.0,
        max_drawdown=0.25,
    )
    s = r.summary()
    assert s["max_drawdown_pct"] == 25.0


def test_backtest_result_roi_no_stakes():
    r = BacktestResult(total_staked=0.0, total_pnl=0.0)
    assert r.roi == 0.0


def test_backtest_result_hit_rate_no_bets():
    r = BacktestResult(total_bets=0)
    assert r.hit_rate == 0.0

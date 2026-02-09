from app.risk import kelly_fraction, size_stake


def test_kelly_fraction_no_edge():
    # p=0.5 at odds=2.0 => f = 0
    assert kelly_fraction(0.5, 2.0) == 0.0


def test_kelly_fraction_positive_edge():
    # p=0.6 at odds=2.0 => b=1 => f=(1*0.6-0.4)/1=0.2
    assert abs(kelly_fraction(0.6, 2.0) - 0.2) < 1e-9


def test_size_stake_caps():
    d = size_stake(bankroll=1000, p=0.8, odds=2.0, max_frac=0.05)
    assert d.stake <= 50.0

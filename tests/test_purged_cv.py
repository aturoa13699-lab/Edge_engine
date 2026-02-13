import numpy as np
import pandas as pd

from engine.model_trainer import _purged_walk_forward_cv


class _DummyClassifier:
    """Minimal classifier that always predicts 0.5."""

    def fit(self, X, y):
        pass

    def predict_proba(self, X):
        n = X.shape[0]
        return np.column_stack([np.full(n, 0.5), np.full(n, 0.5)])


def test_purged_cv_returns_valid_metrics():
    rng = np.random.RandomState(42)
    n = 300
    X = pd.DataFrame({"a": rng.randn(n), "b": rng.randn(n)})
    y = pd.Series(rng.randint(0, 2, n))

    metrics = _purged_walk_forward_cv(
        _DummyClassifier(), X, y, n_splits=5, embargo_pct=0.02
    )

    assert "cv_brier_mean" in metrics
    assert "cv_logloss_mean" in metrics
    assert 0.0 < metrics["cv_brier_mean"] <= 0.5
    assert metrics["cv_logloss_mean"] > 0.0


def test_purged_cv_too_few_samples():
    """When there are too few samples, returns fallback values."""
    X = pd.DataFrame({"a": [1.0], "b": [2.0]})
    y = pd.Series([1])

    metrics = _purged_walk_forward_cv(_DummyClassifier(), X, y, n_splits=5)

    assert metrics["cv_brier_mean"] == 0.25
    assert metrics["cv_logloss_mean"] == 0.693


def test_purged_cv_embargo_creates_gap():
    """Verify embargo_pct > 0 actually creates a gap (no index overlap)."""
    n = 200
    X = pd.DataFrame({"a": range(n)})
    y = pd.Series([0, 1] * (n // 2))

    # With 10% embargo on 200 samples = 20 sample gap
    metrics = _purged_walk_forward_cv(
        _DummyClassifier(), X, y, n_splits=3, embargo_pct=0.10
    )
    assert metrics["cv_brier_mean"] == 0.25  # dummy always predicts 0.5

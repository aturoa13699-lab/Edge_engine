from unittest.mock import MagicMock

from engine.backfill import backfill_predictions


def test_backfill_empty_season():
    """Backfill returns zeros when no resolved matches exist."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.mappings.return_value.all.return_value = []

    result = backfill_predictions(mock_engine, season=9999)
    assert result["season"] == 9999
    assert result["backfilled"] == 0
    assert result["skipped"] == 0

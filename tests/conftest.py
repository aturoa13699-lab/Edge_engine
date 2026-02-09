import os
import sys

import pytest
from sqlalchemy import create_engine, text

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture()
def sqlite_engine():
    eng = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE model_prediction (season integer, p_fair real, outcome_known integer, outcome_home_win integer)"))
        conn.execute(text("CREATE TABLE calibration_params (season integer PRIMARY KEY, params text, fitted_at text)"))
    return eng

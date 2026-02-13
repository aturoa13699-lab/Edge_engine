from __future__ import annotations

import compileall
from pathlib import Path


def test_engine_compile_smoke() -> None:
    engine_dir = Path(__file__).resolve().parents[1] / "engine"
    assert compileall.compile_dir(str(engine_dir), quiet=1)

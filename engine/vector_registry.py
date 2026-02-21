"""Canonical vector registry — exposes the locked list of NRL vectors.

The registry is loaded from the lock file at import time.  Any code that
needs to enumerate atomic, hybrid or context vector names should import
from this module rather than hard-coding names.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

_REGISTRY_PATH = (
    Path(__file__).parent
    / "static"
    / "registry"
    / "nrl.global_vector_registry.v1_0.lock.json"
)
_METRIC_DICT_PATH = (
    Path(__file__).parent
    / "static"
    / "registry"
    / "nrl.vector_metric_dictionary.v1_0.lock.json"
)

_registry: Dict = json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))
_metric_dict: Dict = json.loads(_METRIC_DICT_PATH.read_text(encoding="utf-8"))


# ── Public API ──────────────────────────────────────────────────────────────


def atomic_vector_names() -> List[str]:
    """Return the canonical list of atomic vector names."""
    return [v["name"] for v in _registry["atomic_vectors"]]


def hybrid_vector_names() -> List[str]:
    """Return the canonical list of hybrid vector names."""
    return [v["name"] for v in _registry["hybrid_vectors"]]


def context_driver_names() -> List[str]:
    """Return the canonical list of context driver names."""
    return [v["name"] for v in _registry["context_drivers"]]


def all_vector_names() -> List[str]:
    """Return all vector names (atomic + hybrid + context)."""
    return atomic_vector_names() + hybrid_vector_names() + context_driver_names()


def registry_version() -> str:
    return _registry["version"]


def metric_dictionary() -> Dict:
    """Return the full metric dictionary."""
    return _metric_dict

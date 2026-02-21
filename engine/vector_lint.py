"""Vector registry lint — enforces governance rules at CI and runtime.

Checks:
  1. Registry caps respected (14 atomic, 10 hybrid, 7 context).
  2. No hybrid references another hybrid (no recursive hybrids).
  3. No duplicate vector names across all categories.
  4. Metric dictionary covers every registered vector.
  5. No unknown keys in metric dictionary beyond the registry.
"""

from __future__ import annotations

import logging
from typing import List

from .vector_registry import (
    atomic_vector_names,
    context_driver_names,
    hybrid_vector_names,
    metric_dictionary,
    registry_version,
)

logger = logging.getLogger("nrl-pillar1")


def lint_vector_registry() -> List[str]:
    """Run all governance checks.  Returns a list of error strings (empty = pass)."""
    errors: List[str] = []

    atomics = atomic_vector_names()
    hybrids = hybrid_vector_names()
    contexts = context_driver_names()
    md = metric_dictionary()

    # 1. Registry caps
    if len(atomics) != 14:
        errors.append(f"atomic count {len(atomics)} != 14")
    if len(hybrids) != 10:
        errors.append(f"hybrid count {len(hybrids)} != 10")
    if len(contexts) != 7:
        errors.append(f"context count {len(contexts)} != 7")

    # 2. No recursive hybrids: hybrid components must only reference atomics
    atomic_set = set(atomics)
    hybrid_set = set(hybrids)
    for hname in hybrids:
        entry = md.get("hybrid_metrics", {}).get(hname, {})
        for comp in entry.get("components", []):
            if comp in hybrid_set:
                errors.append(f"recursive hybrid: '{hname}' references hybrid '{comp}'")

    # 3. No duplicate names across categories
    all_names = atomics + hybrids + contexts
    seen: set[str] = set()
    for name in all_names:
        if name in seen:
            errors.append(f"duplicate vector name: '{name}'")
        seen.add(name)

    # 4. Metric dictionary covers every registered vector
    for name in atomics:
        if name not in md.get("atomic_metrics", {}):
            errors.append(f"atomic '{name}' missing from metric dictionary")
    for name in hybrids:
        if name not in md.get("hybrid_metrics", {}):
            errors.append(f"hybrid '{name}' missing from metric dictionary")
    for name in contexts:
        if name not in md.get("context_drivers", {}):
            errors.append(f"context '{name}' missing from metric dictionary")

    # 5. No unknown keys in metric dictionary beyond the registry
    for key in md.get("atomic_metrics", {}):
        if key not in atomic_set:
            errors.append(f"unknown atomic in metric dict: '{key}'")
    for key in md.get("hybrid_metrics", {}):
        if key not in hybrid_set:
            errors.append(f"unknown hybrid in metric dict: '{key}'")
    context_set = set(contexts)
    for key in md.get("context_drivers", {}):
        if key not in context_set:
            errors.append(f"unknown context in metric dict: '{key}'")

    return errors


def enforce_vector_registry_lint() -> None:
    """Raise ``ValueError`` if any governance check fails."""
    errors = lint_vector_registry()
    if errors:
        msg = "Vector registry lint failed:\n" + "\n".join(f"  - {e}" for e in errors)
        logger.error(msg)
        raise ValueError(msg)
    logger.info(
        "Vector registry lint passed (v%s): 14 atomic, 10 hybrid, 7 context",
        registry_version(),
    )


def validate_vector_keys(
    data: dict,
    *,
    category: str,
) -> List[str]:
    """Check that keys in *data* are a subset of the registered vector names.

    *category* must be ``"atomics"``, ``"hybrids"``, or ``"context"``.
    Returns list of unknown keys (empty = valid).
    """
    if category == "atomics":
        allowed = set(atomic_vector_names())
    elif category == "hybrids":
        allowed = set(hybrid_vector_names())
    elif category == "context":
        allowed = set(context_driver_names())
    else:
        return [f"unknown category: {category}"]

    return sorted(set(data.keys()) - allowed)

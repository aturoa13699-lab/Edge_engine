from engine.vector_registry import (
    all_vector_names,
    atomic_vector_names,
    context_driver_names,
    hybrid_vector_names,
    metric_dictionary,
    registry_version,
)


def test_registry_version():
    assert registry_version() == "1.0.0"


def test_atomic_vector_count():
    names = atomic_vector_names()
    assert len(names) == 14
    assert "line_breaks_per80" in names
    assert "involvement_rate" in names


def test_hybrid_vector_count():
    names = hybrid_vector_names()
    assert len(names) == 10
    assert "carry_dominance" in names
    assert "field_position_impact" in names


def test_context_driver_count():
    names = context_driver_names()
    assert len(names) == 7
    assert "matchup_score" in names
    assert "turnaround_days" in names


def test_all_vector_names_total():
    assert len(all_vector_names()) == 14 + 10 + 7


def test_no_duplicate_names():
    names = all_vector_names()
    assert len(names) == len(set(names))


def test_metric_dictionary_has_all_atomics():
    md = metric_dictionary()
    for name in atomic_vector_names():
        assert name in md["atomic_metrics"], f"missing atomic metric: {name}"


def test_metric_dictionary_has_all_hybrids():
    md = metric_dictionary()
    for name in hybrid_vector_names():
        assert name in md["hybrid_metrics"], f"missing hybrid metric: {name}"

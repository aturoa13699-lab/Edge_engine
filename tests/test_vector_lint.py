from engine.vector_lint import (
    enforce_vector_registry_lint,
    lint_vector_registry,
    validate_vector_keys,
)


class TestLintVectorRegistry:
    def test_lint_passes_clean(self):
        errors = lint_vector_registry()
        assert errors == [], f"unexpected lint errors: {errors}"

    def test_enforce_does_not_raise_on_clean(self):
        # Should complete without raising
        enforce_vector_registry_lint()


class TestValidateVectorKeys:
    def test_valid_atomic_keys(self):
        data = {"line_breaks_per80": 2.5, "errors_per80": 1.0}
        unknown = validate_vector_keys(data, category="atomics")
        assert unknown == []

    def test_valid_hybrid_keys(self):
        data = {"carry_dominance": 0.6, "defensive_pressure": 0.4}
        unknown = validate_vector_keys(data, category="hybrids")
        assert unknown == []

    def test_valid_context_keys(self):
        data = {"matchup_score": 0.5, "venue_score": 1.0}
        unknown = validate_vector_keys(data, category="context")
        assert unknown == []

    def test_unknown_atomic_key(self):
        data = {"line_breaks_per80": 2.5, "bogus_metric": 99}
        unknown = validate_vector_keys(data, category="atomics")
        assert unknown == ["bogus_metric"]

    def test_unknown_hybrid_key(self):
        data = {"carry_dominance": 0.6, "xyzzy": 0.1}
        unknown = validate_vector_keys(data, category="hybrids")
        assert unknown == ["xyzzy"]

    def test_unknown_context_key(self):
        data = {"matchup_score": 0.5, "not_a_driver": 0.0}
        unknown = validate_vector_keys(data, category="context")
        assert unknown == ["not_a_driver"]

    def test_invalid_category(self):
        result = validate_vector_keys({}, category="nope")
        assert result == ["unknown category: nope"]

    def test_empty_data_is_valid(self):
        assert validate_vector_keys({}, category="atomics") == []
        assert validate_vector_keys({}, category="hybrids") == []
        assert validate_vector_keys({}, category="context") == []

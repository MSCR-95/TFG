"""
Tests for the algorithm registry system.
Each test operates on an isolated copy of the registry.
"""

from __future__ import annotations

import pytest

from alglab.engine import (
    build_algorithm,
    register_algorithm,
)
from alglab.engine import registry as _reg
from alglab.engine.core import Algorithm
from alglab.engine.registry import parse_algo_spec

# ---------------------------------------------------------------------------
# Fixture: isolates the registry between tests
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def isolated_registry():
    """Save and restore the global registry after each test."""
    saved_algo = dict(_reg._ALGO_REGISTRY)
    yield
    _reg._ALGO_REGISTRY.clear()
    _reg._ALGO_REGISTRY.update(saved_algo)


# ---------------------------------------------------------------------------
# Helper algorithms defined at module level (required for pickle)
# ---------------------------------------------------------------------------


class _SimpleAlgo(Algorithm):
    def run(self, context, *, seed=None):
        return {}


class _OtherAlgo(Algorithm):
    def run(self, context, *, seed=None):
        return {}


# ===========================================================================
# register_algorithm
# ===========================================================================


class TestRegisterAlgorithm:
    def test_registers_by_name(self):
        register_algorithm("test_simple")(_SimpleAlgo)
        assert "test_simple" in _reg._ALGO_REGISTRY

    def test_sets_dunder_algo_name(self):
        register_algorithm("test_name")(_SimpleAlgo)
        assert _SimpleAlgo.__algo_name__ == "test_name"

    def test_name_is_lowercased(self):
        register_algorithm("TEST_LOWER")(_SimpleAlgo)
        assert "test_lower" in _reg._ALGO_REGISTRY

    def test_name_is_stripped(self):
        register_algorithm("  spaced  ")(_SimpleAlgo)
        assert "spaced" in _reg._ALGO_REGISTRY

    def test_duplicate_raises_value_error(self):
        register_algorithm("dup")(_SimpleAlgo)
        with pytest.raises(ValueError, match="already registered"):
            register_algorithm("dup")(_OtherAlgo)

    def test_returns_the_class_unchanged(self):
        result = register_algorithm("ret_test")(_SimpleAlgo)
        assert result is _SimpleAlgo


# ===========================================================================
# build_algorithm
# ===========================================================================


class TestBuildAlgorithm:
    def test_returns_instance(self):
        register_algorithm("ba_test")(_SimpleAlgo)
        algo = build_algorithm("ba_test")
        assert isinstance(algo, _SimpleAlgo)

    def test_each_call_returns_new_instance(self):
        register_algorithm("ba_new")(_SimpleAlgo)
        a = build_algorithm("ba_new")
        b = build_algorithm("ba_new")
        assert a is not b

    def test_name_is_case_insensitive(self):
        register_algorithm("ba_case")(_SimpleAlgo)
        algo = build_algorithm("BA_CASE")
        assert isinstance(algo, _SimpleAlgo)

    def test_unknown_name_raises_key_error(self):
        with pytest.raises(KeyError, match="Unknown algorithm"):
            build_algorithm("does_not_exist_xyz")

    def test_error_message_lists_registered(self):
        register_algorithm("listed_algo")(_SimpleAlgo)
        with pytest.raises(KeyError) as exc_info:
            build_algorithm("not_here")
        assert "listed_algo" in str(exc_info.value)


class TestParseAlgoSpec:
    def test_parses_kwargs(self):
        name, kwargs = parse_algo_spec("algo:num_reads=10,temp=1.5,label=x,debug=true,save=false")
        assert name == "algo"
        assert kwargs == {
            "num_reads": 10,
            "temp": 1.5,
            "label": "x",
            "debug": True,
            "save": False,
        }

    @pytest.mark.parametrize("spec", ["algo:", "algo:key", ":x=1", "algo:=1", "algo:x="])
    def test_malformed_specs_raise_clear_error(self, spec):
        with pytest.raises(ValueError, match="Invalid algorithm spec"):
            parse_algo_spec(spec)

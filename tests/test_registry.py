"""
Tests del sistema de registro de algoritmos.
Cada test opera sobre una copia aislada del registro.
"""
from __future__ import annotations

import pytest

from framework.core import Algorithm
from framework import (
    build_algorithm,
    build_algorithms,
    build_algorithms_by_family,
    list_families,
    register_algorithm,
)
from framework import registry as _reg


# ---------------------------------------------------------------------------
# Fixture: aisla el registro entre tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolated_registry():
    """Guarda y restaura el registro global tras cada test."""
    saved_algo = dict(_reg._ALGO_REGISTRY)
    saved_family = {k: list(v) for k, v in _reg._FAMILY_REGISTRY.items()}
    yield
    _reg._ALGO_REGISTRY.clear()
    _reg._ALGO_REGISTRY.update(saved_algo)
    _reg._FAMILY_REGISTRY.clear()
    _reg._FAMILY_REGISTRY.update(saved_family)


# ---------------------------------------------------------------------------
# Algoritmos auxiliares definidos a nivel de módulo (necesario para pickle)
# ---------------------------------------------------------------------------

class _SimpleAlgo(Algorithm):
    def run(self, file_path):
        return {}

class _OtherAlgo(Algorithm):
    def run(self, file_path):
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
        with pytest.raises(ValueError, match="ya está registrado"):
            register_algorithm("dup")(_OtherAlgo)

    def test_returns_the_class_unchanged(self):
        result = register_algorithm("ret_test")(_SimpleAlgo)
        assert result is _SimpleAlgo

    def test_registers_family(self):
        register_algorithm("fam_algo", family="myfamily")(_SimpleAlgo)
        assert "myfamily" in _reg._FAMILY_REGISTRY
        assert "fam_algo" in _reg._FAMILY_REGISTRY["myfamily"]

    def test_family_is_lowercased(self):
        register_algorithm("fam_algo2", family="MyFamily")(_SimpleAlgo)
        assert "myfamily" in _reg._FAMILY_REGISTRY

    def test_sets_dunder_algo_family(self):
        register_algorithm("fam_algo3", family="grp")(_SimpleAlgo)
        assert _SimpleAlgo.__algo_family__ == "grp"

    def test_no_family_does_not_pollute_family_registry(self):
        before = set(_reg._FAMILY_REGISTRY.keys())
        register_algorithm("no_family_algo")(_SimpleAlgo)
        after = set(_reg._FAMILY_REGISTRY.keys())
        assert before == after

    def test_multiple_algos_same_family(self):
        register_algorithm("a1", family="grp")(_SimpleAlgo)
        register_algorithm("a2", family="grp")(_OtherAlgo)
        assert set(_reg._FAMILY_REGISTRY["grp"]) == {"a1", "a2"}


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
        with pytest.raises(KeyError, match="Algoritmo desconocido"):
            build_algorithm("does_not_exist_xyz")

    def test_error_message_lists_registered(self):
        register_algorithm("listed_algo")(_SimpleAlgo)
        with pytest.raises(KeyError) as exc_info:
            build_algorithm("not_here")
        assert "listed_algo" in str(exc_info.value)


# ===========================================================================
# build_algorithms
# ===========================================================================

class TestBuildAlgorithms:
    def test_returns_list_of_instances(self):
        register_algorithm("bl_a")(_SimpleAlgo)
        register_algorithm("bl_b")(_OtherAlgo)
        result = build_algorithms(["bl_a", "bl_b"])
        assert len(result) == 2
        assert isinstance(result[0], _SimpleAlgo)
        assert isinstance(result[1], _OtherAlgo)

    def test_empty_list(self):
        assert build_algorithms([]) == []

    def test_unknown_name_raises(self):
        with pytest.raises(KeyError):
            build_algorithms(["unknown_xyz"])


# ===========================================================================
# build_algorithms_by_family
# ===========================================================================

class TestBuildAlgorithmsByFamily:
    def test_returns_all_family_members(self):
        register_algorithm("fam1_a", family="fam1")(_SimpleAlgo)
        register_algorithm("fam1_b", family="fam1")(_OtherAlgo)
        result = build_algorithms_by_family("fam1")
        assert len(result) == 2

    def test_family_name_case_insensitive(self):
        register_algorithm("fam2_a", family="fam2")(_SimpleAlgo)
        result = build_algorithms_by_family("FAM2")
        assert len(result) == 1

    def test_unknown_family_raises_key_error(self):
        with pytest.raises(KeyError, match="Familia desconocida"):
            build_algorithms_by_family("non_existent_family_xyz")

    def test_error_lists_known_families(self):
        register_algorithm("fam3_a", family="known_fam")(_SimpleAlgo)
        with pytest.raises(KeyError) as exc_info:
            build_algorithms_by_family("bad_family")
        assert "known_fam" in str(exc_info.value)


# ===========================================================================
# list_families
# ===========================================================================

class TestListFamilies:
    def test_returns_sorted_list(self):
        register_algorithm("lf_z", family="zzz")(_SimpleAlgo)
        register_algorithm("lf_a", family="aaa")(_OtherAlgo)
        families = list_families()
        # "aaa" debe aparecer antes que "zzz" (están entre los ya existentes)
        aaa_pos = families.index("aaa")
        zzz_pos = families.index("zzz")
        assert aaa_pos < zzz_pos

    def test_includes_newly_registered(self):
        register_algorithm("lf_new", family="brand_new_family")(_SimpleAlgo)
        assert "brand_new_family" in list_families()

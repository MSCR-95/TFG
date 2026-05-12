"""
Tests del generador de instancias k-CNF.
Cubre:
  - generar_clausula_k: tamaño, unicidad de variables, rango, errores
  - generar_formula_kcnf: estructura, cobertura completa de variables, errores
  - formula_a_dimacs: formato DIMACS correcto
  - generar_instancias: ficheros creados, parseable, reproducibilidad con semilla
"""
from __future__ import annotations

import random
from pathlib import Path

import pytest

from algorithms.max2sat.generator import (
    generar_clausula_k,
    generar_formula_kcnf,
    formula_a_dimacs,
    generar_instancias,
)
from algorithms.max2sat.brute import _parse_dimacs


# ===========================================================================
# generar_clausula_k
# ===========================================================================

class TestGenerarClausulaK:
    def _rng(self, seed=0):
        return random.Random(seed)

    def test_returns_exactly_k_literals(self):
        clausula = generar_clausula_k(5, 3, self._rng())
        assert len(clausula) == 3

    def test_no_repeated_variables(self):
        clausula = generar_clausula_k(10, 5, self._rng())
        vars_abs = [abs(lit) for lit in clausula]
        assert len(vars_abs) == len(set(vars_abs))

    def test_literals_in_valid_range(self):
        num_vars = 8
        clausula = generar_clausula_k(num_vars, 4, self._rng())
        assert all(1 <= abs(lit) <= num_vars for lit in clausula)

    def test_k_equals_1(self):
        clausula = generar_clausula_k(5, 1, self._rng())
        assert len(clausula) == 1

    def test_k_equals_num_vars(self):
        clausula = generar_clausula_k(4, 4, self._rng())
        assert len(clausula) == 4
        assert len({abs(lit) for lit in clausula}) == 4

    def test_k_zero_raises(self):
        with pytest.raises(ValueError, match="k debe ser >= 1"):
            generar_clausula_k(5, 0, self._rng())

    def test_k_greater_than_num_vars_raises(self):
        with pytest.raises(ValueError, match="no puede ser mayor"):
            generar_clausula_k(3, 5, self._rng())

    def test_signs_can_be_positive_or_negative(self):
        """Con suficientes muestras deben aparecer literales negativos."""
        rng = self._rng(42)
        signs = set()
        for _ in range(50):
            for lit in generar_clausula_k(10, 5, rng):
                signs.add(lit > 0)
        assert True in signs
        assert False in signs


# ===========================================================================
# generar_formula_kcnf
# ===========================================================================

class TestGenerarFormulaKCNF:
    def _rng(self, seed=0):
        return random.Random(seed)

    def test_returns_correct_number_of_clauses(self):
        formula = generar_formula_kcnf(5, 10, 2, self._rng())
        assert len(formula) == 10

    def test_each_clause_has_exactly_k_literals(self):
        formula = generar_formula_kcnf(6, 8, 3, self._rng())
        assert all(len(c) == 3 for c in formula)

    def test_no_repeated_variables_within_clause(self):
        formula = generar_formula_kcnf(10, 20, 4, self._rng())
        for clausula in formula:
            vars_abs = [abs(lit) for lit in clausula]
            assert len(vars_abs) == len(set(vars_abs)), f"Variables repetidas en {clausula}"

    def test_all_variables_appear_at_least_once(self):
        num_vars = 8
        formula = generar_formula_kcnf(num_vars, 20, 2, self._rng())
        vars_usadas = {abs(lit) for c in formula for lit in c}
        assert vars_usadas == set(range(1, num_vars + 1))

    def test_literals_in_valid_range(self):
        num_vars = 7
        formula = generar_formula_kcnf(num_vars, 15, 3, self._rng())
        assert all(1 <= abs(lit) <= num_vars for c in formula for lit in c)

    def test_k_equals_num_vars(self):
        formula = generar_formula_kcnf(4, 5, 4, self._rng())
        assert len(formula) == 5
        assert all(len(c) == 4 for c in formula)

    def test_num_vars_zero_raises(self):
        with pytest.raises(ValueError):
            generar_formula_kcnf(0, 5, 1, self._rng())

    def test_num_clausulas_zero_raises(self):
        with pytest.raises(ValueError):
            generar_formula_kcnf(5, 0, 2, self._rng())

    def test_k_zero_raises(self):
        with pytest.raises(ValueError):
            generar_formula_kcnf(5, 5, 0, self._rng())

    def test_k_greater_than_num_vars_raises(self):
        with pytest.raises(ValueError, match="no puede ser mayor"):
            generar_formula_kcnf(3, 5, 5, self._rng())

    def test_impossible_coverage_raises(self):
        """10 variables, 1 cláusula de 2 literales → imposible cubrir todas."""
        with pytest.raises(ValueError, match="Imposible cubrir"):
            generar_formula_kcnf(10, 1, 2, self._rng())

    def test_different_seeds_different_formulas(self):
        f1 = generar_formula_kcnf(5, 10, 2, random.Random(1))
        f2 = generar_formula_kcnf(5, 10, 2, random.Random(999))
        assert f1 != f2

    def test_same_seed_same_formula(self):
        f1 = generar_formula_kcnf(5, 10, 2, random.Random(42))
        f2 = generar_formula_kcnf(5, 10, 2, random.Random(42))
        assert f1 == f2


# ===========================================================================
# formula_a_dimacs
# ===========================================================================

class TestFormulaADimacs:
    def _formula(self):
        return [[1, -2], [-1, 3], [2, -3]]

    def test_has_header_line(self):
        text = formula_a_dimacs(self._formula(), 3)
        assert "p cnf 3 3" in text

    def test_clause_count_in_header_matches_formula(self):
        clausulas = self._formula()
        text = formula_a_dimacs(clausulas, 3)
        header = [l for l in text.splitlines() if l.startswith("p cnf")][0]
        _, _, _, n_clausulas = header.split()
        assert int(n_clausulas) == len(clausulas)

    def test_each_clause_ends_in_zero(self):
        text = formula_a_dimacs(self._formula(), 3)
        data_lines = [l for l in text.splitlines() if l and not l.startswith(("c", "p"))]
        assert all(l.endswith(" 0") for l in data_lines)

    def test_comment_included_when_provided(self):
        text = formula_a_dimacs(self._formula(), 3, comentario="mi comentario")
        assert "c mi comentario" in text

    def test_no_comment_when_empty_string(self):
        text = formula_a_dimacs(self._formula(), 3, comentario="")
        assert not any(l.startswith("c") for l in text.splitlines())

    def test_ends_with_newline(self):
        text = formula_a_dimacs(self._formula(), 3)
        assert text.endswith("\n")

    def test_parseable_by_dimacs_parser(self):
        clausulas = self._formula()
        text = formula_a_dimacs(clausulas, 3)
        num_vars, parsed = _parse_dimacs(text)
        assert num_vars == 3
        assert parsed == clausulas


# ===========================================================================
# generar_instancias
# ===========================================================================

class TestGenerarInstancias:
    def test_creates_correct_number_of_files(self, tmp_path: Path):
        rutas = generar_instancias(5, 4, 6, 2, tmp_path, seed=0)
        assert len(rutas) == 5
        assert all(p.exists() for p in rutas)

    def test_files_have_cnf_extension(self, tmp_path: Path):
        rutas = generar_instancias(3, 4, 6, 2, tmp_path, seed=0)
        assert all(p.suffix == ".cnf" for p in rutas)

    def test_creates_output_directory_if_missing(self, tmp_path: Path):
        output = tmp_path / "subdir" / "deep"
        generar_instancias(1, 4, 6, 2, output, seed=0)
        assert output.exists()

    def test_files_are_valid_dimacs(self, tmp_path: Path):
        rutas = generar_instancias(3, 5, 8, 2, tmp_path, seed=1)
        for ruta in rutas:
            text = ruta.read_text(encoding="utf-8")
            num_vars, clausulas = _parse_dimacs(text)
            assert num_vars == 5
            assert len(clausulas) == 8

    def test_prefix_applied_to_filenames(self, tmp_path: Path):
        rutas = generar_instancias(2, 4, 6, 2, tmp_path, prefix="caso", seed=0)
        assert all(p.stem.startswith("caso") for p in rutas)

    def test_reproducible_with_same_seed(self, tmp_path: Path):
        dir1 = tmp_path / "run1"
        dir2 = tmp_path / "run2"
        rutas1 = generar_instancias(3, 4, 6, 2, dir1, seed=7)
        rutas2 = generar_instancias(3, 4, 6, 2, dir2, seed=7)
        for r1, r2 in zip(rutas1, rutas2):
            assert r1.read_text() == r2.read_text()

    def test_different_seed_different_content(self, tmp_path: Path):
        dir1 = tmp_path / "run1"
        dir2 = tmp_path / "run2"
        rutas1 = generar_instancias(1, 6, 10, 2, dir1, seed=1)
        rutas2 = generar_instancias(1, 6, 10, 2, dir2, seed=2)
        assert rutas1[0].read_text() != rutas2[0].read_text()

    def test_n_files_zero_raises(self, tmp_path: Path):
        with pytest.raises(ValueError, match="n_files debe ser >= 1"):
            generar_instancias(0, 4, 6, 2, tmp_path)

    def test_returns_paths_in_order(self, tmp_path: Path):
        rutas = generar_instancias(4, 4, 6, 2, tmp_path, seed=0)
        names = [p.name for p in rutas]
        assert names == sorted(names)

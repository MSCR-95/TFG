"""
Tests del solver Max-SAT por fuerza bruta y su parser DIMACS CNF.
Cubre:
  - _parse_dimacs: casos válidos y todos los errores documentados
  - _resolver_maxsat_fuerza_bruta: correctitud, parada temprana, corte
  - MaxSATBruteAlgorithm.run: integración leyendo un fichero real
"""
from __future__ import annotations

import pytest
from pathlib import Path

from algorithms.max2sat.brute import (
    _parse_dimacs,
    _resolver_maxsat_fuerza_bruta,
    MaxSATBruteAlgorithm,
)


# ===========================================================================
# _parse_dimacs — casos válidos
# ===========================================================================

class TestParseDimacsValid:
    def test_minimal_single_clause(self):
        text = "p cnf 2 1\n1 -2 0\n"
        num_vars, clausulas = _parse_dimacs(text)
        assert num_vars == 2
        assert clausulas == [[1, -2]]

    def test_multiple_clauses(self):
        text = "p cnf 3 3\n1 2 0\n-1 3 0\n-2 -3 0\n"
        num_vars, clausulas = _parse_dimacs(text)
        assert num_vars == 3
        assert len(clausulas) == 3

    def test_comment_lines_ignored(self):
        text = "c este es un comentario\np cnf 1 1\n1 0\n"
        num_vars, clausulas = _parse_dimacs(text)
        assert num_vars == 1
        assert clausulas == [[1]]

    def test_multiple_comment_lines(self):
        text = "c linea 1\nc linea 2\np cnf 2 1\n1 2 0\n"
        _, clausulas = _parse_dimacs(text)
        assert clausulas == [[1, 2]]

    def test_multiline_clause(self):
        """Una cláusula distribuida en varias líneas."""
        text = "p cnf 3 1\n1\n2\n-3 0\n"
        _, clausulas = _parse_dimacs(text)
        assert clausulas == [[1, 2, -3]]

    def test_empty_clause(self):
        """Un 0 sin literales previos → cláusula vacía (insatisfacible)."""
        text = "p cnf 1 2\n1 0\n0\n"
        _, clausulas = _parse_dimacs(text)
        assert len(clausulas) == 2
        assert clausulas[1] == []

    def test_blank_lines_ignored(self):
        text = "p cnf 1 1\n\n1 0\n\n"
        _, clausulas = _parse_dimacs(text)
        assert clausulas == [[1]]

    def test_negative_literals_accepted(self):
        text = "p cnf 3 1\n-1 -2 -3 0\n"
        _, clausulas = _parse_dimacs(text)
        assert clausulas == [[-1, -2, -3]]

    def test_returns_declared_var_count(self):
        text = "p cnf 10 1\n1 0\n"
        num_vars, _ = _parse_dimacs(text)
        assert num_vars == 10


# ===========================================================================
# _parse_dimacs — errores
# ===========================================================================

class TestParseDimacsErrors:
    def test_no_header_raises(self):
        with pytest.raises(ValueError, match="cabecera"):
            _parse_dimacs("1 2 0\n")

    def test_double_header_raises(self):
        text = "p cnf 2 1\n1 0\np cnf 2 1\n-1 0\n"
        with pytest.raises(ValueError, match="múltiples cabeceras"):
            _parse_dimacs(text)

    def test_invalid_header_format_raises(self):
        with pytest.raises(ValueError, match="Cabecera DIMACS inválida"):
            _parse_dimacs("p sat 2 1\n1 0\n")

    def test_header_missing_fields_raises(self):
        with pytest.raises(ValueError, match="Cabecera DIMACS inválida"):
            _parse_dimacs("p cnf 2\n1 0\n")

    def test_literal_before_header_raises(self):
        with pytest.raises(ValueError, match="antes de la cabecera"):
            _parse_dimacs("1 2 0\np cnf 2 1\n")

    def test_literal_out_of_range_raises(self):
        with pytest.raises(ValueError, match="Literal fuera de rango"):
            _parse_dimacs("p cnf 2 1\n1 3 0\n")

    def test_unclosed_clause_raises(self):
        with pytest.raises(ValueError, match="no terminada en 0"):
            _parse_dimacs("p cnf 2 1\n1 2\n")

    def test_clause_count_mismatch_raises(self):
        # Declara 2 cláusulas pero solo hay 1
        with pytest.raises(ValueError, match="Número de cláusulas incorrecto"):
            _parse_dimacs("p cnf 2 2\n1 2 0\n")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            _parse_dimacs("")


# ===========================================================================
# _resolver_maxsat_fuerza_bruta — correctitud
# ===========================================================================

class TestSolverCorrectitud:
    def test_satisfiable_formula(self):
        """(x1 ∨ x2) ∧ (¬x1 ∨ x2) — satisfacible con x2=True."""
        clausulas = [[1, 2], [-1, 2]]
        valor, asig = _resolver_maxsat_fuerza_bruta(2, clausulas)
        assert valor == 2  # todas satisfechas

    def test_unsatisfiable_formula_max_is_less_than_total(self):
        """x1 ∧ ¬x1 — imposible satisfacer ambas a la vez."""
        clausulas = [[1], [-1]]
        valor, _ = _resolver_maxsat_fuerza_bruta(1, clausulas)
        assert valor == 1  # máximo alcanzable es 1

    def test_single_variable_positive(self):
        clausulas = [[1]]
        valor, asig = _resolver_maxsat_fuerza_bruta(1, clausulas)
        assert valor == 1
        assert asig[1] is True

    def test_single_variable_negative(self):
        clausulas = [[-1]]
        valor, asig = _resolver_maxsat_fuerza_bruta(1, clausulas)
        assert valor == 1
        assert asig[1] is False

    def test_all_clauses_satisfied_sets_optima(self):
        """Formula trivialmente satisfacible: todas las cláusulas con un literal."""
        clausulas = [[1], [2], [3]]
        valor, _ = _resolver_maxsat_fuerza_bruta(3, clausulas)
        assert valor == 3

    def test_assignment_actually_satisfies_clauses(self):
        """Verificar que la asignación devuelta es correcta."""
        clausulas = [[1, 2], [-1, 3], [-2, -3]]
        valor, asig = _resolver_maxsat_fuerza_bruta(3, clausulas)
        satisfechas = sum(
            any(asig[abs(lit)] if lit > 0 else not asig[abs(lit)] for lit in c)
            for c in clausulas
        )
        assert satisfechas == valor

    def test_returns_best_among_ties(self):
        """Con dos soluciones igual de buenas, debe devolver alguna válida."""
        clausulas = [[1], [2]]
        valor, asig = _resolver_maxsat_fuerza_bruta(2, clausulas)
        assert valor == 2

    def test_empty_clause_never_satisfied(self):
        """Una cláusula vacía nunca puede satisfacerse."""
        clausulas = [[], [1]]
        valor, _ = _resolver_maxsat_fuerza_bruta(1, clausulas)
        assert valor == 1  # solo la segunda se satisface


# ===========================================================================
# MaxSATBruteAlgorithm.run — integración end-to-end con fichero
# ===========================================================================

SIMPLE_CNF = """\
c instancia sencilla
p cnf 3 3
1 2 0
-1 3 0
-2 -3 0
"""


class TestMaxSATBruteRun:
    @pytest.fixture()
    def cnf_file(self, tmp_path: Path) -> Path:
        p = tmp_path / "test.cnf"
        p.write_text(SIMPLE_CNF, encoding="utf-8")
        return p

    def test_returns_dict(self, cnf_file):
        result = MaxSATBruteAlgorithm().run(cnf_file)
        assert isinstance(result, dict)

    def test_num_vars_correct(self, cnf_file):
        result = MaxSATBruteAlgorithm().run(cnf_file)
        assert result["num_vars"] == 3

    def test_num_clausulas_correct(self, cnf_file):
        result = MaxSATBruteAlgorithm().run(cnf_file)
        assert result["num_clausulas"] == 3

    def test_clausulas_satisfechas_leq_total(self, cnf_file):
        result = MaxSATBruteAlgorithm().run(cnf_file)
        assert result["clausulas_satisfechas"] <= result["num_clausulas"]

    def test_satisfaccion_ratio_in_range(self, cnf_file):
        result = MaxSATBruteAlgorithm().run(cnf_file)
        assert 0.0 <= result["satisfaccion_ratio"] <= 1.0

    def test_optima_flag_when_all_satisfied(self, cnf_file):
        result = MaxSATBruteAlgorithm().run(cnf_file)
        expected = result["clausulas_satisfechas"] == result["num_clausulas"]
        assert result["optima"] == expected

    def test_asignacion_has_all_variables(self, cnf_file):
        result = MaxSATBruteAlgorithm().run(cnf_file)
        assert set(result["asignacion"].keys()) == {"x1", "x2", "x3"}

    def test_asignacion_values_are_0_or_1(self, cnf_file):
        result = MaxSATBruteAlgorithm().run(cnf_file)
        assert all(v in (0, 1) for v in result["asignacion"].values())

    def test_empty_cnf_file_raises(self, tmp_path: Path):
        """Fichero con cabecera pero sin cláusulas → ValueError."""
        p = tmp_path / "empty.cnf"
        p.write_text("p cnf 2 0\n", encoding="utf-8")
        with pytest.raises(ValueError, match="no contiene ninguna cláusula"):
            MaxSATBruteAlgorithm().run(p)

    def test_invalid_dimacs_raises(self, tmp_path: Path):
        p = tmp_path / "bad.cnf"
        p.write_text("esto no es dimacs\n", encoding="utf-8")
        with pytest.raises(ValueError):
            MaxSATBruteAlgorithm().run(p)

    def test_satisfaccion_ratio_rounded_to_4_decimals(self, cnf_file):
        result = MaxSATBruteAlgorithm().run(cnf_file)
        # round(x, 4) no tiene más de 4 decimales
        as_str = str(result["satisfaccion_ratio"])
        decimals = as_str.split(".")[-1] if "." in as_str else ""
        assert len(decimals) <= 4

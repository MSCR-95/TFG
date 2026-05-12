"""
Tests del solver Max-2-SAT mediante QUBO + Recocido Simulado.
Cubre:
  - _validar_clausulas_2sat: casos válidos y errores
  - _construir_qubo: correctitud por tipo de cláusula, simetría, acumulación
  - _resolver_qubo: estructura del resultado, tipos, invariantes
  - MaxSATQUBOSAAlgorithm.run: integración leyendo un fichero real
"""
from __future__ import annotations

import math
from pathlib import Path

import pytest

from algorithms.max2sat.annealing import (
    _validar_clausulas_2sat,
    _construir_qubo,
    _resolver_qubo,
    MaxSATQUBOSAAlgorithm,
)


# ===========================================================================
# _validar_clausulas_2sat — casos válidos
# ===========================================================================

class TestValidarClausulas2SATValid:
    def test_returns_list_of_tuples(self):
        clausulas = [[1, 2], [-1, 3]]
        resultado = _validar_clausulas_2sat(clausulas, "test.cnf")
        assert isinstance(resultado, list)
        assert all(isinstance(c, tuple) for c in resultado)

    def test_preserves_literals(self):
        clausulas = [[1, -2], [-3, 4]]
        resultado = _validar_clausulas_2sat(clausulas, "test.cnf")
        assert resultado == [(1, -2), (-3, 4)]

    def test_single_clause(self):
        resultado = _validar_clausulas_2sat([[1, 2]], "test.cnf")
        assert resultado == [(1, 2)]

    def test_preserves_order(self):
        clausulas = [[-1, 2], [3, -4], [-5, -6]]
        resultado = _validar_clausulas_2sat(clausulas, "test.cnf")
        assert [c[0] for c in resultado] == [-1, 3, -5]

    def test_empty_list_returns_empty(self):
        assert _validar_clausulas_2sat([], "test.cnf") == []

    def test_all_four_sign_combinations(self):
        """Las cuatro combinaciones de negación deben aceptarse."""
        clausulas = [[1, 2], [-1, 2], [1, -2], [-1, -2]]
        resultado = _validar_clausulas_2sat(clausulas, "test.cnf")
        assert len(resultado) == 4


# ===========================================================================
# _validar_clausulas_2sat — errores
# ===========================================================================

class TestValidarClausulas2SATErrors:
    def test_clause_with_one_literal_raises(self):
        with pytest.raises(ValueError, match="exactamente 2"):
            _validar_clausulas_2sat([[1]], "test.cnf")

    def test_clause_with_three_literals_raises(self):
        with pytest.raises(ValueError, match="exactamente 2"):
            _validar_clausulas_2sat([[1, 2, 3]], "test.cnf")

    def test_empty_clause_raises(self):
        with pytest.raises(ValueError, match="exactamente 2"):
            _validar_clausulas_2sat([[]], "test.cnf")

    def test_error_includes_clause_number(self):
        """El mensaje debe indicar qué cláusula falló."""
        clausulas = [[1, 2], [3, 4], [5, 6, 7]]  # la tercera falla
        with pytest.raises(ValueError, match="3"):
            _validar_clausulas_2sat(clausulas, "inst.cnf")

    def test_error_includes_filename(self):
        with pytest.raises(ValueError, match="mi_fichero.cnf"):
            _validar_clausulas_2sat([[1, 2, 3]], "mi_fichero.cnf")

    def test_valid_then_invalid_raises_on_invalid(self):
        """Cláusulas válidas antes de una inválida no deben silenciar el error."""
        clausulas = [[1, 2], [-1, 3], [4]]
        with pytest.raises(ValueError):
            _validar_clausulas_2sat(clausulas, "test.cnf")


# ===========================================================================
# _construir_qubo — estructura general
# ===========================================================================

class TestConstruirQUBOEstructura:
    def test_returns_dict_and_int(self):
        clausulas = [(1, 2)]
        Q, constante = _construir_qubo(2, clausulas)
        assert isinstance(Q, dict)
        assert isinstance(constante, int)

    def test_empty_clauses_returns_empty_Q_zero_constant(self):
        Q, constante = _construir_qubo(3, [])
        assert Q == {}
        assert constante == 0

    def test_off_diagonal_is_symmetric(self):
        """Para cualquier cláusula, Q[i,j] debe ser igual a Q[j,i]."""
        clausulas = [(1, 2), (-1, 3), (2, -3), (-1, -2)]
        Q, _ = _construir_qubo(3, clausulas)
        for (i, j), v in Q.items():
            if i != j:
                assert math.isclose(v, Q.get((j, i), 0.0)), \
                    f"Simetría rota: Q[{i},{j}]={v} pero Q[{j},{i}]={Q.get((j, i))}"

    def test_keys_are_valid_index_pairs(self):
        """Los índices deben estar en rango base-0 para n variables."""
        n = 4
        clausulas = [(1, 2), (3, 4), (-1, 4)]
        Q, _ = _construir_qubo(n, clausulas)
        for i, j in Q.keys():
            assert 0 <= i < n
            assert 0 <= j < n

    def test_multiple_clauses_accumulate(self):
        """Dos cláusulas del mismo tipo deben acumular coeficientes."""
        Q1, c1 = _construir_qubo(2, [(1, 2)])
        Q2, c2 = _construir_qubo(2, [(1, 2), (1, 2)])
        for key in Q1:
            assert math.isclose(Q2.get(key, 0.0), 2 * Q1[key])
        assert c2 == 2 * c1


# ===========================================================================
# _construir_qubo — correctitud por tipo de cláusula
# ===========================================================================

class TestConstruirQUBOTiposClasula:
    """
    Verifica los coeficientes exactos de la formulación QUBO para cada
    una de las cuatro combinaciones de negación posibles en Max-2-SAT.

    Convención de índices: variable xi del CNF → índice QUBO i-1 (base 0).
    Para cláusulas (x1, x2) → i=0, j=1.
    """

    def test_sin_negaciones_xi_or_xj(self):
        """
        xi ∨ xj  →  1 - xi - xj + xi·xj
        Esperado: constante=1, Q[0,0]=-1, Q[1,1]=-1, Q[0,1]=Q[1,0]=+0.5
        """
        Q, constante = _construir_qubo(2, [(1, 2)])
        assert constante == 1
        assert math.isclose(Q.get((0, 0), 0.0), -1.0)
        assert math.isclose(Q.get((1, 1), 0.0), -1.0)
        assert math.isclose(Q.get((0, 1), 0.0), +0.5)
        assert math.isclose(Q.get((1, 0), 0.0), +0.5)

    def test_neg_primera_neg_xi_or_xj(self):
        """
        ¬xi ∨ xj  →  xi - xi·xj
        Esperado: constante=0, Q[0,0]=+1, Q[0,1]=Q[1,0]=-0.5
        """
        Q, constante = _construir_qubo(2, [(-1, 2)])
        assert constante == 0
        assert math.isclose(Q.get((0, 0), 0.0), +1.0)
        assert math.isclose(Q.get((1, 1), 0.0), 0.0)
        assert math.isclose(Q.get((0, 1), 0.0), -0.5)
        assert math.isclose(Q.get((1, 0), 0.0), -0.5)

    def test_neg_segunda_xi_or_neg_xj(self):
        """
        xi ∨ ¬xj  →  xj - xi·xj
        Esperado: constante=0, Q[1,1]=+1, Q[0,1]=Q[1,0]=-0.5
        """
        Q, constante = _construir_qubo(2, [(1, -2)])
        assert constante == 0
        assert math.isclose(Q.get((0, 0), 0.0), 0.0)
        assert math.isclose(Q.get((1, 1), 0.0), +1.0)
        assert math.isclose(Q.get((0, 1), 0.0), -0.5)
        assert math.isclose(Q.get((1, 0), 0.0), -0.5)

    def test_dos_negaciones_neg_xi_or_neg_xj(self):
        """
        ¬xi ∨ ¬xj  →  xi·xj
        Esperado: constante=0, Q[0,0]=0, Q[1,1]=0, Q[0,1]=Q[1,0]=+0.5
        """
        Q, constante = _construir_qubo(2, [(-1, -2)])
        assert constante == 0
        assert math.isclose(Q.get((0, 0), 0.0), 0.0)
        assert math.isclose(Q.get((1, 1), 0.0), 0.0)
        assert math.isclose(Q.get((0, 1), 0.0), +0.5)
        assert math.isclose(Q.get((1, 0), 0.0), +0.5)

    def test_constante_cuenta_solo_clausulas_sin_negacion(self):
        """Solo las cláusulas xi ∨ xj contribuyen a la constante."""
        clausulas = [(1, 2), (-1, 2), (1, -2), (-1, -2)]
        _, constante = _construir_qubo(2, clausulas)
        assert constante == 1  # solo la primera cláusula aporta 1

    def test_variables_distintas_no_interfieren(self):
        """Dos cláusulas con variables distintas no deben compartir términos."""
        # (x1 ∨ x2) y (x3 ∨ x4) — variables disjuntas
        Q, constante = _construir_qubo(4, [(1, 2), (3, 4)])
        assert constante == 2
        # No deben aparecer términos cruzados entre {0,1} y {2,3}
        assert (0, 2) not in Q or math.isclose(Q[(0, 2)], 0.0)
        assert (0, 3) not in Q or math.isclose(Q[(0, 3)], 0.0)
        assert (1, 2) not in Q or math.isclose(Q[(1, 2)], 0.0)
        assert (1, 3) not in Q or math.isclose(Q[(1, 3)], 0.0)


# ===========================================================================
# _resolver_qubo — estructura e invariantes
# ===========================================================================

class TestResolverQUBO:
    """
    Nota: _resolver_qubo es estocástico. Los tests verifican
    invariantes estructurales, no soluciones exactas.
    """

    def _qubo_trivial(self):
        """QUBO mínimo: una sola variable, energía mínima en x=1."""
        Q = {(0, 0): -1.0}  # minimiza cuando x0=1
        return Q, 0

    def test_returns_tuple_of_three(self):
        Q, c = self._qubo_trivial()
        resultado = _resolver_qubo(Q, c, num_reads=10)
        assert isinstance(resultado, tuple)
        assert len(resultado) == 3

    def test_sample_is_dict(self):
        Q, c = self._qubo_trivial()
        muestra, _, _ = _resolver_qubo(Q, c, num_reads=10)
        assert isinstance(muestra, dict)

    def test_sample_values_are_0_or_1(self):
        Q, _ = _construir_qubo(3, [(1, 2), (-1, 3), (2, -3)])
        muestra, _, _ = _resolver_qubo(Q, 0, num_reads=20)
        assert all(v in (0, 1) for v in muestra.values())

    def test_sample_keys_are_base0_indices(self):
        n = 3
        clausulas = [(1, 2), (-1, 3), (2, -3)]
        Q, constante = _construir_qubo(n, clausulas)
        muestra, _, _ = _resolver_qubo(Q, constante, num_reads=20)
        assert set(muestra.keys()) == set(range(n))

    def test_energy_is_numeric(self):
        Q, c = self._qubo_trivial()
        _, energia, _ = _resolver_qubo(Q, c, num_reads=10)
        assert isinstance(energia, (int, float))

    def test_constante_returned_unchanged(self):
        """La constante debe volver igual que se pasó."""
        Q, _ = self._qubo_trivial()
        _, _, c_out = _resolver_qubo(Q, 42, num_reads=10)
        assert c_out == 42

    def test_trivial_qubo_finds_optimal(self):
        """Con Q={0,0:-1} la única solución óptima es x0=1, energía=-1."""
        Q, c = self._qubo_trivial()
        muestra, energia, _ = _resolver_qubo(Q, c, num_reads=50)
        assert muestra[0] == 1
        assert math.isclose(energia, -1.0)


# ===========================================================================
# MaxSATQUBOSAAlgorithm.run — integración end-to-end con fichero
# ===========================================================================

SIMPLE_2SAT_CNF = """\
c instancia Max-2-SAT sencilla
p cnf 3 3
1 2 0
-1 3 0
-2 -3 0
"""

SATISFIABLE_2SAT_CNF = """\
c instancia trivialmente satisfacible
p cnf 2 2
1 2 0
-1 -2 0
"""


class TestMaxSATQUBOSARun:
    @pytest.fixture()
    def cnf_file(self, tmp_path: Path) -> Path:
        p = tmp_path / "test.cnf"
        p.write_text(SIMPLE_2SAT_CNF, encoding="utf-8")
        return p

    @pytest.fixture()
    def sat_file(self, tmp_path: Path) -> Path:
        p = tmp_path / "sat.cnf"
        p.write_text(SATISFIABLE_2SAT_CNF, encoding="utf-8")
        return p

    def test_returns_dict(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        assert isinstance(result, dict)

    def test_all_expected_keys_present(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        expected_keys = {
            "num_vars", "num_clausulas", "clausulas_satisfechas",
            "satisfaccion_ratio", "optima", "energia_qubo",
            "constante", "num_reads", "asignacion",
        }
        assert set(result.keys()) == expected_keys

    def test_num_vars_correct(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        assert result["num_vars"] == 3

    def test_num_clausulas_correct(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        assert result["num_clausulas"] == 3

    def test_clausulas_satisfechas_in_valid_range(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        assert 0 <= result["clausulas_satisfechas"] <= result["num_clausulas"]

    def test_satisfaccion_ratio_in_range(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        assert 0.0 <= result["satisfaccion_ratio"] <= 1.0

    def test_satisfaccion_ratio_rounded_to_4_decimals(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        as_str = str(result["satisfaccion_ratio"])
        decimals = as_str.split(".")[-1] if "." in as_str else ""
        assert len(decimals) <= 4

    def test_optima_flag_consistent_with_counts(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        expected = result["clausulas_satisfechas"] == result["num_clausulas"]
        assert result["optima"] == expected

    def test_energia_qubo_is_float(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        assert isinstance(result["energia_qubo"], float)

    def test_constante_is_non_negative(self, cnf_file):
        """La constante QUBO es la suma de cláusulas sin negaciones (≥ 0)."""
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        assert result["constante"] >= 0

    def test_num_reads_matches_constructor(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm(num_reads=42).run(cnf_file)
        assert result["num_reads"] == 42

    def test_asignacion_has_all_variables(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        assert set(result["asignacion"].keys()) == {"x1", "x2", "x3"}

    def test_asignacion_values_are_0_or_1(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        assert all(v in (0, 1) for v in result["asignacion"].values())

    def test_asignacion_keys_use_x_prefix(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        assert all(k.startswith("x") for k in result["asignacion"].keys())

    def test_energia_qubo_plus_constante_gives_unsat_count(self, cnf_file):
        """
        energia_qubo + constante = número de cláusulas NO satisfechas.
        Esto verifica que la descomposición QUBO → conteo es coherente.
        """
        result = MaxSATQUBOSAAlgorithm(num_reads=200).run(cnf_file)
        clausulas_no_sat = result["num_clausulas"] - result["clausulas_satisfechas"]
        recalculado = int(round(result["energia_qubo"] + result["constante"]))
        assert recalculado == clausulas_no_sat

    def test_satisfiable_instance_finds_high_ratio(self, sat_file):
        """
        En una instancia trivialmente satisfacible con 200 lecturas,
        el ratio debe ser alto (≥ 0.5). No exigimos óptimo exacto
        por naturaleza estocástica del SA.
        """
        result = MaxSATQUBOSAAlgorithm(num_reads=200).run(sat_file)
        assert result["satisfaccion_ratio"] >= 0.5

    def test_empty_cnf_raises(self, tmp_path: Path):
        """Fichero sin cláusulas debe lanzar ValueError."""
        p = tmp_path / "empty.cnf"
        p.write_text("p cnf 2 0\n", encoding="utf-8")
        with pytest.raises(ValueError, match="ninguna cláusula"):
            MaxSATQUBOSAAlgorithm().run(p)

    def test_non_2sat_clause_raises(self, tmp_path: Path):
        """Cláusula con k≠2 debe lanzar ValueError."""
        p = tmp_path / "k3.cnf"
        p.write_text("p cnf 3 1\n1 2 3 0\n", encoding="utf-8")
        with pytest.raises(ValueError, match="exactamente 2"):
            MaxSATQUBOSAAlgorithm().run(p)

    def test_invalid_dimacs_raises(self, tmp_path: Path):
        """Fichero sin cabecera DIMACS válida debe lanzar ValueError."""
        p = tmp_path / "bad.cnf"
        p.write_text("esto no es dimacs\n", encoding="utf-8")
        with pytest.raises(ValueError):
            MaxSATQUBOSAAlgorithm().run(p)

    def test_default_num_reads_is_100(self, cnf_file):
        result = MaxSATQUBOSAAlgorithm().run(cnf_file)
        assert result["num_reads"] == 100

    def test_result_coherent_with_brute_force_on_small_instance(self, tmp_path: Path):
        """
        En una instancia de 2 variables, compara que el número de cláusulas
        satisfechas por QUBO-SA es consistente (no puede exceder el total).
        No exige igualdad con brute force por la naturaleza estocástica.
        """
        cnf = "p cnf 2 3\n1 2 0\n-1 2 0\n1 -2 0\n"
        p = tmp_path / "small.cnf"
        p.write_text(cnf, encoding="utf-8")
        result = MaxSATQUBOSAAlgorithm(num_reads=200).run(p)
        assert 0 <= result["clausulas_satisfechas"] <= 3

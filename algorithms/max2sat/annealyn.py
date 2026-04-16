"""
Max-2-SAT mediante QUBO + Recocido Simulado (dimod).

Formulación QUBO por penalización de cláusulas:
  Sin negaciones  (xi  ∨  xj):   1 - xi - xj + xi·xj
  Una negación    (¬xi ∨  xj):   xi - xi·xj
  Una negación    (xi  ∨ ¬xj):   xj - xi·xj
  Dos negaciones  (¬xi ∨ ¬xj):   xi·xj

La matriz Q se construye con convención simétrica: el coeficiente
off-diagonal se reparte a partes iguales entre Q[i,j] y Q[j,i].

Formato de entrada: DIMACS CNF con cláusulas de exactamente 2 literales.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import dimod

from framework.core import Algorithm
from framework.registry import register_algorithm
from algorithms.max2sat.brute import _parse_dimacs  # reutilizamos el parser robusto


# ============================================================================
# Funciones puras (sin estado)
# ============================================================================


def _validar_clausulas_2sat(clausulas: list[list[int]], nombre_fichero: str) -> list[tuple[int, int]]:
    """
    Comprueba que todas las cláusulas tienen exactamente 2 literales
    y devuelve la lista como tuplas (lit1, lit2).

    Raises:
        ValueError: Si alguna cláusula no tiene exactamente 2 literales.
    """
    resultado: list[tuple[int, int]] = []
    for idx, clausula in enumerate(clausulas):
        if len(clausula) != 2:
            raise ValueError(
                f"{nombre_fichero}: la cláusula {idx + 1} tiene {len(clausula)} "
                f"literal(es); este solver requiere exactamente 2 (Max-2-SAT)."
            )
        resultado.append((clausula[0], clausula[1]))
    return resultado


def _construir_qubo(
    n_variables: int,
    clausulas: list[tuple[int, int]],
) -> tuple[dict[tuple[int, int], float], int]:
    """
    Construye la matriz QUBO como diccionario {(i, j): coeficiente}.

    Las variables QUBO están indexadas desde 0 (variable xi del CNF → índice i-1).
    Los términos off-diagonal se distribuyen simétricamente entre Q[i,j] y Q[j,i].

    Returns:
        Q:          Diccionario de la matriz QUBO.
        constante:  Término constante aditivo (suma de penalizaciones fijas).
    """
    Q: dict[tuple[int, int], float] = {}
    constante = 0

    def add(i: int, j: int, valor: float) -> None:
        """Añade `valor` al término Q[i,j] respetando la simetría."""
        if i == j:
            Q[(i, i)] = Q.get((i, i), 0.0) + valor
        else:
            mitad = valor / 2
            Q[(i, j)] = Q.get((i, j), 0.0) + mitad
            Q[(j, i)] = Q.get((j, i), 0.0) + mitad

    for lit1, lit2 in clausulas:
        neg1 = lit1 < 0
        neg2 = lit2 < 0
        i = abs(lit1) - 1  # índice QUBO base-0
        j = abs(lit2) - 1

        if not neg1 and not neg2:
            # xi ∨ xj  →  1 - xi - xj + xi·xj
            constante += 1
            add(i, i, -1.0)
            add(j, j, -1.0)
            add(i, j, +1.0)

        elif neg1 and not neg2:
            # ¬xi ∨ xj  →  xi - xi·xj
            add(i, i, +1.0)
            add(i, j, -1.0)

        elif not neg1 and neg2:
            # xi ∨ ¬xj  →  xj - xi·xj
            add(j, j, +1.0)
            add(i, j, -1.0)

        else:
            # ¬xi ∨ ¬xj  →  xi·xj
            add(i, j, +1.0)

    return Q, constante


def _resolver_qubo(
    Q: dict[tuple[int, int], float],
    constante: int,
    num_reads: int = 100,
) -> tuple[dict[int, int], float, int]:
    """
    Resuelve el QUBO con el simulador de recocido simulado de dimod.

    Returns:
        muestra:    Asignación binaria {índice_base0: 0|1}.
        energia:    Energía QUBO de la mejor muestra.
        constante:  La misma constante recibida (para calcular cláusulas fuera).
    """
    bqm = dimod.BinaryQuadraticModel.from_qubo(Q)
    sampler = dimod.SimulatedAnnealingSampler()
    resultado = sampler.sample(bqm, num_reads=num_reads)
    mejor = resultado.first
    return mejor.sample, mejor.energy, constante


# ============================================================================
# Algoritmo registrado en el framework
# ============================================================================


@register_algorithm("maxsat_qubo_sa", family="maxsat")
class MaxSATQUBOSAAlgorithm(Algorithm):
    """
    Resuelve Max-2-SAT mediante QUBO y recocido simulado (dimod).

    Lee instancias en formato DIMACS CNF con cláusulas de exactamente
    2 literales. Falla explícitamente si el fichero contiene cláusulas
    de otro tamaño (k ≠ 2).

    El resultado tiene las mismas claves que ``maxsat_brute`` para
    facilitar la comparación directa en el benchmark.
    """

    def __init__(self, num_reads: int = 100) -> None:
        self.num_reads = num_reads

    def run(self, file_path: Path) -> dict[str, Any]:
        text = file_path.read_text(encoding="utf-8")
        num_vars, clausulas_raw = _parse_dimacs(text)

        if not clausulas_raw:
            raise ValueError(
                f"La instancia no contiene ninguna cláusula en {file_path.name}"
            )

        # Valida k=2 y convierte a lista de tuplas
        clausulas = _validar_clausulas_2sat(clausulas_raw, file_path.name)
        num_clausulas = len(clausulas)

        Q, constante = _construir_qubo(num_vars, clausulas)
        muestra, energia_qubo, constante = _resolver_qubo(Q, constante, self.num_reads)

        clausulas_no_sat = int(round(energia_qubo + constante))
        clausulas_sat = num_clausulas - clausulas_no_sat

        return {
            "num_vars": num_vars,
            "num_clausulas": num_clausulas,
            "clausulas_satisfechas": clausulas_sat,
            "satisfaccion_ratio": round(clausulas_sat / num_clausulas, 4),
            "optima": clausulas_sat == num_clausulas,
            "energia_qubo": round(energia_qubo, 6),
            "constante": constante,
            "num_reads": self.num_reads,
            "asignacion": {
                f"x{k + 1}": int(v) for k, v in sorted(muestra.items())
            },
        }
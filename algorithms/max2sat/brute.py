"""
Max-SAT por fuerza bruta.

Formato de fichero de entrada: DIMACS CNF
  c  líneas de comentario (opcionales)
  p cnf <num_vars> <num_clausulas>
  <lit1> <lit2> ... <litk> 0     ← cláusulas terminadas en 0
  ...

Los literales pueden ocupar una o varias líneas; cada cláusula termina en 0.
Se admiten cláusulas de cualquier tamaño, incluida la cláusula vacía.
"""

from __future__ import annotations

import itertools
from pathlib import Path
from typing import Any, Sequence

from framework.core import Algorithm
from framework.registry import register_algorithm


# ============================================================================
# Núcleo del solver (funciones puras, sin estado)
# ============================================================================


def _eval_literal(lit: int, asignacion: dict[int, bool]) -> bool:
    valor = asignacion[abs(lit)]
    return valor if lit > 0 else not valor


def _eval_clausula(clausula: Sequence[int], asignacion: dict[int, bool]) -> bool:
    return any(_eval_literal(lit, asignacion) for lit in clausula)


def _resolver_maxsat_fuerza_bruta(
    num_vars: int, clausulas: list[list[int]]
) -> tuple[int, dict[int, bool]]:
    """
    Fuerza bruta con dos optimizaciones:
    1. Parada temprana si todas las cláusulas quedan satisfechas.
    2. Corte: si la asignación actual ya no puede superar la mejor,
       se abandona sin evaluar el resto de cláusulas.
    """
    num_clausulas = len(clausulas)
    mejor_valor = -1
    mejor_asignacion: dict[int, bool] = {}

    for bits in itertools.product([False, True], repeat=num_vars):
        asignacion = dict(enumerate(bits, start=1))
        satisfechas = 0

        for i, clausula in enumerate(clausulas):
            if _eval_clausula(clausula, asignacion):
                satisfechas += 1

            # Corte: el máximo posible con esta asignación parcial no mejora al mejor
            max_posible = satisfechas + (num_clausulas - i - 1)
            if max_posible <= mejor_valor:
                break

        if satisfechas > mejor_valor:
            mejor_valor = satisfechas
            mejor_asignacion = asignacion

        # Óptimo absoluto: todas las cláusulas satisfechas
        if mejor_valor == num_clausulas:
            break

    return mejor_valor, mejor_asignacion


# ============================================================================
# Parser DIMACS CNF
# ============================================================================


def _parse_dimacs(text: str) -> tuple[int, list[list[int]]]:
    """
    Parsea un fichero DIMACS CNF.

    - Líneas que empiezan por 'c' son comentarios.
    - La línea 'p cnf N M' define variables y cláusulas.
    - Cada cláusula es una secuencia de literales terminada en 0.
    - Las cláusulas pueden distribuirse en varias líneas.
    - Una cláusula vacía (0 sin literales previos) se acepta como
      cláusula insatisfacible.

    Raises:
        ValueError: Si el fichero no tiene cabecera válida, si hay múltiples
            cabeceras, si aparecen literales fuera de rango, si una cláusula
            queda sin cerrar, o si el número de cláusulas leídas no coincide
            con el declarado en la cabecera.
    """
    cabecera_leida = False
    num_vars = 0
    num_clausulas_esperadas: int | None = None
    clausulas: list[list[int]] = []
    clausula_actual: list[int] = []

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        parts = line.split()
        head = parts[0]

        if head == "c":
            continue

        if head == "p":
            if cabecera_leida:
                raise ValueError("Se encontraron múltiples cabeceras DIMACS")
            if len(parts) != 4 or parts[1] != "cnf":
                raise ValueError("Cabecera DIMACS inválida")

            num_vars = int(parts[2])
            num_clausulas_esperadas = int(parts[3])
            cabecera_leida = True
            continue

        if not cabecera_leida:
            raise ValueError("Se encontraron literales antes de la cabecera 'p cnf'")

        for token in parts:
            num = int(token)
            if num == 0:
                clausulas.append(clausula_actual)
                clausula_actual = []
            else:
                if abs(num) > num_vars:
                    raise ValueError(
                        f"Literal fuera de rango: {num} con num_vars={num_vars}"
                    )
                clausula_actual.append(num)

    if clausula_actual:
        raise ValueError("Última cláusula no terminada en 0")

    if not cabecera_leida:
        raise ValueError("No se encontró una cabecera 'p cnf' válida")

    if (
        num_clausulas_esperadas is not None
        and len(clausulas) != num_clausulas_esperadas
    ):
        raise ValueError(
            f"Número de cláusulas incorrecto: "
            f"esperadas {num_clausulas_esperadas}, leídas {len(clausulas)}"
        )

    return num_vars, clausulas


# ============================================================================
# Algoritmo registrado
# ============================================================================


@register_algorithm("maxsat_brute", family="maxsat")
class MaxSATBruteAlgorithm(Algorithm):
    """
    Resuelve Max-SAT por fuerza bruta con parada temprana y corte.
    Lee instancias en formato DIMACS CNF.

    Advertencia: complejidad O(2^n). Solo viable para num_vars pequeños.
    """

    def run(self, file_path: Path) -> dict[str, Any]:
        text = file_path.read_text(encoding="utf-8")
        num_vars, clausulas = _parse_dimacs(text)

        if not clausulas:
            raise ValueError(
                f"La instancia no contiene ninguna cláusula en {file_path.name}"
            )

        mejor_valor, mejor_asignacion = _resolver_maxsat_fuerza_bruta(
            num_vars, clausulas
        )
        num_clausulas = len(clausulas)

        return {
            "num_vars": num_vars,
            "num_clausulas": num_clausulas,
            "clausulas_satisfechas": mejor_valor,
            "satisfaccion_ratio": round(mejor_valor / num_clausulas, 4),
            "optima": mejor_valor == num_clausulas,
            "asignacion": {
                f"x{k}": int(v) for k, v in sorted(mejor_asignacion.items())
            },
        }

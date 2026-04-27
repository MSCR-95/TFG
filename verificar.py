"""
Verificador de soluciones Max-2-SAT.

Lee un problema .cnf y un .jsonl con una asignación de variables y verifica
manualmente cláusula a cláusula si la solución es válida.

Uso:
    python verificar.py <fichero.cnf> <fichero.jsonl>

Los ficheros deben estar en la carpeta 'verificacion/'.

Ejemplo:
    python verificar.py verificacion/instancia_07.cnf verificacion/instancia_07.jsonl
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


# ============================================================================
# Parser DIMACS CNF
# ============================================================================

def _parse_dimacs(text: str) -> tuple[int, list[list[int]]]:
    cabecera_leida = False
    num_vars = 0
    num_clausulas_esperadas = None
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
                raise ValueError("Múltiples cabeceras DIMACS")
            if len(parts) != 4 or parts[1] != "cnf":
                raise ValueError("Cabecera DIMACS inválida")
            num_vars = int(parts[2])
            num_clausulas_esperadas = int(parts[3])
            cabecera_leida = True
            continue
        if not cabecera_leida:
            raise ValueError("Literales antes de cabecera")

        for token in parts:
            num = int(token)
            if num == 0:
                clausulas.append(clausula_actual)
                clausula_actual = []
            else:
                if abs(num) > num_vars:
                    raise ValueError(f"Literal fuera de rango: {num}")
                clausula_actual.append(num)

    if clausula_actual:
        raise ValueError("Última cláusula no terminada en 0")
    if not cabecera_leida:
        raise ValueError("No se encontró cabecera 'p cnf'")
    if num_clausulas_esperadas is not None and len(clausulas) != num_clausulas_esperadas:
        raise ValueError(
            f"Cláusulas esperadas: {num_clausulas_esperadas}, leídas: {len(clausulas)}"
        )

    return num_vars, clausulas


# ============================================================================
# Verificación manual cláusula a cláusula
# ============================================================================

def _verificar_manual(
    asignacion: dict[int, int],
    clausulas: list[list[int]],
) -> tuple[int, list[bool]]:
    """
    Evalúa la asignación contra cada cláusula del problema.

    Args:
        asignacion: {índice_base0: 0|1}
        clausulas:  cláusulas en formato DIMACS (literales con signo, base-1)

    Returns:
        (n_satisfechas, lista_booleana_por_clausula)
    """
    resultados = []
    for clausula in clausulas:
        satisfecha = False
        for lit in clausula:
            idx = abs(lit) - 1
            val = asignacion[idx]
            val_literal = (1 - val) if lit < 0 else val
            if val_literal:
                satisfecha = True
                break
        resultados.append(satisfecha)
    return sum(resultados), resultados


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    if len(sys.argv) != 3:
        print("Uso: python verificar.py <fichero.cnf> <fichero.jsonl>")
        sys.exit(1)

    cnf_path  = Path(sys.argv[1])
    jsonl_path = Path(sys.argv[2])

    # Validar que los ficheros están en la carpeta verificacion/
    for p in (cnf_path, jsonl_path):
        if not p.exists():
            print(f"Error: no se encuentra el fichero '{p}'")
            sys.exit(1)

    # Leer y parsear el CNF
    num_vars, clausulas = _parse_dimacs(cnf_path.read_text(encoding="utf-8"))
    num_clausulas = len(clausulas)

    # Leer el JSONL (primera línea)
    with jsonl_path.open(encoding="utf-8") as f:
        registro = json.loads(f.readline())

    # Formato 1 (framework): {"result": {"asignacion": {...}}}
    # Formato 2 (test):      {"asignacion": {...}}
    if "asignacion" in registro:
        asignacion_raw: dict[str, int] = registro["asignacion"]
    else:
        result = registro.get("result", {})
        asignacion_raw = result.get("asignacion", {})

    if not asignacion_raw:
        print("Error: el fichero no contiene el campo 'asignacion'")
        sys.exit(1)

    # Convertir asignacion {"x1": 0, "x2": 1, ...} → {0: 0, 1: 1, ...} (base-0)
    asignacion: dict[int, int] = {
        int(k[1:]) - 1: int(v) for k, v in asignacion_raw.items()
    }

    # Verificar
    n_sat, detalle = _verificar_manual(asignacion, clausulas)
    es_valida = n_sat == num_clausulas

    # ── Cabecera ──────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("VERIFICACIÓN DE SOLUCIÓN")
    print("=" * 60)
    print(f"  Problema:              {cnf_path.name}")
    print(f"  Solución:              {jsonl_path.name}")
    print(f"  Algoritmo:             {registro.get('algorithm', '?')}")
    print(f"  Variables:             {num_vars}")
    print(f"  Cláusulas satisfechas: {n_sat} / {num_clausulas}")
    print(f"  Ratio:                 {n_sat / num_clausulas:.2%}")
    print(f"  Solución válida:       {'✓ SÍ' if es_valida else '✗ NO'}")
    print()

    if es_valida:
        print("  Todas las cláusulas quedan satisfechas.")
    else:
        # Mostrar qué cláusulas no se satisfacen y qué variables involucran
        print("  Cláusulas NO satisfechas:")
        print()
        for idx, (ok, clausula) in enumerate(zip(detalle, clausulas), start=1):
            if not ok:
                # Formatear la cláusula de forma legible
                literales_str = " ∨ ".join(
                    f"¬x{abs(lit)}" if lit < 0 else f"x{abs(lit)}"
                    for lit in clausula
                )
                # Valores actuales de cada variable en la cláusula
                valores_str = "  →  " + ",  ".join(
                    f"x{abs(lit)}={'1→¬0' if lit < 0 else asignacion_raw.get(f'x{abs(lit)}', '?')}"
                    if False else
                    f"x{abs(lit)}={asignacion_raw.get(f'x{abs(lit)}', '?')}"
                    for lit in clausula
                )
                print(f"    Cláusula {idx:>3}: ( {literales_str} ){valores_str}")

        print()
        # Variables que aparecen en cláusulas no satisfechas
        vars_conflicto = sorted({
            abs(lit)
            for idx, (ok, clausula) in enumerate(zip(detalle, clausulas))
            if not ok
            for lit in clausula
        })
        print(f"  Variables involucradas en cláusulas fallidas: "
              f"{', '.join(f'x{v}' for v in vars_conflicto)}")

    print()
    print("  Asignación completa:")
    for k, v in sorted(asignacion_raw.items(), key=lambda x: int(x[0][1:])):
        print(f"    {k} = {v}")
    print()


if __name__ == "__main__":
    main()
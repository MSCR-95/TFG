"""
Generación del dataset completo de problemas Max-2-SAT.

Estructura generada:
    data/max2sat/
        10/
            10/   → PROBLEM_010_010_01.cnf ... PROBLEM_010_010_50.cnf
            20/   → PROBLEM_010_020_01.cnf ... PROBLEM_010_020_50.cnf
            ...
            100/  → PROBLEM_010_100_01.cnf ... PROBLEM_010_100_50.cnf
        20/
            20/   → PROBLEM_020_020_01.cnf ... PROBLEM_020_020_50.cnf
            ...
        ...
        100/
            100/  → PROBLEM_100_100_01.cnf ... PROBLEM_100_100_50.cnf

Variables:  10, 20, 30, ..., 100  (saltos de 10)
Cláusulas:  desde N_vars hasta 100 (saltos de 10)
Problemas:  50 por cada combinación (nombrados 01-50)
"""

from __future__ import annotations

from pathlib import Path

from algorithms.max2sat.generator import generar_instancias


# ============================================================================
# CONFIGURACIÓN
# ============================================================================

N_PROBLEMAS   = 50          # problemas por combinación
K             = 2           # literales por cláusula (Max-2-SAT)
OUTPUT_ROOT   = Path("data/max2sat")

VARS_MIN      = 10
VARS_MAX      = 100
VARS_STEP     = 10

CLAUSULAS_MAX = 100
CLAUSULAS_STEP = 10


# ============================================================================
# Generación
# ============================================================================

def main() -> None:
    variables = list(range(VARS_MIN, VARS_MAX + 1, VARS_STEP))

    # Calcular total de combinaciones para el progreso
    combinaciones = [
        (v, c)
        for v in variables
        for c in range(v, CLAUSULAS_MAX + 1, CLAUSULAS_STEP)
    ]
    total_combinaciones = len(combinaciones)
    total_ficheros = total_combinaciones * N_PROBLEMAS

    print(f"Dataset Max-2-SAT")
    print(f"  Variables:     {variables}")
    print(f"  Cláusulas:     desde N_vars hasta {CLAUSULAS_MAX} (paso {CLAUSULAS_STEP})")
    print(f"  Problemas:     {N_PROBLEMAS} por combinación")
    print(f"  Combinaciones: {total_combinaciones}")
    print(f"  Ficheros:      {total_ficheros}")
    print(f"  Destino:       {OUTPUT_ROOT}/")
    print()

    generados_total = 0

    for idx, (num_vars, num_clausulas) in enumerate(combinaciones, start=1):
        output_dir = OUTPUT_ROOT / str(num_vars) / str(num_clausulas)
        prefix = f"PROBLEM_{num_vars:03d}_{num_clausulas:03d}"

        rutas = generar_instancias(
            n_files=N_PROBLEMAS,
            num_vars=num_vars,
            num_clausulas=num_clausulas,
            k=K,
            output_dir=output_dir,
            prefix=prefix,
        )

        generados_total += len(rutas)
        print(
            f"  [{idx:>3}/{total_combinaciones}] "
            f"vars={num_vars:>3}  clausulas={num_clausulas:>3}  "
            f"→ {output_dir}  ({len(rutas)} ficheros)"
        )

    print()
    print(f"Generados {generados_total} ficheros en {OUTPUT_ROOT}/")


if __name__ == "__main__":
    main()

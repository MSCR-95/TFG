"""
Resolución del dataset Max-2-SAT completo con maxsat_qubo_sa (annealyn).

Lee los problemas generados por generar_dataset.py y los resuelve agrupando
por (N_variables, N_clausulas). Cada grupo produce un fichero JSONL separado:

    output/max2sat/annealyn/
        10/
            maxsat_annealyn_010_010.jsonl   ← 50 resultados
            maxsat_annealyn_010_020.jsonl
            ...
            maxsat_annealyn_010_100.jsonl
        20/
            maxsat_annealyn_020_020.jsonl
            ...
        ...
        100/
            maxsat_annealyn_100_100.jsonl

Ejecución:
    python resolver_dataset_annealyn.py
"""

from __future__ import annotations

import logging
from pathlib import Path

import algorithms  # noqa: F401 — activa @register_algorithm

from framework import JSONLResultSink, RunnerV2


# ============================================================================
# CONFIGURACIÓN
# ============================================================================

DATA_ROOT    = Path("data/max2sat")
OUTPUT_ROOT  = Path("output/max2sat/annealyn")
ALGORITHM    = "maxsat_qubo_sa"

N_JOBS       = 4        # workers paralelos

VARS_MIN     = 10
VARS_MAX     = 100
VARS_STEP    = 10

CLAUSULAS_MAX  = 100
CLAUSULAS_STEP = 10


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )

    variables = list(range(VARS_MIN, VARS_MAX + 1, VARS_STEP))

    combinaciones = [
        (v, c)
        for v in variables
        for c in range(v, CLAUSULAS_MAX + 1, CLAUSULAS_STEP)
    ]

    logging.info(
        "Dataset: %d combinaciones × 50 problemas = %d jobs totales",
        len(combinaciones),
        len(combinaciones) * 50,
    )

    for idx, (num_vars, num_clausulas) in enumerate(combinaciones, start=1):
        data_dir   = DATA_ROOT / str(num_vars) / str(num_clausulas)
        output_dir = OUTPUT_ROOT / f"{num_vars:03d}"
        output_dir.mkdir(parents=True, exist_ok=True)

        jsonl_path = output_dir / f"maxsat_annealyn_{num_vars:03d}_{num_clausulas:03d}.jsonl"

        if not data_dir.exists():
            logging.warning(
                "[%d/%d] vars=%d clausulas=%d — directorio no existe, saltando: %s",
                idx, len(combinaciones), num_vars, num_clausulas, data_dir,
            )
            continue

        ficheros = sorted(data_dir.glob("*.cnf"))
        if not ficheros:
            logging.warning(
                "[%d/%d] vars=%d clausulas=%d — sin ficheros .cnf en %s",
                idx, len(combinaciones), num_vars, num_clausulas, data_dir,
            )
            continue

        logging.info(
            "[%d/%d] vars=%3d  clausulas=%3d  ficheros=%d  → %s",
            idx, len(combinaciones), num_vars, num_clausulas,
            len(ficheros), jsonl_path.name,
        )

        runner = RunnerV2(max_workers=N_JOBS)
        runner.submit_directory(
            directory=data_dir,
            algorithms=[ALGORITHM],
            pattern="*.cnf",
        )

        sink = JSONLResultSink(jsonl_path)
        written = sink.write_all(runner.run_stream())

        m = runner.metrics
        logging.info(
            "    ok=%d  errores=%d  timeouts=%d  escritos=%d",
            m.completed_ok, m.algorithm_errors, m.timeouts, written,
        )

    logging.info("Completado. Resultados en %s", OUTPUT_ROOT)


if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()

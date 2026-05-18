"""
Resolución del dataset Max-2-SAT con maxsat_brute para un valor de N_VARIABLES.

Edita NUM_VARS antes de cada ejecución para procesar un grupo distinto.
Cada grupo de (N_variables, N_clausulas) produce un fichero JSONL:

    output/max2sat/brute/
        010/
            maxsat_brute_010_010.jsonl   ← 50 resultados
            maxsat_brute_010_020.jsonl
            ...
            maxsat_brute_010_100.jsonl
        020/
            maxsat_brute_020_020.jsonl
            ...

Ejecución:
    python resolver_dataset_brute.py
"""

from __future__ import annotations

import logging
from pathlib import Path

import algorithms  # noqa: F401 — activa @register_algorithm

from framework import JSONLResultSink, RunnerV2


# ============================================================================
# CONFIGURACIÓN — edita NUM_VARS antes de cada ejecución
# ============================================================================

NUM_VARS     = 10       # ← cambia este valor: 10, 20, 30, ..., 100

DATA_ROOT    = Path("data/max2sat")
OUTPUT_ROOT  = Path("output/max2sat/brute")
ALGORITHM    = "maxsat_brute"

N_JOBS       = 4        # workers paralelos
TIMEOUT      = 3600     # 1 hora por problema

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

    clausulas_list = list(range(NUM_VARS, CLAUSULAS_MAX + 1, CLAUSULAS_STEP))
    output_dir = OUTPUT_ROOT / f"{NUM_VARS:03d}"
    output_dir.mkdir(parents=True, exist_ok=True)

    logging.info(
        "Brute force — vars=%d  combinaciones=%d  timeout=%ds",
        NUM_VARS, len(clausulas_list), TIMEOUT,
    )

    for idx, num_clausulas in enumerate(clausulas_list, start=1):
        data_dir   = DATA_ROOT / str(NUM_VARS) / str(num_clausulas)
        jsonl_path = output_dir / f"maxsat_brute_{NUM_VARS:03d}_{num_clausulas:03d}.jsonl"

        if not data_dir.exists():
            logging.warning(
                "[%d/%d] clausulas=%d — directorio no existe, saltando: %s",
                idx, len(clausulas_list), num_clausulas, data_dir,
            )
            continue

        ficheros = sorted(data_dir.glob("*.cnf"))
        if not ficheros:
            logging.warning(
                "[%d/%d] clausulas=%d — sin ficheros .cnf en %s",
                idx, len(clausulas_list), num_clausulas, data_dir,
            )
            continue

        logging.info(
            "[%d/%d] vars=%3d  clausulas=%3d  ficheros=%d  → %s",
            idx, len(clausulas_list), NUM_VARS, num_clausulas,
            len(ficheros), jsonl_path.name,
        )

        runner = RunnerV2(max_workers=N_JOBS, default_timeout=TIMEOUT)
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

    logging.info(
        "Completado vars=%d. Resultados en %s",
        NUM_VARS, output_dir,
    )


if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()

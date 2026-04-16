"""
Prueba rápida: maxsat_brute vs maxsat_qubo_sa sobre instancias fáciles.

Genera 5 instancias 2-CNF pequeñas (10 variables, 15 cláusulas) en un
directorio temporal, las resuelve con ambos algoritmos y compara resultados.

Ejecución:
    python test_smoke.py
"""

import tempfile
from pathlib import Path

import algorithms  # activa el registro de todos los algoritmos

from framework.runner import RunnerV2
from framework.sinks import CSVResultSink
from algorithms.max2sat.generator import generar_instancias


# ── Parámetros de las instancias de prueba ───────────────────────────────────

N_INSTANCIAS = 5
NUM_VARS     = 10
NUM_CLAUSULAS = 15
K            = 2
SEED         = 42


# ── Generación y ejecución ───────────────────────────────────────────────────

def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir) / "instancias"

        print(f"Generando {N_INSTANCIAS} instancias 2-CNF "
              f"({NUM_VARS} vars, {NUM_CLAUSULAS} cláusulas)…")
        generar_instancias(
            n_files=N_INSTANCIAS,
            num_vars=NUM_VARS,
            num_clausulas=NUM_CLAUSULAS,
            k=K,
            output_dir=data_dir,
            prefix="smoke",
            seed=SEED,
        )

        runner = RunnerV2(max_workers=2)
        runner.submit_directory(
            directory=data_dir,
            pattern="*.cnf",
            algorithms=["maxsat_brute", "maxsat_qubo_sa"],
        )

        print(f"\n{'Fichero':<20} {'Algoritmo':<20} {'Sat':>4} {'Total':>5} {'Ratio':>6}  OK?")
        print("-" * 62)

        for r in runner.run_stream():
            if r.error:
                print(f"{'':20} {'ERROR en ' + r.algorithm:<20}  {r.result.get('error', '?')}")
                continue
            res = r.result
            fichero = Path(r.file).stem
            ok = "✓" if res["optima"] else " "
            print(
                f"{fichero:<20} {r.algorithm:<20} "
                f"{res['clausulas_satisfechas']:>4} / {res['num_clausulas']:<5}"
                f"{res['satisfaccion_ratio']:>6.2%}  {ok}"
            )

        m = runner.metrics
        print(f"\nMétricas: {m.completed_ok} ok  |  {m.algorithm_errors} errores  |  {m.timeouts} timeouts")


if __name__ == "__main__":
    main()
import dimod
from dimod.serialization.format import Formatter
from pathlib import Path
from Generador import Generador

# ── Configuración del experimento ────────────────────────────────────────────
CARPETA_TEST  = Path("test")
N_PROBLEMAS   = 10       # ficheros a generar
N_VARIABLES   = 50       # variables por problema
N_CLAUSULAS   = 50       # cláusulas por problema
N_RUNS        = 20       # ejecuciones por problema
N_READS       = 100      # muestras por ejecución


# ─────────────────────────────────────────────────────────────────────────────
# Lectura de ficheros DIMACS CNF
# ─────────────────────────────────────────────────────────────────────────────
def leer_clausulas(path: str) -> tuple[int, list[tuple[int, int]]]:
    clausulas = []
    n_variables = 0
    with open(path) as f:
        for linea in f:
            linea = linea.strip()
            if not linea or linea.startswith("c"):
                continue
            if linea.startswith("p cnf"):
                partes = linea.split()
                n_variables = int(partes[2])
                continue
            literales = [int(x) for x in linea.split() if x != "0"]
            if len(literales) == 2:
                clausulas.append((literales[0], literales[1]))
    return n_variables, clausulas


# ─────────────────────────────────────────────────────────────────────────────
# Construcción de la matriz QUBO
# ─────────────────────────────────────────────────────────────────────────────
def construir_qubo(n_variables: int, clausulas: list[tuple[int, int]]) -> tuple[dict, int]:
    Q = {}
    constante = 0

    def add(i, j, valor):
        if i == j:
            Q[(i, i)] = Q.get((i, i), 0) + valor
        else:
            Q[(i, j)] = Q.get((i, j), 0) + valor / 2
            Q[(j, i)] = Q.get((j, i), 0) + valor / 2

    for lit1, lit2 in clausulas:
        neg1 = lit1 < 0
        neg2 = lit2 < 0
        i = abs(lit1) - 1
        j = abs(lit2) - 1

        if not neg1 and not neg2:       # xi ∨ xj  →  1 - xi - xj + xi*xj
            constante += 1
            add(i, i, -1)
            add(j, j, -1)
            add(i, j, +1)
        elif neg1 and not neg2:         # ¬xi ∨ xj  →  xi - xi*xj   (penaliza xi=1,xj=0)
            add(i, i, +1)
            add(i, j, -1)
        elif not neg1 and neg2:         # xi ∨ ¬xj  →  xj - xi*xj   (penaliza xi=0,xj=1)
            add(j, j, +1)
            add(i, j, -1)
        else:                           # ¬xi ∨ ¬xj  →  xi*xj
            add(i, j, +1)

    return Q, constante


# ─────────────────────────────────────────────────────────────────────────────
# Resolución con recocido simulado
# ─────────────────────────────────────────────────────────────────────────────
def resolver(Q: dict, n_reads: int = N_READS):
    """Devuelve el sampleset completo."""
    return dimod.SimulatedAnnealingSampler().sample_qubo(Q, num_reads=n_reads)


# ─────────────────────────────────────────────────────────────────────────────
# Experimento: ¿coincide el primer sample con la solución encontrada?
# ─────────────────────────────────────────────────────────────────────────────
def experimento(ficheros: list[Path]):
    """
    Para cada fichero, ejecuta N_RUNS veces y comprueba si el primer sample
    del sampleset (ss.first) coincide con la mejor solución hallada
    en el conjunto de todos los runs.

    Como el sampleset de dimod se devuelve ya ordenado por energía,
    ss.first ES SIEMPRE el mejor sample de ese run. Lo que comprobamos es:
    ¿esa energía coincide con el óptimo global encontrado entre todos los runs?
    """
    print(f"\n{'Problema':<40} {'Runs':>5} {'Con óptimo':>12} {'% óptimo':>10} {'E_óptimo':>10}")
    print("─" * 82)

    resumen_global = {"total_runs": 0, "runs_optimo": 0}

    for path in ficheros:
        n_vars, clausulas = leer_clausulas(path)
        Q, constante = construir_qubo(n_vars, clausulas)

        # ── Fase 1: recoger todos los runs ───────────────────────────────────
        samplesets = [resolver(Q) for _ in range(N_RUNS)]

        # Mejor energía encontrada entre todos los runs
        energia_optima = min(ss.first.energy for ss in samplesets)

        # ── Fase 2: contar cuántos runs dieron el óptimo ─────────────────────
        runs_con_optimo = sum(
            1 for ss in samplesets if ss.first.energy == energia_optima
        )
        pct = 100 * runs_con_optimo / N_RUNS

        print(f"{path.name:<40} {N_RUNS:>5} {runs_con_optimo:>12} {pct:>9.1f}% "
              f"{energia_optima:>10.1f}")

        resumen_global["total_runs"]  += N_RUNS
        resumen_global["runs_optimo"] += runs_con_optimo

    # ── Resumen final ─────────────────────────────────────────────────────────
    total  = resumen_global["total_runs"]
    optimo = resumen_global["runs_optimo"]
    print("─" * 82)
    print(f"{'TOTAL':<40} {total:>5} {optimo:>12} {100*optimo/total:>9.1f}%")


# ─────────────────────────────────────────────────────────────────────────────
# Utilidades de visualización (uso puntual, no en el experimento)
# ─────────────────────────────────────────────────────────────────────────────
def mostrar_resultado(ss, clausulas, constante):
    mejor = ss.first
    muestra = mejor.sample
    energia_qubo = mejor.energy

    clausulas_no_sat = energia_qubo + constante
    clausulas_sat    = len(clausulas) - clausulas_no_sat

    print(f"\nSolución encontrada:")
    for var, val in sorted(muestra.items()):
        print(f"  x{var+1} = {val}")
    print(f"\nEnergía QUBO:             {energia_qubo}")
    print(f"Constante aditiva:        {constante}")
    print(f"Cláusulas no satisfechas: {int(clausulas_no_sat)}")
    print(f"Cláusulas satisfechas:    {int(clausulas_sat)} / {len(clausulas)}")

    print(f"\nVerificación por cláusula:")
    for lit1, lit2 in clausulas:
        v1   = muestra[abs(lit1) - 1]
        v2   = muestra[abs(lit2) - 1]
        val1 = (1 - v1) if lit1 < 0 else v1
        val2 = (1 - v2) if lit2 < 0 else v2
        sat  = "S" if (val1 or val2) else "N"
        print(f"  ({'+' if lit1>0 else '-'}x{abs(lit1)} ∨ {'+' if lit2>0 else '-'}x{abs(lit2)}) → {sat}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":

    # ── 1. Generar problemas en carpeta test/ ─────────────────────────────────
    gen = Generador(nSat=2)
    gen.cambiar_nVariables(N_VARIABLES)
    gen.cambiar_nClausulas(N_CLAUSULAS)
    gen.generar_ficheros(N_PROBLEMAS, carpeta=CARPETA_TEST)

    ficheros = sorted(CARPETA_TEST.glob("PROBLEM_*.txt"))
    print(f"Generados {len(ficheros)} problemas en '{CARPETA_TEST}/'")
    print(f"Configuración: {N_VARIABLES} variables, {N_CLAUSULAS} cláusulas, "
          f"{N_RUNS} runs × {N_READS} lecturas\n")

    # ── 2. Ejecutar el experimento ────────────────────────────────────────────
    experimento(ficheros)
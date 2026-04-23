"""
Test standalone de annealyn.

Genera un problema 2-CNF en un directorio temporal, ejecuta el algoritmo
10 veces y por cada ejecución analiza las 10 primeras posiciones del
sampleset. Para cada muestra verifica manualmente cláusula a cláusula
cuántas satisface y lo compara con el valor reportado por el QUBO.

Configuración del problema al principio del script.
"""

from __future__ import annotations

import random
import sys
import io
import tempfile
from pathlib import Path

from dimod.serialization.format import Formatter

import dimod
import neal

from algorithms.max2sat.generator import generar_instancias

# ============================================================================
# CONFIGURACIÓN — edita estos valores
# ============================================================================

N_VARIABLES  = 100    # número de variables del problema
N_CLAUSULAS  = 100    # número de cláusulas
K            = 2      # literales por cláusula (debe ser 2 para Max-2-SAT)
SEED         = random.randint(1, 1000)  # semilla para reproducibilidad (None = aleatorio)

OUTPUT_PATH        = Path(f"pruebas/test_annealyn_{N_VARIABLES}_{N_CLAUSULAS}_{SEED}.txt")  # fichero de salida

N_EJECUCIONES      = 10   # cuántas veces se ejecuta el algoritmo
TOP_K_SAMPLESET    = 10   # cuántas posiciones del sampleset se analizan
NUM_READS          = 100  # lecturas del sampler por ejecución


# ============================================================================
# Parser DIMACS CNF (copiado de brute.py para ser autónomo)
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
        raise ValueError(f"Cláusulas esperadas: {num_clausulas_esperadas}, leídas: {len(clausulas)}")

    return num_vars, clausulas


# ============================================================================
# Construcción de la matriz QUBO
# ============================================================================

def _construir_qubo(
    n_variables: int,
    clausulas: list[tuple[int, int]],
) -> tuple[dict[tuple[int, int], float], int]:
    Q: dict[tuple[int, int], float] = {}
    constante = 0

    def add(i: int, j: int, valor: float) -> None:
        if i == j:
            Q[(i, i)] = Q.get((i, i), 0.0) + valor
        else:
            mitad = valor / 2
            Q[(i, j)] = Q.get((i, j), 0.0) + mitad
            Q[(j, i)] = Q.get((j, i), 0.0) + mitad

    for lit1, lit2 in clausulas:
        neg1 = lit1 < 0
        neg2 = lit2 < 0
        i = abs(lit1) - 1
        j = abs(lit2) - 1

        if not neg1 and not neg2:
            constante += 1
            add(i, i, -1.0)
            add(j, j, -1.0)
            add(i, j, +1.0)
        elif neg1 and not neg2:
            add(i, i, +1.0)
            add(i, j, -1.0)
        elif not neg1 and neg2:
            add(j, j, +1.0)
            add(i, j, -1.0)
        else:
            add(i, j, +1.0)

    return Q, constante


# ============================================================================
# Verificación manual cláusula a cláusula
# ============================================================================

def _verificar_manual(
    muestra: dict[int, int],
    clausulas: list[list[int]],
) -> tuple[int, list[bool]]:
    """
    Evalúa la muestra contra cada cláusula del problema.

    Args:
        muestra:   {índice_base0: 0|1}
        clausulas: cláusulas en formato DIMACS (literales con signo, base-1)

    Returns:
        (n_satisfechas, lista_booleana_por_clausula)
    """
    resultados = []
    for clausula in clausulas:
        satisfecha = False
        for lit in clausula:
            idx = abs(lit) - 1          # base-0
            val = muestra[idx]
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
    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir)

        rutas = generar_instancias(
            n_files=1,
            num_vars=N_VARIABLES,
            num_clausulas=N_CLAUSULAS,
            k=K,
            output_dir=data_dir,
            prefix="test",
            seed=SEED,
        )
        cnf_path = rutas[0]
        dimacs = cnf_path.read_text(encoding="utf-8")

        print(f"Problema generado: {N_VARIABLES} vars  {N_CLAUSULAS} cláusulas  k={K}  seed={SEED}")
        print(f"Fichero: {cnf_path}\n")
        print(dimacs)

        # Parsear
        num_vars, clausulas_raw = _parse_dimacs(dimacs)
        clausulas_2sat = [(c[0], c[1]) for c in clausulas_raw]

        # Construir QUBO
        Q, constante = _construir_qubo(num_vars, clausulas_2sat)

        # Rastrear la mejor solución global entre todas las ejecuciones
        mejor_global_sat      = -1
        mejor_global_muestra  = None
        mejor_global_energia  = None
        mejor_global_ejecucion = None

        # Ejecutar N_EJECUCIONES veces
        for ejecucion in range(1, N_EJECUCIONES + 1):
            print("=" * 70)
            print(f"EJECUCIÓN {ejecucion}/{N_EJECUCIONES}")
            print("=" * 70)

            bqm = dimod.BinaryQuadraticModel.from_qubo(Q)
            sampler = neal.SimulatedAnnealingSampler()
            sampleset = sampler.sample(bqm, num_reads=NUM_READS)
            # print(f"  ****************************")
            # Formatter().fprint(sampleset)
            
            # Truncar al top-K y recorrer
            top = list(sampleset.data(
                fields=["sample", "energy", "num_occurrences"],
                sorted_by="energy",
            ))[:TOP_K_SAMPLESET]

            for pos, dato in enumerate(top, start=1):
                muestra = dato.sample          # type: ignore
                energia = dato.energy          # type: ignore
                n_ocurr = dato.num_occurrences # type: ignore

                # Cálculo por energía (lo que reporta el algoritmo)
                sat_qubo = int(N_CLAUSULAS - (energia + constante))

                # Verificación manual cláusula a cláusula
                sat_manual, detalle = _verificar_manual(muestra, clausulas_raw)

                # Coincidencia
                coincide = sat_qubo == sat_manual
                marca    = "✓" if coincide else "✗ DISCREPANCIA"

                print(f"  Pos {pos:>2} | energía: {energia:>8.2f} | "
                      f"ocurr: {n_ocurr:>3} | "
                      f"QUBO: {sat_qubo:>3}/{N_CLAUSULAS} | "
                      f"manual: {sat_manual:>3}/{N_CLAUSULAS} | {marca}")

                # Si hay discrepancia, mostrar qué cláusulas fallan
                if not coincide:
                    fallidas = [i+1 for i, ok in enumerate(detalle) if not ok]
                    print(f"         Cláusulas NO satisfechas: {fallidas}")

                # Actualizar mejor global
                if sat_manual > mejor_global_sat:
                    mejor_global_sat       = sat_manual
                    mejor_global_muestra   = dict(muestra)
                    mejor_global_energia   = energia
                    mejor_global_ejecucion = ejecucion

            print()

        # Resumen final
        print("=" * 70)
        print("MEJOR SOLUCIÓN ENCONTRADA")
        print("=" * 70)
        print(f"  Ejecución:             {mejor_global_ejecucion}/{N_EJECUCIONES}")
        print(f"  Energía QUBO:          {mejor_global_energia:.2f}")
        print(f"  Cláusulas satisfechas: {mejor_global_sat}/{N_CLAUSULAS}")
        print(f"  Ratio:                 {mejor_global_sat/N_CLAUSULAS:.2%}")
        print(f"  Óptima:                {'Sí' if mejor_global_sat == N_CLAUSULAS else 'No'}")
        print()
        print("  Asignación de variables:")
        for idx in sorted(mejor_global_muestra.keys()): # type: ignore
            print(f"    x{idx + 1} = {mejor_global_muestra[idx]}") # type: ignore
        print()


if __name__ == "__main__":
    # Capturar toda la salida
    buffer = io.StringIO()
    tee = io.TextIOWrapper(buffer.buffer if hasattr(buffer, 'buffer') else buffer, write_through=True) if False else buffer

    class Tee:
        """Escribe simultáneamente en stdout y en el buffer."""
        def __init__(self, *streams):
            self.streams = streams
        def write(self, data):
            for s in self.streams:
                s.write(data)
        def flush(self):
            for s in self.streams:
                s.flush()

    tee = Tee(sys.stdout, buffer)
    sys.stdout = tee  # type: ignore

    main()

    sys.stdout = sys.__stdout__

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(buffer.getvalue(), encoding="utf-8")
    print(f"Salida guardada en {OUTPUT_PATH}")
"""
Verificación masiva de soluciones annealyn.

Para cada resultado exitoso en output/max2sat/annealyn/, coge la asignación
devuelta por el algoritmo, la evalúa cláusula a cláusula contra el .cnf
original, y comprueba que el conteo propio coincide con el que reportó el
algoritmo (clausulas_satisfechas).

Una discrepancia indica un bug: en la formación QUBO, en la lectura del
sampleset, o en el conteo interno del algoritmo.

Uso:
    python verificar_annealyn.py [--data-root DATA] [--output-root OUTPUT]
                              [--verbose]

Defaults:
    --data-root    data/max2sat
    --output-root  output/max2sat/annealyn
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# Parser DIMACS CNF
# ─────────────────────────────────────────────────────────────────────────────

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
        if parts[0] == "c":
            continue
        if parts[0] == "p":
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


# ─────────────────────────────────────────────────────────────────────────────
# Evaluación independiente
# ─────────────────────────────────────────────────────────────────────────────

def _evaluar(asignacion: dict[int, int], clausulas: list[list[int]]) -> int:
    """Cuenta cláusulas satisfechas evaluando la asignación nosotros mismos."""
    n = 0
    for clausula in clausulas:
        for lit in clausula:
            val = asignacion[abs(lit) - 1]
            if (lit > 0 and val == 1) or (lit < 0 and val == 0):
                n += 1
                break
    return n


# ─────────────────────────────────────────────────────────────────────────────
# Lectura del JSONL (último intento por job_id)
# ─────────────────────────────────────────────────────────────────────────────

def _load_final_records(jsonl_path: Path) -> list[dict]:
    last: dict[str, dict] = {}
    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            jid = r.get("job_id", "") # asumimos que cada job_id corresponde a una instancia concreta
            if jid not in last or r.get("attempt", 1) > last[jid].get("attempt", 1):
                last[jid] = r
    return list(last.values())


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root",   default="data/max2sat")
    parser.add_argument("--output-root", default="output/max2sat/annealyn")
    parser.add_argument("--verbose", action="store_true",
                        help="Mostrar cada discrepancia encontrada")
    args = parser.parse_args()

    data_root   = Path(args.data_root)
    output_root = Path(args.output_root)

    if not output_root.exists():
        print(f"Error: no existe '{output_root}'")
        return

    jsonl_files = sorted(output_root.rglob("*.jsonl"))
    if not jsonl_files:
        print(f"No se encontraron ficheros JSONL en '{output_root}'")
        return

    total         = 0   # resultados exitosos procesados
    errores_cnf   = 0   # CNF no encontrado o no parseable
    ok            = 0   # asignación coincide con lo reportado
    discrepancias = 0   # asignación NO coincide

    discrepancia_detalle: list[str] = []

    for jsonl_path in sorted(jsonl_files):
        for r in _load_final_records(jsonl_path):

            # Sólo resultados exitosos
            if r.get("error", True):
                continue

            result = r.get("result", {})
            asignacion_raw: dict[str, int] | None = result.get("asignacion")
            reportado: int | None = result.get("clausulas_satisfechas")

            if not asignacion_raw or reportado is None:
                errores_cnf += 1
                continue

            # Localizar el CNF
            cnf_path = Path(r["file"])
            if not cnf_path.exists():
                matches = list(data_root.rglob(cnf_path.name))
                if not matches:
                    errores_cnf += 1
                    if args.verbose:
                        print(f"[CNF NO ENCONTRADO] {cnf_path.name}")
                    continue
                cnf_path = matches[0]

            try:
                _, clausulas = _parse_dimacs(cnf_path.read_text(encoding="utf-8"))
            except Exception as e:
                errores_cnf += 1
                if args.verbose:
                    print(f"[ERROR PARSE] {cnf_path.name}: {e}")
                continue

            # Convertir asignación {"x1": 0, ...} → {0: 0, 1: 1, ...} base-0
            asignacion = {int(k[1:]) - 1: int(v) for k, v in asignacion_raw.items()}

            verificado = _evaluar(asignacion, clausulas)
            total += 1

            if verificado == reportado:
                ok += 1
            else:
                discrepancias += 1
                msg = (
                    f"  {cnf_path.name:<40}"
                    f"  reportado={reportado:>4}"
                    f"  verificado={verificado:>4}"
                    f"  diff={verificado - reportado:+d}"
                    f"  total_clausulas={len(clausulas)}"
                )
                discrepancia_detalle.append(msg)

    # ── Informe ───────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("VERIFICACIÓN MASIVA  —  annealyn")
    print("=" * 60)
    print(f"  Resultados procesados:   {total:>6}")
    print(f"  CNF no localizados:      {errores_cnf:>6}")
    print(f"  Sin discrepancias (OK):  {ok:>6}")
    print(f"  Con discrepancias:       {discrepancias:>6}")
    print()

    if discrepancias == 0:
        print("  ✓ Todas las asignaciones coinciden con lo reportado.")
        print("  La implementación del algoritmo es consistente.")
    else:
        pct = discrepancias / total * 100
        print(f"  ✗ {discrepancias} discrepancias ({pct:.1f}% del total).")
        print("  Revisar: formación QUBO, lectura del sampleset o conteo interno.")
        if args.verbose:
            print()
            print("  Detalle:")
            for d in discrepancia_detalle:
                print(d)
        else:
            print()
            print("  Ejecuta con --verbose para ver el detalle instancia a instancia.")

    print()


if __name__ == "__main__":
    main()
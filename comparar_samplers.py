"""
Compara los resultados de annealingAntiguo (neal) vs annealing (dwave.samplers).

Uso:
    python comparar_samplers.py \
        --antiguo output/max2sat/annealingAntiguo \
        --nuevo   output/max2sat/annealing \
        --out     comparacion_samplers.csv
"""

import json
import csv
import argparse
from pathlib import Path
from collections import defaultdict

# ---------------------------------------------------------------------------
# Carga
# ---------------------------------------------------------------------------

def cargar_jsonl(carpeta: Path) -> dict[str, dict]:
    """Lee todos los .jsonl de una carpeta (recursivo) y devuelve {file -> registro}."""
    registros = {}
    for path in sorted(carpeta.rglob("*.jsonl")):
        with open(path, encoding="utf-8") as f:
            for linea in f:
                linea = linea.strip()
                if not linea:
                    continue
                r = json.loads(linea)
                clave = r["file"]          # path al .cnf → identificador único del problema
                if clave in registros:
                    # Si hay duplicados quedarse con el de menor duration (mejor run)
                    if r["duration_s"] < registros[clave]["duration_s"]:
                        registros[clave] = r
                else:
                    registros[clave] = r
    return registros


# ---------------------------------------------------------------------------
# Comparación
# ---------------------------------------------------------------------------

def comparar(antiguo: dict, nuevo: dict) -> list[dict]:
    """
    Para cada problema presente en ambos, genera una fila con las métricas de ambos samplers.
    Solo se incluyen problemas que aparecen en los dos conjuntos.
    """
    claves_comunes = sorted(set(antiguo) & set(nuevo))
    print(f"Problemas en antiguo : {len(antiguo)}")
    print(f"Problemas en nuevo   : {len(nuevo)}")
    print(f"Problemas en común   : {len(claves_comunes)}")

    filas = []
    for clave in claves_comunes:
        a = antiguo[clave]
        n = nuevo[clave]
        ra = a["result"]
        rn = n["result"]

        filas.append({
            "file":                     clave,
            "num_vars":                 ra["num_vars"],
            "num_clausulas":            ra["num_clausulas"],
            "ratio_clausulas_vars":     round(ra["num_clausulas"] / ra["num_vars"], 4),
            # Tiempos
            "dur_antiguo_s":            a["duration_s"],
            "dur_nuevo_s":              n["duration_s"],
            "speedup":                  round(a["duration_s"] / n["duration_s"], 4) if n["duration_s"] > 0 else None,
            # Calidad
            "sat_ratio_antiguo":        ra["satisfaccion_ratio"],
            "sat_ratio_nuevo":          rn["satisfaccion_ratio"],
            "optima_antiguo":           ra["optima"],
            "optima_nuevo":             rn["optima"],
            # Diferencia en cláusulas satisfechas
            "delta_clausulas":          rn["clausulas_satisfechas"] - ra["clausulas_satisfechas"],
        })
    return filas


# ---------------------------------------------------------------------------
# Resumen agregado por (num_vars, num_clausulas)
# ---------------------------------------------------------------------------

def resumen_agregado(filas: list[dict]) -> list[dict]:
    grupos = defaultdict(list)
    for f in filas:
        grupos[(f["num_vars"], f["num_clausulas"])].append(f)

    resumen = []
    for (nv, nc), grupo in sorted(grupos.items()):
        n = len(grupo)
        resumen.append({
            "num_vars":              nv,
            "num_clausulas":         nc,
            "ratio_c_v":             round(nc / nv, 4),
            "n_problemas":           n,
            # Tiempos medios
            "dur_media_antiguo_s":   round(sum(f["dur_antiguo_s"] for f in grupo) / n, 6),
            "dur_media_nuevo_s":     round(sum(f["dur_nuevo_s"]   for f in grupo) / n, 6),
            "speedup_medio":         round(sum(f["speedup"] for f in grupo if f["speedup"]) / n, 4),
            # Calidad media
            "sat_ratio_media_antiguo": round(sum(f["sat_ratio_antiguo"] for f in grupo) / n, 4),
            "sat_ratio_media_nuevo":   round(sum(f["sat_ratio_nuevo"]   for f in grupo) / n, 4),
            # Tasa de óptimos
            "pct_optima_antiguo":    round(sum(1 for f in grupo if f["optima_antiguo"]) / n * 100, 2),
            "pct_optima_nuevo":      round(sum(1 for f in grupo if f["optima_nuevo"])   / n * 100, 2),
        })
    return resumen


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

def guardar_csv(filas: list[dict], path: Path):
    if not filas:
        print(f"⚠ Sin datos para guardar en {path}")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)
    print(f"✓ Guardado: {path}  ({len(filas)} filas)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--antiguo", required=True, help="Carpeta annealingAntiguo (neal)")
    parser.add_argument("--nuevo",   required=True, help="Carpeta annealing (dwave.samplers)")
    parser.add_argument("--out",     default="comparacion_samplers.csv",
                        help="CSV de comparación por problema")
    args = parser.parse_args()

    carpeta_antiguo = Path(args.antiguo)
    carpeta_nuevo   = Path(args.nuevo)

    print(f"Cargando antiguo desde: {carpeta_antiguo}")
    antiguo = cargar_jsonl(carpeta_antiguo)

    print(f"Cargando nuevo desde:   {carpeta_nuevo}")
    nuevo = cargar_jsonl(carpeta_nuevo)

    filas = comparar(antiguo, nuevo)

    # CSV detallado (un problema por fila)
    out_detalle = Path(args.out)
    guardar_csv(filas, out_detalle)

    # CSV resumen agregado por (vars, clausulas)
    out_resumen = out_detalle.with_name(out_detalle.stem + "_resumen.csv")
    resumen = resumen_agregado(filas)
    guardar_csv(resumen, out_resumen)

    # Estadística global rápida
    if filas:
        speedups = [f["speedup"] for f in filas if f["speedup"]]
        mejor = max(speedups)
        peor  = min(speedups)
        medio = sum(speedups) / len(speedups)
        print(f"\n--- Speedup global (antiguo/nuevo) ---")
        print(f"  Medio : {medio:.4f}x")
        print(f"  Máximo: {mejor:.4f}x  (nuevo más rápido)")
        print(f"  Mínimo: {peor:.4f}x")


if __name__ == "__main__":
    main()

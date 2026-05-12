"""
analizador.py
=============
Carga todos los resultados JSONL de la carpeta output/, extrae los campos
relevantes y genera las gráficas comparativas para el TFG.

Uso:
    python analizador.py                     # busca en ./output/
    python analizador.py --dir mis_resultados
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import seaborn as sns


# ============================================================================
# 1. Carga de datos
# ============================================================================

def cargar_resultados(directorio: Path) -> pd.DataFrame:
    ficheros = sorted(directorio.rglob("*.jsonl"))
    if not ficheros:
        raise FileNotFoundError(f"No se encontraron ficheros .jsonl en {directorio}")

    print(f"Cargando {len(ficheros)} fichero(s) JSONL...")
    df = pd.concat(
        [pd.read_json(f, lines=True) for f in ficheros],
        ignore_index=True,
    )
    print(f"  → {len(df)} registros totales")

    # Quedarse solo con el último intento de cada job (por si hay reintentos)
    df = (
        df.sort_values("attempt")
          .groupby("job_id", as_index=False)
          .last()
    )
    print(f"  → {len(df)} jobs únicos tras filtrar reintentos intermedios")
    return df


# ============================================================================
# 2. Expansión del campo result
# ============================================================================

def expandir_result(df: pd.DataFrame) -> pd.DataFrame:
    """Extrae los campos del dict result a columnas planas."""
    campos = ["num_vars", "num_clausulas", "clausulas_satisfechas",
              "satisfaccion_ratio", "optima"]

    ok = df[~df["error"]].copy()
    for campo in campos:
        ok[campo] = ok["result"].apply(
            lambda r, c=campo: r.get(c) if isinstance(r, dict) else None
        )
    return ok


def construir_lookup_vars(df_ok: pd.DataFrame) -> dict:
    """Mapa fichero → num_vars construido desde los registros exitosos."""
    return (
        df_ok.dropna(subset=["num_vars"])
             .set_index("file")["num_vars"]
             .astype(int)
             .to_dict()
    )


# ============================================================================
# 3. Gráficas
# ============================================================================

def grafica_tiempo(df_ok: pd.DataFrame, out: Path) -> None:
    """
    Boxplot de duration_s por num_vars y algoritmo en escala log.
    Añade puntos individuales (stripplot) encima para ver la dispersión real.
    """
    fig, ax = plt.subplots(figsize=(14, 6))

    sns.boxplot(
        data=df_ok,
        x="num_vars",
        y="duration_s",
        hue="algorithm",
        flierprops={"marker": ""},
        ax=ax,
    )
    sns.stripplot(
        data=df_ok,
        x="num_vars",
        y="duration_s",
        hue="algorithm",
        dodge=True,
        size=2.5,
        alpha=0.4,
        legend=False,
        ax=ax,
    )

    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda v, _: f"{v:g}s"
    ))
    ax.set_title("Tiempo de ejecución por número de variables")
    ax.set_xlabel("Número de variables")
    ax.set_ylabel("Duración (escala logarítmica)")
    ax.legend(title="Algoritmo")
    fig.tight_layout()

    path = out / "boxplot_tiempo.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Guardada: {path}")


def grafica_ratio_satisfaccion(df_ok: pd.DataFrame, out: Path) -> None:
    """
    Violin plot del ratio de satisfacción para QUBO-SA (todos los tamaños).
    Brute force se superpone como puntos donde tiene datos.
    """
    fig, ax = plt.subplots(figsize=(14, 6))

    qubo = df_ok[df_ok["algorithm"] == "maxsat_qubo_sa"]
    brute = df_ok[df_ok["algorithm"] == "maxsat_brute"]

    sns.violinplot(
        data=qubo,
        x="num_vars",
        y="satisfaccion_ratio",
        color="tab:orange",
        inner="box",
        ax=ax,
    )
    sns.stripplot(
        data=brute,
        x="num_vars",
        y="satisfaccion_ratio",
        color="tab:blue",
        size=4,
        alpha=0.6,
        ax=ax,
    )

    # Leyenda manual para evitar duplicados que genera violinplot
    import matplotlib.patches as mpatches
    import matplotlib.lines as mlines
    leyenda = [
        mpatches.Patch(color="tab:orange", label="maxsat_qubo_sa"),
        mlines.Line2D([], [], color="tab:blue", marker="o", linestyle="",
                      markersize=5, label="maxsat_brute"),
    ]
    ax.set_title("Ratio de satisfacción por número de variables")
    ax.set_xlabel("Número de variables")
    ax.set_ylabel("Cláusulas satisfechas / total")
    ax.legend(handles=leyenda, title="Algoritmo")
    fig.tight_layout()

    path = out / "violin_ratio_satisfaccion.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Guardada: {path}")


def grafica_timeout_brute(df_full: pd.DataFrame, df_ok: pd.DataFrame, out: Path) -> None:
    """
    Barras apiladas para brute force: completados vs timeout por num_vars.
    Deja claro visualmente dónde deja de ser viable.
    """
    lookup = construir_lookup_vars(df_ok)

    brute = df_full[df_full["algorithm"] == "maxsat_brute"].copy()
    brute["num_vars"] = brute["file"].map(lookup)
    brute = brute.dropna(subset=["num_vars"])
    brute["num_vars"] = brute["num_vars"].astype(int)

    resumen = (
        brute.groupby("num_vars")
             .agg(completados=("timed_out", lambda x: (~x).sum()),
                  timeouts=("timed_out", "sum"))
             .reset_index()
             .sort_values("num_vars")
    )

    fig, ax = plt.subplots(figsize=(10, 5))
    x = resumen["num_vars"].astype(str)
    ax.bar(x, resumen["completados"], label="Completados", color="tab:blue")
    ax.bar(x, resumen["timeouts"], bottom=resumen["completados"],
           label="Timeout", color="tab:red", alpha=0.8)

    ax.set_title("Brute force: completados vs timeout por número de variables")
    ax.set_xlabel("Número de variables")
    ax.set_ylabel("Número de instancias")
    ax.legend(title="Estado")
    fig.tight_layout()

    path = out / "barras_timeout_brute.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Guardada: {path}")


def grafica_optimalidad_qubo(df_ok: pd.DataFrame, out: Path) -> None:
    """
    Tasa de optimalidad de QUBO-SA por ratio clausulas/variables.
    Muestra la transición de fase: cerca de ratio ~1.5 el problema es más duro.
    """
    qubo = df_ok[df_ok["algorithm"] == "maxsat_qubo_sa"].copy()
    qubo["ratio_cv"] = (qubo["num_clausulas"] / qubo["num_vars"]).round(2)

    tasa = (
        qubo.groupby("ratio_cv")["optima"]
            .mean()
            .reset_index()
            .rename(columns={"optima": "tasa_optima"})
    )

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.lineplot(
        data=tasa,
        x="ratio_cv",
        y="tasa_optima",
        marker="o",
        color="tab:orange",
        ax=ax,
    )

    ax.axvline(x=1.5, color="gray", linestyle="--", linewidth=1,
               label="Transición de fase (~1.5)")
    ax.set_title("Tasa de satisfacibilidad completa por ratio cláusulas/variables")
    ax.set_xlabel("Ratio cláusulas / variables")
    ax.set_ylabel("Fracción de instancias con todas las cláusulas satisfechas")
    ax.annotate("Caída brusca:\ninstancias pasan a\nser insatisfacibles",
                xy=(1.5, 0.55), xytext=(2.5, 0.7),
                arrowprops=dict(arrowstyle="->", color="gray"),
                fontsize=9, color="gray")
    ax.set_ylim(-0.05, 1.05)
    ax.legend()
    fig.tight_layout()

    path = out / "linea_optimalidad_ratio.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Guardada: {path}")


# ============================================================================
# 4. Resumen por consola
# ============================================================================

def imprimir_resumen(df_full: pd.DataFrame, df_ok: pd.DataFrame) -> None:
    print("\n--- Resumen general ---")
    tmp = df_full.copy()
    tmp["completado"] = ~tmp["error"]
    tmp["error_puro"] = tmp["error"] & ~tmp["timed_out"]
    print(tmp.groupby("algorithm").agg(
        total=("job_id", "count"),
        completados=("completado", "sum"),
        timeouts=("timed_out", "sum"),
        errores=("error_puro", "sum"),
    ).to_string())

    print("\n--- Tiempo (s) por algoritmo (solo completados) ---")
    print(df_ok.groupby("algorithm")["duration_s"].describe().round(4).to_string())

    print("\n--- Ratio de satisfacción por algoritmo (solo completados) ---")
    print(df_ok.groupby("algorithm")["satisfaccion_ratio"].describe().round(4).to_string())


# ============================================================================
# 5. Main
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Analizador de resultados Max-2-SAT")
    parser.add_argument(
        "--dir",
        type=Path,
        default=Path("output"),
        help="Directorio con los ficheros .jsonl (default: ./output)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("output/graficas"),
        help="Directorio donde guardar las gráficas (default: ./output/graficas)",
    )
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)

    # Carga
    df_full = cargar_resultados(args.dir)

    # Expansión
    df_ok = expandir_result(df_full)
    df_ok["num_vars"] = df_ok["num_vars"].astype(int)

    # Resumen
    imprimir_resumen(df_full, df_ok)

    # Gráficas
    print("\nGenerando gráficas...")
    grafica_tiempo(df_ok, args.out)
    grafica_ratio_satisfaccion(df_ok, args.out)
    grafica_timeout_brute(df_full, df_ok, args.out)
    grafica_optimalidad_qubo(df_ok, args.out)

    print(f"\nListo. Gráficas guardadas en {args.out}/")


if __name__ == "__main__":
    main()
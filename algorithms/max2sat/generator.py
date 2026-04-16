"""
Generador de instancias k-CNF en formato DIMACS CNF.

Uso como CLI:
    python -m algorithms.max2sat.generator \
        --n-files 5 \
        --vars 10 \
        --clauses 15 \
        --k 2 \
        --output-dir data/max2sat \
        [--prefix instancia] \
        [--seed 42]

Genera N ficheros .cnf con exactamente `vars` variables distintas
y `clauses` cláusulas de `k` literales cada una.

Garantías:
- Cada cláusula contiene exactamente `k` literales.
- Dentro de una misma cláusula no hay variables repetidas.
- En cada fórmula aparecen todas las variables 1..vars al menos una vez.
- El signo de cada literal se elige aleatoriamente.
"""

from __future__ import annotations

import argparse
import random
from pathlib import Path


# ============================================================================
# Generación de fórmulas k-CNF
# ============================================================================


def generar_clausula_k(num_vars: int, k: int, rng: random.Random) -> list[int]:
    """
    Genera una cláusula aleatoria con exactamente `k` literales y variables distintas.

    Args:
        num_vars: Número total de variables disponibles.
        k:        Número de literales por cláusula.
        rng:      Generador aleatorio.

    Returns:
        Lista de enteros con signo, por ejemplo [1, -3, 4].

    Raises:
        ValueError: Si k < 1 o k > num_vars.
    """
    if k < 1:
        raise ValueError("k debe ser >= 1.")
    if k > num_vars:
        raise ValueError(
            f"k={k} no puede ser mayor que num_vars={num_vars} "
            "si se exigen variables distintas dentro de cada cláusula."
        )

    variables = rng.sample(range(1, num_vars + 1), k)
    return [v if rng.random() < 0.5 else -v for v in variables]


def generar_formula_kcnf(
    num_vars: int,
    num_clausulas: int,
    k: int,
    rng: random.Random,
) -> list[list[int]]:
    """
    Genera una fórmula k-CNF usando exactamente `num_vars` variables distintas.

    La construcción es directa, sin reintentos globales:
    - cada variable 1..num_vars se coloca al menos una vez;
    - después se rellenan los huecos restantes;
    - dentro de cada cláusula no se repiten variables.

    Args:
        num_vars:      Número de variables distintas que debe usar la fórmula.
        num_clausulas: Número de cláusulas de la fórmula.
        k:             Número de literales por cláusula.
        rng:           Generador aleatorio.

    Returns:
        Lista de cláusulas, donde cada cláusula es una lista de enteros con signo.

    Raises:
        ValueError: Si la combinación de parámetros es imposible.
        RuntimeError: Si ocurre una inconsistencia interna inesperada.
    """
    if num_vars < 1 or num_clausulas < 1 or k < 1:
        raise ValueError("num_vars, num_clausulas y k deben ser >= 1.")
    if k > num_vars:
        raise ValueError(f"k={k} no puede ser mayor que num_vars={num_vars}.")
    if num_clausulas * k < num_vars:
        raise ValueError(
            f"Imposible cubrir {num_vars} variables con "
            f"{num_clausulas}×{k}={num_clausulas * k} posiciones."
        )

    todas_vars = list(range(1, num_vars + 1))

    # Cada cláusula se construye primero como conjunto de variables sin signo.
    # Usar sets simplifica evitar repeticiones dentro de la cláusula.
    clausulas_sets: list[set[int]] = [set() for _ in range(num_clausulas)]
    capacidad = [k] * num_clausulas

    # ------------------------------------------------------------------------
    # Paso 1: colocar cada variable al menos una vez en alguna cláusula
    # ------------------------------------------------------------------------
    pendientes = todas_vars.copy()
    rng.shuffle(pendientes)

    for var in pendientes:
        candidatas = [i for i in range(num_clausulas) if capacidad[i] > 0]
        if not candidatas:
            raise RuntimeError(
                "No se pudo asignar cobertura completa de variables (bug interno)."
            )

        idx = rng.choice(candidatas)
        clausulas_sets[idx].add(var)
        capacidad[idx] -= 1

    # ------------------------------------------------------------------------
    # Paso 2: rellenar los huecos restantes sin repetir variables dentro de
    # cada cláusula
    # ------------------------------------------------------------------------
    for i in range(num_clausulas):
        faltan = k - len(clausulas_sets[i])
        if faltan > 0:
            disponibles = [v for v in todas_vars if v not in clausulas_sets[i]]
            clausulas_sets[i].update(rng.sample(disponibles, faltan))

    # ------------------------------------------------------------------------
    # Paso 3: asignar signo aleatorio y desordenar el orden interno de cada
    # cláusula
    # ------------------------------------------------------------------------
    clausulas: list[list[int]] = []
    for vars_set in clausulas_sets:
        vars_list = list(vars_set)
        rng.shuffle(vars_list)
        clausula = [v if rng.random() < 0.5 else -v for v in vars_list]
        clausulas.append(clausula)

    # Mezclar también el orden de las cláusulas en la fórmula.
    rng.shuffle(clausulas)
    return clausulas


# ============================================================================
# Serialización DIMACS CNF
# ============================================================================


def formula_a_dimacs(
    clausulas: list[list[int]],
    num_vars: int,
    comentario: str = "",
) -> str:
    """
    Serializa una fórmula CNF al formato DIMACS CNF.

    Formato:
        c comentario opcional
        p cnf <num_vars> <num_clausulas>
        <lit1> <lit2> ... <litk> 0

    Args:
        clausulas:  Lista de cláusulas.
        num_vars:   Número de variables.
        comentario: Comentario opcional en cabecera.

    Returns:
        Texto DIMACS terminado en salto de línea.
    """
    lines: list[str] = []
    if comentario:
        lines.append(f"c {comentario}")
    lines.append(f"p cnf {num_vars} {len(clausulas)}")
    for clausula in clausulas:
        lines.append(" ".join(str(lit) for lit in clausula) + " 0")
    return "\n".join(lines) + "\n"


# ============================================================================
# API pública (importable desde otros módulos)
# ============================================================================


def generar_instancias(
    n_files: int,
    num_vars: int,
    num_clausulas: int,
    k: int,
    output_dir: Path,
    prefix: str = "instancia",
    seed: int | None = None,
) -> list[Path]:
    """
    Genera `n_files` ficheros DIMACS CNF en `output_dir`.

    Args:
        n_files:        Número de ficheros a generar.
        num_vars:       Número de variables distintas por fórmula.
        num_clausulas:  Número de cláusulas por fórmula.
        k:              Número de literales por cláusula.
        output_dir:     Directorio de salida (se crea si no existe).
        prefix:         Prefijo del nombre de fichero.
        seed:           Semilla para reproducibilidad (None = aleatorio).

    Returns:
        Lista de rutas a los ficheros generados.
    """
    if n_files < 1:
        raise ValueError("n_files debe ser >= 1.")

    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    generados: list[Path] = []

    digits = len(str(n_files))

    for i in range(1, n_files + 1):
        clausulas = generar_formula_kcnf(
            num_vars=num_vars,
            num_clausulas=num_clausulas,
            k=k,
            rng=rng,
        )
        comentario = (
            f"{prefix}_{i:0{digits}d}  vars={num_vars}  "
            f"clausulas={num_clausulas}  k={k}"
        )
        contenido = formula_a_dimacs(clausulas, num_vars, comentario)

        path = output_dir / f"{prefix}_{i:0{digits}d}.cnf"
        path.write_text(contenido, encoding="utf-8")
        generados.append(path)

    return generados


# ============================================================================
# CLI
# ============================================================================


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generador de instancias k-CNF en formato DIMACS CNF"
    )
    p.add_argument(
        "--n-files",
        type=int,
        required=True,
        help="Número de ficheros a generar",
    )
    p.add_argument(
        "--vars",
        type=int,
        required=True,
        dest="num_vars",
        help="Número de variables distintas por fórmula",
    )
    p.add_argument(
        "--clauses",
        type=int,
        required=True,
        dest="num_clausulas",
        help="Número de cláusulas por fórmula",
    )
    p.add_argument(
        "--k",
        type=int,
        required=True,
        help="Número de literales por cláusula",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/max2sat"),
        help="Directorio de salida (default: data/max2sat)",
    )
    p.add_argument(
        "--prefix",
        default="instancia",
        help="Prefijo del nombre de fichero (default: instancia)",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Semilla aleatoria para reproducibilidad",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    try:
        rutas = generar_instancias(
            n_files=args.n_files,
            num_vars=args.num_vars,
            num_clausulas=args.num_clausulas,
            k=args.k,
            output_dir=args.output_dir,
            prefix=args.prefix,
            seed=args.seed,
        )
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    print(f"Generados {len(rutas)} ficheros en {args.output_dir}/")
    for ruta in rutas:
        print(f"  {ruta}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

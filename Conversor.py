"""
Conversor de problemas 3-SAT a Max-2-SAT.

Transformación basada en:
    Garey, Johnson & Stockmeyer (1976).

Cada cláusula 3-SAT  (x₁ ∨ x₂ ∨ x₃)  se reemplaza por 10 cláusulas de 2
literales introduciendo una variable auxiliar x₄:

    (x₁), (x₂), (x₃), (x₄),
    (¬x₁ ∨ ¬x₂), (¬x₂ ∨ ¬x₃), (¬x₁ ∨ ¬x₃),
    (x₁ ∨ ¬x₄), (x₂ ∨ ¬x₄), (x₃ ∨ ¬x₄)

Propiedad clave:
    - Cláusula 3-SAT satisfacible  → exactamente 7/10 cláusulas 2-SAT satisfechas.
    - Cláusula 3-SAT insatisfacible → máximo 6/10 cláusulas 2-SAT satisfechas.

Formato de los ficheros generados (mismo que los 3-SAT):
    Cada línea es una cláusula. Los literales se separan por espacios y la
    cláusula termina con '0'. Los negativos se indican con '-'.
    Ejemplo:  1 -3 0

Nombre del fichero de salida:
    PROBLEM_{n_clausulas_2sat:03}_{n_variables_2sat:03}_{i}.txt
    donde:
        n_clausulas_2sat  = n_clausulas_3sat * 10
        n_variables_2sat  = n_variables_3sat + n_clausulas_3sat  (una auxiliar por cláusula)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Tuple


# ---------------------------------------------------------------------------
# Lógica de conversión
# ---------------------------------------------------------------------------

def convertir_clausula(
    literales: List[int], var_auxiliar: int
) -> List[List[int]]:
    """Convierte una cláusula 3-SAT en 10 cláusulas Max-2-SAT.

    Args:
        literales:     lista de exactamente 3 enteros con signo (ej. [1, -2, 3]).
        var_auxiliar:  índice de la variable auxiliar nueva para esta cláusula.

    Returns:
        Lista de 10 cláusulas, cada una como lista de literales enteros.
        Las cláusulas de un solo literal se representan como [literal, literal]
        para mantener el formato de 2-SAT (cláusula con literal repetido equivale
        a (xᵢ ∨ xᵢ) = xᵢ).
    """
    a, b, c = literales
    aux = var_auxiliar

    return [
        # Literales solos: (x) ≡ (x ∨ x)
        [a, a],          # (x₁)
        [b, b],          # (x₂)
        [c, c],          # (x₃)
        [aux, aux],      # (x₄)
        # Pares negados
        [-a, -b],        # (¬x₁ ∨ ¬x₂)
        [-b, -c],        # (¬x₂ ∨ ¬x₃)
        [-a, -c],        # (¬x₁ ∨ ¬x₃)
        # Pares con auxiliar negado
        [a, -aux],       # (x₁ ∨ ¬x₄)
        [b, -aux],       # (x₂ ∨ ¬x₄)
        [c, -aux],       # (x₃ ∨ ¬x₄)
    ]


def convertir_fichero(
    ruta_entrada: Path,
    n_variables_orig: int,
) -> Tuple[List[List[int]], int, int]:
    """Lee un fichero 3-SAT y devuelve las cláusulas Max-2-SAT resultantes.

    Args:
        ruta_entrada:      ruta al fichero 3-SAT.
        n_variables_orig:  número de variables del problema 3-SAT original
                           (se extrae del nombre del fichero por el Conversor).

    Returns:
        (clausulas_2sat, n_clausulas_2sat, n_variables_2sat)
    """
    clausulas_3sat: List[List[int]] = []
    with ruta_entrada.open("r") as f:
        for linea in f:
            partes = linea.strip().split()
            if not partes:
                continue
            # Literales = todos los enteros antes del '0' final
            literales = [int(p) for p in partes if p != "0"]
            if len(literales) == 3:
                clausulas_3sat.append(literales)

    clausulas_2sat: List[List[int]] = []
    for idx, literales in enumerate(clausulas_3sat):
        # Variable auxiliar: primera variable libre después de las originales
        var_auxiliar = n_variables_orig + idx + 1
        clausulas_2sat.extend(convertir_clausula(literales, var_auxiliar))

    n_clausulas_2sat = len(clausulas_2sat)                        # = n_3sat * 10
    n_variables_2sat = n_variables_orig + len(clausulas_3sat)     # + una auxiliar por cláusula

    return clausulas_2sat, n_clausulas_2sat, n_variables_2sat


def _clausula_a_str(literales: List[int]) -> str:
    """Serializa una cláusula a la representación de texto del proyecto."""
    return " ".join(str(l) for l in literales) + " 0"


# ---------------------------------------------------------------------------
# Clase principal
# ---------------------------------------------------------------------------

class Conversor:
    """Convierte ficheros 3-SAT a Max-2-SAT y los guarda en una carpeta de salida."""

    def __init__(self, carpeta_salida: str = "problemas") -> None:
        self.carpeta_salida = carpeta_salida
        os.makedirs(self.carpeta_salida, exist_ok=True)

    def convertir_fichero(self, ruta_entrada: Path) -> Path:
        """Convierte un fichero 3-SAT a Max-2-SAT y guarda el resultado.

        El nombre del fichero de salida refleja las dimensiones reales del
        problema Max-2-SAT:
            PROBLEM_{n_clausulas_2sat:03}_{n_variables_2sat:03}_{i}.txt

        Returns:
            Ruta del fichero Max-2-SAT generado.
        """
        # Extraer dimensiones originales y número de instancia del nombre
        partes = ruta_entrada.stem.split("_")  # ['PROBLEM', '005', '005', '1']
        n_variables_orig = int(partes[2])
        instancia = partes[3]

        clausulas_2sat, n_clausulas_2sat, n_variables_2sat = convertir_fichero(
            ruta_entrada, n_variables_orig
        )

        nombre_salida = (
            f"PROBLEM_{n_clausulas_2sat:03}_{n_variables_2sat:03}_{instancia}.txt"
        )
        ruta_salida = Path(self.carpeta_salida) / nombre_salida

        with ruta_salida.open("w") as f:
            for clausula in clausulas_2sat:
                f.write(_clausula_a_str(clausula) + "\n")

        return ruta_salida

    def convertir_carpeta(self, carpeta_entrada: Path, patron: str = "*.txt") -> List[Path]:
        """Convierte todos los ficheros 3-SAT de una carpeta a Max-2-SAT.

        Args:
            carpeta_entrada: carpeta con los ficheros 3-SAT.
            patron:          glob para seleccionar ficheros (por defecto '*.txt').

        Returns:
            Lista de rutas de los ficheros Max-2-SAT generados.
        """
        ficheros = sorted(carpeta_entrada.glob(patron))
        if not ficheros:
            print(f"  [Conversor] No se encontraron ficheros en {carpeta_entrada}")
            return []

        rutas_salida = []
        for ruta in ficheros:
            ruta_salida = self.convertir_fichero(ruta)
            rutas_salida.append(ruta_salida)

        print(f"  [Conversor] {len(rutas_salida)} ficheros convertidos a Max-2-SAT")
        return rutas_salida

import threading
from itertools import product

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Importa la interfaz del framework
from Framework import Algorithm, register_algorithm


@register_algorithm("fuerza_bruta_optimizado")
class Fuerza_bruta_optimizado(Algorithm):
    """Algoritmo de fuerza bruta optimizado para resolver problemas Max-SAT.

    Optimizaciones respecto a Fuerza_bruta:
    - Caché de combinaciones compartida entre instancias (thread-safe)
    - Para en cuanto encuentra una solución que satisface TODAS las cláusulas,
      sin seguir buscando combinaciones que no pueden mejorar el resultado
    """

    # Compartida entre TODAS las instancias y workers
    _cache_combinaciones: Dict[int, List[Tuple]] = {}
    _lock = threading.Lock()

    def run(self, file_path: Path) -> Dict[str, Any]:
        """Método requerido por el framework: lee el fichero y resuelve."""
        terminos = self.leer_terminos(file_path)
        n_variables = self.extraer_n_variables(file_path)
        solucion, clausulas_satisfechas = self.resolver(terminos, n_variables)

        n_clausulas = len(terminos)
        satisfacible = clausulas_satisfechas == n_clausulas

        return {
            "satisfacible": satisfacible,
            "solucion": solucion,
            "n_variables": n_variables,
            "clausulas_satisfechas": clausulas_satisfechas,
            "n_clausulas": n_clausulas,
        }

    def _get_combinaciones(self, n_variables: int) -> List[Tuple]:
        """Devuelve las combinaciones para n_variables, generándolas solo la primera vez."""
        if n_variables in self._cache_combinaciones:
            return self._cache_combinaciones[n_variables]

        with self._lock:
            # Double-checked locking: comprueba de nuevo dentro del lock
            if n_variables not in self._cache_combinaciones:
                self._cache_combinaciones[n_variables] = list(
                    product([0, 1], repeat=n_variables)
                )
        return self._cache_combinaciones[n_variables]

    def extraer_n_variables(self, file_path: Path) -> int:
        """Extrae el número de variables del nombre del fichero.

        El nombre sigue el patrón: PROBLEM_{nClausuras:03}_{nVariables:03}_{i}.txt
        Ejemplo: PROBLEM_005_005_1.txt → 5 variables
        """
        partes = file_path.stem.split("_")  # ['PROBLEM', '005', '005', '1']
        return int(partes[2])

    def leer_terminos(self, file_path: Path) -> List[List[str]]:
        with file_path.open("r") as f:
            lineas = f.readlines()
        return [linea.strip().split() for linea in lineas if linea.strip()]

    def evaluar_condicion(self, termino: List[str], valores: Dict[int, int]) -> bool:
        """Evalúa si una cláusula (OR de literales) se satisface con los valores dados."""
        for literal in termino:
            if literal == "0":  # Fin de la cláusula
                break
            var = abs(int(literal))
            valor = valores.get(var, 0)
            if (literal.startswith("-") and valor == 0) or (
                not literal.startswith("-") and valor == 1
            ):
                return True  # Basta con que un literal sea True (OR)
        return False

    def contar_clausulas_satisfechas(
        self, terminos: List[List[str]], valores: Dict[int, int]
    ) -> int:
        """Cuenta cuántas cláusulas se satisfacen con una asignación dada."""
        return sum(
            1 for termino in terminos if self.evaluar_condicion(termino, valores)
        )

    def resolver(
        self, terminos: List[List[str]], n_variables: int
    ) -> Tuple[Optional[Dict[int, int]], int]:
        """Encuentra la primera asignación que satisface TODAS las cláusulas.

        A diferencia de Fuerza_bruta, no sigue buscando una vez encontrada
        la solución óptima (todas las cláusulas satisfechas).

        Devuelve:
            - La mejor asignación encontrada
            - El número de cláusulas que satisface
        """
        variables = list(range(1, n_variables + 1))
        combinaciones = self._get_combinaciones(n_variables)

        mejor_solucion: Optional[Dict[int, int]] = None
        mejor_count = -1
        n_clausulas = len(terminos)

        for combinacion in combinaciones:
            valores = dict(zip(variables, combinacion))
            count = self.contar_clausulas_satisfechas(terminos, valores)

            if count > mejor_count:
                mejor_count = count
                mejor_solucion = valores

                # OPTIMIZACIÓN: si satisface todas, para inmediatamente
                if mejor_count == n_clausulas:
                    break

        return mejor_solucion, mejor_count
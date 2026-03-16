import threading
from itertools import product

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Importa la interfaz del framework
from Framework import Algorithm, register_algorithm

MAX_VARIABLES_CACHE = 20  # 2²⁰ = ~1M combinaciones, seguro en memoria


@register_algorithm("fuerza_bruta")
class Fuerza_bruta(Algorithm):
    """Algoritmo de fuerza bruta para resolver problemas Max-SAT.

    Recorre TODAS las combinaciones posibles sin parar antes de tiempo,
    garantizando encontrar la asignación que satisface el MAYOR número
    de cláusulas posible.
    """

    # Compartida entre TODAS las instancias y workers
    _cache_combinaciones: Dict[int, List[Tuple]] = {}
    _lock = threading.Lock()  # para la concurrencia en el acceso a la caché

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

    def _get_combinaciones(self, n_variables: int):
        """Usa caché para n_variables <= MAX_VARIABLES_CACHE, generador lazy para los grandes."""
        if n_variables <= MAX_VARIABLES_CACHE:
            if n_variables in self._cache_combinaciones:
                return self._cache_combinaciones[n_variables]
            with self._lock:
                if n_variables not in self._cache_combinaciones:
                    self._cache_combinaciones[n_variables] = list(
                        product([0, 1], repeat=n_variables)
                    )
            return self._cache_combinaciones[n_variables]
        else:
            return product([0, 1], repeat=n_variables)

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
                return True
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
        """Recorre TODAS las combinaciones para encontrar la asignación óptima.

        No para aunque encuentre una solución completa — garantiza el óptimo
        global a costa de explorar el espacio de búsqueda completo.
        """
        variables = list(range(1, n_variables + 1))
        combinaciones = self._get_combinaciones(n_variables)

        mejor_solucion: Optional[Dict[int, int]] = None
        mejor_count = -1

        for combinacion in combinaciones:
            valores = dict(zip(variables, combinacion))
            count = self.contar_clausulas_satisfechas(terminos, valores)

            if count > mejor_count:
                mejor_count = count
                mejor_solucion = valores
            # No hay break: siempre recorre todas las combinaciones

        return mejor_solucion, mejor_count
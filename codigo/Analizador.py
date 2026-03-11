"""
Analizador de resultados del framework.

Procesa los ResultRecord generados por el Runner para extraer
estadísticas de rendimiento agrupadas por algoritmo y complejidad
del problema (n_clausulas, n_variables).

Uso actual:
    - Tiempo total de ejecución por algoritmo

Preparado para el futuro:
    - Gráficas de tiempo vs complejidad
    - Comparativa entre algoritmos
    - Tasa de satisfacibilidad
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
from collections import defaultdict

from codigo.Framework import ResultRecord, ResultSink


class Analizador(ResultSink):
    """Procesa ResultRecord para calcular estadísticas de rendimiento.

    Al ser un ResultSink, se puede usar directamente con MultiSink
    junto a JSONLResultSink o CSVResultSink.

    Ejemplo de uso:
        analizador = Analizador()
        sink = MultiSink(JSONLResultSink(Path("resultados.jsonl")), analizador)
        sink.write_all(results)
        analizador.imprimir_resumen()
    """

    def __init__(self):
        # Almacena todos los registros procesados
        self._records: List[ResultRecord] = []

    def write_all(self, records) -> None:
        """Recibe los ResultRecord del Runner y los almacena para su análisis."""
        self._records = list(records)

    # ------------------------------------------------------------------
    # Estadísticas generales
    # ------------------------------------------------------------------

    def tiempo_total(self, algoritmo: Optional[str] = None) -> float:
        """Tiempo total de ejecución en segundos.

        Args:
            algoritmo: Si se especifica, filtra solo ese algoritmo.
                       Si es None, suma todos los algoritmos.
        """
        registros = self._filtrar_por_algoritmo(algoritmo)
        return round(sum(r.duration_s for r in registros), 6)

    def tiempo_medio(self, algoritmo: Optional[str] = None) -> float:
        """Tiempo medio de ejecución por fichero en segundos."""
        registros = self._filtrar_por_algoritmo(algoritmo)
        if not registros:
            return 0.0
        return round(sum(r.duration_s for r in registros) / len(registros), 6)

    def tiempo_por_algoritmo(self) -> Dict[str, float]:
        """Devuelve el tiempo total agrupado por algoritmo."""
        tiempos: Dict[str, float] = defaultdict(float)
        for r in self._records:
            tiempos[r.algorithm] += r.duration_s
        return {algo: round(t, 6) for algo, t in tiempos.items()}

    # ------------------------------------------------------------------
    # Estadísticas por complejidad (preparado para gráficas)
    # ------------------------------------------------------------------

    def tiempo_por_n_clausulas(self, algoritmo: Optional[str] = None) -> Dict[int, float]:
        """Devuelve el tiempo medio agrupado por número de cláusulas.

        Útil para graficar cómo crece el tiempo al aumentar la complejidad.
        Extrae n_clausulas del nombre del fichero: PROBLEM_{nClausulas:03}_...
        """
        registros = self._filtrar_por_algoritmo(algoritmo)

        tiempos_por_grupo: Dict[int, List[float]] = defaultdict(list)
        for r in registros:
            n_clausulas = self._extraer_n_clausulas(r.file)
            tiempos_por_grupo[n_clausulas].append(r.duration_s)

        return {
            n: round(sum(ts) / len(ts), 6)
            for n, ts in sorted(tiempos_por_grupo.items())
        }

    # ------------------------------------------------------------------
    # Imprimir resultados por consola
    # ------------------------------------------------------------------

    def imprimir_resumen(self) -> None:
        """Imprime un resumen de tiempos de ejecución por consola."""
        if not self._records:
            print("No hay resultados que analizar.")
            return

        print("\n" + "=" * 50)
        print("RESUMEN DE EJECUCIÓN")
        print("=" * 50)

        tiempos = self.tiempo_por_algoritmo()
        for algoritmo, tiempo in tiempos.items():
            registros = self._filtrar_por_algoritmo(algoritmo)
            print(f"\nAlgoritmo: {algoritmo}")
            print(f"  Ficheros procesados : {len(registros)}")
            print(f"  Tiempo total        : {tiempo:.6f} s")
            print(f"  Tiempo medio        : {self.tiempo_medio(algoritmo):.6f} s")

            tiempos_complejidad = self.tiempo_por_n_clausulas(algoritmo)
            if tiempos_complejidad:
                print("  Tiempo medio por nº de cláusulas:")
                for n_clausulas, t_medio in tiempos_complejidad.items():
                    print(f"    {n_clausulas:>4} cláusulas → {t_medio:.6f} s")

        print("=" * 50 + "\n")

    # ------------------------------------------------------------------
    # Métodos auxiliares privados
    # ------------------------------------------------------------------

    def _filtrar_por_algoritmo(self, algoritmo: Optional[str]) -> List[ResultRecord]:
        """Filtra registros por algoritmo, o devuelve todos si es None."""
        if algoritmo is None:
            return self._records
        return [r for r in self._records if r.algorithm == algoritmo]

    def _extraer_n_clausulas(self, file_path: str) -> int:
        """Extrae n_clausulas del nombre del fichero.

        El nombre sigue el patrón: PROBLEM_{nClausulas:03}_{nVariables:03}_{i}.txt
        """
        nombre = Path(file_path).stem          # 'PROBLEM_005_005_1'
        partes = nombre.split("_")             # ['PROBLEM', '005', '005', '1']
        return int(partes[1])

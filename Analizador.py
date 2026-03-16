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
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from Framework import ResultRecord, ResultSink


class Analizador(ResultSink):
    """Procesa ResultRecord para calcular estadísticas de rendimiento.

    Al ser un ResultSink, se puede usar directamente con MultiSink
    junto a JSONLResultSink o CSVResultSink.

    Ejemplo de uso:
        analizador = Analizador(n_jobs=2)
        sink = MultiSink(JSONLResultSink(Path("resultados.jsonl")), analizador)
        sink.write_all(results)
        analizador.imprimir_resumen()
    """

    def __init__(self, n_jobs: int = 1):
        self.n_jobs = n_jobs
        self._records: List[ResultRecord] = []

    def write_all(self, records) -> None:
        """Recibe los ResultRecord del Runner y los almacena para su análisis."""
        self._records = list(records)

    # ------------------------------------------------------------------
    # Estadísticas generales
    # ------------------------------------------------------------------

    def tiempo_total(self, algoritmo: Optional[str] = None) -> float:
        """Tiempo total de ejecución en segundos."""
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

    def tiempo_por_dimensiones(
        self, algoritmo: Optional[str] = None
    ) -> Dict[Tuple[int, int], float]:
        """Devuelve el tiempo medio agrupado por (n_clausulas, n_variables)."""
        registros = self._filtrar_por_algoritmo(algoritmo)

        tiempos_por_grupo: Dict[Tuple[int, int], List[float]] = defaultdict(list)
        for r in registros:
            clave = self._extraer_dimensiones(r.file)
            tiempos_por_grupo[clave].append(r.duration_s)

        return {
            clave: round(sum(ts) / len(ts), 6)
            for clave, ts in sorted(tiempos_por_grupo.items())
        }

    # ------------------------------------------------------------------
    # Imprimir resultados por consola
    # ------------------------------------------------------------------

    def imprimir_resumen(self) -> None:
        """Imprime un resumen de tiempos de ejecución por consola."""
        if not self._records:
            print("No hay resultados que analizar.")
            return

        print("\n" + "=" * 55)
        print("RESUMEN DE EJECUCIÓN")
        print(f"Workers (n_jobs): {self.n_jobs}")
        print("=" * 55)

        tiempos = self.tiempo_por_algoritmo()
        for algoritmo, tiempo in tiempos.items():
            registros = self._filtrar_por_algoritmo(algoritmo)
            print(f"\nAlgoritmo: {algoritmo}")
            print(f"  Ficheros procesados : {len(registros)}")
            print(f"  Tiempo total        : {tiempo:.6f} s")
            print(f"  Tiempo medio        : {self.tiempo_medio(algoritmo):.6f} s")

            tiempos_dimensiones = self.tiempo_por_dimensiones(algoritmo)
            if tiempos_dimensiones:
                print(f"\n  {'Cláusulas':>10}  {'Variables':>10}  {'Tiempo medio':>14}")
                print(f"  {'-'*10}  {'-'*10}  {'-'*14}")
                for (n_clausulas, n_variables), t_medio in tiempos_dimensiones.items():
                    print(
                        f"  {n_clausulas:>10}  {n_variables:>10}  {t_medio:>12.6f} s"
                    )

        print("\n" + "=" * 55 + "\n")

    # ------------------------------------------------------------------
    # Métodos auxiliares privados
    # ------------------------------------------------------------------

    def _filtrar_por_algoritmo(self, algoritmo: Optional[str]) -> List[ResultRecord]:
        """Filtra registros por algoritmo, o devuelve todos si es None."""
        if algoritmo is None:
            return self._records
        return [r for r in self._records if r.algorithm == algoritmo]

    def _extraer_dimensiones(self, file_path: str) -> Tuple[int, int]:
        """Extrae (n_clausulas, n_variables) del nombre del fichero.

        El nombre sigue el patrón: PROBLEM_{nClausulas:03}_{nVariables:03}_{i}.txt
        Ejemplo: PROBLEM_005_010_1.txt → (5, 10)
        """
        nombre = Path(file_path).stem       # 'PROBLEM_005_010_1'
        partes = nombre.split("_")          # ['PROBLEM', '005', '010', '1']
        return int(partes[1]), int(partes[2])
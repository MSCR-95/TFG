"""
Analizador de resultados del framework.

Procesa los ResultRecord generados por el Runner para extraer
estadísticas de rendimiento agrupadas por algoritmo y complejidad
del problema (n_clausulas, n_variables).

Métricas disponibles:
    - Tiempo CPU acumulado: suma de duration_s de todos los jobs.
      Refleja el coste computacional total independientemente del paralelismo.
    - Tiempo wall clock: desde el primer started_at hasta el último ended_at.
      Refleja el tiempo real transcurrido durante la ejecución paralela.

Preparado para el futuro:
    - Gráficas de tiempo vs complejidad
    - Comparativa entre algoritmos
    - Tasa de satisfacibilidad
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from Framework import ResultRecord, ResultSink

_DATETIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%f+00:00",
    "%Y-%m-%dT%H:%M:%S+00:00",
]


def _parse_dt(s: str) -> datetime:
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Formato de fecha no reconocido: {s}")


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
    # Tiempo CPU acumulado (suma de duration_s)
    # ------------------------------------------------------------------

    def tiempo_cpu(self, algoritmo: Optional[str] = None) -> float:
        """Suma de duration_s de todos los jobs (tiempo CPU acumulado)."""
        registros = self._filtrar_por_algoritmo(algoritmo)
        return round(sum(r.duration_s for r in registros), 6)

    def tiempo_medio(self, algoritmo: Optional[str] = None) -> float:
        """Tiempo medio de CPU por fichero en segundos."""
        registros = self._filtrar_por_algoritmo(algoritmo)
        if not registros:
            return 0.0
        return round(sum(r.duration_s for r in registros) / len(registros), 6)

    def tiempo_cpu_por_algoritmo(self) -> Dict[str, float]:
        """Devuelve el tiempo CPU acumulado agrupado por algoritmo."""
        tiempos: Dict[str, float] = defaultdict(float)
        for r in self._records:
            tiempos[r.algorithm] += r.duration_s
        return {algo: round(t, 6) for algo, t in tiempos.items()}

    # ------------------------------------------------------------------
    # Tiempo wall clock (primer started_at → último ended_at)
    # ------------------------------------------------------------------

    def tiempo_wall_clock(self, algoritmo: Optional[str] = None) -> float:
        """Tiempo real transcurrido en segundos (wall clock).

        Calculado como la diferencia entre el primer started_at y el
        último ended_at de los registros del algoritmo indicado.
        """
        registros = self._filtrar_por_algoritmo(algoritmo)
        if not registros:
            return 0.0
        t_inicio = min(_parse_dt(r.started_at) for r in registros)
        t_fin    = max(_parse_dt(r.ended_at)   for r in registros)
        return round((t_fin - t_inicio).total_seconds(), 6)

    def tiempo_wall_clock_por_algoritmo(self) -> Dict[str, float]:
        """Devuelve el tiempo wall clock agrupado por algoritmo."""
        algoritmos = {r.algorithm for r in self._records}
        return {algo: self.tiempo_wall_clock(algo) for algo in sorted(algoritmos)}

    # ------------------------------------------------------------------
    # Estadísticas por complejidad (preparado para gráficas)
    # ------------------------------------------------------------------

    def tiempo_por_dimensiones(
        self, algoritmo: Optional[str] = None
    ) -> Dict[Tuple[int, int], float]:
        """Devuelve el tiempo medio de CPU agrupado por (n_clausulas, n_variables)."""
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

        tiempos_cpu = self.tiempo_cpu_por_algoritmo()
        tiempos_wc  = self.tiempo_wall_clock_por_algoritmo()

        for algoritmo in tiempos_cpu:
            registros = self._filtrar_por_algoritmo(algoritmo)
            print(f"\nAlgoritmo: {algoritmo}")
            print(f"  Ficheros procesados : {len(registros)}")
            print(f"  Tiempo CPU total    : {tiempos_cpu[algoritmo]:.6f} s  "
                  f"(suma de todos los jobs)")
            print(f"  Tiempo wall clock   : {tiempos_wc.get(algoritmo, 0):.6f} s  "
                  f"(tiempo real transcurrido)")
            print(f"  Tiempo CPU medio    : {self.tiempo_medio(algoritmo):.6f} s")

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
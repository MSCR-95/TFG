"""
framework.sinks
===============
Sinks de persistencia para ``ResultRecord``.

Un sink consume un iterable de ``ResultRecord`` en streaming y lo persiste
en el formato elegido. El diseño deliberado en torno a iterables permite que
el CLI y la API programática pasen directamente el generador de
``run_stream()``, sin materializar todos los registros en memoria.

Formatos disponibles
--------------------
``JSONLResultSink``
    JSON Lines (una línea = un JSON completo). Conserva el campo ``result``
    como dict anidado. Ideal para pipelines, procesamiento con scripts y
    relectura incremental.

``CSVResultSink``
    CSV estándar con cabecera. El campo ``result`` se serializa como JSON
    string dentro de la celda correspondiente. Útil para inspección rápida
    en hojas de cálculo o herramientas BI.

Extensibilidad
--------------
Para añadir un formato nuevo, hereda de ``ResultSink`` e implementa
``write_all(records) -> int``.
"""

from __future__ import annotations

import csv
import dataclasses
import json
from abc import ABC, abstractmethod
from collections.abc import Iterable
from pathlib import Path

from framework.core import ResultRecord


# ============================================================================
# Sinks de resultados
# ============================================================================

class ResultSink(ABC):
    """
    Contrato base para todos los sinks de resultados.

    Un sink recibe un iterable de ``ResultRecord`` y los persiste en algún
    formato o destino. La interfaz es intencionalmente mínima para facilitar
    la implementación de nuevos sinks.
    """

    @abstractmethod
    def write_all(self, records: Iterable[ResultRecord]) -> int:
        """
        Escribe todos los registros del iterable y devuelve el total escrito.

        Parameters
        ----------
        records:
            Iterable de ``ResultRecord``. Puede ser un generador (streaming);
            el sink no debe materializarlo en memoria completo.

        Returns
        -------
        int
            Número de registros escritos.
        """
        ...


class JSONLResultSink(ResultSink):
    """
    Sink que escribe resultados en formato JSON Lines (JSONL).

    Cada ``ResultRecord`` se serializa como un objeto JSON completo en
    su propia línea. El campo ``result`` se conserva como dict anidado.
    El directorio de salida se crea automáticamente si no existe.

    Parameters
    ----------
    path:
        Ruta del fichero de salida (``.jsonl`` por convención).

    Examples
    --------
    ::

        sink = JSONLResultSink(Path("output/results.jsonl"))
        n = sink.write_all(runner.run_stream())
        print(f"Escritos {n} registros")
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def write_all(self, records: Iterable[ResultRecord]) -> int:
        count = 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(dataclasses.asdict(r), ensure_ascii=False) + "\n")
                count += 1
        return count


class CSVResultSink(ResultSink):
    """
    Sink que escribe resultados en formato CSV.

    La cabecera se genera a partir de los campos de ``ResultRecord``. El
    campo ``result`` (un dict) se serializa como JSON string dentro de su
    celda. El directorio de salida se crea automáticamente si no existe.

    Parameters
    ----------
    path:
        Ruta del fichero de salida (``.csv`` por convención).

    Notes
    -----
    Para recuperar el campo ``result`` desde el CSV::

        import json, csv
        with open("results.csv") as f:
            for row in csv.DictReader(f):
                result = json.loads(row["result"])

    Examples
    --------
    ::

        sink = CSVResultSink(Path("output/results.csv"))
        n = sink.write_all(runner.run_stream())
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def write_all(self, records: Iterable[ResultRecord]) -> int:
        count = 0
        fieldnames = [f.name for f in dataclasses.fields(ResultRecord)]
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in records:
                row = dataclasses.asdict(r)
                row["result"] = json.dumps(row["result"], ensure_ascii=False)
                writer.writerow(row)
                count += 1
        return count

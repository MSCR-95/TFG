"""
Framework para ejecutar algoritmos con una interfaz común sobre
los ficheros de un directorio, en paralelo con joblib, midiendo tiempos
y registrando resultados.

Patrones usados:
- Strategy: cada algoritmo implementa la misma interfaz (Algorithm).
- Registry/Factory: registro de algoritmos por nombre y construcción dinámica.
- Strategy (salidas): distintos ResultSink para persistir resultados.
- Template Method (opcional): hooks before_run/after_run en Algorithm.

Ejemplo de uso:

    python framework.py \
        --dir ./data \
        --pattern "*.txt" \
        --algos word_count sha256 \
        --out jsonl \
        --out-path results.jsonl \
        --n-jobs -1

Requisitos: joblib (pip install joblib)
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, cast

from joblib import Parallel, delayed

# ---------------------------------------------------------------------------
# Modelo de resultado
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ResultRecord:
    file: str
    algorithm: str
    duration_s: float
    started_at: str
    ended_at: str
    result: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        d = dataclasses.asdict(self)
        return d

# ---------------------------------------------------------------------------
# Interfaz de algoritmo (Strategy) + Registro (Factory)
# ---------------------------------------------------------------------------

_ALGO_REGISTRY: Dict[str, type["Algorithm"]] = {}


def register_algorithm(name: str):
    """Decorator para registrar algoritmos por nombre."""
    def _wrap(cls: type["Algorithm"]):
        key = name.strip().lower()
        if key in _ALGO_REGISTRY:
            raise ValueError(f"Algorithm '{name}' ya está registrado")
        _ALGO_REGISTRY[key] = cls
        cls.__algo_name__ = key  # type: ignore[attr-defined]
        return cls
    return _wrap


def build_algorithms(names: Sequence[str]) -> List["Algorithm"]:
    algos: List[Algorithm] = []
    for n in names:
        key = n.strip().lower()
        try:
            algos.append(_ALGO_REGISTRY[key]())
        except KeyError:
            raise KeyError(f"Algoritmo desconocido: '{n}'. Registrados: {list(_ALGO_REGISTRY)}")
    return algos


class Algorithm(ABC):
    """Interfaz base de algoritmos.

    Implementa Template Method con hooks opcionales.
    """

    @property
    def name(self) -> str:
        return getattr(self, "__algo_name__", self.__class__.__name__).lower()

    def before_run(self, file_path: Path) -> None:
        """Hook opcional antes de ejecutar."""

    @abstractmethod
    def run(self, file_path: Path) -> Dict[str, Any]:
        """Ejecuta el algoritmo sobre el fichero y devuelve un dict serializable."""

    def after_run(self, file_path: Path, result: Dict[str, Any]) -> None:
        """Hook opcional después de ejecutar."""

# ---------------------------------------------------------------------------
# Sinks de resultados (Strategy)
# ---------------------------------------------------------------------------

class ResultSink(ABC):
    @abstractmethod
    def write_all(self, records: Iterable[ResultRecord]) -> None:
        ...


class JSONLResultSink(ResultSink):
    def __init__(self, path: Path) -> None:
        self.path = path

    def write_all(self, records: Iterable[ResultRecord]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r.to_dict(), ensure_ascii=False) + "\n")


class CSVResultSink(ResultSink):
    def __init__(self, path: Path) -> None:
        self.path = path

    def write_all(self, records: Iterable[ResultRecord]) -> None:
        import csv

        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "file",
                    "algorithm",
                    "duration_s",
                    "started_at",
                    "ended_at",
                    "result",
                ],
            )
            writer.writeheader()
            for r in records:
                row = r.to_dict()
                # "result" puede ser dict; lo serializamos como JSON
                row["result"] = json.dumps(row["result"], ensure_ascii=False)
                writer.writerow(row)


class MultiSink(ResultSink):
    def __init__(self, *sinks: ResultSink) -> None:
        self.sinks = sinks

    def write_all(self, records: Iterable[ResultRecord]) -> None:
        # materializamos para poder iterar varias veces
        data = list(records)
        for s in self.sinks:
            s.write_all(data)

# ---------------------------------------------------------------------------
# Ejecutores
# ---------------------------------------------------------------------------

class DirectoryJob:
    """Empaqueta una combinación (algoritmo, fichero) para ejecución."""

    def __init__(self, file_path: Path, algorithm: Algorithm) -> None:
        self.file_path = file_path
        self.algorithm = algorithm

    def __repr__(self) -> str:  # útil para logs
        return f"DirectoryJob(file={self.file_path.name}, algo={self.algorithm.name})"


class Runner:
    def __init__(
        self,
        algorithms: Sequence[Algorithm],
        n_jobs: int = 1,
        backend: Optional[str] = None,
    ) -> None:
        self.algorithms = list(algorithms)
        self.n_jobs = n_jobs
        self.backend = backend

    def _run_one(self, job: DirectoryJob) -> ResultRecord:
        file_path = job.file_path
        algo = job.algorithm
        logging.debug("Iniciando %s", job)
        start = dt.datetime.now(dt.timezone.utc)
        t0 = dt.datetime.now().timestamp()
        try:
            algo.before_run(file_path)
            result = algo.run(file_path)
            algo.after_run(file_path, result)
        except Exception as e:
            logging.exception("Fallo en %s", job)
            result = {"error": str(e)}
        t1 = dt.datetime.now().timestamp()
        end = dt.datetime.now(dt.timezone.utc)
        return ResultRecord(
            file=str(file_path),
            algorithm=algo.name,
            duration_s=round(t1 - t0, 6),
            started_at=start.isoformat(),
            ended_at=end.isoformat(),
            result=result,
        )

    def run_directory(self, directory: Path, pattern: str = "*", recursive: bool = False) -> List[ResultRecord]:
        files = sorted(directory.rglob(pattern) if recursive else directory.glob(pattern))
        jobs = [DirectoryJob(f, algo) for f in files for algo in self.algorithms]
        if not jobs:
            logging.warning("No hay trabajos que ejecutar (revisa --dir y --pattern)")
            return []
        logging.info("Ejecutando %d trabajos con n_jobs=%s", len(jobs), self.n_jobs)
        
        results: List[ResultRecord] = cast(List[ResultRecord],Parallel(n_jobs=self.n_jobs, backend=self.backend)(
        delayed(self._run_one)(job) for job in jobs
        )
    )
        return results
    
    


# ---------------------------------------------------------------------------
# Algoritmos de ejemplo
# ---------------------------------------------------------------------------

@register_algorithm("word_count")
class WordCountAlgorithm(Algorithm):
    def run(self, file_path: Path) -> Dict[str, Any]:
        text = file_path.read_text(encoding="utf-8", errors="ignore")
        words = [w for w in text.split() if w.strip()]
        return {
            "num_lines": text.count("\n") + 1 if text else 0,
            "num_words": len(words),
            "num_chars": len(text),
        }


@register_algorithm("sha256")
class SHA256Algorithm(Algorithm):
    def run(self, file_path: Path) -> Dict[str, Any]:
        h = hashlib.sha256()
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return {"sha256": h.hexdigest()}

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_sink(kind: str, out_path: Path) -> ResultSink:
    k = kind.lower()
    if k == "jsonl":
        return JSONLResultSink(out_path)
    elif k == "csv":
        return CSVResultSink(out_path)
    else:
        raise ValueError(f"Tipo de salida no soportado: {kind}")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Framework para ejecutar algoritmos sobre ficheros con joblib")
    p.add_argument("--dir", required=True, type=Path, help="Directorio de entrada")
    p.add_argument("--pattern", default="*", help="Glob de ficheros, p.ej. *.txt")
    p.add_argument("--recursive", action="store_true", help="Buscar recursivamente")
    p.add_argument("--algos", nargs="+", required=True, help="Nombres de algoritmos a ejecutar")
    p.add_argument("--n-jobs", type=int, default=1, help="Número de workers para joblib (usa -1 para todos)")
    p.add_argument("--backend", default=None, help="Backend de joblib (loky, threading, multiprocessing)")
    p.add_argument("--out", default="jsonl", choices=["jsonl", "csv"], help="Formato de salida")
    p.add_argument("--out-path", type=Path, default=Path("results.jsonl"), help="Ruta del fichero de salida")
    p.add_argument("--log-level", default="INFO", help="Nivel de log (DEBUG, INFO, WARNING, ...)")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    if not args.dir.exists() or not args.dir.is_dir():
        logging.error("--dir no existe o no es un directorio: %s", args.dir)
        return 2

    # Construimos algoritmos desde el registro
    algorithms = build_algorithms(args.algos)

    runner = Runner(algorithms, n_jobs=args.n_jobs, backend=args.backend)
    results = runner.run_directory(args.dir, pattern=args.pattern, recursive=args.recursive)

    # Persistimos
    sink = build_sink(args.out, args.out_path)
    sink.write_all(results)

    logging.info("Guardados %d resultados en %s", len(results), args.out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

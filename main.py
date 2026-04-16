from __future__ import annotations

import argparse
import dataclasses
import logging
import os
from pathlib import Path

# Importar el paquete de algoritmos para activar todos los @register_algorithm
import algorithms  # noqa: F401

from framework import (
    CSVResultSink,
    JSONLResultSink,
    RETRY_TIMEOUT,
    VALID_RETRY_REASONS,
    RetryPolicy,
    RunnerV2,
    build_algorithms_by_family,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Framework concurrente de algoritmos con timeout duro, retry y streaming (Pebble)"
    )

    p.add_argument("--dir", required=True, type=Path, help="Directorio de entrada")
    p.add_argument("--pattern", default="*", help="Glob (ej: *.txt)")
    p.add_argument("--recursive", action="store_true")

    sel = p.add_mutually_exclusive_group(required=True)
    sel.add_argument(
        "--algos",
        nargs="+",
        metavar="ALGO",
        help="Uno o más algoritmos por nombre (ej: maxsat_brute sha256)",
    )
    sel.add_argument(
        "--family",
        metavar="FAMILY",
        help="Ejecuta todos los algoritmos de una familia (ej: maxsat, prueba)",
    )

    p.add_argument("--n-jobs", type=int, default=1, help="-1 = todos los CPUs")
    p.add_argument("--timeout", type=float, default=None)

    p.add_argument("--retries", type=int, default=0)
    p.add_argument(
        "--retry-on",
        nargs="*",
        default=[RETRY_TIMEOUT],
        choices=sorted(VALID_RETRY_REASONS),
    )

    p.add_argument("--buffer-factor", type=int, default=2)

    p.add_argument("--out", default="jsonl", choices=["jsonl", "csv"])
    p.add_argument("--out-path", type=Path, default=Path("output/results.jsonl"))

    p.add_argument("--log-level", default="INFO")

    return p.parse_args()


_SINK_CLASSES = {"jsonl": JSONLResultSink, "csv": CSVResultSink}


def _stream_with_progress(runner: RunnerV2, total: int):
    """Itera run_stream() emitiendo una línea de progreso por cada resultado."""

    done = 0
    width = len(str(total))

    for record in runner.run_stream():
        done += 1
        remaining = max(total - done, 0)

        status = (
            f"TIMEOUT ({record.duration_s:.1f}s)" if record.timed_out
            else "CANCELLED" if record.cancelled
            else "ERROR" if record.error
            else "ok"
        )
        duration_str = "" if record.timed_out else f"  {record.duration_s:.3f}s"

        retry_tag = (
            f"  -> reintento {record.attempt + 1}/{record.max_attempts}"
            if record.will_retry
            else ""
        )

        logging.info(
            "[%*d/%d] %-16s  %-30s  %s%s  (quedan %d)%s",
            width,
            done,
            total,
            record.algorithm,
            Path(record.file).name,
            status,
            duration_str,
            remaining,
            retry_tag,
        )
        yield record


def resolve_algorithms(args: argparse.Namespace) -> list:
    """Devuelve la lista de algoritmos a partir de --algos o --family."""

    if args.family:
        try:
            algos = build_algorithms_by_family(args.family)
        except KeyError as exc:
            raise SystemExit(f"Error: {exc}") from exc
        logging.info(
            "Familia '%s': %d algoritmo(s) -> %s",
            args.family,
            len(algos),
            [a.name for a in algos],
        )
        return algos
    return args.algos


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s %(message)s",
    )

    if not args.dir.is_dir():
        logging.error("--dir no es directorio válido: %s", args.dir)
        return 2

    workers = (os.cpu_count() or 1) if args.n_jobs == -1 else max(1, args.n_jobs)

    retry_policy = RetryPolicy(
        retries=args.retries,
        retry_on=frozenset(args.retry_on),
    )

    runner = RunnerV2(
        max_workers=workers,
        default_timeout=args.timeout,
        retry_policy=retry_policy,
        buffer_factor=args.buffer_factor,
    )

    runner.submit_directory(
        directory=args.dir,
        algorithms=resolve_algorithms(args),
        pattern=args.pattern,
        recursive=args.recursive,
    )

    total = runner.metrics.submitted_jobs
    logging.info("Lanzando %d jobs con %d worker(s)...", total, workers)

    sink = _SINK_CLASSES[args.out](args.out_path)
    written = sink.write_all(_stream_with_progress(runner, total))

    logging.info("Guardados %d resultados en %s", written, args.out_path)
    logging.info("Metrics: %s", dataclasses.asdict(runner.metrics))

    return 0


if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()
    raise SystemExit(main())


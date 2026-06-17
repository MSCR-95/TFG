from __future__ import annotations

import contextlib
import os
import sys
from dataclasses import replace
from pathlib import Path

from loguru import logger

from alglab._logging import setup
from alglab.engine import JSONLResultSink, Runner, derive_seed, iter_directory_jobs


def run_experiment(
    *,
    directory: str,
    algorithms: list[str],
    pattern: str = "*",
    recursive: bool = False,
    sort_files: bool = False,
    n_jobs: int = 1,
    timeout: float | None = None,
    out_path: str = "output/results.jsonl",
    append: bool = False,
    seed: int | None = None,
    log_level: str = "INFO",
    log_dir: str | None = None,
) -> int:
    """Run algorithms over a directory of instance files and write results to JSONL.

    Configures logging, builds jobs via :func:`~alglab.engine.iter_directory_jobs`,
    optionally derives per-file seeds, then streams all results through
    :class:`~alglab.engine.Runner` into a :class:`~alglab.engine.JSONLResultSink`.

    Args:
        directory: Path to the directory containing instance files.
        algorithms: Algorithm specs in ``"name"`` or ``"name:k=v,k2=v2"`` form.
        pattern: Glob pattern for matching instance files (default ``"*"``).
        recursive: Whether to search subdirectories recursively.
        sort_files: Whether to sort matched files before scheduling.
        n_jobs: Number of parallel worker processes.  ``-1`` uses all CPUs.
        timeout: Per-job wall-clock time limit in seconds; ``None`` is unlimited.
        out_path: Path to the output JSONL file.
        append: If ``True``, append to an existing JSONL file instead of
            overwriting it.
        seed: Global seed for deterministic per-file seed derivation.
            ``None`` disables seeding.
        log_level: Loguru log level string (e.g. ``"INFO"``, ``"DEBUG"``).
        log_dir: Directory for rotating log files; ``None`` logs to stderr only.

    Returns:
        ``0`` on success (no errors or timeouts), ``1`` if any job failed or
        timed out, ``2`` for configuration errors (invalid directory or
        algorithm name).
    """
    if hasattr(sys.stdout, "reconfigure"):
        with contextlib.suppress(Exception):
            sys.stdout.reconfigure(encoding="utf-8")

    setup(level=log_level, log_dir=Path(log_dir) if log_dir else None)

    dir_path = Path(directory)
    if not dir_path.is_dir():
        logger.error("--dir is not a valid directory: {}", dir_path)
        return 2

    if n_jobs == 0 or n_jobs < -1:
        logger.error("--n-jobs must be -1 or a positive integer: {}", n_jobs)
        return 2

    workers = (os.cpu_count() or 1) if n_jobs == -1 else max(1, n_jobs)

    import alglab.algorithms  # noqa: F401 — activate all @register_algorithm

    try:
        jobs = iter_directory_jobs(
            dir_path,
            algorithms=algorithms,
            pattern=pattern,
            recursive=recursive,
            sort_files=sort_files,
        )
    except KeyError as exc:
        logger.error("Error: {}", exc)
        return 2

    if seed is not None:
        jobs = (replace(j, seed=derive_seed(seed, j.file_path)) for j in jobs)

    runner = Runner(max_workers=workers, timeout=timeout)
    sink = JSONLResultSink(Path(out_path), append=append)

    processed = ok = errors = timeouts = 0
    with sink:
        for result in runner.run_stream(jobs):
            sink.write(result)
            processed += 1
            match result.status:
                case "ok":
                    ok += 1
                case "timeout":
                    timeouts += 1
                case _:
                    errors += 1
            phases = (result.pre_run_duration_s, result.run_duration_s, result.post_run_duration_s)
            duration_str = (
                f"  {sum(p for p in phases if p is not None):.3f}s"
                if any(p is not None for p in phases)
                else ""
            )
            logger.info(
                "[done={}] {:<16}  {:<30}  {}{}",
                processed,
                result.algorithm,
                Path(result.file).name,
                result.status,
                duration_str,
            )

    logger.info("processed={} ok={} timeout={} errors={}", processed, ok, timeouts, errors)
    return 1 if errors or timeouts else 0

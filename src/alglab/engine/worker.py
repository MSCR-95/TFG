"""Worker functions that execute inside subprocess workers.

All callables here must be defined at module level so they are picklable on
platforms that use the ``spawn`` start method (Windows, macOS default).

The per-worker algorithm instance cache (``_ALGORITHM_CACHE``) avoids
rebuilding the same algorithm object for every job.  Since each worker process
handles many jobs sequentially, caching amortises constructor cost and, for
algorithms that load large models, avoids repeated I/O.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from loguru import logger

from .core import Algorithm, Job
from .registry import build_algorithm

_ALGORITHM_CACHE: dict[tuple[str, tuple[tuple[str, Any], ...]], Algorithm[Any]] = {}


@dataclass(frozen=True)
class _JobOutcome:
    """Internal result container returned from the worker process to the runner.

    Attributes:
        ok: ``True`` if the algorithm completed without raising an exception.
        payload: Algorithm output dict on success, or
            ``{"error": "<message>"}`` on failure.
        pre_run_duration_s: Wall-clock seconds in
            :meth:`~alglab.engine.core.Algorithm.before_run`.
        run_duration_s: Wall-clock seconds in
            :meth:`~alglab.engine.core.Algorithm.run`.
        post_run_duration_s: Wall-clock seconds in
            :meth:`~alglab.engine.core.Algorithm.after_run`.
    """

    ok: bool
    payload: dict[str, Any]
    pre_run_duration_s: float | None = None
    run_duration_s: float | None = None
    post_run_duration_s: float | None = None


def _worker_init() -> None:
    """Initialise a worker process by populating the algorithm registry.

    Called once per worker process by pebble's initializer hook.  Importing
    ``alglab.algorithms`` triggers all ``@register_algorithm`` decorators in
    that package, ensuring the registry is populated before the first job
    arrives — even in ``spawn`` mode where each worker starts with an empty
    module namespace.
    """
    import alglab.algorithms  # noqa: F401


def _algorithm_cache_key(job: Job) -> tuple[str, tuple[tuple[str, Any], ...]]:
    """Compute a hashable cache key for the algorithm instance required by *job*.

    Args:
        job: The job whose algorithm name and constructor kwargs form the key.

    Returns:
        A ``(name, sorted_kwargs)`` tuple that uniquely identifies the
        algorithm configuration.

    Raises:
        TypeError: If any value in ``job.algo_kwargs`` is not hashable, since
            unhashable kwargs cannot be cached safely.
    """
    key = (job.algorithm, tuple(sorted(job.algo_kwargs.items())))
    try:
        hash(key)
    except TypeError as exc:
        raise TypeError(
            "Algorithm constructor kwargs must be hashable to use the worker instance cache"
        ) from exc
    return key


def _get_algorithm(job: Job) -> Algorithm[Any]:
    """Return the cached algorithm instance for *job*, building it if necessary.

    Args:
        job: The job whose algorithm name and kwargs identify the instance.

    Returns:
        An :class:`~alglab.engine.core.Algorithm` instance for the requested
        configuration.
    """
    key = _algorithm_cache_key(job)
    algorithm = _ALGORITHM_CACHE.get(key)
    if algorithm is None:
        algorithm = build_algorithm(job.algorithm, **job.algo_kwargs)
        _ALGORITHM_CACHE[key] = algorithm
    return algorithm


def _execute_job(job: Job) -> _JobOutcome:
    """Execute one algorithm job inside the worker process.

    Runs the full :class:`~alglab.engine.core.Algorithm` lifecycle —
    ``before_run → run → after_run`` — and records the wall-clock duration
    of each phase.  Any exception raised during any phase is caught and
    returned as a failed :class:`_JobOutcome` rather than propagated, so the
    runner can record it as ``status="algorithm_error"`` without crashing the
    worker.

    Args:
        job: The job to execute.

    Returns:
        A :class:`_JobOutcome` with ``ok=True`` on success or ``ok=False``
        with an ``{"error": ...}`` payload on failure.
    """
    pre_run_duration_s: float | None = None
    run_duration_s: float | None = None
    post_run_duration_s: float | None = None

    try:
        algorithm = _get_algorithm(job)

        logger.debug("Running {} on {}", algorithm.name, job.file_path.name)
        phase_t0 = time.perf_counter()
        try:
            context = algorithm.before_run(job.file_path)
        finally:
            pre_run_duration_s = round(time.perf_counter() - phase_t0, 6)
        phase_t0 = time.perf_counter()
        try:
            payload = algorithm.run(context, seed=job.seed)
        finally:
            run_duration_s = round(time.perf_counter() - phase_t0, 6)
        phase_t0 = time.perf_counter()
        try:
            final_payload = algorithm.after_run(context, payload)
            if final_payload is None:
                raise TypeError("Algorithm.after_run() must return dict[str, Any]")
            payload = final_payload
        finally:
            post_run_duration_s = round(time.perf_counter() - phase_t0, 6)
        ok = True
    except Exception as exc:
        logger.debug("Worker exception in {} on {}: {}", job.algorithm, job.file_path.name, exc)
        ok = False
        payload = {"error": f"{type(exc).__name__}: {exc}"}

    logger.debug("Done {} on {} ok={}", job.algorithm, job.file_path.name, ok)

    return _JobOutcome(
        ok=ok,
        payload=payload,
        pre_run_duration_s=pre_run_duration_s,
        run_duration_s=run_duration_s,
        post_run_duration_s=post_run_duration_s,
    )

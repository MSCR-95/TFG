"""Concurrent job runner backed by a pebble process pool.

:class:`Runner` schedules :class:`~alglab.engine.core.Job` objects on a
``pebble.ProcessPool`` and yields :class:`~alglab.engine.core.Result` objects
as soon as each job completes.  Each job runs in a separate OS process, which
enables hard timeouts (the process is killed by the OS) — something that is
impossible with threads.

The runner maintains a sliding window of in-flight futures capped at
``max_workers * 2``.  This bounds memory usage for large job streams without
materialising the full job list upfront.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, wait
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Any

from loguru import logger
from pebble import ProcessPool
from pebble.common import ProcessExpired

from .core import Job, Result, ResultStatus
from .worker import _execute_job, _JobOutcome, _worker_init


class Runner:
    """Executes jobs in parallel subprocesses and streams results.

    Attributes:
        max_workers: Number of worker processes in the pool.
        timeout: Per-job wall-clock time limit in seconds, or ``None`` for
            no limit.

    Example:
        >>> from alglab.engine import Runner
        >>> runner = Runner(max_workers=4, timeout=30.0)
        >>> results = list(runner.run_stream(jobs))
    """

    def __init__(
        self,
        *,
        max_workers: int = 1,
        timeout: float | None = None,
    ) -> None:
        """Initialise the runner.

        Args:
            max_workers: Number of parallel worker processes.  Values less
                than 1 are clamped to 1.
            timeout: Maximum wall-clock seconds per job.  ``None`` means
                unlimited.
        """
        self.max_workers = max(1, max_workers)
        self.timeout = timeout

    def run_stream(self, jobs: Iterable[Job]) -> Iterator[Result]:
        """Submit *jobs* to the process pool and yield results as they complete.

        Results are emitted in completion order, not submission order.  The
        pool is kept alive until all futures have been resolved.

        The sliding window (``max_workers * 2`` in-flight futures) ensures that
        slow producers do not starve the pool and fast producers do not exhaust
        memory.

        Args:
            jobs: Iterable of :class:`~alglab.engine.core.Job` objects.

        Yields:
            One :class:`~alglab.engine.core.Result` per job, in completion
            order.
        """
        max_inflight = self.max_workers * 2
        inflight: dict[Any, Job] = {}
        jobs_iter = iter(jobs)
        exhausted = False

        with ProcessPool(max_workers=self.max_workers, initializer=_worker_init) as pool:
            while not exhausted or inflight:
                while not exhausted and len(inflight) < max_inflight:
                    try:
                        job = next(jobs_iter)
                    except StopIteration:
                        exhausted = True
                        break
                    kw: dict[str, Any] = {"args": [job]}
                    if self.timeout is not None:
                        kw["timeout"] = self.timeout
                    future = pool.schedule(_execute_job, **kw)
                    inflight[future] = job
                    logger.debug("SCHEDULE {} on {}", job.algorithm, job.file_path.name)

                if not inflight:
                    break

                done, _ = wait(inflight.keys(), return_when=FIRST_COMPLETED)
                for future in done:
                    job = inflight.pop(future)
                    yield _finalize(future, job, self.timeout)


def _make_result(
    job: Job,
    status: ResultStatus,
    result: dict[str, Any] | None,
    error_message: str | None,
    pre: float | None = None,
    run: float | None = None,
    post: float | None = None,
) -> Result:
    return Result(
        file=str(job.file_path),
        algorithm=job.algorithm,
        status=status,
        result=result,
        error_message=error_message,
        seed=job.seed,
        pre_run_duration_s=pre,
        run_duration_s=run,
        post_run_duration_s=post,
    )


def _finalize(
    future: Any,
    job: Job,
    timeout: float | None,
) -> Result:
    """Convert a completed pebble future into a :class:`~alglab.engine.core.Result`.

    Handles all four failure modes that pebble can report: normal algorithm
    errors (``_JobOutcome.ok=False``), timeouts, process expiration (OOM or
    OS kill), and unexpected engine-level exceptions.

    Args:
        future: Completed pebble future from the process pool.
        job: The :class:`~alglab.engine.core.Job` that produced this future.
        timeout: The time limit that was active when the job was scheduled,
            used to populate the error message on timeout.

    Returns:
        A :class:`~alglab.engine.core.Result` with the appropriate status and
        payload.
    """
    try:
        outcome: _JobOutcome = future.result()
        if outcome.ok:
            return _make_result(
                job,
                "ok",
                outcome.payload,
                None,
                outcome.pre_run_duration_s,
                outcome.run_duration_s,
                outcome.post_run_duration_s,
            )
        return _make_result(
            job,
            "algorithm_error",
            None,
            str(outcome.payload.get("error", "Algorithm error")),
            outcome.pre_run_duration_s,
            outcome.run_duration_s,
            outcome.post_run_duration_s,
        )
    except FuturesTimeoutError:
        return _make_result(
            job,
            "timeout",
            None,
            f"TimeoutError: exceeded time limit of {timeout} s",
        )
    except ProcessExpired as exc:
        return _make_result(job, "process_expired", None, f"ProcessExpired: {exc}")
    except Exception as exc:
        logger.exception("ENGINE ERROR {} on {}", job.algorithm, job.file_path.name)
        return _make_result(job, "engine_error", None, f"{type(exc).__name__}: {exc}")

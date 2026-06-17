"""Core data types and base class for the engine layer.

Defines the :class:`Algorithm` abstract base class, the :class:`Job` and
:class:`Result` dataclasses, and :func:`derive_seed`.  These are the only
public contracts shared between the engine modules and algorithm
implementations.

Execution model:

    Job  →  Algorithm.before_run()  →  context
         →  Algorithm.run(context)  →  payload
         →  Algorithm.after_run(context, payload)  →  final_payload
         →  Result
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

ResultStatus = Literal["ok", "algorithm_error", "timeout", "process_expired", "engine_error"]


def derive_seed(global_seed: int, file_path: Path) -> int:
    """Derive a deterministic per-file seed from a global seed and a file path.

    Uses MD5 (non-security) to map the ``(global_seed, file_path)`` pair to a
    stable 64-bit unsigned integer.  The result is consistent across runs and
    platforms, so the same ``global_seed`` always produces the same per-file
    seed for a given path.

    Args:
        global_seed: Base seed supplied by the caller (e.g. via ``--seed``).
        file_path: Path to the instance file.

    Returns:
        A non-negative integer suitable for seeding random number generators.
    """
    payload = f"{global_seed}|{file_path}"
    digest = hashlib.md5(payload.encode("utf-8"), usedforsecurity=False).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


class Algorithm[TContext](ABC):
    """Abstract base class for all engine algorithms.

    Subclasses must implement :meth:`run` and may override :meth:`before_run`
    and :meth:`after_run` when per-file setup or post-processing is needed.

    The three-phase lifecycle separates I/O and parsing (``before_run``),
    computation (``run``), and result formatting (``after_run``) so the engine
    can instrument each phase independently with wall-clock timings.

    All per-file state must live in the context object returned by
    ``before_run``; the algorithm instance itself must remain stateless so that
    the worker process can reuse it across many jobs without interference.

    Note:
        Subclasses must be defined at module level — not as inner or local
        classes — to be picklable across process boundaries on all platforms.

    Attributes:
        __algo_name__: Registry key assigned by ``@register_algorithm``.
            Set automatically; do not assign manually.

    Example:
        >>> from alglab.engine.core import Algorithm
        >>> from alglab.engine.registry import register_algorithm
        >>> from pathlib import Path
        >>> from typing import Any
        >>>
        >>> @register_algorithm("my_solver")
        ... class MySolver(Algorithm[Path]):
        ...     def run(self, context: Path, *, seed: int | None = None) -> dict[str, Any]:
        ...         return {"result": 42}
    """

    __algo_name__: str = ""

    @property
    def name(self) -> str:
        """Registry key if registered via decorator; lowercase class name otherwise."""
        return self.__algo_name__ or self.__class__.__name__.lower()

    def before_run(self, file_path: Path) -> TContext:
        """Load and pre-process a single instance file.

        Called once per job in the worker subprocess before :meth:`run`.
        Override to parse the file, build problem structures, or allocate any
        resources that ``run`` will need.

        Args:
            file_path: Absolute or relative path to the instance file.

        Returns:
            A context object carrying all data needed by :meth:`run` and
            :meth:`after_run`.  The default implementation returns
            ``file_path`` cast to ``TContext``.
        """
        return cast(TContext, file_path)

    @abstractmethod
    def run(self, context: TContext, *, seed: int | None = None) -> dict[str, Any]:
        """Execute the algorithm on the pre-loaded instance.

        Args:
            context: Object returned by :meth:`before_run`.
            seed: Per-job deterministic seed derived from the global seed and
                the file path.  Algorithms that are deterministic may ignore it.

        Returns:
            A plain ``dict`` with raw algorithm output.  This dict is passed
            verbatim to :meth:`after_run` for optional post-processing.
        """
        ...

    def after_run(self, context: TContext, payload: dict[str, Any]) -> dict[str, Any]:
        """Finalise and validate the algorithm output.

        Called in the worker subprocess immediately after :meth:`run`.
        Override to verify, recount, or enrich the payload before it is
        serialised to JSONL.

        Args:
            context: Object returned by :meth:`before_run`.
            payload: Dict returned by :meth:`run`.

        Returns:
            The final result dict stored in :attr:`Result.result`.  Must not
            be ``None``; the engine raises :class:`TypeError` if it is.
        """
        return payload


@dataclass(frozen=True)
class Job:
    """A single unit of work: one algorithm applied to one instance file.

    Attributes:
        file_path: Path to the input instance.
        algorithm: Registered algorithm name (e.g. ``"maxsat_brute"``).
        seed: Per-file deterministic seed, or ``None`` when seeding is disabled.
        algo_kwargs: Constructor keyword arguments forwarded to the algorithm
            class when the worker builds the instance.
    """

    file_path: Path
    algorithm: str
    seed: int | None = None
    algo_kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Result:
    """Outcome of a single :class:`Job` execution, serialised as one JSONL line.

    Attributes:
        file: String representation of the input file path.
        algorithm: Registered algorithm name.
        status: Execution outcome.  Possible values:

            - ``"ok"`` — algorithm completed successfully.
            - ``"algorithm_error"`` — algorithm raised an exception.
            - ``"timeout"`` — job exceeded the per-job time limit.
            - ``"process_expired"`` — worker process was killed by the OS.
            - ``"engine_error"`` — internal error in the runner.

        result: Algorithm payload dict when ``status="ok"``; ``None`` otherwise.
        error_message: Human-readable error description; ``None`` when
            ``status="ok"``.
        seed: Per-file seed used during execution; ``None`` when seeding is
            disabled.
        pre_run_duration_s: Wall-clock seconds spent in
            :meth:`Algorithm.before_run`; ``None`` if the phase did not run.
        run_duration_s: Wall-clock seconds spent in :meth:`Algorithm.run`;
            ``None`` if the phase did not run.
        post_run_duration_s: Wall-clock seconds spent in
            :meth:`Algorithm.after_run`; ``None`` if the phase did not run.
    """

    file: str
    algorithm: str
    status: ResultStatus
    result: dict[str, Any] | None
    error_message: str | None
    seed: int | None
    pre_run_duration_s: float | None = None
    run_duration_s: float | None = None
    post_run_duration_s: float | None = None

"""Job generation from an instance directory.

Provides :func:`iter_directory_jobs`, which yields one :class:`~alglab.engine.core.Job`
per ``(file, algorithm)`` pair found under a given directory.  Algorithm names
are validated eagerly before any job is yielded, so invalid names fail fast
rather than mid-stream.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

from .core import Job
from .registry import build_algorithm, parse_algo_spec


def iter_directory_jobs(
    directory: Path,
    *,
    algorithms: Sequence[str],
    pattern: str = "*",
    recursive: bool = False,
    sort_files: bool = False,
) -> Iterator[Job]:
    """Yield :class:`~alglab.engine.core.Job` objects for every ``(file, algorithm)`` pair.

    Files are discovered by globbing *directory* with *pattern* and emitted in
    filesystem order by default.  Passing ``sort_files=True`` materialises the
    full path list and sorts it before yielding, which is useful for
    reproducible JSONL ordering but uses more memory for large directories.

    Algorithm names are validated against the registry before iteration begins.
    An unknown name raises :class:`KeyError` immediately, not when the bad job
    is eventually scheduled.

    Each element of *algorithms* can be a plain name (``"maxsat_brute"``) or a
    spec with parameters (``"maxsat_qubo_sa:num_reads=500"``).  See
    :func:`~alglab.engine.registry.parse_algo_spec` for the spec syntax.

    Args:
        directory: Root directory containing the instance files.
        algorithms: Algorithm names or specs to run on each file.
        pattern: Glob pattern used to filter files within *directory*.
        recursive: If ``True``, searches subdirectories recursively via
            :py:meth:`~pathlib.Path.rglob`.
        sort_files: If ``True``, sorts discovered paths before yielding.

    Yields:
        One :class:`~alglab.engine.core.Job` per ``(file, algorithm)`` pair,
        with all algorithms for a given file grouped together.

    Raises:
        KeyError: If any algorithm name is not in the registry.

    Example:
        >>> from pathlib import Path
        >>> jobs = list(iter_directory_jobs(
        ...     Path("data/max2sat"),
        ...     algorithms=["maxsat_brute", "maxsat_qubo_sa:num_reads=200"],
        ...     pattern="*.cnf",
        ...     recursive=True,
        ... ))
    """
    algo_specs: list[tuple[str, dict[str, Any]]] = [parse_algo_spec(a) for a in algorithms]
    for name, kwargs in algo_specs:
        build_algorithm(name, **kwargs)

    def _gen() -> Iterator[Job]:
        glob_fn = directory.rglob if recursive else directory.glob
        files: Iterable[Path] = (f for f in glob_fn(pattern) if f.is_file())
        if sort_files:
            files = sorted(files)
        for f in files:
            for name, kwargs in algo_specs:
                yield Job(file_path=f, algorithm=name, algo_kwargs=kwargs)

    return _gen()

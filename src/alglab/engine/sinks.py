"""Result sinks: write :class:`~alglab.engine.core.Result` objects to persistent storage.

Provides an abstract :class:`ResultSink` base class and a concrete
:class:`JSONLResultSink` that appends one JSON line per result to a file.

:class:`JSONLResultSink` is designed for streaming use: results are buffered
and flushed every :data:`_FLUSH_EVERY` records, so the file is always
recoverable even if the process is interrupted mid-run.
"""

from __future__ import annotations

import dataclasses
import json
from abc import ABC, abstractmethod
from collections.abc import Iterable
from pathlib import Path
from typing import IO

from loguru import logger

from .core import Result

_FLUSH_EVERY = 100


class ResultSink(ABC):
    """Abstract base class for result sinks.

    A sink receives :class:`~alglab.engine.core.Result` objects and persists
    them in some form.  Subclasses must implement :meth:`write`.
    """

    @abstractmethod
    def write(self, result: Result) -> None:
        """Persist a single result.

        Args:
            result: The completed job result to store.
        """
        ...

    def write_all(self, results: Iterable[Result]) -> int:
        """Write all results from an iterable and return the count.

        Args:
            results: An iterable of :class:`~alglab.engine.core.Result` objects.

        Returns:
            The total number of results written.
        """
        count = 0
        for r in results:
            self.write(r)
            count += 1
        return count


class JSONLResultSink(ResultSink):
    """Writes results to a JSONL file, one JSON object per line.

    Must be used as a context manager or via :meth:`write_all` to ensure the
    file is properly flushed and closed.  Writes are buffered and flushed every
    :data:`_FLUSH_EVERY` records to balance I/O overhead with crash safety.

    Args:
        path: Output file path.  Parent directories are created automatically
            on open.
        append: If ``True``, results are appended to an existing file instead
            of overwriting it.  Useful for resuming interrupted experiments.

    Example:
        >>> from pathlib import Path
        >>> sink = JSONLResultSink(Path("output/results.jsonl"))
        >>> with sink:
        ...     sink.write(result)
        ...
        >>> # or consume an iterator directly:
        >>> n = JSONLResultSink(Path("output/results.jsonl")).write_all(results_iter)
    """

    def __init__(self, path: Path, *, append: bool = False) -> None:
        """Initialise the JSONL sink.

        Args:
            path: Output file path.
            append: If ``True``, append to an existing file instead of
                overwriting it.
        """
        self.path = path
        self._append = append
        self._file: IO[str] | None = None
        self._buffer: list[str] = []

    def __enter__(self) -> JSONLResultSink:
        """Open the output file and prepare for writing.

        Raises:
            RuntimeError: If the sink is already open.
        """
        if self._file is not None:
            raise RuntimeError("JSONLResultSink is already open")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if self._append else "w"
        logger.debug("Opening JSONL sink: {} (mode={})", self.path, mode)
        self._file = self.path.open(mode, encoding="utf-8")
        return self

    def __exit__(self, *_: object) -> None:
        """Flush the buffer and close the file."""
        if self._file is not None:
            self._flush_buffer()
            self._file.close()
            self._file = None
            logger.debug("JSONL sink closed: {}", self.path)

    def _flush_buffer(self) -> None:
        if self._buffer and self._file is not None:
            self._file.write("".join(self._buffer))
            self._file.flush()
            self._buffer.clear()

    def write(self, result: Result) -> None:
        """Serialise *result* to JSON and append it to the internal buffer.

        Flushes the buffer to disk every :data:`_FLUSH_EVERY` records.

        Args:
            result: The result to write.

        Raises:
            RuntimeError: If called outside of a ``with`` block.
        """
        if self._file is None:
            raise RuntimeError(
                "JSONLResultSink not open. Use as context manager or call write_all()."
            )
        self._buffer.append(json.dumps(dataclasses.asdict(result), ensure_ascii=False) + "\n")
        if len(self._buffer) >= _FLUSH_EVERY:
            self._flush_buffer()

    def write_all(self, results: Iterable[Result]) -> int:
        """Open the sink, write all *results*, close the sink, and return the count.

        Convenience method that manages the context manager lifecycle
        automatically.

        Args:
            results: An iterable of :class:`~alglab.engine.core.Result` objects.

        Returns:
            The total number of results written.
        """
        count = 0
        with self:
            for r in results:
                self.write(r)
                count += 1
        logger.debug("JSONL sink: {} records written to {}", count, self.path)
        return count

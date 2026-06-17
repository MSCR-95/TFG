"""Public engine API for registering, running, and persisting algorithms."""

from .core import Algorithm, Job, Result, derive_seed
from .jobs import iter_directory_jobs
from .registry import build_algorithm, register_algorithm
from .runner import Runner
from .sinks import JSONLResultSink, ResultSink

__all__ = [
    "Algorithm",
    "Job",
    "Result",
    "derive_seed",
    "Runner",
    "iter_directory_jobs",
    "register_algorithm",
    "build_algorithm",
    "ResultSink",
    "JSONLResultSink",
]

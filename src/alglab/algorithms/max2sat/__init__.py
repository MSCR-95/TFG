"""Max-2-SAT algorithms and optional QUBO sampler registrations."""

from . import brute  # activates @register_algorithm("maxsat_brute")

try:
    from . import qubo  # activates @register_algorithm("maxsat_qubo_*")
except ImportError:
    from loguru import logger

    logger.warning(
        "QUBO samplers unavailable: install dwave-ocean-sdk to enable them. "
        "Run: uv sync --extra qubo"
    )
    qubo = None  # type: ignore[assignment]

__all__ = [
    "brute",
    "qubo",
]

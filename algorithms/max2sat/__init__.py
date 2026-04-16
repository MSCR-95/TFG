from algorithms.max2sat import brute    # activa @register_algorithm("maxsat_brute")
from algorithms.max2sat import annealyn  # activa @register_algorithm("maxsat_qubo_sa")

__all__ = ["brute", "annealyn"]
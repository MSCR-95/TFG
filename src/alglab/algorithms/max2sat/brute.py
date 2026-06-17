"""Brute-force Max-SAT solver (``maxsat_brute``).

Enumerates all 2^n truth assignments in Gray-code-adjacent order using
``itertools.product``, applying an early-exit pruning strategy: once the
remaining unexamined clauses cannot improve on the current best, the current
assignment is abandoned.

This solver is exact but exponential in *n*.  It is suitable only for small
instances (typically n ≤ 25 on modern hardware).
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger

from alglab.engine.core import Algorithm
from alglab.engine.registry import register_algorithm

from ._common import (
    assignment_payload,
    clause_satisfied,
    require_clauses,
    result_payload,
)
from .parser import _parse_dimacs


def _solve_maxsat_brute_force(
    num_vars: int, clauses: list[list[int]]
) -> tuple[int, dict[int, bool]]:
    """Find the assignment that maximises the number of satisfied clauses.

    Iterates over all 2^n assignments with clause-level pruning: once the
    number of remaining unchecked clauses cannot raise the tally above the
    current best, the loop moves to the next assignment.  Terminates early if
    all clauses are satisfied.

    Args:
        num_vars: Number of Boolean variables.
        clauses: List of clauses, each a list of non-zero signed integers.

    Returns:
        A ``(best_value, best_assignment)`` tuple where *best_value* is the
        maximum number of simultaneously satisfied clauses and
        *best_assignment* is the corresponding variable assignment.
    """
    num_clauses = len(clauses)
    best_value = -1
    best_bits: tuple[bool, ...] = ()
    assignment: dict[int, bool] = {}

    for bits in itertools.product([False, True], repeat=num_vars):
        for i, bit in enumerate(bits, start=1):
            assignment[i] = bit
        satisfied = 0

        for i, clause in enumerate(clauses):
            if clause_satisfied(clause, assignment):
                satisfied += 1
            max_possible = satisfied + (num_clauses - i - 1)
            if max_possible <= best_value:
                break

        if satisfied > best_value:
            best_value = satisfied
            best_bits = bits

        if best_value == num_clauses:
            break

    return best_value, dict(enumerate(best_bits, start=1))


@dataclass(frozen=True)
class MaxSATBruteContext:
    """Per-file context for the brute-force solver.

    Attributes:
        source_name: File name (used for error messages and logging).
        num_vars: Number of variables in the instance.
        clauses: List of clauses parsed from the DIMACS CNF file.
    """

    source_name: str
    num_vars: int
    clauses: list[list[int]]


@register_algorithm("maxsat_brute")
class MaxSATBruteAlgorithm(Algorithm[MaxSATBruteContext]):
    """Exact Max-SAT solver by exhaustive enumeration.

    Reads DIMACS CNF instances.  Suitable for small instances (n ≤ ~25).

    Args:
        include_assignment: When ``True``, the result payload includes an
            ``"assignment"`` key with the full optimal variable assignment.
    """

    def __init__(self, include_assignment: bool = False) -> None:
        """Initialise the brute-force solver.

        Args:
            include_assignment: When ``True``, include the full optimal
                assignment in the result payload.
        """
        self.include_assignment = include_assignment

    def before_run(self, file_path: Path) -> MaxSATBruteContext:
        """Parse the DIMACS CNF file and validate the clause list.

        Args:
            file_path: Path to the ``.cnf`` instance file.

        Returns:
            A :class:`MaxSATBruteContext` with the parsed instance data.

        Raises:
            ValueError: If the file has no valid header, inconsistent clause
                count, literals out of range, or no clauses.
        """
        text = file_path.read_text(encoding="utf-8")
        num_vars, clauses = _parse_dimacs(text)
        require_clauses(clauses, file_path.name)
        if num_vars > 25:
            logger.warning(
                "{}: {} variables — brute-force is O(2^n); 2^{} ≈ {:.2e} assignments. "
                "Consider a QUBO solver for large instances.",
                file_path.name,
                num_vars,
                num_vars,
                2.0**num_vars,
            )
        return MaxSATBruteContext(
            source_name=file_path.name,
            num_vars=num_vars,
            clauses=clauses,
        )

    def run(self, context: MaxSATBruteContext, *, seed: int | None = None) -> dict[str, Any]:
        """Enumerate all assignments and return the best result.

        Args:
            context: Parsed instance data from :meth:`before_run`.
            seed: Ignored; this solver is deterministic.

        Returns:
            Result payload dict from :func:`~._common.result_payload`, with an
            optional ``"assignment"`` key when ``include_assignment=True``.
        """
        num_vars = context.num_vars
        clauses = context.clauses
        best_value, best_assignment = _solve_maxsat_brute_force(num_vars, clauses)
        payload = result_payload(
            num_vars=num_vars,
            num_clauses=len(clauses),
            satisfied_clauses=best_value,
        )
        if self.include_assignment:
            payload["assignment"] = assignment_payload(best_assignment)
        return payload

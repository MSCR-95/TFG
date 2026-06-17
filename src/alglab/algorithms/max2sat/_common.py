"""Shared evaluation utilities for Max-SAT algorithm implementations.

Provides clause satisfaction checking, result payload construction, and
assignment serialisation.  These utilities are stateless and have no
dependency on the engine layer, so they can be used by any algorithm that
operates on DIMACS CNF instances.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def require_clauses(clauses: Sequence[Sequence[int]], file_name: str) -> None:
    """Raise :class:`ValueError` if the clause list is empty.

    Args:
        clauses: List of clauses parsed from a DIMACS CNF file.
        file_name: Source file name, used in the error message.

    Raises:
        ValueError: If *clauses* is empty.
    """
    if not clauses:
        raise ValueError(f"Instance contains no clauses: {file_name}")


def literal_satisfied(literal: int, assignment: Mapping[int, Any]) -> bool:
    """Return ``True`` if *literal* is satisfied by *assignment*.

    A positive literal ``x_i`` is satisfied when ``assignment[i]`` is truthy.
    A negative literal ``-x_i`` is satisfied when ``assignment[i]`` is falsy.

    Args:
        literal: A non-zero integer representing a variable (positive) or its
            negation (negative).
        assignment: Mapping from variable index to a value that can be
            coerced to ``bool`` (e.g. ``dict[int, bool]`` or ``dict[int, int]``
            from a dimod sample).

    Returns:
        ``True`` if the literal is satisfied, ``False`` otherwise.
    """
    value = bool(assignment.get(abs(literal), False))
    return value if literal > 0 else not value


def clause_satisfied(clause: Sequence[int], assignment: Mapping[int, Any]) -> bool:
    """Return ``True`` if at least one literal in *clause* is satisfied.

    Args:
        clause: Sequence of non-zero integers representing literals.
        assignment: Mapping from variable index to a truthy/falsy value.

    Returns:
        ``True`` if any literal in *clause* is satisfied by *assignment*.
    """
    return any(literal_satisfied(literal, assignment) for literal in clause)


def count_satisfied(
    clauses: Sequence[Sequence[int]],
    assignment: Mapping[int, Any],
) -> int:
    """Count the number of clauses satisfied by *assignment*.

    Args:
        clauses: List of clauses, each a sequence of non-zero integers.
        assignment: Mapping from variable index to a truthy/falsy value.

    Returns:
        The number of clauses for which at least one literal is satisfied.
    """
    return sum(clause_satisfied(clause, assignment) for clause in clauses)


def result_payload(
    *,
    num_vars: int,
    num_clauses: int,
    satisfied_clauses: int,
    verification_mismatch: bool = False,
) -> dict[str, Any]:
    """Build the standard result payload dict for a Max-SAT solver.

    Args:
        num_vars: Number of variables in the instance.
        num_clauses: Total number of clauses in the instance.
        satisfied_clauses: Number of clauses satisfied by the solution.
        verification_mismatch: ``True`` when the satisfaction count inferred
            from the solver's internal metric (e.g. QUBO energy) differs from
            the directly counted value.

    Returns:
        Dict with keys ``"num_vars"``, ``"num_clauses"``,
        ``"satisfied_clauses"``, ``"satisfaction_ratio"`` (rounded to 4
        decimal places), and ``"verification_mismatch"``.
    """
    return {
        "num_vars": num_vars,
        "num_clauses": num_clauses,
        "satisfied_clauses": satisfied_clauses,
        "satisfaction_ratio": round(satisfied_clauses / num_clauses, 4),
        "verification_mismatch": verification_mismatch,
    }


def assignment_payload(assignment: Mapping[int, Any]) -> dict[str, int]:
    """Serialise a variable assignment as a sorted dict of ``{"x<i>": 0|1}`` entries.

    Args:
        assignment: Mapping from variable index to a truthy/falsy value.

    Returns:
        Dict with keys ``"x1"``, ``"x2"``, … sorted by variable index, with
        values ``0`` or ``1``.
    """
    return {f"x{k}": int(v) for k, v in sorted(assignment.items())}

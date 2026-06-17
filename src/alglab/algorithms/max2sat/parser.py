"""DIMACS CNF parser for Max-SAT instance files."""

from __future__ import annotations


def _parse_dimacs(text: str) -> tuple[int, list[list[int]]]:
    """Parse a DIMACS CNF string and return ``(num_vars, clauses)``.

    Accepts the standard DIMACS CNF format:

    - Lines starting with ``c`` are comments and are ignored.
    - The problem line ``p cnf <vars> <clauses>`` declares the instance size.
    - Remaining lines contain space-separated literals terminated by ``0``.

    Args:
        text: Full text content of a ``.cnf`` file.

    Returns:
        A ``(num_vars, clauses)`` tuple where *clauses* is a list of lists of
        non-zero signed integers.

    Raises:
        ValueError: If the file has no valid ``p cnf`` header, multiple
            headers, literals before the header, literals out of the declared
            variable range, an unclosed clause at the end of file, or an
            actual clause count that differs from the declared count.
    """
    header_read = False
    num_vars = 0
    expected_clause_count: int | None = None
    clauses: list[list[int]] = []
    current_clause: list[int] = []

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        parts = line.split()
        head = parts[0]

        if head == "c":
            continue

        if head == "p":
            if header_read:
                raise ValueError("Multiple DIMACS headers found")
            if len(parts) != 4 or parts[1] != "cnf":
                raise ValueError("Invalid DIMACS header")
            num_vars = int(parts[2])
            expected_clause_count = int(parts[3])
            header_read = True
            continue

        if not header_read:
            raise ValueError("Literals found before 'p cnf' header")

        for token in parts:
            num = int(token)
            if num == 0:
                clauses.append(current_clause)
                current_clause = []
            else:
                if abs(num) > num_vars:
                    raise ValueError(f"Literal out of range: {num} with num_vars={num_vars}")
                current_clause.append(num)

    if current_clause:
        raise ValueError("Last clause not terminated with 0")

    if not header_read:
        raise ValueError("No valid 'p cnf' header found")

    if expected_clause_count is not None and len(clauses) != expected_clause_count:
        raise ValueError(
            f"Wrong clause count: expected {expected_clause_count}, got {len(clauses)}"
        )

    return num_vars, clauses

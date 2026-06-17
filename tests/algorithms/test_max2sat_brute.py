from __future__ import annotations

from typing import Any

from alglab.algorithms.max2sat.brute import MaxSATBruteAlgorithm


class _CountingFile:
    name = "counting.cnf"

    def __init__(self, text: str) -> None:
        self.text = text
        self.read_count = 0

    def read_text(self, **kwargs: Any) -> str:
        assert kwargs == {"encoding": "utf-8"}
        self.read_count += 1
        return self.text


def test_brute_reads_instance_in_before_run_only() -> None:
    file_path = _CountingFile("p cnf 2 3\n1 2 0\n-1 -2 0\n1 -2 0\n")
    algo = MaxSATBruteAlgorithm(include_assignment=True)

    context = algo.before_run(file_path)
    payload = algo.run(context)
    result = algo.after_run(context, payload)

    assert file_path.read_count == 1
    assert context.source_name == "counting.cnf"
    assert context.num_vars == 2
    assert context.clauses == [[1, 2], [-1, -2], [1, -2]]
    assert result["satisfied_clauses"] == 3
    assert result["verification_mismatch"] is False
    assert result["assignment"]


def test_brute_run_uses_explicit_context() -> None:
    file_path = _CountingFile("p cnf 1 1\n1 0\n")
    algo = MaxSATBruteAlgorithm()

    context = algo.before_run(file_path)
    result = algo.run(context)

    assert result["satisfied_clauses"] == 1

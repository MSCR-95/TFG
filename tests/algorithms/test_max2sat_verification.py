import pytest

pytest.importorskip("dimod")

from alglab.algorithms.max2sat.qubo import (
    MaxSATQUBOExactAlgorithm,
    MaxSATQUBOSAAlgorithm,
    _sampler_seed,
)


def test_qubo_verification(tmp_path):
    # Simple DIMACS Max-2-SAT instance: 2 variables, 3 clauses
    # (1 2), (-1 -2), (1 -2)
    cnf = "p cnf 2 3\n1 2 0\n-1 -2 0\n1 -2 0\n"
    f = tmp_path / "test.cnf"
    f.write_text(cnf)

    algo = MaxSATQUBOExactAlgorithm()

    # Simulate engine lifecycle
    context = algo.before_run(f)
    payload = algo.run(context)
    result = algo.after_run(context, payload)

    assert "satisfied_clauses" in result

    # Optimal solution: x1=T, x2=F -> (T or F), (F or T), (T or T) -> 3 sat
    assert result["satisfied_clauses"] == 3
    assert result["verification_mismatch"] is False
    assert "qubo_energy" not in result
    assert "_raw_sample" not in result
    assert "assignment" not in result
    print(f"\nVerified result: {result['satisfied_clauses']} clauses satisfied")


def test_qubo_assignment_is_optional(tmp_path):
    cnf = "p cnf 2 3\n1 2 0\n-1 -2 0\n1 -2 0\n"
    f = tmp_path / "test.cnf"
    f.write_text(cnf)

    algo = MaxSATQUBOExactAlgorithm(include_assignment=True)
    context = algo.before_run(f)
    payload = algo.run(context)
    result = algo.after_run(context, payload)

    assert result["satisfied_clauses"] == 3
    assert result["verification_mismatch"] is False
    assert result["assignment"]


def test_qubo_uses_dimacs_variable_ids(tmp_path):
    cnf = "p cnf 5 2\n3 -5 0\n4 5 0\n"
    f = tmp_path / "test.cnf"
    f.write_text(cnf)

    algo = MaxSATQUBOExactAlgorithm()
    context = algo.before_run(f)

    assert set(context.bqm.variables) == {3, 4, 5}
    assert 1 not in context.bqm.variables
    assert 2 not in context.bqm.variables


def test_qubo_verification_mismatch_is_visible(tmp_path):
    cnf = "p cnf 2 3\n1 2 0\n-1 -2 0\n1 -2 0\n"
    f = tmp_path / "test.cnf"
    f.write_text(cnf)

    algo = MaxSATQUBOExactAlgorithm()
    context = algo.before_run(f)
    payload = algo.run(context)
    payload["qubo_energy"] = 99.0
    result = algo.after_run(context, payload)

    assert result["satisfied_clauses"] == 3
    assert result["verification_mismatch"] is True
    assert "qubo_energy" not in result
    assert "_raw_sample" not in result
    assert "assignment" not in result


def test_qubo_sampler_seed_is_uint32() -> None:
    assert _sampler_seed(16254492983259844270) == 256396974


def test_qubo_sa_accepts_engine_derived_seed(tmp_path):
    cnf = "p cnf 2 3\n1 2 0\n-1 -2 0\n1 -2 0\n"
    f = tmp_path / "test.cnf"
    f.write_text(cnf)

    algo = MaxSATQUBOSAAlgorithm(num_reads=1)
    context = algo.before_run(f)
    payload = algo.run(context, seed=16254492983259844270)
    result = algo.after_run(context, payload)

    assert "satisfied_clauses" in result
    assert "qubo_energy" not in result
    assert "_raw_sample" not in result


if __name__ == "__main__":
    pytest.main([__file__])

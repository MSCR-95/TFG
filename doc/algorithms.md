# Algorithms

## Included algorithms

### Max-2-SAT

The **Max-2-SAT** problem seeks the assignment of boolean variables that maximises the number of satisfied clauses in a CNF formula where each clause has exactly 2 literals.

Instances are read in **DIMACS CNF** format (`.cnf`).

#### `maxsat_brute`

Exhaustive brute-force search with early pruning. Guarantees the optimal solution.

**Complexity**: O(2^n) — only viable for small instances (n ≲ 25 variables).

**Output**:

| Field | Type | Description |
|-------|------|-------------|
| `num_vars` | int | Number of variables |
| `num_clauses` | int | Number of clauses |
| `satisfied_clauses` | int | Satisfied clauses (objective value) |
| `satisfaction_ratio` | float | Satisfied fraction ∈ [0, 1] |
| `verification_mismatch` | bool | True if verification detected a discrepancy; always `false` in brute |

**Parameters**:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `include_assignment` | `false` | Adds `assignment` to the output. Disabled by default to avoid inflating large JSONL files. |

Example:

```bash
uv run alglab run \
    --dir data/max2sat \
    -A "maxsat_brute:include_assignment=true" \
    --pattern "*.cnf" \
    --timeout 30 \
    --out-path output/brute.jsonl
```

The assignment is stored as a compact object:

```json
{
  "assignment": {"x1": 1, "x2": 0, "x3": 1}
}
```

#### QUBO solvers (`maxsat_qubo_*`)

Transform the Max-2-SAT problem into a QUBO (Quadratic Unconstrained Binary Optimization) problem and solve it with various samplers from `dwave.samplers`.

Require the optional dependency `dwave-ocean-sdk`:

```bash
uv sync --extra qubo
```

Only work with **Max-2-SAT** instances (exactly 2 literals per clause). Will raise `ValueError` with instances k > 2.

**Common output**:

| Field | Type | Description |
|-------|------|-------------|
| `num_vars` | int | Number of variables |
| `num_clauses` | int | Number of clauses |
| `satisfied_clauses` | int | Final satisfied clauses after verification |
| `satisfaction_ratio` | float | Satisfied fraction ∈ [0, 1] |
| `verification_mismatch` | bool | True if the count inferred by QUBO does not match the verified count |

The seed used appears in the top-level `seed` field of the JSONL, not inside `result`.
`satisfied_clauses` is always the final verified value. To detect corrected or
inconsistent solutions in the output, filter by `result.verification_mismatch == true`.

**Common parameters**:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `num_reads` | `100` | Number of samples to take. |
| `include_assignment` | `false` | Adds `assignment` to the output. |

```bash
alglab run --dir data/ -A "maxsat_qubo_sa:num_reads=500,include_assignment=true"
```

| Algorithm | Sampler | Notes |
|-----------|---------|-------|
| `maxsat_qubo_exact` | `dimod.ExactSolver` | Exact; only viable for very small instances (≲ 20 variables) |
| `maxsat_qubo_sa` | `SimulatedAnnealingSampler` | Simulated annealing; good quality/time trade-off |
| `maxsat_qubo_ts` | `TabuSampler` | Tabu search; fast, good on structured problems |
| `maxsat_qubo_sds` | `SteepestDescentSampler` | Steepest descent; deterministic, very fast |
| `maxsat_qubo_rs` | `RandomSampler` | Random sampling; serves as a probabilistic baseline |

Example comparison:

```bash
uv sync --extra qubo --reinstall-package alglab

uv run alglab run \
    --dir data/max2sat \
    -A maxsat_brute \
    -A "maxsat_qubo_sa:num_reads=100" \
    -A "maxsat_qubo_ts:num_reads=100" \
    -A "maxsat_qubo_sds:num_reads=100" \
    --pattern "*.cnf" \
    --recursive \
    --n-jobs -1 \
    --timeout 60 \
    --seed 42 \
    --out-path output/max2sat_qubo_compare.jsonl
```

If QUBO dependencies are not installed, importing `alglab.algorithms.max2sat`
registers `maxsat_brute` and logs a warning for the unavailable QUBO solvers.

---

## Adding a new algorithm

### 1. Create the module

Create a file in `src/alglab/algorithms/`. It can be in a new subdirectory or an existing one.

```python
# src/alglab/algorithms/example_problem/example_algorithm.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from alglab.engine.core import Algorithm
from alglab.engine.registry import register_algorithm


@dataclass(frozen=True)
class ExampleContext:
    source_name: str
    instance: object


@register_algorithm("example_algorithm")
class ExampleAlgorithm(Algorithm[ExampleContext]):
    """Solve example_problem with a heuristic method."""

    def __init__(self, max_iter: int = 1000) -> None:
        """Initialise the heuristic.

        Args:
            max_iter: Maximum number of solver iterations.
        """
        self.max_iter = max_iter

    def before_run(self, file_path: Path) -> ExampleContext:
        """Parse one input file into an immutable context.

        Args:
            file_path: Path to the instance file.

        Returns:
            Context object passed to `run`.
        """
        text = file_path.read_text(encoding="utf-8")
        return ExampleContext(
            source_name=file_path.name,
            instance=_parse(text),
        )

    def run(self, context: ExampleContext, *, seed: int | None = None) -> dict[str, Any]:
        """Run the heuristic for one parsed instance.

        Args:
            context: Parsed instance context from `before_run`.
            seed: Deterministic per-file seed, or `None` when seeding is disabled.

        Returns:
            JSON-serialisable result payload.
        """
        solution = _solve(context.instance, max_iter=self.max_iter, seed=seed)

        return {
            "objective_value": solution.value,
            "local_time": solution.time,
        }
```

**Important rules**:

- The class must be defined at **module level** (not as an inner class).
- Use `Algorithm[ContextType]` and keep per-file data in the context returned by `before_run()`.
- `run` must return a `dict[str, Any]` — all keys are serialised to JSON.
- The engine always passes `seed=job.seed` to `run()`. Algorithms that don't use randomness can accept and ignore it.
- `after_run` is optional, but if implemented it must return the final result dictionary.
- The constructor can have parameters; they are passed with the spec `name:param=value`.
- Public production code should use Google-style docstrings (`Args:`,
  `Returns:`, `Raises:`, `Attributes:`). Ruff enforces this in `src/alglab`.

### 2. Register the module in the package `__init__`

Edit (or create) `src/alglab/algorithms/example_problem/__init__.py`:

```python
from alglab.algorithms.example_problem import example_algorithm  # activates @register_algorithm
```

And the main `__init__` of `alglab.algorithms`:

```python
# src/alglab/algorithms/__init__.py
from alglab.algorithms import example_problem
```

The worker imports `alglab.algorithms` on startup, which activates all registrations.

### 3. Verify

```bash
uv run alglab run --dir data/ --algos example_algorithm --timeout 30
```

If the name is not registered, the engine fails with:

```
KeyError: Unknown algorithm: 'example_algorithm'. Registered: [...]
```

### 4. Add tests

Create `tests/algorithms/test_example_algorithm.py`:

```python
from pathlib import Path
from alglab.algorithms.example_problem.example_algorithm import ExampleAlgorithm

def test_run_basic(tmp_path):
    # Create a minimal instance
    instance = tmp_path / "instance.txt"
    instance.write_text("...")

    algo = ExampleAlgorithm(max_iter=10)
    context = algo.before_run(instance)
    payload = algo.run(context)
    result = algo.after_run(context, payload)

    assert "objective_value" in result
    assert result["objective_value"] >= 0
```

Also add a registry or CLI smoke test when the algorithm should be available
from `alglab run`:

```python
import alglab.algorithms
from alglab.engine.registry import build_algorithm


def test_algorithm_is_registered() -> None:
    algo = build_algorithm("example_algorithm", max_iter=10)
    assert algo.name == "example_algorithm"
```

Run the focused test set:

```bash
uv run pytest tests/algorithms/test_example_algorithm.py
uv run ruff check src/alglab/algorithms tests/algorithms
```

---

## Result design guidance

Keep result payloads compact and stable. Prefer numeric fields and short
booleans over nested solver-specific structures:

```json
{
  "objective_value": 123.0,
  "feasible": true,
  "iterations": 250,
  "verification_mismatch": false
}
```

Use consistent key names across algorithms solving the same problem. This lets
the study YAML compare algorithms with one shared `MeasureSpec`.

Avoid storing large assignments by default. Expose an `include_assignment`
constructor option when full solutions are useful for debugging.

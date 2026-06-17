# Execution engine (`alglab.engine`)

The engine manages the lifecycle of experiments: algorithm definition, job generation, parallel execution with timeout, and result writing.

---

## `Algorithm` — base class

`alglab.engine.core.Algorithm`

Abstract class that defines the interface every algorithm must implement.

```python
from __future__ import annotations

from dataclasses import dataclass
from alglab.engine.core import Algorithm
from alglab.engine.registry import register_algorithm
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ExampleContext:
    source_name: str
    text: str


@register_algorithm("example_algorithm")
class ExampleAlgorithm(Algorithm[ExampleContext]):
    """Count lines in a text instance."""

    def before_run(self, file_path: Path) -> ExampleContext:
        """Load one instance file into a reusable context.

        Args:
            file_path: Path to the instance file.

        Returns:
            Context consumed by `run`.
        """
        return ExampleContext(
            source_name=file_path.name,
            text=file_path.read_text(encoding="utf-8"),
        )

    def run(self, context: ExampleContext, *, seed: int | None = None) -> dict[str, Any]:
        """Return the number of lines in the loaded instance.

        Args:
            context: Context returned by `before_run`.
            seed: Per-file seed supplied by the engine. This algorithm ignores it.

        Returns:
            JSON-serialisable result payload.
        """
        return {"line_count": len(context.text.splitlines())}
```

Production algorithms in `src/alglab` use Google-style docstrings. At minimum,
document public classes, public methods, parameters, return values, raised
exceptions, and any lifecycle behaviour that callers must preserve.

### Methods

#### `before_run(file_path: Path) -> TContext`

Creates the per-job context consumed by `run()` and `after_run()`. The default
implementation returns `file_path`, which is appropriate for simple algorithms
that use `Algorithm[Path]`.

Use this hook for parsing input files or building per-instance structures. Data
for the current file should live in the returned context, not in instance
attributes.

#### `run(context: TContext, *, seed: int | None = None) -> dict[str, Any]`  *(abstract)*

Main method. Receives the context returned by `before_run()` and returns a
dictionary with the results. All dictionary keys appear in the JSONL under
`result.*`.

The engine always passes `seed=job.seed` to every `run()` call. Algorithms that
don't use randomness accept and ignore it. Algorithms that do use it pass it to
their sampler or RNG directly.

#### `after_run(context: TContext, payload: dict[str, Any]) -> dict[str, Any]`

Final payload transformation hook. The default implementation returns `payload`
unchanged. Custom implementations must return the final result dictionary; a
`None` return value is treated as an algorithm error.

#### `name: str`  *(property)*

Returns the algorithm name. By default it is the value of `__algo_name__` (set by the decorator) or `self.__class__.__name__.lower()`.

### Pickling requirements

Subclasses must be defined **at module level** (not as inner classes or lambdas). This is required so that `pickle` can serialise instances in spawn mode (Linux fork mode does not require it, but it is necessary for Windows/macOS).

### Worker instance cache

Worker processes cache algorithm instances by algorithm name and constructor
parameters:

```python
(job.algorithm, tuple(sorted(job.algo_kwargs.items())))
```

Constructor parameters must be hashable. Keep per-job data in the lifecycle
context returned by `before_run()` so cached instances can safely process many
jobs.

---

## `@register_algorithm` — registration decorator

`alglab.engine.registry.register_algorithm(name: str)`

Registers an `Algorithm` class under a string name. The name is normalised to lowercase.

```python
from alglab.engine.registry import register_algorithm

@register_algorithm("maxsat_brute")
class MaxSATBruteAlgorithm(Algorithm[MaxSATBruteContext]):
    ...
```

Raises `ValueError` if the name is already registered.

### `build_algorithm(name: str, **kwargs) -> Algorithm[Any]`

Builds an instance of the algorithm registered under `name`. The `kwargs` are passed to the constructor.

```python
from alglab.engine.registry import build_algorithm

algo = build_algorithm("maxsat_qubo_sa", num_reads=500)
```

### `parse_algo_spec(spec: str) -> tuple[str, dict[str, Any]]`

Parses an algorithm spec with optional parameters:

```python
parse_algo_spec("maxsat_brute")
# → ("maxsat_brute", {})

parse_algo_spec("maxsat_qubo_sa:num_reads=500,include_assignment=true")
# → ("maxsat_qubo_sa", {"num_reads": 500, "include_assignment": True})
```

Values are automatically converted to `bool`, `int`, `float`, or `str` in order of precedence.

---

## `Job` — work descriptor

`alglab.engine.core.Job`

Immutable dataclass that describes an execution job.

```python
@dataclass(frozen=True)
class Job:
    file_path: Path        # path to the instance file
    algorithm: str         # algorithm name (key in the registry)
    seed: int | None       # optional seed
    algo_kwargs: dict      # extra parameters for the algorithm constructor
```

`Job` objects are created by `iter_directory_jobs` and consumed by `Runner`.

---

## `iter_directory_jobs` — job generation

`alglab.engine.jobs.iter_directory_jobs`

Generates `Job` objects for the Cartesian product (files × algorithms). By default
it traverses the glob in filesystem order without materialising the complete list of
paths. If you need a reproducible order in the JSONL, enable `sort_files=True`
to sort by path before creating the jobs.

```python
from alglab.engine.jobs import iter_directory_jobs
from pathlib import Path

jobs = iter_directory_jobs(
    Path("data/"),
    algorithms=["maxsat_brute", "maxsat_qubo_sa:num_reads=200"],
    pattern="*.cnf",        # glob to filter files
    recursive=False,        # search in subdirectories
    sort_files=False,       # True = sort by path before iterating
)
```

Validates algorithm names **eagerly** (before iterating) to fail fast on typos.

---

## `Runner` — parallel execution

`alglab.engine.runner.Runner`

Executes jobs in a process pool (pebble) with per-job timeout.

```python
from alglab.engine.runner import Runner

runner = Runner(
    max_workers=4,    # parallel workers; default 1
    timeout=60.0,     # per-job timeout in seconds; None = no limit
)

for result in runner.run_stream(jobs):
    print(result.algorithm, result.status, result.run_duration_s)
```

### `run_stream(jobs: Iterable[Job]) -> Iterator[Result]`

Returns an iterator of `Result`. Results arrive in the order workers finish, not in job order. Keeps up to `max_workers × 2` jobs in flight simultaneously.

### Failure behaviour

- If the algorithm raises an exception: `status="algorithm_error"`, the worker stays alive.
- If the job exceeds the timeout: `status="timeout"`, the process is terminated.
- If the process dies unexpectedly (OOM, signal): `status="process_expired"`.
- If the engine encounters an error collecting the result: `status="engine_error"`.

In all error cases, `result.result` is `None` and `result.error_message` contains the details.

Example JSONL timeout record:

```json
{
  "file": "data/instance_0001.cnf",
  "algorithm": "slow_solver",
  "status": "timeout",
  "result": null,
  "error_message": "Job timed out after 30.0 seconds",
  "seed": 123,
  "pre_run_duration_s": null,
  "run_duration_s": null,
  "post_run_duration_s": null
}
```

Timeouts and process expiry do not stop the whole run; they are recorded as
results and the runner continues processing remaining jobs.

---

## `Result` — job result

`alglab.engine.core.Result`

Immutable dataclass with the result of executing a `Job`.

```python
from alglab.engine.core import ResultStatus


@dataclass(frozen=True)
class Result:
    file: str                      # str(job.file_path)
    algorithm: str                 # algorithm name
    status: ResultStatus           # "ok" | "algorithm_error" | "timeout" | ...
    result: dict[str, Any] | None  # algorithm result (None on error)
    error_message: str | None      # error description if status != "ok"
    seed: int | None               # seed used
    pre_run_duration_s: float | None   # time for before_run()
    run_duration_s: float | None       # time for run()
    post_run_duration_s: float | None  # time for after_run()
```

`pre_run_duration_s`, `run_duration_s`, and `post_run_duration_s` measure each phase inside the worker. They are `None` when the phase did not execute (timeout, prior error). The sum of the three is the total algorithm time.

Study configurations that compare runtime should normally use the sum of these
three fields when they need end-to-end algorithm time. Use `run_duration_s`
alone only when setup and post-processing time should be excluded deliberately.

---

## Worker execution model

Each worker subprocess maintains a local cache of algorithm instances. For every job:

```
worker process
  algorithm_cache: (algorithm_name, sorted_kwargs) → Algorithm[Any]

  for each job:
    algorithm = get_or_build_cached_instance(job.algorithm, job.algo_kwargs)
    context   = algorithm.before_run(job.file_path)    # pre_run_duration_s
    payload   = algorithm.run(context, seed=job.seed)  # run_duration_s
    result    = algorithm.after_run(context, payload)  # post_run_duration_s
```

Phase timings are measured around each call. If a phase raises an exception, only
the phases that started have a timing value; later phases are `None`. The timeout
covers the full job (`before_run + run + after_run`): if exceeded, pebble kills the
worker process and discards its instance cache.

---

## `derive_seed` — per-file seed derivation

`alglab.engine.core.derive_seed`

Derives a deterministic, per-file seed from a global base seed and a file path.

```python
def derive_seed(global_seed: int, file_path: Path) -> int: ...
```

Used internally by `alglab run --seed N` so that every instance in a run gets a unique but reproducible seed rather than the same global value. The derivation uses MD5 over `"{global_seed}|{file_path}"` and returns the first 8 bytes as an unsigned integer.

**Properties:**
- Same inputs always produce the same seed (deterministic).
- Different `file_path` values produce different seeds for the same `global_seed`.
- Different `global_seed` values produce different seeds for the same file.

```python
from alglab.engine import derive_seed
from pathlib import Path

s = derive_seed(42, Path("data/instance_0001.cnf"))  # reproducible int
```

---

## `JSONLResultSink` — result writing

`alglab.engine.sinks.JSONLResultSink`

Writes `Result` objects to a JSONL file, one per line. Must be used as a context manager.

```python
from alglab.engine.sinks import JSONLResultSink
from pathlib import Path

sink = JSONLResultSink(Path("output/results.jsonl"), append=False)

with sink:
    for result in runner.run_stream(jobs):
        sink.write(result)
```

### Constructor

```python
JSONLResultSink(path: Path, *, append: bool = False)
```

- `append=False`: overwrites the file if it already exists.
- `append=True`: appends to the existing file (or creates it if it does not exist).

The parent directory is created automatically.

### `write_all(results: Iterable[Result]) -> int`

Convenient alternative that opens/closes the file automatically and returns the number of records written.

```python
sink.write_all(runner.run_stream(jobs))
```

Use `append=True` only when combining compatible runs:

```python
sink = JSONLResultSink(Path("output/results.jsonl"), append=True)
```

Compatible means the same benchmark identity convention, especially the same
absolute or relative form for `Result.file`.

---

## Common implementation patterns

### Deterministic algorithm

Deterministic algorithms still accept `seed`; they simply ignore it:

```python
def run(self, context: ExampleContext, *, seed: int | None = None) -> dict[str, Any]:
    return {"objective_value": exact_solve(context.instance)}
```

### Randomised algorithm

Randomised algorithms should create a local RNG from the supplied seed:

```python
import random

def run(self, context: ExampleContext, *, seed: int | None = None) -> dict[str, Any]:
    rng = random.Random(seed)
    value = randomised_search(context.instance, rng=rng)
    return {"objective_value": value}
```

Do not use module-level RNG state in worker processes; cached algorithm
instances process multiple jobs.

### Verification in `after_run`

Use `after_run` when a solver returns an internal representation that should be
checked or normalised before JSONL serialisation:

```python
def after_run(self, context: ExampleContext, payload: dict[str, Any]) -> dict[str, Any]:
    verified = verify_solution(context.instance, payload["assignment"])
    return {
        **payload,
        "objective_value": verified.objective_value,
        "verification_mismatch": verified.objective_value != payload["objective_value"],
    }
```

---

## Complete programmatic usage example

```python
import alglab.algorithms  # activates registration of all algorithms
from alglab.engine.jobs import iter_directory_jobs
from alglab.engine.runner import Runner
from alglab.engine.sinks import JSONLResultSink
from pathlib import Path

jobs = iter_directory_jobs(
    Path("data/"),
    algorithms=["maxsat_brute", "maxsat_qubo_sa:num_reads=300"],
    pattern="*.cnf",
)

runner = Runner(max_workers=8, timeout=120.0)
sink = JSONLResultSink(Path("output/results.jsonl"))

with sink:
    for result in runner.run_stream(jobs):
        sink.write(result)
        if result.status == "ok":
            ratio = result.result["satisfaction_ratio"]
            print(f"{result.algorithm} {Path(result.file).name}: {ratio:.3f}")
```

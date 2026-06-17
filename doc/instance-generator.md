# k-CNF Instance Generator

`alglab.algorithms.max2sat.generator`

Generates massive sweeps of random instances in **DIMACS CNF** format for Max-2-SAT or k-SAT. The CLI works by density:

```text
clauses = round(vars * density)
```

## DIMACS CNF Format

Generated files follow the DIMACS standard and add metadata in comments:

```text
c seed=13168458065840286439
c vars=20
c clauses=50
c density=2.5
c k=2
c file_index=1
p cnf 20 50
-3 7 0
1 -12 0
5 -8 0
...
```

The DIMACS parser ignores `c` lines and reads the generated files normally.

---

## CLI Usage

The official path is the Python module; there is no maintained `generate.sh` wrapper.
For full reproducible experiments, use `alglab experiment run` and let the experiment
YAML write this data into `experiments/<name>/data/`.

```bash
uv run python -m alglab.algorithms.max2sat.generator \
    --n-files 30 \
    --vars 20:100:20 \
    --densities 1.0,1.5,2.0 \
    --k 2 \
    --jobs -1 \
    --out data/max2sat/ \
    --seed-base 42
```

### Options

| Option | Description |
|--------|-------------|
| `--vars SPEC` | Integer, CSV, or inclusive range `start:stop:step`. |
| `--densities SPEC` | Float, CSV, or inclusive range `start:stop:step`. |
| `--n-files N` | Number of files per `(vars, density)` combination. |
| `--k N` | Literals per clause. |
| `--jobs N` | Parallel workers. `-1` uses `os.cpu_count() or 1`. |
| `--out DIR` | Root output directory. |
| `--seed-base N` | Deterministic base for deriving per-file seeds. |
| `--dry-run` | Validates and prints all jobs without creating or deleting files. |
| `--clean` | Deletes `--out` before generating. Has no effect with `--dry-run`. |

`--jobs 0`, values `< -1`, `k > vars`, densities `<= 0`, `n-files < 1`, malformed ranges, and combinations where `round(vars * density) * k < vars` are all rejected.

When generating data for algorithms registered through the installed package,
run `uv sync --reinstall-package alglab` after source changes so the CLI sees
the latest generator and algorithm code.

---

## Output Structure

Each combination gets its own directory:

```text
data/max2sat/
  vars_00020_density_1.0/
    instance_0001.cnf
    instance_0002.cnf
  vars_00020_density_1.5/
    instance_0001.cnf
```

Variables are formatted with 5 digits. Densities strip unnecessary trailing zeros but keep one decimal for integers (`1.0`, `1.5`, `2.25`).

For a sweep with `--vars 20:60:20`, `--densities 2.0,3.0`, and
`--n-files 5`, the generator creates `3 * 2 * 5 = 30` files.

---

## Reproducibility

Each file uses a stable seed derived with MD5 from:

```text
seed_base, vars, clauses, density, k, file_index
```

No global RNG or order-dependent sequence is used. Changing `--jobs`, the scheduling order, or increasing `--n-files` does not alter existing instances.

Parallel generation uses `pebble.ProcessPool`; each worker creates its own `random.Random(seed)`.

Use the same `seed_base`, `vars`, `densities`, `k`, and `file_index` to
regenerate identical instances. Changing `density` changes the rounded clause
count and therefore the generated seed.

---

## Dry Run

```bash
uv run python -m alglab.algorithms.max2sat.generator \
    --n-files 2 \
    --vars 20 \
    --densities 1.0:2.0:0.5 \
    --k 2 \
    --out data/max2sat/ \
    --dry-run
```

Prints a summary and all jobs with path, seed, variables, density, clauses, `k`, and file index. Does not create directories or files.

Use dry run before large sweeps to confirm the total number of files and output
paths:

```bash
uv run python -m alglab.algorithms.max2sat.generator \
    --n-files 200 \
    --vars 10:50:10 \
    --densities 2.0,3.5,4.5 \
    --k 2 \
    --out data/sweep \
    --dry-run
```

---

## Programmatic Usage

```python
from pathlib import Path

from alglab.algorithms.max2sat.generator import generate_instances

paths = generate_instances(
    n_files=50,
    vars_values=[30, 50],
    densities=[1.5, 2.0],
    k=2,
    out=Path("data/experiment/"),
    seed_base=1234,
    jobs=4,
)
```

Clean and regenerate an output directory:

```python
paths = generate_instances(
    n_files=10,
    vars_values=[20],
    densities=[2.5],
    k=2,
    out=Path("data/smoke"),
    seed_base=42,
    jobs=-1,
    clean=True,
)
```

---

## Low-Level Functions

```python
import random

from alglab.algorithms.max2sat.generator import generate_kcnf_formula, formula_to_dimacs

rng = random.Random(42)
formula = generate_kcnf_formula(
    num_vars=20,
    num_clauses=50,
    k=2,
    rng=rng,
)

text = formula_to_dimacs(
    formula,
    num_vars=20,
    seed=42,
    density=2.5,
    k=2,
    file_index=1,
)
```

The generator guarantees full coverage when `num_clauses * k >= num_vars`: every variable appears at least once.

---

## DIMACS Parser

`alglab.algorithms.max2sat.parser._parse_dimacs`

```python
from pathlib import Path

from alglab.algorithms.max2sat.parser import _parse_dimacs

text = Path("data/max2sat/vars_00020_density_2.5/instance_0001.cnf").read_text(
    encoding="utf-8"
)
num_vars, clauses = _parse_dimacs(text)
```

Validates unique header, literals in range, clauses terminated with `0`, and clause count matching the header.

---

## Experiment YAML integration

`alglab experiment run` passes the `generation` section to the same generator:

```yaml
generation:
  n_files: 50
  vars: "20:100:20"
  densities: "2.0,3.0,4.0"
  k: 2
  jobs: -1
  seed_base: 42
```

The generated files are written under `experiments/<name>/data/`. The engine
phase should then use `recursive: true` because each `(vars, density)`
combination is placed in its own subdirectory:

```yaml
engine:
  pattern: "*.cnf"
  recursive: true
```

---

## Choosing sweep sizes

The total job count in the engine phase is:

```text
generated_files * number_of_algorithms
```

For example:

```text
vars values:       5
density values:    3
files per combo: 200
algorithms:        4

engine jobs = 5 * 3 * 200 * 4 = 12,000
```

Use `--dry-run` to validate the generation size, and set per-job `--timeout`
conservatively when comparing exponential algorithms such as `maxsat_brute`.

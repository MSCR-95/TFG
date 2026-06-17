# Getting started

## Prerequisites

- Python 3.13, matching `.python-version`
- [uv](https://docs.astral.sh/uv/) installed
- git

## Installation

```bash
git clone <repository-url>
cd AlgsFmwk
```

### Base dependencies

Install the package with its production dependencies:

```bash
uv sync
```

This creates `.venv/` and registers the `alglab` command.

After editing files under `src/alglab`, refresh the editable install before
using the CLI or integration tests:

```bash
uv sync --reinstall-package alglab
```

### Optional dependencies: QUBO solvers

The QUBO solvers (`maxsat_qubo_sa`, `maxsat_qubo_ts`, `maxsat_qubo_sds`, `maxsat_qubo_rs`, `maxsat_qubo_exact`) require `dwave-ocean-sdk`:

```bash
uv sync --extra qubo
```

If not installed, alglab works without QUBO and shows a warning when trying to use those solvers.

After source changes, combine the extra with the reinstall flag:

```bash
uv sync --extra qubo --reinstall-package alglab
```

### Development tools

```bash
uv sync --group dev          # pytest, ruff, mypy, pre-commit
uv run pre-commit install    # enables git hooks
```

---

## Verify installation

```bash
uv run alglab --help
```

Should show:

```
Usage: alglab [OPTIONS] COMMAND [ARGS]...

  Algorithm lab: run experiments and analyse results.

Commands:
  experiment  Create and run reproducible experiment directories.
  run    Run algorithms on problem instances.
  study  Analyse experiment results.
```

---

## First complete experiment

The recommended approach is to declare the experiment in YAML and let `alglab` create a
self-contained folder under `experiments/`:

```yaml
# experiment.yaml
generation:
  n_files: 30
  vars: "20"
  densities: "2.5"
  k: 2
  jobs: -1
  seed_base: 42

engine:
  algorithms:
    - maxsat_brute
    - maxsat_qubo_sa
  pattern: "*.cnf"
  recursive: true
  sort_files: false
  n_jobs: 4
  timeout: 30
  seed: 42

study:
  alpha: 0.05
  measures:
    - path: result.satisfaction_ratio
      label: Satisfaction
      higher_is_better: true
      aggregation: mean
    - path: run_duration_s
      label: Time (s)
      higher_is_better: false
      aggregation: mean
```

```bash
uv run alglab experiment run --name primer_max2sat --config experiment.yaml
```

This creates:

```text
experiments/primer_max2sat/
  config/   # experiment.yaml + manifest.json
  data/     # generated DIMACS instances
  engine/   # results.jsonl + logs
  study/    # analysis.txt, matrices/, plots/
```

Inspect the generated experiment:

```bash
uv run python -m json.tool experiments/primer_max2sat/config/manifest.json
sed -n '1,120p' experiments/primer_max2sat/study/analysis.txt
find experiments/primer_max2sat/study/plots -maxdepth 1 -type f | sort
```

Run the same experiment again from scratch:

```bash
uv run alglab experiment run \
  --name primer_max2sat \
  --config experiment.yaml \
  --overwrite
```

The following steps show the equivalent manual workflow.

### Step 1: Generate instances

Creates 30 Max-2-SAT instances with 20 variables and density 2.5. The generator computes `clauses = round(vars * density)`, so each instance has 50 clauses:

```bash
uv run python -m alglab.algorithms.max2sat.generator \
    --n-files 30 \
    --vars 20 \
    --densities 2.5 \
    --k 2 \
    --jobs -1 \
    --out data/max2sat/ \
    --seed-base 42
```

Files are generated in DIMACS CNF format:

```
c seed=13168458065840286439
c vars=20
c clauses=50
c density=2.5
c k=2
c file_index=1
p cnf 20 50
-3 7 0
1 -12 0
...
```

### Step 2: Run experiment

Compares two algorithms with 4 parallel workers and a timeout of 30 seconds per instance:

```bash
uv run alglab run \
    --dir data/max2sat/ \
    -A maxsat_brute -A maxsat_qubo_sa \
    --pattern "*.cnf" \
    --recursive \
    --n-jobs 4 \
    --timeout 30 \
    --seed 42 \
    --out-path output/results.jsonl
```

Progress output:

```
[done=1] maxsat_brute      instance_0001.cnf    ok   0.042s
[done=2] maxsat_qubo_sa    instance_0001.cnf    ok   0.387s
...
processed=60 ok=60 timeout=0 errors=0
```

The file `output/results.jsonl` contains one JSON line per execution.

For deterministic line order during debugging, add `--sort-files`. For large
sweeps, leave sorting disabled so file discovery can stream.

Preview the results:

```bash
head -n 1 output/results.jsonl | uv run python -m json.tool
wc -l output/results.jsonl
```

Append another algorithm to the same JSONL:

```bash
uv run alglab run \
    --dir "$(pwd)/data/max2sat" \
    -A "maxsat_qubo_sds:num_reads=100" \
    --pattern "*.cnf" \
    --recursive \
    --n-jobs 4 \
    --timeout 30 \
    --seed 42 \
    --out-path output/results.jsonl \
    --append
```

Use the same absolute `--dir` every time when appending, otherwise the `file`
field can differ for the same instance.

### Step 3: Analyse results

Requires a YAML config referencing the JSONL file:

```yaml
# study.yaml
input_jsonl: output/results.jsonl
measures:
  - path: result.satisfaction_ratio
    label: Satisfaction
    higher_is_better: true
    aggregation: mean
  - path: run_duration_s
    label: Time (s)
    higher_is_better: false
    aggregation: mean
```

```bash
uv run alglab study run --config study.yaml --output report/
```

Console output (k=2: Wilcoxon only, no Iman-Davenport):

```
** STATISTIC ANALYSIS ***********************************************

>> Summary

Variable:	Satisfaction
Goodness:	Positive
Ranking:	maxsat_brute >= maxsat_qubo_sa

                RANK   MIN   MAX  MEAN  STDEV
maxsat_brute       1  0.85  1.00  0.97   0.04
maxsat_qubo_sa     2  0.70  1.00  0.91   0.07

>> Assumption diagnostics
...

>> Wilcoxon 's test

Statistic = 79
P-value   = 0.021405
```

Output files written to `report/`:

```
report/
  analysis.txt
  matrices/
    Satisfaction.csv
    Time (s).csv
  plots/
    Satisfaction-boxplot.{pdf,svg,png}
    Satisfaction-density.{pdf,svg,png}
    Satisfaction-qq.{pdf,svg,png}
    Time (s)-boxplot.{pdf,svg,png}
    ...
```

---

## Analysis with YAML configuration

`alglab study run` always requires a YAML config. For multiple measures or reproducible configuration:

```yaml
# study.yaml
input_jsonl: output/results.jsonl
alpha: 0.05
measures:
  - path: result.satisfaction_ratio
    label: Satisfaction
    higher_is_better: true
    aggregation: mean
  - path: run_duration_s
    label: Time (s)
    higher_is_better: false
    aggregation: mean
```

```bash
uv run alglab study run --config study.yaml --output report/
```

Generates `report/analysis.txt`, one CSV matrix per measure under `report/matrices/`, and PDF/SVG/PNG plots per measure under `report/plots/`.

Inspect the matrix used by the statistical tests:

```bash
sed -n '1,10p' report/matrices/Satisfaction.csv
sed -n '1,180p' report/analysis.txt
```

If the YAML lives next to the result file, `input_jsonl` can be relative to the
YAML location:

```text
reports/
  study.yaml
  results.jsonl
```

```yaml
input_jsonl: results.jsonl
```

---

## Run tests

```bash
uv run pytest                        # all tests
uv run pytest tests/framework/       # engine tests only
uv run pytest tests/study/           # statistical pipeline tests only
uv run pytest -v                     # verbose
```

## Linting and type checking

Ruff enforces Google-style docstrings in `src/alglab` through pydocstyle (`D`)
rules. Test files are excluded from docstring linting, but still run through the
normal lint and format checks.

### Quality gate

```bash
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run mypy src/alglab/
uv run pytest tests/
```

For documentation-only changes, at least run:

```bash
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

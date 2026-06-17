# alglab Architecture

## Overview

alglab is organized into three independent layers:

```
┌─────────────────────────────────────────────────────┐
│                      CLI (_cli)                     │
│   alglab run   alglab study   alglab experiment     │
└──────────────┬──────────────────────┬──────────────┘
               │                      │
┌──────────────▼──────────┐  ┌────────▼──────────────┐
│        Engine           │  │        Study           │
│  Registry · Runner      │  │  io · stats · plots    │
│  Jobs · Sinks · Worker  │  │  analysis · schema     │
└──────────────┬──────────┘  └───────────────────────┘
               │
┌──────────────▼──────────┐
│      Algorithms         │
│  max2sat: brute, QUBO…  │
└─────────────────────────┘
```

The layers are independent: `Study` only reads JSONL files produced by `Engine`. `Engine` does not know about `Study`. `Algorithms` only know about `Engine`.

---

## Modules

### `alglab.engine`

Execution core. No domain dependencies.

| Module | Responsibility |
|--------|----------------|
| `core.py` | Base classes `Algorithm`, `Job`, `Result` |
| `registry.py` | Global algorithm registry keyed by string name |
| `jobs.py` | `Job` generation from a directory |
| `runner.py` | `Runner`: process pool with timeout and streaming |
| `worker.py` | `_execute_job` function that runs in the subprocess |
| `sinks.py` | `JSONLResultSink`: writes `Result` to JSONL |

### `alglab.study`

Statistical analysis pipeline. Only reads data; does not execute algorithms.

| Module | Responsibility |
|--------|----------------|
| `schema.py` | `MeasureSpec`, `StudyConfig` (with `from_yaml()`) |
| `io.py` | JSONL reading → filtering → extraction → pivoting → imputation; `build_matrix`, `prepare_matrix` |
| `stats.py` | `rank_matrix`, `summary_table`, `wilcoxon_signed_test`, `iman_davenport_test`, `shapiro_diagnostics`, `levene_diagnostic`, `raw_wilcoxon_pvalues`, Hommel/Bergmann-Hommel/Shaffer corrections |
| `plots.py` | `plot_boxplot`, `plot_density`, `plot_qq`, `plot_ranking`, `plot_pvalues`, `plot_all`; outputs PDF/SVG/PNG |
| `analysis.py` | `run_study(config, output_dir)` — main entry point; orchestrates io → stats → plots; writes `analysis.txt` |
| `_formatting.py` | Shared compact number formatting for text reports and plot annotations |

### `alglab.algorithms`

Reference implementations. Each imported module activates `@register_algorithm`.

```
algorithms/
└── max2sat/
    ├── __init__.py          # imports all modules → activates registration
    ├── _common.py           # MaxSAT evaluation and shared payloads
    ├── parser.py            # DIMACS CNF parser
    ├── generator.py         # k-CNF instance generator
    ├── brute.py             # maxsat_brute
    └── qubo.py              # QUBO base and maxsat_qubo_exact/sa/ts/sds/rs
```

### `alglab._cli`

Command-line interface built with Typer.

| Module | Responsibility |
|--------|----------------|
| `main.py` | Root `alglab` command; registers sub-commands |
| `run.py` | `alglab run`: launches `run_experiment` |
| `study.py` | `alglab study`: dispatches to `_study.py` |
| `_study.py` | Analysis logic (separated from Typer glue) |
| `experiment.py` | `alglab experiment`: orchestrates generation → engine → study |

---

## Data Flow

### Experiment (`alglab run`)

```
Instance directory
        │
        ▼
iter_directory_jobs()  →  Iterator[Job]
        │
        ▼
Runner.run_stream()
  └── ProcessPool (pebble)
       └── _execute_job(job)  [subprocess]
             ├── get cached algorithm instance
             ├── context = algorithm.before_run(file_path)
             ├── payload = algorithm.run(context, seed=job.seed)   →  dict[str, Any]
             └── result = algorithm.after_run(context, payload)
        │
        ▼
Iterator[Result]
        │
        ▼
JSONLResultSink  →  output/results.jsonl
```

### Analysis (`alglab study`)

```
output/results.jsonl
        │
        ▼
read_jsonl()           →  DataFrame (pd.json_normalize)
        │
filter_ok()            →  status="ok" only
        │
extract_measure()      →  (file, algorithm, seed, value)
        │
aggregate_seeds()      →  (file, algorithm, value) — aggregates seeds
        │
pivot_matrix()         →  DataFrame  [file × algorithm]
        │
impute_nan()           →  NaN imputation (column_worst/row_worst/drop/warn)
        │
        ▼
run_study(config, output_dir)
  → analysis.txt (text report)
  → matrices/<label>.csv
  → plots/<label>-*.{pdf,svg,png}
```

---

## Key Design Decisions

**Process pool (pebble).** Each job runs in a separate process, enabling hard timeouts (the process is killed by the OS) without affecting the main runner. `ThreadPoolExecutor` cannot kill threads.

**Lazy registration.** Algorithms are registered when `alglab.algorithms` is imported. The worker imports this package in the subprocess, ensuring the registry is available even in spawn mode (Windows/macOS).

**Stateless algorithm lifecycle.** Algorithm instances may be reused inside a worker process. Per-file data is carried through an explicit context returned by `before_run()` and consumed by `run()` and `after_run()`.

**Worker instance cache.** Each worker caches algorithm instances by `(algorithm_name, sorted constructor kwargs)`. Constructor kwargs must be hashable.

**JSONL as output format.** Each `Result` is an independent JSON line. It can be processed with `jq`, Python, R, etc. The file is recoverable if the process is interrupted mid-way.

**Engine / study separation.** The engine never imports numpy/pandas/scipy. The study never imports pebble/multiprocessing. Both layers are independently testable.

**Text report + vector plots.** `analysis.txt` is the printable statistical report. Figures are generated as PDF/SVG/PNG for use in papers or presentations.

**Documentation as a checked contract.** Public source documentation uses
Google-style docstrings and is enforced by Ruff's pydocstyle (`D`) rules for
`src/alglab`. Tests are excluded from docstring linting so test names can carry
the behaviour description.

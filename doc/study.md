# Study pipeline (`alglab.study`)

The `study` module transforms a JSONL file of experimental results into statistical output. It operates exclusively on data; it does not execute algorithms.

---

## Entry point

`run_study(config, output_dir)` is the single entry point. It handles everything end-to-end.

```python
from alglab.study import run_study, StudyConfig, MeasureSpec
from pathlib import Path

config = StudyConfig(
    input_jsonl=Path("output/results.jsonl"),
    measures=[
        MeasureSpec("result.satisfaction_ratio", label="sat", higher_is_better=True),
        MeasureSpec("run_duration_s", label="time", higher_is_better=False),
    ],
    alpha=0.05,
)
paths = run_study(config, Path("report/"))
```

Output under `report/`:
- `analysis.txt` — full text analysis (summary, diagnostics, tests, tables)
- `matrices/<label>.csv` — file × algorithm value matrix per measure
- `plots/<label>-{boxplot,density,qq}.{pdf,svg,png}` — always
- `plots/<label>-{ranking,pvalues}.{pdf,svg,png}` — only when k ≥ 3

Returned: sorted list of all generated `Path` objects.

---

## Configuration

### `StudyConfig`

```python
from alglab.study.schema import StudyConfig, MeasureSpec

config = StudyConfig(
    input_jsonl=Path("results.jsonl"),   # required
    measures=[...],                       # list[MeasureSpec], required
    alpha=0.05,                           # optional, default 0.05
)
```

Load from YAML:

```python
config = StudyConfig.from_yaml(Path("study.yaml"))
```

`input_jsonl` in the YAML is resolved relative to the YAML file directory.

### YAML schema

```yaml
input_jsonl: output/results.jsonl
alpha: 0.05
measures:
  - path: result.satisfaction_ratio
    label: Satisfaction
    higher_is_better: true
    aggregation: mean          # mean | median | best | worst

  - path: result.verification_mismatch
    label: Mismatch
    higher_is_better: false
    aggregation: mean
    cast_bool_to_int: true     # converts True/False → 1/0

  - path: run_duration_s
    label: Algorithm time (s)
    higher_is_better: false
    aggregation: mean
```

### `MeasureSpec`

```python
MeasureSpec(
    path="result.satisfaction_ratio",  # dot-notation field path in JSONL
    label="Satisfaction",              # display name (defaults to last segment of path)
    higher_is_better=True,
    aggregation="mean",                # mean | median | best | worst
    cast_bool_to_int=False,
)
```

`aggregation="best"` → max if `higher_is_better`, min otherwise. `"worst"` inverts that.

If a measure is missing from the JSONL, analysis fails with `KeyError` instead
of silently producing NaN. This is intentional: the measure path is part of the
study contract and should be corrected in the YAML.

---

## I/O pipeline (`alglab.study.io`)

### Canonical flow

```
read_jsonl() → filter_ok() → extract_measure() → aggregate_seeds()
    → pivot_matrix() → impute_nan()
```

Top-level convenience:

```python
from alglab.study.io import build_matrix, prepare_matrix

# From JSONL file directly:
matrix = build_matrix(Path("results.jsonl"), spec)

# From an already-loaded DataFrame:
matrix = prepare_matrix(df, spec, filter_status=True)
```

### Functions

**`read_jsonl(path)`** — reads JSONL, flattens nested dicts with `pd.json_normalize`. Fields under `result.*` appear as `result.field` columns.

**`filter_ok(df)`** — discards rows where `status != "ok"`. Raises `ValueError` if nothing remains.

**`extract_measure(df, spec)`** — extracts `(file, algorithm, seed, value)`. Raises `KeyError` if `spec.path` does not exist.

**`aggregate_seeds(df, spec)`** — collapses multiple seeds per `(file, algorithm)` using `spec.aggregation`.

**`pivot_matrix(df)`** — converts long DataFrame to `file × algorithm` matrix.

**`impute_nan(matrix, spec, strategy)`** — fills NaN cells (instances with no result for an algorithm):

| Strategy | Behaviour |
|----------|-----------|
| `column_worst` | Default. Fills NaN with the worst value in that algorithm's column. |
| `row_worst` | Fills NaN with the worst value in that benchmark's row. |
| `worst` | Alias for `row_worst`. |
| `drop` | Removes rows that contain any NaN. |
| `warn` | Emits a warning, does not modify data. |

`column_worst` is the default because it penalises missing runs using the
worst observed value for that same algorithm. Use `drop` only when removing
incomplete benchmark rows is statistically acceptable.

### Seed aggregation example

If the JSONL contains repeated runs for the same `(file, algorithm)` pair with
different seeds, the study layer collapses them before pivoting:

```json
{"file": "a.cnf", "algorithm": "sa", "seed": 1, "result": {"score": 0.90}, "status": "ok"}
{"file": "a.cnf", "algorithm": "sa", "seed": 2, "result": {"score": 0.95}, "status": "ok"}
```

With:

```yaml
measures:
  - path: result.score
    aggregation: best
    higher_is_better: true
```

the matrix cell for `(a.cnf, sa)` becomes `0.95`.

---

## Statistical tests (`alglab.study.stats`)

### Core functions

```python
from alglab.study.stats import (
    rank_matrix,         # rank rows; best = rank 1; average ties
    summary_table,       # RANK, MIN, MAX, MEAN, STDEV per algorithm
    wilcoxon_signed_test,     # paired Wilcoxon; returns (T, pnorm(z))
    iman_davenport_test,      # Iman-Davenport F-test; returns (F, p, df1, df2)
    shapiro_diagnostics,      # Shapiro-Wilk per column; returns DataFrame
    levene_diagnostic,        # Levene (center=median); returns (stat, p, conclusion)
    raw_wilcoxon_pvalues,     # k×k or 1×k raw p-value matrix
    p_adjust_hommel,          # Hommel correction; preserves NaN positions
    correct_control_matrix,   # Hommel correction on 1×k raw matrix
    correct_pairwise,         # Bergmann-Hommel (k≤9) or Shaffer (k>9) on k×k matrix
)
```

### Analysis behaviour by k

**k == 2:** Runs direct paired Wilcoxon on raw values. No Iman-Davenport, Hommel, or Bergmann-Hommel. Plots: boxplot, density, Q-Q.

**k ≥ 3:** Iman-Davenport global test → Hommel 1×N (each algorithm as control, on rank matrix) → Bergmann-Hommel N×N (on rank matrix; Shaffer if k > 9). Plots: boxplot, density, Q-Q, ranking, p-value heatmap.

Post-hoc uses `rank_matrix(matrix, higher_is_better)` — matching scmamp's `postHocTest(use.rank=TRUE)` default.

For runtime measures, choose the field deliberately. `run_duration_s` excludes
input parsing and post-processing. To compare end-to-end algorithm cost, sum
`pre_run_duration_s`, `run_duration_s`, and `post_run_duration_s` before
building the study input.

### Corrections

`correct_pairwise(raw, method)` applies correction to a symmetric k×k matrix:

```python
raw = raw_wilcoxon_pvalues(ranks)           # k×k, diagonal=NaN
corrected = correct_pairwise(raw)           # method="bergmann" (default)
corrected = correct_pairwise(raw, method="shaffer")
```

`exhaustive_sets(values)` is cached with `@functools.lru_cache` — Bergmann-Hommel is O(2^k).

---

## Diagnostics

- **Shapiro-Wilk** — normality test per algorithm. `p ≤ alpha` → `"reject"`, else `"not_rejected"`.
- **Levene** (center=median, Brown-Forsythe variant) — homoscedasticity test. Conclusion: `"reject_equal_variances"` or `"not_rejected"`.
- These are diagnostics only. They do not gate the pipeline — all paths use non-parametric rank-based tests regardless.
- `iman_davenport_test` returns `(inf, 0.0, df1, df2)` when denominator ≤ 0 (perfect rank separation).

### Reading the report

The report is organised per measure:

```text
Variable: Satisfaction ratio
Goodness: Positive
Ranking: maxsat_brute >= maxsat_qubo_sa
```

`Goodness: Positive` means larger values are better. `Goodness: Negative`
means smaller values are better. The ranking line is based on average ranks,
not on raw mean values.

Very small p-values may appear in scientific notation. Statistical
significance does not imply practical importance; always compare the summary
statistics and plots alongside corrected p-values.

---

## Plots (`alglab.study.plots`)

```python
from alglab.study.plots import plot_all, FIGURE_FORMATS

paths = plot_all(
    data=matrix,
    summary=summary,
    corrected_pairwise=corrected_pw,   # None for k==2
    variable="sat",
    output_dir=Path("plots/"),
    alpha=0.05,
    formats=FIGURE_FORMATS,            # ("pdf", "svg", "png")
    include_nway_plots=(k >= 3),
)
```

Individual plot functions: `plot_boxplot`, `plot_density`, `plot_qq`, `plot_ranking`, `plot_pvalues`.

`plot_ranking` draws a CD-style axis: non-significant pairs (p > alpha after correction) are connected by horizontal lines.

Tests must call `matplotlib.use("Agg")` before importing plots — done in `tests/study/conftest.py`.

### Programmatic matrix preparation

Use `prepare_matrix` directly when you need to transform or enrich data before
analysis:

```python
import pandas as pd

from alglab.study import MeasureSpec, prepare_matrix

df = pd.read_json("output/results.jsonl", lines=True)
df["total_duration_s"] = (
    df["pre_run_duration_s"].fillna(0)
    + df["run_duration_s"].fillna(0)
    + df["post_run_duration_s"].fillna(0)
)

matrix = prepare_matrix(
    df,
    MeasureSpec("total_duration_s", label="total_time", higher_is_better=False),
)
```

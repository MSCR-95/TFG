# CLI Reference

## `alglab`

Root command. Without a sub-command, shows help.

```
Usage: alglab [OPTIONS] COMMAND [ARGS]...

Commands:
  experiment  Create and run reproducible experiment directories.
  run    Run algorithms on problem instances.
  study  Analyse experiment results.
```

---

## `alglab run`

Runs one or more algorithms on a directory of instances. Each (instance, algorithm) pair is an independent job that runs in a subprocess.

### Synopsis

```bash
alglab run \
  --dir <directory> \
  --algos <algo1> [--algos <algo2>] ... \
  [--pattern <glob>] \
  [--recursive] \
  [--sort-files / --no-sort-files] \
  [--n-jobs <n>] \
  [--timeout <seconds>] \
  [--out-path <file.jsonl>] \
  [--append] \
  [--seed <integer>] \
  [--log-level <level>] \
  [--log-dir <directory>]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--dir PATH` | *(required)* | Directory containing the input instances. |
| `--algos NAME` | *(required, repeatable)* | Registered algorithm name. Can be repeated to compare several. Supports specs with parameters (see below). |
| `--pattern GLOB` | `*` | Glob pattern to filter files within `--dir`. Example: `*.cnf`. |
| `--recursive` | `False` | If enabled, searches for files in subdirectories recursively. |
| `--sort-files / --no-sort-files` | `--no-sort-files` | Sorts files by path before creating jobs. By default the filesystem order is used so that large experiments can be streamed without materialising all paths. |
| `--n-jobs N` | `1` | Number of parallel workers. `-1` uses all available CPUs. `0` and values `< -1` are rejected. |
| `--timeout FLOAT` | `None` | Maximum time per job in seconds. Jobs that exceed the limit have `status=timeout`. |
| `--out-path PATH` | `output/results.jsonl` | JSONL file where results are written. The parent directory is created automatically. |
| `--append` | `False` | Appends results to the existing file instead of overwriting it. Useful for resuming experiments. |
| `--seed INT` | `None` | Base seed for reproducibility. Each job derives a unique deterministic seed from `(INT, file_path)` via `derive_seed()`, so different instances always get different seeds. Algorithms that accept `seed` will receive the derived value. |
| `--log-level STR` | `INFO` | Logging level: `DEBUG`, `INFO`, `WARNING`, `ERROR`. |
| `--log-dir PATH` | `None` | If specified, writes rotating logs to this directory in addition to stdout. |

### Algorithm specs with parameters

In addition to a simple name, `--algos` accepts specs with parameters in the format `name:key=value,key=value`:

```bash
alglab run --dir data/ \
    --algos maxsat_brute \
    --algos "maxsat_qubo_sa:num_reads=500" \
    --algos "maxsat_qubo_ts:num_reads=200"
```

Values are automatically converted to `bool`, `int` or `float` where possible; otherwise they remain as `str`.

`--algos` has the short alias `-A`, which is useful for longer comparisons:

```bash
alglab run --dir data/ \
    -A maxsat_brute \
    -A "maxsat_qubo_sa:num_reads=500"
```

When using `--append`, keep the `file` values consistent across runs. If one
run uses a relative `--dir` and another uses an absolute `--dir`, the same
instance will appear as different benchmark rows in the study matrix.

### JSONL output format

Each line of the output file is a JSON object with the following structure:

```json
{
  "file": "data/max2sat/instancia_01.cnf",
  "algorithm": "maxsat_brute",
  "status": "ok",
  "result": {
    "num_vars": 20,
    "num_clauses": 50,
    "satisfied_clauses": 50,
    "satisfaction_ratio": 1.0,
    "verification_mismatch": false
  },
  "pre_run_duration_s": 0.006,
  "run_duration_s": 0.031,
  "post_run_duration_s": 0.001,
  "error_message": null,
  "seed": null
}
```

`satisfied_clauses` contains the final verified value. In QUBO solvers, `after_run()`
recounts the assignment against the original clauses. If `verification_mismatch` is
`true`, the count inferred from the QUBO energy did not match the verified count.

The timings `pre_run_duration_s`, `run_duration_s` and `post_run_duration_s` measure the
`before_run()`, `run()` and `after_run()` phases inside the worker. They may be `null` if
the phase did not execute.

#### `status` values

| Value | Description |
|-------|-------------|
| `ok` | The algorithm finished without error. |
| `algorithm_error` | The algorithm raised an exception. `result` is `null`; the error is in `error_message`. |
| `timeout` | The job exceeded the `--timeout` limit. |
| `process_expired` | The subprocess was terminated by the OS (OOM or other reason). |
| `engine_error` | Internal engine error when retrieving the result from the future. |

### Exit codes

`alglab run` returns `0` only if all jobs finish with `status=ok`.
Returns `1` if any job produces `algorithm_error`, `timeout`,
`process_expired` or `engine_error`. Usage errors, such as invalid `--dir`,
unknown algorithms or `--n-jobs 0`, return `2`.

### Examples

```bash
# Basic: one algorithm, no parallelism
alglab run --dir data/ --algos maxsat_brute

# Comparison with 8 workers and 60 s timeout
alglab run \
    --dir data/ \
    -A maxsat_brute -A maxsat_qubo_sa -A maxsat_qubo_ts \
    --n-jobs 8 \
    --timeout 60 \
    --out-path output/exp1.jsonl

# Append more results to an existing file
alglab run --dir data/ --algos maxsat_qubo_sds \
    --out-path output/exp1.jsonl --append

# Detailed logs to file
alglab run --dir data/ --algos maxsat_brute \
    --log-level DEBUG --log-dir logs/

# With reproducible seed
alglab run --dir data/ --algos maxsat_qubo_sa \
    --seed 1234

# Reproducible JSONL order by path
alglab run --dir data/ --algos maxsat_brute \
    --sort-files

# Same algorithm with two configurations
alglab run \
    --dir data/ \
    -A "maxsat_qubo_sa:num_reads=50" \
    -A "maxsat_qubo_sa:num_reads=500" \
    --timeout 60 \
    --out-path output/sa_sweep.jsonl
```

### Interpreting progress

During execution, the command prints one line per completed result:

```text
[done=17] maxsat_qubo_sa  instance_0009.cnf  ok  0.184s
```

The order is completion order, not input order. Fast algorithms and short
instances can appear before earlier submitted jobs. Use `--sort-files` only
when you need deterministic JSONL ordering for debugging or comparisons.

---

## `alglab study run`

Runs statistical analysis from a YAML config and writes reports.

### Synopsis

```bash
alglab study run \
  --config <study.yaml> \
  [--output <directory>] \
  [--alpha <level>]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--config PATH` | *(required)* | YAML file with study configuration (input JSONL + measures + alpha). |
| `--output DIR` | `report` | Directory where output is written. Created if it does not exist. |
| `--alpha FLOAT` | *(YAML value)* | Significance level. Overrides the value in the YAML when provided. |

### Output

For each measure defined in the config:
- `<output>/analysis.txt` — full text analysis (summary, diagnostics, tests, tables)
- `<output>/matrices/<label>.csv` — file × algorithm value matrix
- `<output>/plots/<label>-boxplot.{pdf,svg,png}` — boxplot ordered by rank
- `<output>/plots/<label>-density.{pdf,svg,png}` — kernel density estimates
- `<output>/plots/<label>-qq.{pdf,svg,png}` — Q-Q plots per algorithm
- `<output>/plots/<label>-ranking.{pdf,svg,png}` — CD-style ranking (k ≥ 3 only)
- `<output>/plots/<label>-pvalues.{pdf,svg,png}` — corrected p-value heatmap (k ≥ 3 only)

### Examples

```bash
# Standard usage
alglab study run --config study.yaml --output report/

# Override alpha
alglab study run --config study.yaml --output report/ --alpha 0.01
```

Minimal study YAML:

```yaml
input_jsonl: output/results.jsonl
measures:
  - path: result.satisfaction_ratio
    higher_is_better: true
```

Boolean metric example:

```yaml
input_jsonl: output/results.jsonl
measures:
  - path: result.verification_mismatch
    label: Verification mismatch
    higher_is_better: false
    aggregation: mean
    cast_bool_to_int: true
```

See [Study pipeline](study.md) for the YAML configuration schema.

---

## `alglab experiment run`

Orchestrates a complete, reproducible experiment: generates instances, runs the
engine and creates statistical reports inside its own folder.

### Synopsis

```bash
alglab experiment run \
  --name <name> \
  --config <experiment.yaml> \
  [--root experiments] \
  [--overwrite]
```

### Output structure

```text
experiments/<name>/
  config/
    experiment.yaml
    manifest.json
  generation/
    logs/
  data/
  engine/
    results.jsonl
    logs/
  study/
```

`config/experiment.yaml` is the reproducible copy of the input configuration.
`manifest.json` records paths, timestamp, number of instances/results and the status
of the `generation`, `engine` and `study` phases.

Typical manifest excerpt:

```json
{
  "name": "max2sat_density_sweep",
  "phases": {
    "generation": {"status": "ok", "instances": 90},
    "engine": {"status": "ok", "exit_code": 0, "results": 180},
    "study": {"status": "ok", "reports": ["study/analysis.txt"]}
  }
}
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--name NAME` | *(required)* | Folder name under `--root`. Accepts letters, numbers, `.`, `_` and `-`; does not accept path separators. |
| `--config PATH` | *(required)* | Declarative YAML for the experiment. |
| `--root PATH` | `experiments` | Root directory where experiments are created. |
| `--overwrite` | `False` | Deletes and recreates `experiments/<name>` if it already exists. |

If the engine returns algorithm errors, timeouts or process errors, the command
returns `1` and does not run the `study` phase. Configuration or name errors return `2`.

### Experiment YAML

```yaml
generation:
  n_files: 30
  vars: "20:100:20"
  densities: "2.0,2.5,3.0"
  k: 2
  jobs: -1
  seed_base: 42

engine:
  algorithms:
    - maxsat_brute
    - maxsat_qubo_sa:num_reads=100
  pattern: "*.cnf"
  recursive: true
  sort_files: false
  n_jobs: -1
  timeout: 30
  seed: 42
  log_level: INFO

study:
  alpha: 0.05
  log_level: INFO
  measures:
    - path: result.satisfaction_ratio
      label: Satisfaction ratio
      higher_is_better: true
      aggregation: mean
    - path: run_duration_s
      label: Algorithm duration
      higher_is_better: false
      aggregation: mean
```

### Example

```bash
alglab experiment run --name max2sat_density_sweep --config experiment.yaml
```

Recreate an existing experiment directory:

```bash
alglab experiment run \
    --name max2sat_density_sweep \
    --config experiment.yaml \
    --overwrite
```

Use a separate root for scratch experiments:

```bash
alglab experiment run \
    --name smoke_test \
    --config experiment.yaml \
    --root /tmp/alglab-experiments
```

"""Bulk generator for random k-CNF instances in DIMACS CNF format.

Instances are organised into per-(vars, density) subdirectories under a
common output root.  Each file receives a deterministic seed derived from
`seed_base` so that regenerating with the same arguments always produces
identical files.

Usage as module:
    python -m alglab.algorithms.max2sat.generator --n-files 5 --vars 100 \
        --densities 1.0,1.5,2.0 --k 2 --jobs -1 --out data/max2sat --seed-base 42

Public API:
    `generate_instances` — high-level parallel generator.
    `build_jobs` / `generate_job` — lower-level building blocks.
    `parse_int_spec` / `parse_float_spec` — CLI string parsing helpers.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import shutil
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from loguru import logger
from pebble import ProcessPool


@dataclass(frozen=True)
class GenerationJob:
    """Immutable specification for a single CNF file to be generated.

    Frozen so instances are safely hashable and can be sent across
    process boundaries without defensive copying.

    Attributes:
        num_vars: Number of boolean variables in the formula.
        density: Clause-to-variable ratio (num_clauses / num_vars).
        num_clauses: Exact clause count, derived as round(num_vars * density).
        k: Literals per clause.
        file_index: 1-based position within the (vars, density) group.
        seed: Fully-derived integer seed for the RNG of this file.
        output_path: Absolute destination path for the DIMACS file.
    """

    num_vars: int
    density: float
    num_clauses: int
    k: int
    file_index: int
    seed: int
    output_path: Path


def generate_kcnf_formula(
    num_vars: int,
    num_clauses: int,
    k: int,
    rng: random.Random,
) -> list[list[int]]:
    """Generate a random k-CNF formula that covers every variable at least once.

    Each clause contains exactly `k` distinct variables (chosen uniformly at
    random) with each literal negated independently with probability 0.5.
    A first pass distributes every variable into a random clause so none is
    left uncovered; remaining literal slots are filled with uniform draws.

    Args:
        num_vars: Total number of boolean variables (1-indexed in output).
        num_clauses: Number of clauses to produce.
        k: Literals per clause; must satisfy k <= num_vars and
            num_clauses * k >= num_vars.
        rng: Seeded random source; controls all randomness so results are
            reproducible given a fixed seed.

    Returns:
        List of clauses, each a list of non-zero integers where a negative
        value represents a negated literal (DIMACS convention).

    Raises:
        ValueError: If the parameters are out of range or coverage is
            impossible with the given clause budget.
    """
    if num_vars < 1 or num_clauses < 1 or k < 1:
        raise ValueError("num_vars, num_clauses, and k must all be >= 1.")
    if k > num_vars:
        raise ValueError(f"k={k} cannot be greater than num_vars={num_vars}.")
    if num_clauses * k < num_vars:
        raise ValueError(
            f"Cannot cover {num_vars} variables with {num_clauses}×{k}={num_clauses * k} positions."
        )

    clause_sets: list[set[int]] = [set() for _ in range(num_clauses)]
    capacity = [k] * num_clauses

    pending = list(range(1, num_vars + 1))
    rng.shuffle(pending)

    for var in pending:
        while True:
            idx = rng.randrange(num_clauses)
            if capacity[idx] > 0:
                clause_sets[idx].add(var)
                capacity[idx] -= 1
                break

    for i in range(num_clauses):
        s = clause_sets[i]
        while len(s) < k:
            s.add(rng.randint(1, num_vars))

    clauses: list[list[int]] = []
    for vars_set in clause_sets:
        vars_list = list(vars_set)
        rng.shuffle(vars_list)
        clause = [v if rng.random() < 0.5 else -v for v in vars_list]
        clauses.append(clause)

    rng.shuffle(clauses)
    return clauses


def formula_to_dimacs(
    clauses: list[list[int]],
    num_vars: int,
    comment: str = "",
    *,
    seed: int | None = None,
    density: float | None = None,
    k: int | None = None,
    file_index: int | None = None,
) -> str:
    """Serialise a clause list to a DIMACS CNF string.

    Emits `c` comment lines for every metadata argument that is not ``None``,
    followed by the standard `p cnf` problem line and one clause per line
    (space-separated literals terminated by `0`).

    Args:
        clauses: Formula as returned by `generate_kcnf_formula`.
        num_vars: Variable count written into the `p cnf` header.
        comment: Optional free-text comment added as the first `c` line.
        seed: If given, written as `c seed=<seed>`.
        density: If given, written as `c density=<density>`.
        k: If given, written as `c k=<k>`.
        file_index: If given, written as `c file_index=<file_index>`.

    Returns:
        Complete DIMACS CNF string, newline-terminated.
    """
    header = []
    if comment:
        header.append(f"c {comment}")
    if seed is not None:
        header.append(f"c seed={seed}")
    header.append(f"c vars={num_vars}")
    header.append(f"c clauses={len(clauses)}")
    if density is not None:
        header.append(f"c density={format_density(density)}")
    if k is not None:
        header.append(f"c k={k}")
    if file_index is not None:
        header.append(f"c file_index={file_index}")
    header.append(f"p cnf {num_vars} {len(clauses)}")
    clause_lines = (" ".join(map(str, c)) + " 0" for c in clauses)
    return "\n".join([*header, *clause_lines]) + "\n"


def parse_int_spec(spec: str) -> list[int]:
    """Parse a compact integer specification into a list of positive integers.

    Accepted formats:

    - ``"N"`` — single value.
    - ``"start:stop:step"`` — inclusive range, all three parts required.
    - ``"a,b,c"`` — explicit comma-separated list.

    Args:
        spec: Non-empty string in one of the formats above.

    Returns:
        Ordered list of positive integers.

    Raises:
        ValueError: If `spec` is empty, malformed, or contains non-positive values.
    """
    if spec == "":
        raise ValueError("Integer specification cannot be empty.")
    if "," in spec:
        parts = spec.split(",")
        if any(part == "" for part in parts):
            raise ValueError(f"Invalid integer CSV: {spec!r}")
        return [_parse_positive_int(part) for part in parts]
    if ":" in spec:
        parts = spec.split(":")
        if len(parts) != 3 or any(part == "" for part in parts):
            raise ValueError(f"Invalid integer range: {spec!r}")
        start, stop, step = (_parse_positive_int(part) for part in parts)
        if step <= 0:
            raise ValueError("Integer range step must be > 0.")
        if start > stop:
            raise ValueError("Integer range start cannot be greater than end.")
        return list(range(start, stop + 1, step))
    return [_parse_positive_int(spec)]


def parse_float_spec(spec: str) -> list[float]:
    """Parse a compact float specification into a list of positive floats.

    Accepted formats mirror `parse_int_spec`:

    - ``"N"`` — single value.
    - ``"start:stop:step"`` — inclusive range walked with `Decimal` arithmetic
      to avoid floating-point drift.
    - ``"a,b,c"`` — explicit comma-separated list.

    Args:
        spec: Non-empty string in one of the formats above.

    Returns:
        Ordered list of positive floats.

    Raises:
        ValueError: If `spec` is empty, malformed, or contains non-positive values.
    """
    if spec == "":
        raise ValueError("Float specification cannot be empty.")
    if "," in spec:
        parts = spec.split(",")
        if any(part == "" for part in parts):
            raise ValueError(f"Invalid float CSV: {spec!r}")
        return [float(_parse_positive_decimal(part)) for part in parts]
    if ":" in spec:
        parts = spec.split(":")
        if len(parts) != 3 or any(part == "" for part in parts):
            raise ValueError(f"Invalid float range: {spec!r}")
        start, stop, step = (_parse_positive_decimal(part) for part in parts)
        if step <= 0:
            raise ValueError("Float range step must be > 0.")
        if start > stop:
            raise ValueError("Float range start cannot be greater than end.")
        values: list[float] = []
        current = start
        while current <= stop:
            values.append(float(current))
            current += step
        return values
    return [float(_parse_positive_decimal(spec))]


def format_vars_padded(num_vars: int) -> str:
    """Zero-pad a variable count to five digits for use in directory names.

    Consistent width keeps directory listings sorted lexicographically by
    variable count without extra tooling.

    Args:
        num_vars: Variable count to format.

    Returns:
        Five-character zero-padded decimal string, e.g. ``"00100"`` for 100.
    """
    return f"{num_vars:05d}"


def format_density(density: float) -> str:
    """Format a density value as the shortest unambiguous decimal string.

    Trailing zeros and redundant decimal points are stripped, but at least one
    digit after the point is kept when the value is a whole number (e.g.
    ``2.0`` rather than ``"2"``).  This canonical form is used in directory
    names and DIMACS comment headers.

    Args:
        density: Positive float to format.

    Returns:
        Minimal decimal string, e.g. ``"2.5"`` for 2.50 or ``"3.0"`` for 3.
    """
    decimal = Decimal(str(density)).normalize()
    text = format(decimal, "f")
    if "." not in text:
        return f"{text}.0"
    return text.rstrip("0").rstrip(".")


def stable_seed(
    seed_base: int,
    num_vars: int,
    num_clauses: int,
    density: float,
    k: int,
    file_index: int,
) -> int:
    """Compute a deterministic 64-bit seed for one (vars, density, index) triple.

    All arguments are encoded as a pipe-delimited UTF-8 string and hashed with
    MD5 (non-security use).  The first eight bytes of the digest are
    interpreted as a big-endian unsigned integer.  This ensures that changing
    any single parameter produces an independent seed while remaining
    reproducible across platforms.

    Args:
        seed_base: User-supplied base seed that namespaces the whole run.
        num_vars: Variable count of the target instance.
        num_clauses: Clause count of the target instance.
        density: Clause-to-variable ratio (used in its canonical string form).
        k: Literals per clause.
        file_index: 1-based position within the (vars, density) group.

    Returns:
        Non-negative integer suitable for seeding `random.Random`.
    """
    payload = "|".join(
        [
            str(seed_base),
            str(num_vars),
            str(num_clauses),
            format_density(density),
            str(k),
            str(file_index),
        ]
    )
    digest = hashlib.md5(payload.encode("utf-8"), usedforsecurity=False).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


def resolve_jobs(jobs: int) -> int:
    """Resolve the worker count, expanding ``-1`` to the logical CPU count.

    Args:
        jobs: Desired parallelism.  ``-1`` means "use all available CPUs".

    Returns:
        Concrete positive worker count.

    Raises:
        ValueError: If `jobs` is 0 or any negative value other than ``-1``.
    """
    if jobs == -1:
        return os.cpu_count() or 1
    if jobs < 1:
        raise ValueError("--jobs must be -1 or a positive integer.")
    return jobs


def build_jobs(
    *,
    vars_values: list[int],
    densities: list[float],
    n_files: int,
    k: int,
    out: Path,
    seed_base: int,
) -> list[GenerationJob]:
    """Build the full list of `GenerationJob` objects for a generation run.

    Iterates the Cartesian product of `vars_values` × `densities` ×
    ``range(1, n_files + 1)``, computing `num_clauses` and a stable seed for
    each combination, and assembles the corresponding output paths under `out`.

    Args:
        vars_values: Variable counts to generate instances for.
        densities: Clause-to-variable ratios to generate instances for.
        n_files: Number of independent instances per (vars, density) pair.
        k: Literals per clause.
        out: Root output directory; subdirectories are created per combination.
        seed_base: Base seed forwarded to `stable_seed`.

    Returns:
        Flat list of `GenerationJob` instances in (vars, density, file_index)
        order.

    Raises:
        ValueError: If any parameter is out of range or the (vars, density, k)
            combination cannot guarantee full variable coverage.
    """
    _validate_generation_inputs(vars_values, densities, n_files, k)

    jobs: list[GenerationJob] = []
    for num_vars in vars_values:
        for density in densities:
            density_label = format_density(density)
            num_clauses = round(num_vars * density)
            _validate_combination(num_vars, density, num_clauses, k)
            output_dir = out / f"vars_{format_vars_padded(num_vars)}_density_{density_label}"
            for file_index in range(1, n_files + 1):
                seed = stable_seed(seed_base, num_vars, num_clauses, density, k, file_index)
                jobs.append(
                    GenerationJob(
                        num_vars=num_vars,
                        density=density,
                        num_clauses=num_clauses,
                        k=k,
                        file_index=file_index,
                        seed=seed,
                        output_path=output_dir / f"instance_{file_index:04d}.cnf",
                    )
                )
    return jobs


def generate_job(job: GenerationJob) -> Path:
    """Execute one `GenerationJob`: generate a formula and write it to disk.

    Designed to be called in a subprocess by `generate_instances`; all
    required data is self-contained in `job` so no shared state is needed.

    Args:
        job: Fully-specified generation task.

    Returns:
        Path of the written DIMACS file (same as ``job.output_path``).
    """
    rng = random.Random(job.seed)
    clauses = generate_kcnf_formula(
        num_vars=job.num_vars,
        num_clauses=job.num_clauses,
        k=job.k,
        rng=rng,
    )
    content = formula_to_dimacs(
        clauses,
        job.num_vars,
        seed=job.seed,
        density=job.density,
        k=job.k,
        file_index=job.file_index,
    )
    job.output_path.parent.mkdir(parents=True, exist_ok=True)
    job.output_path.write_text(content, encoding="utf-8")
    return job.output_path


def generate_instances(
    *,
    n_files: int,
    vars_values: list[int],
    densities: list[float],
    k: int,
    out: Path,
    seed_base: int = 0,
    jobs: int = 1,
    clean: bool = False,
    dry_run: bool = False,
) -> list[Path]:
    """Orchestrate parallel generation of all CNF instances for a run.

    Builds the full job list via `build_jobs`, then dispatches work either
    serially (``jobs == 1``) or through a `pebble.ProcessPool`.  Progress is
    logged every 5 % of total files.

    Args:
        n_files: Instances per (vars, density) combination.
        vars_values: Variable counts to sweep.
        densities: Clause-to-variable ratios to sweep.
        k: Literals per clause.
        out: Root output directory.
        seed_base: Base seed for `stable_seed`; defaults to ``0``.
        jobs: Worker count; ``-1`` expands to CPU count.
        clean: If ``True``, deletes `out` before generating.
        dry_run: If ``True``, prints the planned jobs and returns immediately
            without writing any files.

    Returns:
        List of paths of all successfully written DIMACS files, in job order.
        Empty list when `dry_run` is ``True``.

    Raises:
        RuntimeError: If any subprocess job raises an exception (wraps the
            original with combo context for easier diagnosis).
        ValueError: Propagated from `build_jobs` or `resolve_jobs`.
    """
    resolved_jobs = resolve_jobs(jobs)
    generation_jobs = build_jobs(
        vars_values=vars_values,
        densities=densities,
        n_files=n_files,
        k=k,
        out=out,
        seed_base=seed_base,
    )
    _validate_out(out)

    if dry_run:
        _print_dry_run(generation_jobs, resolved_jobs)
        return []

    if clean and out.exists():
        shutil.rmtree(out)

    total = len(generation_jobs)
    logger.info("Generating {} CNF instances with {} worker(s) into {}", total, resolved_jobs, out)
    progress_every = max(1, total // 20)

    if resolved_jobs == 1:
        serial_paths = []
        for idx, job in enumerate(generation_jobs, start=1):
            serial_paths.append(generate_job(job))
            if idx == total or idx % progress_every == 0:
                logger.info("Generation progress: {}/{} files", idx, total)
        return serial_paths

    paths: list[Path] = []
    with ProcessPool(max_workers=resolved_jobs) as pool:
        futures = [(job, pool.schedule(generate_job, args=(job,))) for job in generation_jobs]
        for idx, (job, future) in enumerate(futures, start=1):
            try:
                paths.append(future.result())
            except Exception as exc:
                raise RuntimeError(f"{_combo_label(job)}: {exc}") from exc
            if idx == total or idx % progress_every == 0:
                logger.info("Generation progress: {}/{} files", idx, total)
    return paths


def _parse_positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid integer: {value!r}") from exc
    if parsed < 1:
        raise ValueError(f"Integer must be >= 1: {value!r}")
    return parsed


def _parse_positive_decimal(value: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid float: {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"Invalid float: {value!r}")
    if parsed <= 0:
        raise ValueError(f"Float must be > 0: {value!r}")
    return parsed


def _validate_generation_inputs(
    vars_values: list[int],
    densities: list[float],
    n_files: int,
    k: int,
) -> None:
    if n_files < 1:
        raise ValueError("--n-files must be >= 1.")
    if k < 1:
        raise ValueError("--k must be >= 1.")
    if not vars_values:
        raise ValueError("--vars cannot be empty.")
    if not densities:
        raise ValueError("--densities cannot be empty.")
    for num_vars in vars_values:
        if num_vars < 1:
            raise ValueError("--vars must contain only values >= 1.")
        if k > num_vars:
            raise ValueError(f"k={k} cannot be greater than vars={num_vars}.")
    for density in densities:
        if density <= 0:
            raise ValueError("--densities must contain only values > 0.")


def _validate_combination(num_vars: int, density: float, num_clauses: int, k: int) -> None:
    if num_clauses * k < num_vars:
        raise ValueError(
            f"Invalid combination: round({num_vars} * {format_density(density)}) * {k} "
            f"= {num_clauses * k} does not cover {num_vars} variables."
        )


def _validate_out(out: Path) -> None:
    if out.exists() and not out.is_dir():
        raise ValueError(f"--out exists and is not a directory: {out}")


def _combo_label(job: GenerationJob) -> str:
    return f"vars={job.num_vars} density={format_density(job.density)} clauses={job.num_clauses}"


def _print_dry_run(jobs: list[GenerationJob], resolved_jobs: int) -> None:
    print(f"DRY-RUN jobs={len(jobs)} workers={resolved_jobs}")
    for job in jobs:
        print(
            "JOB "
            f"path={job.output_path} "
            f"seed={job.seed} "
            f"vars={job.num_vars} "
            f"density={format_density(job.density)} "
            f"clauses={job.num_clauses} "
            f"k={job.k} "
            f"file_index={job.file_index}"
        )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bulk k-CNF generator by density")
    p.add_argument("--vars", required=True, dest="vars_spec")
    p.add_argument("--densities", required=True, dest="densities_spec")
    p.add_argument("--n-files", type=int, required=True)
    p.add_argument("--k", type=int, required=True)
    p.add_argument("--jobs", type=int, default=1)
    p.add_argument("--out", type=Path, default=Path("data/max2sat"))
    p.add_argument("--seed-base", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--clean", action="store_true")
    return p.parse_args()


def main() -> int:
    """CLI entry point for the bulk k-CNF generator.

    Parses command-line arguments, validates inputs, and delegates to
    :func:`generate_instances`.  Per-combination progress is printed to stdout
    on success.

    Returns:
        ``0`` on success, ``1`` if any argument is invalid or generation fails.
    """
    args = _parse_args()
    try:
        vars_values = parse_int_spec(args.vars_spec)
        densities = parse_float_spec(args.densities_spec)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    try:
        generated = generate_instances(
            n_files=args.n_files,
            vars_values=vars_values,
            densities=densities,
            k=args.k,
            out=args.out,
            seed_base=args.seed_base,
            jobs=args.jobs,
            clean=args.clean,
            dry_run=args.dry_run,
        )
    except (ValueError, RuntimeError) as exc:
        print(f"Error: {exc}")
        return 1

    if not args.dry_run:
        generation_jobs = build_jobs(
            vars_values=vars_values,
            densities=densities,
            n_files=args.n_files,
            k=args.k,
            out=args.out,
            seed_base=args.seed_base,
        )
        _print_progress(generation_jobs, generated, [])
    return 0


def _print_progress(
    jobs: list[GenerationJob],
    generated: list[Path],
    errors: list[tuple[GenerationJob, str]],
) -> None:
    expected_by_combo: dict[tuple[int, str, int], int] = defaultdict(int)
    ok_by_combo: dict[tuple[int, str, int], int] = defaultdict(int)
    error_by_combo: dict[tuple[int, str, int], list[str]] = defaultdict(list)
    generated_set = set(generated)

    for job in jobs:
        key = (job.num_vars, format_density(job.density), job.num_clauses)
        expected_by_combo[key] += 1
        if job.output_path in generated_set:
            ok_by_combo[key] += 1
    for job, message in errors:
        key = (job.num_vars, format_density(job.density), job.num_clauses)
        error_by_combo[key].append(message)

    for key in expected_by_combo:
        num_vars, density, num_clauses = key
        if error_by_combo[key]:
            print(
                f"ERR vars={num_vars} density={density} clauses={num_clauses} "
                f"files={ok_by_combo[key]}/{expected_by_combo[key]} "
                f"({'; '.join(error_by_combo[key])})"
            )
        else:
            print(
                f"OK  vars={num_vars} density={density} clauses={num_clauses} "
                f"files={ok_by_combo[key]}"
            )


if __name__ == "__main__":
    raise SystemExit(main())

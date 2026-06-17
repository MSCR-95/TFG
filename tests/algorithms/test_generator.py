from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from alglab.algorithms.max2sat.generator import (
    build_jobs,
    format_density,
    formula_to_dimacs,
    parse_float_spec,
    parse_int_spec,
    resolve_jobs,
    stable_seed,
)
from alglab.algorithms.max2sat.parser import _parse_dimacs


def test_parse_int_spec_single_csv_and_range() -> None:
    assert parse_int_spec("100") == [100]
    assert parse_int_spec("100,250,500") == [100, 250, 500]
    assert parse_int_spec("100:300:100") == [100, 200, 300]


@pytest.mark.parametrize("spec", ["", "1,,2", "1:", "1:2", "1:2:0", "3:1:1", "x", "0", "-1"])
def test_parse_int_spec_rejects_invalid_specs(spec: str) -> None:
    with pytest.raises(ValueError):
        parse_int_spec(spec)


def test_parse_float_spec_single_csv_and_range() -> None:
    assert parse_float_spec("1.0") == [1.0]
    assert parse_float_spec("1.0,1.5,2.25") == [1.0, 1.5, 2.25]
    assert parse_float_spec("1.0:2.0:0.5") == [1.0, 1.5, 2.0]


@pytest.mark.parametrize(
    "spec",
    ["", "1.0,,2.0", "1:", "1:2", "1:2:0", "3:1:1", "x", "0", "-1", "nan", "inf"],
)
def test_parse_float_spec_rejects_invalid_specs(spec: str) -> None:
    with pytest.raises(ValueError):
        parse_float_spec(spec)


def test_resolve_jobs_minus_one_uses_cpu_count(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(os, "cpu_count", lambda: 7)
    assert resolve_jobs(-1) == 7

    monkeypatch.setattr(os, "cpu_count", lambda: None)
    assert resolve_jobs(-1) == 1


@pytest.mark.parametrize("jobs", [0, -2])
def test_resolve_jobs_rejects_invalid_values(jobs: int) -> None:
    with pytest.raises(ValueError):
        resolve_jobs(jobs)


def test_stable_seed_is_deterministic_and_sensitive_to_inputs() -> None:
    first = stable_seed(42, 100, 150, 1.5, 2, 1)
    assert stable_seed(42, 100, 150, 1.5, 2, 1) == first
    assert stable_seed(43, 100, 150, 1.5, 2, 1) != first
    assert stable_seed(42, 100, 150, 1.5, 2, 2) != first


def test_build_jobs_keeps_existing_seeds_when_n_files_grows(tmp_path: Path) -> None:
    jobs_small = build_jobs(
        vars_values=[10],
        densities=[1.0],
        n_files=2,
        k=2,
        out=tmp_path,
        seed_base=99,
    )
    jobs_large = build_jobs(
        vars_values=[10],
        densities=[1.0],
        n_files=4,
        k=2,
        out=tmp_path,
        seed_base=99,
    )

    assert [job.seed for job in jobs_large[:2]] == [job.seed for job in jobs_small]
    assert jobs_large[0].output_path == tmp_path / "vars_00010_density_1.0" / "instance_0001.cnf"


@pytest.mark.parametrize(
    ("density", "expected"),
    [(1.0, "1.0"), (1.5, "1.5"), (2.25, "2.25"), (1.500000, "1.5")],
)
def test_format_density_removes_unneeded_zeroes(density: float, expected: str) -> None:
    assert format_density(density) == expected


def test_build_jobs_rejects_uncoverable_formula(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="does not cover"):
        build_jobs(
            vars_values=[10],
            densities=[0.4],
            n_files=1,
            k=2,
            out=tmp_path,
            seed_base=0,
        )


def test_formula_to_dimacs_includes_metadata_and_parser_accepts_it() -> None:
    text = formula_to_dimacs(
        [[1, -2], [-1, 2]],
        2,
        seed=123,
        density=1.0,
        k=2,
        file_index=1,
    )

    assert "c seed=123\n" in text
    assert "c vars=2\n" in text
    assert "c clauses=2\n" in text
    assert "c density=1.0\n" in text
    assert "c k=2\n" in text
    assert "c file_index=1\n" in text
    assert _parse_dimacs(text) == (2, [[1, -2], [-1, 2]])


def test_dry_run_does_not_create_output(tmp_path: Path) -> None:
    out = tmp_path / "generated"
    result = _run_generator(
        tmp_path,
        "--vars",
        "4",
        "--densities",
        "1.0",
        "--n-files",
        "1",
        "--k",
        "2",
        "--jobs",
        "1",
        "--out",
        str(out),
        "--seed-base",
        "7",
        "--dry-run",
    )

    assert result.returncode == 0
    assert "DRY-RUN jobs=1 workers=1" in result.stdout
    assert "JOB path=" in result.stdout
    assert not out.exists()


def test_generation_is_identical_across_worker_counts(tmp_path: Path) -> None:
    out_one = tmp_path / "one"
    out_two = tmp_path / "two"
    out_all = tmp_path / "all"

    for out, workers in [(out_one, "1"), (out_two, "2"), (out_all, "-1")]:
        result = _run_generator(
            tmp_path,
            "--vars",
            "6",
            "--densities",
            "1.0,1.5",
            "--n-files",
            "2",
            "--k",
            "2",
            "--jobs",
            workers,
            "--out",
            str(out),
            "--seed-base",
            "11",
        )
        assert result.returncode == 0, result.stderr + result.stdout
        assert "OK  vars=6 density=1.0 clauses=6 files=2" in result.stdout
        assert "OK  vars=6 density=1.5 clauses=9 files=2" in result.stdout

    assert _snapshot(out_one) == _snapshot(out_two) == _snapshot(out_all)


def test_cli_rejects_existing_out_file(tmp_path: Path) -> None:
    out = tmp_path / "not-a-dir"
    out.write_text("", encoding="utf-8")

    result = _run_generator(
        tmp_path,
        "--vars",
        "4",
        "--densities",
        "1.0",
        "--n-files",
        "1",
        "--k",
        "2",
        "--out",
        str(out),
    )

    assert result.returncode == 1
    assert "exists and is not a directory" in result.stdout


def _run_generator(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alglab.algorithms.max2sat.generator", *args],
        check=False,
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )


def _snapshot(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(root.rglob("*.cnf"))
    }

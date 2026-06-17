"""
Tests for Runner.run_stream and iter_directory_jobs.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from alglab.engine import Runner, iter_directory_jobs
from alglab.engine.core import Algorithm, Job
from alglab.engine.registry import register_algorithm

# ---------------------------------------------------------------------------
# Test algorithms (module-level for fork/pickle compatibility)
# ---------------------------------------------------------------------------


@register_algorithm("_rtest_obj")
class _ObjAlgo(Algorithm):
    output_keys = {"cost": "arbitrary cost metric"}

    def run(self, context: Path, *, seed: int | None = None) -> dict[str, Any]:
        return {"cost": 7.0}


@register_algorithm("_rtest_seed_aware")
class _SeedAwareAlgo(Algorithm):
    def run(self, context: Path, *, seed: int | None = None) -> dict[str, Any]:
        return {"seed_received": seed}


@register_algorithm("_rtest_seed_unaware")
class _SeedUnawareAlgo(Algorithm):
    def run(self, context: Path, *, seed: int | None = None) -> dict[str, Any]:
        return {"ok": True}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def f(tmp_path: Path) -> Path:
    p = tmp_path / "sample.txt"
    p.write_text("hello world\n")
    return p


@pytest.fixture()
def dir_with_files(tmp_path: Path) -> Path:
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "b.txt").write_text("b")
    (tmp_path / "c.txt").write_text("c")
    (tmp_path / "d.dat").write_text("d")
    return tmp_path


# ===========================================================================
# iter_directory_jobs
# ===========================================================================


class TestIterDirectoryJobs:
    def test_yields_all_files_by_default(self, dir_with_files):
        jobs = list(iter_directory_jobs(dir_with_files, algorithms=["_rtest_obj"]))
        assert len(jobs) == 4

    def test_pattern_filters_files(self, dir_with_files):
        jobs = list(iter_directory_jobs(dir_with_files, algorithms=["_rtest_obj"], pattern="*.txt"))
        assert len(jobs) == 3

    def test_multiple_algorithms_cross_product(self, dir_with_files):
        jobs = list(
            iter_directory_jobs(
                dir_with_files, algorithms=["_rtest_obj", "_rtest_seed_unaware"], pattern="*.txt"
            )
        )
        assert len(jobs) == 6

    def test_recursive_finds_nested(self, tmp_path):
        (tmp_path / "sub").mkdir()
        (tmp_path / "root.txt").write_text("r")
        (tmp_path / "sub" / "nested.txt").write_text("n")
        jobs = list(
            iter_directory_jobs(
                tmp_path, algorithms=["_rtest_obj"], pattern="*.txt", recursive=True
            )
        )
        assert len(jobs) == 2

    def test_non_recursive_skips_subdirs(self, tmp_path):
        (tmp_path / "sub").mkdir()
        (tmp_path / "root.txt").write_text("r")
        (tmp_path / "sub" / "nested.txt").write_text("n")
        jobs = list(
            iter_directory_jobs(
                tmp_path, algorithms=["_rtest_obj"], pattern="*.txt", recursive=False
            )
        )
        assert len(jobs) == 1

    def test_no_matching_files_yields_nothing(self, dir_with_files):
        jobs = list(iter_directory_jobs(dir_with_files, algorithms=["_rtest_obj"], pattern="*.xyz"))
        assert jobs == []

    def test_files_sorted_deterministically(self, dir_with_files):
        jobs = list(
            iter_directory_jobs(
                dir_with_files,
                algorithms=["_rtest_obj"],
                pattern="*.txt",
                sort_files=True,
            )
        )
        paths = [j.file_path for j in jobs]
        assert paths == sorted(paths)

    def test_files_stream_in_glob_order_by_default(self):
        class FakeFile:
            def __init__(self, name: str) -> None:
                self.name = name

            def is_file(self) -> bool:
                return True

        class FakeDirectory:
            def __init__(self) -> None:
                self.files = [FakeFile("b.txt"), FakeFile("a.txt")]

            def glob(self, pattern: str):
                assert pattern == "*.txt"
                return iter(self.files)

        directory = FakeDirectory()
        jobs = list(iter_directory_jobs(directory, algorithms=["_rtest_obj"], pattern="*.txt"))
        assert [job.file_path.name for job in jobs] == ["b.txt", "a.txt"]

    def test_yields_job_objects(self, dir_with_files):
        jobs = list(iter_directory_jobs(dir_with_files, algorithms=["_rtest_obj"], pattern="a.txt"))
        assert len(jobs) == 1
        assert isinstance(jobs[0], Job)
        assert jobs[0].algorithm == "_rtest_obj"

    def test_unknown_algorithm_raises(self, dir_with_files):
        with pytest.raises(KeyError):
            iter_directory_jobs(dir_with_files, algorithms=["no_existe_xyz"])

    def test_seed_defaults_to_none(self, dir_with_files):
        jobs = list(iter_directory_jobs(dir_with_files, algorithms=["_rtest_obj"], pattern="a.txt"))
        assert jobs[0].seed is None


# ===========================================================================
# Runner.run_stream — basic behavior
# ===========================================================================


class TestRunStream:
    def test_all_jobs_produce_a_result(self, dir_with_files):
        runner = Runner(max_workers=2)
        jobs = iter_directory_jobs(dir_with_files, algorithms=["_rtest_obj"], pattern="*.txt")
        results = list(runner.run_stream(jobs))
        assert len(results) == 3

    def test_empty_jobs_yields_nothing(self):
        runner = Runner()
        results = list(runner.run_stream([]))
        assert results == []

    def test_successful_result_status_ok(self, f):
        runner = Runner()
        results = list(runner.run_stream([Job(file_path=f, algorithm="_rtest_obj")]))
        assert results[0].status == "ok"

    def test_result_payload_stored(self, f):
        runner = Runner()
        r = list(runner.run_stream([Job(file_path=f, algorithm="_rtest_obj")]))[0]
        assert "cost" in r.result

    def test_result_fields_populated(self, f):
        runner = Runner()
        r = list(runner.run_stream([Job(file_path=f, algorithm="_rtest_obj")]))[0]
        assert r.file == str(f)
        assert r.algorithm == "_rtest_obj"
        assert r.pre_run_duration_s is not None
        assert r.run_duration_s is not None
        assert r.post_run_duration_s is not None

    def test_runner_reusable(self, dir_with_files):
        runner = Runner(max_workers=2)
        jobs1 = iter_directory_jobs(dir_with_files, algorithms=["_rtest_obj"], pattern="*.txt")
        jobs2 = iter_directory_jobs(
            dir_with_files, algorithms=["_rtest_seed_unaware"], pattern="*.txt"
        )
        r1 = list(runner.run_stream(jobs1))
        r2 = list(runner.run_stream(jobs2))
        assert len(r1) == 3
        assert len(r2) == 3


# ===========================================================================
# Concurrency
# ===========================================================================


class TestConcurrency:
    def test_multiple_workers_complete_all_jobs(self, tmp_path):
        for i in range(8):
            (tmp_path / f"f{i}.txt").write_text(f"file {i}")

        runner = Runner(max_workers=4)
        jobs = iter_directory_jobs(tmp_path, algorithms=["_rtest_obj"])
        results = list(runner.run_stream(jobs))

        assert len(results) == 8
        assert all(r.status == "ok" for r in results)


# ===========================================================================
# Seed
# ===========================================================================


class TestSeed:
    def test_seed_stored_in_result(self, f):
        runner = Runner()
        r = list(runner.run_stream([Job(file_path=f, algorithm="_rtest_obj", seed=5)]))[0]
        assert r.seed == 5

    def test_none_seed_stored_in_result(self, f):
        runner = Runner()
        r = list(runner.run_stream([Job(file_path=f, algorithm="_rtest_obj")]))[0]
        assert r.seed is None

    def test_seed_propagated_to_algorithm(self, f):
        runner = Runner()
        r = list(runner.run_stream([Job(file_path=f, algorithm="_rtest_seed_aware", seed=42)]))[0]
        assert r.result["seed_received"] == 42

    def test_no_error_for_seed_unaware_algorithm(self, f):
        runner = Runner()
        r = list(runner.run_stream([Job(file_path=f, algorithm="_rtest_seed_unaware", seed=42)]))[0]
        assert r.status == "ok"

    def test_seed_via_replace(self, dir_with_files):
        runner = Runner()
        base_jobs = iter_directory_jobs(dir_with_files, algorithms=["_rtest_obj"], pattern="*.txt")
        jobs = [replace(j, seed=99) for j in base_jobs]
        results = list(runner.run_stream(jobs))
        assert all(r.seed == 99 for r in results)


# ===========================================================================
# Sliding window under load
# ===========================================================================


class TestSlidingWindow:
    def test_high_volume_jobs_complete_without_error(self, tmp_path):
        n = 200
        for i in range(n):
            (tmp_path / f"f{i:04d}.txt").write_text(str(i))
        runner = Runner(max_workers=4)
        jobs = iter_directory_jobs(tmp_path, algorithms=["_rtest_obj"])
        results = list(runner.run_stream(jobs))
        assert len(results) == n
        assert all(r.status == "ok" for r in results)

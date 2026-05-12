"""
Tests del comportamiento del RunnerV2: submit, submit_directory,
prioridades, backpressure, concurrencia, ResultRecord, edge cases.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from framework import RunnerV2, RetryPolicy, RETRY_ERROR


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _runner(**kwargs) -> RunnerV2:
    defaults = dict(max_workers=1, buffer_factor=1)
    defaults.update(kwargs)
    return RunnerV2(**defaults)


@pytest.fixture()
def f(tmp_path: Path) -> Path:
    p = tmp_path / "sample.txt"
    p.write_text("hola mundo\n")
    return p


@pytest.fixture()
def dir_with_files(tmp_path: Path) -> Path:
    """Directorio con 3 ficheros .txt y 1 .dat."""
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "b.txt").write_text("b")
    (tmp_path / "c.txt").write_text("c")
    (tmp_path / "d.dat").write_text("d")
    return tmp_path


# ===========================================================================
# submit
# ===========================================================================

class TestSubmit:
    def test_returns_job_id(self, f):
        runner = _runner()
        jid = runner.submit(job_id="myjob", file_path=f, algorithm="word_count")
        assert jid == "myjob"

    def test_increments_submitted_jobs(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        runner.submit(job_id="j2", file_path=f, algorithm="word_count")
        assert runner.metrics.submitted_jobs == 2

    def test_accepts_algorithm_instance(self, f):
        from algorithms.prueba.word_count import WordCountAlgorithm
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm=WordCountAlgorithm())
        records = runner.run()
        assert records[0].error is False

    def test_accepts_algorithm_string(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        records = runner.run()
        assert records[0].error is False

    def test_unknown_algorithm_raises(self, f):
        runner = _runner()
        with pytest.raises(KeyError):
            runner.submit(job_id="j1", file_path=f, algorithm="no_existe_xyz")

    def test_max_attempts_from_retry_policy(self, f):
        policy = RetryPolicy(retries=3, retry_on=frozenset({RETRY_ERROR}))
        runner = _runner(retry_policy=policy)
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo")
        records = runner.run()
        assert all(r.max_attempts == 4 for r in records)

    def test_explicit_max_attempts_overrides_policy(self, f):
        policy = RetryPolicy(retries=5, retry_on=frozenset({RETRY_ERROR}))
        runner = _runner(retry_policy=policy)
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo", max_attempts=2)
        records = runner.run()
        assert all(r.max_attempts == 2 for r in records)
        assert len(records) == 2

    def test_timeout_overrides_default(self, f):
        runner = _runner(default_timeout=999.0)
        runner.submit(job_id="j1", file_path=f, algorithm="word_count", timeout=None)
        # timeout=None en submit → usa default_timeout
        records = runner.run()
        assert records[0].error is False


# ===========================================================================
# submit_directory
# ===========================================================================

class TestSubmitDirectory:
    def test_returns_job_ids(self, dir_with_files):
        runner = _runner()
        ids = runner.submit_directory(
            directory=dir_with_files, algorithms=["word_count"]
        )
        assert len(ids) == 4  # 3 txt + 1 dat
        assert all(isinstance(i, str) for i in ids)

    def test_pattern_filters_files(self, dir_with_files):
        runner = _runner()
        ids = runner.submit_directory(
            directory=dir_with_files, algorithms=["word_count"], pattern="*.txt"
        )
        assert len(ids) == 3

    def test_multiple_algorithms_creates_cross_product(self, dir_with_files):
        runner = _runner()
        ids = runner.submit_directory(
            directory=dir_with_files,
            algorithms=["word_count", "sha256"],
            pattern="*.txt",
        )
        assert len(ids) == 6  # 3 files × 2 algos

    def test_recursive_finds_nested_files(self, tmp_path):
        (tmp_path / "sub").mkdir()
        (tmp_path / "root.txt").write_text("r")
        (tmp_path / "sub" / "nested.txt").write_text("n")
        runner = _runner()
        ids = runner.submit_directory(
            directory=tmp_path,
            algorithms=["word_count"],
            pattern="*.txt",
            recursive=True,
        )
        assert len(ids) == 2

    def test_non_recursive_ignores_subdirs(self, tmp_path):
        (tmp_path / "sub").mkdir()
        (tmp_path / "root.txt").write_text("r")
        (tmp_path / "sub" / "nested.txt").write_text("n")
        runner = _runner()
        ids = runner.submit_directory(
            directory=tmp_path,
            algorithms=["word_count"],
            pattern="*.txt",
            recursive=False,
        )
        assert len(ids) == 1

    def test_no_matching_files_returns_empty(self, dir_with_files):
        runner = _runner()
        ids = runner.submit_directory(
            directory=dir_with_files, algorithms=["word_count"], pattern="*.xyz"
        )
        assert ids == []

    def test_no_matching_files_run_returns_empty(self, dir_with_files):
        runner = _runner()
        runner.submit_directory(
            directory=dir_with_files, algorithms=["word_count"], pattern="*.xyz"
        )
        assert runner.run() == []

    def test_job_id_contains_algo_and_path(self, dir_with_files):
        runner = _runner()
        ids = runner.submit_directory(
            directory=dir_with_files, algorithms=["word_count"], pattern="a.txt"
        )
        assert len(ids) == 1
        assert "word_count" in ids[0]
        assert "a.txt" in ids[0]

    def test_files_sorted_deterministically(self, dir_with_files):
        runner = _runner()
        ids = runner.submit_directory(
            directory=dir_with_files, algorithms=["word_count"], pattern="*.txt"
        )
        # Los job_ids deben reflejar orden lexicográfico de ficheros
        filenames = [Path(i.split(":", 1)[1]).name for i in ids]
        assert filenames == sorted(filenames)


# ===========================================================================
# run / run_stream
# ===========================================================================

class TestRun:
    def test_run_returns_list(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        result = runner.run()
        assert isinstance(result, list)

    def test_run_empty_returns_empty_list(self):
        runner = _runner()
        assert runner.run() == []

    def test_run_stream_is_generator(self, f):
        import types
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        gen = runner.run_stream()
        assert isinstance(gen, types.GeneratorType)

    def test_all_jobs_produce_a_record(self, dir_with_files):
        runner = _runner(max_workers=2)
        runner.submit_directory(
            directory=dir_with_files, algorithms=["word_count"], pattern="*.txt"
        )
        records = runner.run()
        assert len(records) == 3

    def test_completed_ok_counts_successes(self, dir_with_files):
        runner = _runner(max_workers=2)
        runner.submit_directory(
            directory=dir_with_files, algorithms=["word_count"], pattern="*.txt"
        )
        runner.run()
        assert runner.metrics.completed_ok == 3
        assert runner.metrics.scheduled_attempts == 3


# ===========================================================================
# Resultado correcto en ResultRecord
# ===========================================================================

class TestResultRecordFields:
    def test_successful_record_fields(self, f):
        runner = _runner()
        runner.submit(
            job_id="test_job",
            file_path=f,
            algorithm="word_count",
            priority=42,
        )
        records = runner.run()
        r = records[0]

        assert r.job_id == "test_job"
        assert r.algorithm == "word_count"
        assert r.priority == 42
        assert r.attempt == 1
        assert r.max_attempts == 1
        assert r.error is False
        assert r.timed_out is False
        assert r.cancelled is False
        assert r.will_retry is False
        assert r.duration_s >= 0.0
        assert "num_words" in r.result

    def test_file_field_matches_submitted_path(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        records = runner.run()
        assert Path(records[0].file) == f

    def test_error_record_has_error_flag(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo")
        records = runner.run()
        assert records[0].error is True
        assert "error" in records[0].result

    def test_timeout_record_has_timed_out_flag(self, f):
        runner = _runner(default_timeout=0.05)
        runner.submit(job_id="j1", file_path=f, algorithm="mixed_demo")
        records = runner.run()
        assert records[0].timed_out is True
        assert records[0].error is True

    def test_timeout_duration_is_at_least_timeout_limit(self, f):
        timeout = 0.1
        runner = _runner(default_timeout=timeout)
        runner.submit(job_id="j1", file_path=f, algorithm="mixed_demo")
        records = runner.run()
        assert records[0].duration_s >= timeout

    def test_timestamps_are_non_empty_strings(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        r = runner.run()[0]
        assert isinstance(r.started_at, str) and r.started_at
        assert isinstance(r.ended_at, str) and r.ended_at


# ===========================================================================
# Prioridades
# ===========================================================================

class TestPriority:
    def test_lower_priority_number_runs_first(self, tmp_path):
        """Con max_workers=1 y buffer_factor=1 los jobs se completan
        estrictamente en orden de prioridad."""
        for name in ("high.txt", "mid.txt", "low.txt"):
            (tmp_path / name).write_text("x")

        runner = RunnerV2(max_workers=1, buffer_factor=1)
        runner.submit(job_id="low",  file_path=tmp_path/"low.txt",  algorithm="word_count", priority=300)
        runner.submit(job_id="mid",  file_path=tmp_path/"mid.txt",  algorithm="word_count", priority=200)
        runner.submit(job_id="high", file_path=tmp_path/"high.txt", algorithm="word_count", priority=100)

        records = runner.run()
        order = [r.job_id for r in records]
        assert order == ["high", "mid", "low"]

    def test_equal_priority_fifo(self, tmp_path):
        """Jobs con igual prioridad se ejecutan en orden de envío (FIFO)."""
        for i in range(3):
            (tmp_path / f"f{i}.txt").write_text("x")

        runner = RunnerV2(max_workers=1, buffer_factor=1)
        for i in range(3):
            runner.submit(
                job_id=f"j{i}",
                file_path=tmp_path / f"f{i}.txt",
                algorithm="word_count",
                priority=100,
            )

        records = runner.run()
        assert [r.job_id for r in records] == ["j0", "j1", "j2"]


# ===========================================================================
# Concurrencia
# ===========================================================================

class TestConcurrency:
    def test_multiple_workers_complete_all_jobs(self, tmp_path):
        for i in range(8):
            (tmp_path / f"f{i}.txt").write_text(f"file {i}")

        runner = RunnerV2(max_workers=4, buffer_factor=2)
        runner.submit_directory(directory=tmp_path, algorithms=["word_count"])
        records = runner.run()

        assert len(records) == 8
        assert runner.metrics.completed_ok == 8

    def test_buffer_factor_limits_inflight(self, tmp_path):
        """buffer_factor=1 con max_workers=2 → máximo 2 jobs en vuelo."""
        for i in range(6):
            (tmp_path / f"f{i}.txt").write_text("x")

        runner = RunnerV2(max_workers=2, buffer_factor=1)
        runner.submit_directory(directory=tmp_path, algorithms=["word_count"])
        records = runner.run()

        assert len(records) == 6
        assert runner.metrics.completed_ok == 6


# ===========================================================================
# Constructor — clamping de parámetros
# ===========================================================================

class TestConstructorDefaults:
    def test_max_workers_clamped_to_1(self):
        runner = RunnerV2(max_workers=0)
        assert runner.max_workers == 1

    def test_max_workers_negative_clamped_to_1(self):
        runner = RunnerV2(max_workers=-5)
        assert runner.max_workers == 1

    def test_buffer_factor_clamped_to_1(self):
        runner = RunnerV2(buffer_factor=0)
        assert runner.buffer_factor == 1

    def test_default_retry_policy_no_retries(self):
        runner = RunnerV2()
        assert runner.retry_policy.retries == 0

    def test_custom_logger_used(self):
        import logging
        logger = logging.getLogger("test_custom")
        runner = RunnerV2(logger=logger)
        assert runner.logger is logger


# ===========================================================================
# cancel
# ===========================================================================

class TestCancel:
    def test_cancel_pending_returns_true(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        assert runner.cancel("j1") is True

    def test_cancel_nonexistent_returns_true(self):
        runner = _runner()
        assert runner.cancel("ghost_job") is True

    def test_cancelled_job_not_counted_as_completed(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        runner.submit(job_id="j2", file_path=f, algorithm="word_count")
        runner.cancel("j2")
        runner.run()
        assert runner.metrics.completed_ok == 1

    def test_cancelled_job_still_emits_record(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        runner.cancel("j1")
        records = runner.run()
        assert len(records) == 1
        assert records[0].cancelled is True

    def test_cancel_does_not_retry(self, f):
        policy = RetryPolicy(retries=3, retry_on=frozenset({RETRY_ERROR}))
        runner = _runner(retry_policy=policy)
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo")
        runner.cancel("j1")
        records = runner.run()
        assert runner.metrics.retries_scheduled == 0
        cancelled = [r for r in records if r.cancelled]
        assert len(cancelled) == 1


# ===========================================================================
# RetryPolicy
# ===========================================================================

class TestRetryPolicy:
    def test_invalid_retry_on_raises_value_error(self):
        with pytest.raises(ValueError, match="no válidos"):
            RetryPolicy(retry_on=frozenset({"invalid_reason"}))

    def test_valid_reasons_accepted(self):
        from framework import RETRY_TIMEOUT, RETRY_ERROR, RETRY_EXPIRED, RETRY_FRAMEWORK
        policy = RetryPolicy(
            retries=1,
            retry_on=frozenset({RETRY_TIMEOUT, RETRY_ERROR, RETRY_EXPIRED, RETRY_FRAMEWORK}),
        )
        assert len(policy.retry_on) == 4

    def test_default_retries_zero(self):
        assert RetryPolicy().retries == 0

    def test_default_retry_on_timeout(self):
        from framework import RETRY_TIMEOUT
        assert RetryPolicy().retry_on == frozenset({RETRY_TIMEOUT})


class TestJobIdUniqueness:
    def test_duplicate_job_id_raises_value_error(self, f):
        runner = _runner()
        runner.submit(job_id="dup", file_path=f, algorithm="word_count")

        with pytest.raises(ValueError, match="job_id duplicado"):
            runner.submit(job_id="dup", file_path=f, algorithm="sha256")


class TestSubmitDirectorySkipDuplicates:
    def test_second_call_raises_without_flag(self, dir_with_files):
        runner = _runner()
        runner.submit_directory(directory=dir_with_files, algorithms=["word_count"], pattern="*.txt")
        with pytest.raises(ValueError, match="job_id duplicado"):
            runner.submit_directory(directory=dir_with_files, algorithms=["word_count"], pattern="*.txt")

    def test_second_call_skips_with_flag(self, dir_with_files):
        runner = _runner()
        first = runner.submit_directory(
            directory=dir_with_files, algorithms=["word_count"], pattern="*.txt"
        )
        second = runner.submit_directory(
            directory=dir_with_files, algorithms=["word_count"], pattern="*.txt",
            skip_duplicates=True,
        )
        assert len(first) == 3
        assert second == []

    def test_skip_duplicates_only_omits_known_ids(self, dir_with_files):
        """La segunda llamada con un algo nuevo crea jobs, los existentes se omiten."""
        runner = _runner()
        runner.submit_directory(directory=dir_with_files, algorithms=["word_count"], pattern="*.txt")
        second = runner.submit_directory(
            directory=dir_with_files, algorithms=["sha256"], pattern="*.txt",
            skip_duplicates=True,
        )
        assert len(second) == 3

    def test_submitted_count_correct_after_skip(self, dir_with_files):
        runner = _runner()
        runner.submit_directory(directory=dir_with_files, algorithms=["word_count"], pattern="*.txt")
        runner.submit_directory(
            directory=dir_with_files, algorithms=["word_count"], pattern="*.txt",
            skip_duplicates=True,
        )
        assert runner.metrics.submitted_jobs == 3  # solo los de la primera llamada


class TestPerJobRetryPolicy:
    def test_job_policy_overrides_runner_policy(self, f):
        """Runner sin reintentos; job con política propia de 2 reintentos."""
        from framework import RETRY_ERROR
        runner = _runner()  # retry_policy default: retries=0
        job_policy = RetryPolicy(retries=2, retry_on=frozenset({RETRY_ERROR}))
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo", retry_policy=job_policy)
        records = runner.run()
        assert len(records) == 3
        assert runner.metrics.retries_scheduled == 2

    def test_job_policy_no_retry_overrides_runner_policy(self, f):
        """Runner con reintentos; job con política propia de 0 reintentos."""
        from framework import RETRY_ERROR
        runner_policy = RetryPolicy(retries=5, retry_on=frozenset({RETRY_ERROR}))
        runner = _runner(retry_policy=runner_policy)
        job_policy = RetryPolicy(retries=0)
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo", retry_policy=job_policy)
        records = runner.run()
        assert len(records) == 1
        assert runner.metrics.retries_scheduled == 0

    def test_max_attempts_derived_from_job_policy(self, f):
        """max_attempts se calcula de la política del job si no se pasa explícitamente."""
        from framework import RETRY_ERROR
        job_policy = RetryPolicy(retries=3, retry_on=frozenset({RETRY_ERROR}))
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo", retry_policy=job_policy)
        records = runner.run()
        assert all(r.max_attempts == 4 for r in records)

    def test_jobs_without_policy_use_runner_policy(self, f):
        """Jobs sin política propia siguen usando la del runner."""
        from framework import RETRY_ERROR
        runner_policy = RetryPolicy(retries=1, retry_on=frozenset({RETRY_ERROR}))
        runner = _runner(retry_policy=runner_policy)
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo")
        records = runner.run()
        assert len(records) == 2
        assert runner.metrics.retries_scheduled == 1


class TestPayloadLogging:
    def test_successful_payload_logged_at_debug(self, f, caplog):
        import logging
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        with caplog.at_level(logging.DEBUG, logger=runner.logger.name):
            runner.run()
        payload_logs = [r for r in caplog.records if "PAYLOAD" in r.message]
        assert len(payload_logs) == 1
        assert "j1" in payload_logs[0].message

    def test_failed_job_does_not_log_payload(self, f, caplog):
        import logging
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo")
        with caplog.at_level(logging.DEBUG, logger=runner.logger.name):
            runner.run()
        payload_logs = [r for r in caplog.records if "PAYLOAD" in r.message]
        assert len(payload_logs) == 0


class TestDurationSemantics:
    def test_success_duration_matches_timestamp_span(self, f):
        from datetime import datetime

        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        record = runner.run()[0]

        started = datetime.fromisoformat(record.started_at)
        ended = datetime.fromisoformat(record.ended_at)
        assert record.duration_s == pytest.approx((ended - started).total_seconds(), abs=0.02)

    def test_timeout_duration_matches_timestamp_span(self, f):
        from datetime import datetime

        runner = _runner(default_timeout=0.1)
        runner.submit(job_id="j1", file_path=f, algorithm="mixed_demo")
        record = runner.run()[0]

        started = datetime.fromisoformat(record.started_at)
        ended = datetime.fromisoformat(record.ended_at)
        assert record.timed_out is True
        assert record.duration_s == pytest.approx((ended - started).total_seconds(), abs=0.02)



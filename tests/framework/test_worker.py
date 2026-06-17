"""
Tests for _execute_job running inside the worker process.
Executed in the main process (without Pebble) to test pure logic.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from alglab.engine.core import Algorithm, Job
from alglab.engine.registry import register_algorithm
from alglab.engine.worker import _ALGORITHM_CACHE, _execute_job

# ---------------------------------------------------------------------------
# Test algorithms (module-level for pickle compatibility)
# ---------------------------------------------------------------------------


@register_algorithm("_wtest_ok")
class _OkAlgo(Algorithm):
    def run(self, context: Path, *, seed: int | None = None):
        return {"lines": context.read_text().count("\n") + 1}


@register_algorithm("_wtest_fail")
class _FailAlgo(Algorithm):
    def run(self, context: Path, *, seed: int | None = None):
        raise ValueError("controlled failure")


@register_algorithm("_wtest_hook_recorder")
class _HookRecorder(Algorithm):
    calls: list[str] = []
    contexts: list[str] = []

    def before_run(self, file_path: Path) -> str:
        _HookRecorder.calls.append("before")
        return f"context:{file_path.name}"

    def run(self, context: str, *, seed: int | None = None):
        _HookRecorder.calls.append("run")
        _HookRecorder.contexts.append(context)
        return {"context_in_run": context}

    def after_run(self, context: str, result):
        _HookRecorder.calls.append("after")
        _HookRecorder.contexts.append(context)
        return {"context_in_after": context, **result}


@register_algorithm("_wtest_fail_before")
class _FailBeforeRun(Algorithm):
    def before_run(self, file_path: Path) -> None:
        raise RuntimeError("error in before_run")

    def run(self, context: Path, *, seed: int | None = None):
        return {}


@register_algorithm("_wtest_fail_after")
class _FailAfterRun(Algorithm):
    def run(self, context: Path, *, seed: int | None = None):
        return {"val": 1}

    def after_run(self, context: Path, result):
        raise RuntimeError("error in after_run")


@register_algorithm("_wtest_fail_run_no_after")
class _FailRunNoAfter(Algorithm):
    after_called: bool = False

    def run(self, context: Path, *, seed: int | None = None):
        raise RuntimeError("boom")

    def after_run(self, context: Path, result):
        _FailRunNoAfter.after_called = True
        return result


@register_algorithm("_wtest_seed_aware")
class _SeedAwareAlgo(Algorithm):
    last_seed: int | None = None

    def run(self, context: Path, *, seed: int | None = None):
        _SeedAwareAlgo.last_seed = seed
        return {"seed_received": seed}


@register_algorithm("_wtest_seed_unaware")
class _SeedUnawareAlgo(Algorithm):
    def run(self, context: Path, *, seed: int | None = None):
        return {"ok": True}


@register_algorithm("_wtest_after_payload")
class _AfterPayloadAlgo(Algorithm):
    def run(self, context: Path, *, seed: int | None = None):
        return {"run": True}

    def after_run(self, context: Path, result):
        return {"final": result["run"]}


@register_algorithm("_wtest_after_none")
class _AfterNoneAlgo(Algorithm):
    def run(self, context: Path, *, seed: int | None = None):
        return {"run": True}

    def after_run(self, context: Path, result):
        return None


@register_algorithm("_wtest_cache")
class _CacheAlgo(Algorithm):
    init_count = 0

    def __init__(self, label: str = "default") -> None:
        type(self).init_count += 1
        self.instance_id = type(self).init_count
        self.label = label
        self.run_count = 0

    def run(self, context: Path, *, seed: int | None = None):
        self.run_count += 1
        return {
            "instance_id": self.instance_id,
            "label": self.label,
            "run_count": self.run_count,
        }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_algo_state() -> None:
    _HookRecorder.calls = []
    _HookRecorder.contexts = []
    _SeedAwareAlgo.last_seed = None
    _FailRunNoAfter.after_called = False
    _CacheAlgo.init_count = 0
    _ALGORITHM_CACHE.clear()


@pytest.fixture()
def f(tmp_path: Path) -> Path:
    p = tmp_path / "test.txt"
    p.write_text("line one\nline two")
    return p


# ===========================================================================
# Successful result
# ===========================================================================


class TestExecuteJobSuccess:
    def test_ok_is_true(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_ok"))
        assert outcome.ok is True

    def test_payload_contains_result(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_ok"))
        assert outcome.payload == {"lines": 2}

    def test_phase_durations_are_populated(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_ok"))
        assert outcome.pre_run_duration_s is not None
        assert outcome.run_duration_s is not None
        assert outcome.post_run_duration_s is not None
        assert outcome.pre_run_duration_s >= 0.0
        assert outcome.run_duration_s >= 0.0
        assert outcome.post_run_duration_s >= 0.0


# ===========================================================================
# Failed result
# ===========================================================================


class TestExecuteJobFailure:
    def test_ok_is_false_on_exception(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_fail"))
        assert outcome.ok is False

    def test_payload_contains_error_key(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_fail"))
        assert "error" in outcome.payload

    def test_payload_error_includes_type(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_fail"))
        assert "ValueError" in outcome.payload["error"]

    def test_payload_error_includes_message(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_fail"))
        assert "controlled failure" in outcome.payload["error"]

    def test_failure_in_before_run_captured(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_fail_before"))
        assert outcome.ok is False
        assert "error in before_run" in outcome.payload["error"]
        assert outcome.pre_run_duration_s is not None
        assert outcome.run_duration_s is None
        assert outcome.post_run_duration_s is None

    def test_failure_in_after_run_captured(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_fail_after"))
        assert outcome.ok is False
        assert "error in after_run" in outcome.payload["error"]
        assert outcome.pre_run_duration_s is not None
        assert outcome.run_duration_s is not None
        assert outcome.post_run_duration_s is not None


# ===========================================================================
# Hook order
# ===========================================================================


class TestExecuteJobHooks:
    def test_hooks_called_in_order(self, f):
        _execute_job(Job(file_path=f, algorithm="_wtest_hook_recorder"))
        assert _HookRecorder.calls == ["before", "run", "after"]

    def test_context_is_passed_to_run_and_after_run(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_hook_recorder"))
        assert _HookRecorder.contexts == ["context:test.txt", "context:test.txt"]
        assert outcome.payload == {
            "context_in_after": "context:test.txt",
            "context_in_run": "context:test.txt",
        }

    def test_after_run_not_called_on_failure(self, f):
        _execute_job(Job(file_path=f, algorithm="_wtest_fail_run_no_after"))
        assert _FailRunNoAfter.after_called is False

    def test_after_run_return_value_is_final_payload(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_after_payload"))
        assert outcome.ok is True
        assert outcome.payload == {"final": True}

    def test_after_run_returning_none_is_clear_error(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_after_none"))
        assert outcome.ok is False
        assert "Algorithm.after_run() must return dict" in outcome.payload["error"]


# ===========================================================================
# Seed injection
# ===========================================================================


class TestExecuteJobSeed:
    def test_seed_passed_to_aware_algo(self, f):
        _execute_job(Job(file_path=f, algorithm="_wtest_seed_aware", seed=99))
        assert _SeedAwareAlgo.last_seed == 99

    def test_none_seed_passed_to_aware_algo(self, f):
        _execute_job(Job(file_path=f, algorithm="_wtest_seed_aware", seed=None))
        assert _SeedAwareAlgo.last_seed is None

    def test_seed_is_optional_for_algorithm_logic(self, f):
        outcome = _execute_job(Job(file_path=f, algorithm="_wtest_seed_unaware", seed=42))
        assert outcome.ok is True


class TestExecuteJobCache:
    def test_same_kwargs_reuse_algorithm_instance(self, f):
        first = _execute_job(Job(file_path=f, algorithm="_wtest_cache", algo_kwargs={"label": "x"}))
        second = _execute_job(
            Job(file_path=f, algorithm="_wtest_cache", algo_kwargs={"label": "x"})
        )

        assert first.payload["instance_id"] == second.payload["instance_id"]
        assert first.payload["run_count"] == 1
        assert second.payload["run_count"] == 2
        assert _CacheAlgo.init_count == 1

    def test_different_kwargs_use_different_algorithm_instances(self, f):
        first = _execute_job(Job(file_path=f, algorithm="_wtest_cache", algo_kwargs={"label": "x"}))
        second = _execute_job(
            Job(file_path=f, algorithm="_wtest_cache", algo_kwargs={"label": "y"})
        )

        assert first.payload["instance_id"] != second.payload["instance_id"]
        assert first.payload["label"] == "x"
        assert second.payload["label"] == "y"
        assert _CacheAlgo.init_count == 2

    def test_unhashable_kwargs_fail_clearly(self, f):
        outcome = _execute_job(
            Job(file_path=f, algorithm="_wtest_cache", algo_kwargs={"label": ["x"]})
        )
        assert outcome.ok is False
        assert "constructor kwargs must be hashable" in outcome.payload["error"]

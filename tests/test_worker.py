"""
Tests de la función _execute_job que corre dentro del worker process.
Se ejecutan en el proceso principal (sin Pebble) para probar la lógica pura.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from framework.core import Algorithm
from framework.worker import _execute_job


# ---------------------------------------------------------------------------
# Algoritmos auxiliares (nivel de módulo → picklables si hicieran falta)
# ---------------------------------------------------------------------------

class _OkAlgo(Algorithm):
    """Devuelve un resultado sencillo."""
    def run(self, file_path: Path):
        return {"lines": file_path.read_text().count("\n") + 1}


class _FailAlgo(Algorithm):
    """Siempre lanza una excepción."""
    def run(self, file_path: Path):
        raise ValueError("fallo controlado")


class _HookRecorder(Algorithm):
    """Registra el orden de llamada de los hooks."""
    def __init__(self):
        self.calls: list[str] = []

    def before_run(self, file_path: Path):
        self.calls.append("before")

    def run(self, file_path: Path):
        self.calls.append("run")
        return {"ok": True}

    def after_run(self, file_path: Path, result):
        self.calls.append("after")


class _FailBeforeRun(Algorithm):
    """Falla en before_run."""
    def before_run(self, file_path: Path):
        raise RuntimeError("error en before_run")

    def run(self, file_path: Path):
        return {}


class _FailAfterRun(Algorithm):
    """Falla en after_run."""
    def run(self, file_path: Path):
        return {"val": 1}

    def after_run(self, file_path: Path, result):
        raise RuntimeError("error en after_run")


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def f(tmp_path: Path) -> Path:
    p = tmp_path / "test.txt"
    p.write_text("línea uno\nlínea dos")  # sin \n final → 2 líneas exactas
    return p


# ===========================================================================
# Resultado exitoso
# ===========================================================================

class TestExecuteJobSuccess:
    def test_ok_is_true(self, f):
        outcome = _execute_job(f, _OkAlgo())
        assert outcome.ok is True

    def test_payload_contains_result(self, f):
        outcome = _execute_job(f, _OkAlgo())
        assert outcome.payload == {"lines": 2}

    def test_pid_is_positive(self, f):
        outcome = _execute_job(f, _OkAlgo())
        assert outcome.pid > 0

    def test_duration_is_non_negative(self, f):
        outcome = _execute_job(f, _OkAlgo())
        assert outcome.duration_s >= 0.0

    def test_started_before_ended(self, f):
        outcome = _execute_job(f, _OkAlgo())
        assert outcome.started_at <= outcome.ended_at

    def test_timestamps_are_iso_strings(self, f):
        from datetime import datetime, timezone
        outcome = _execute_job(f, _OkAlgo())
        # No deben lanzar excepción al parsear
        datetime.fromisoformat(outcome.started_at)
        datetime.fromisoformat(outcome.ended_at)


# ===========================================================================
# Resultado fallido
# ===========================================================================

class TestExecuteJobFailure:
    def test_ok_is_false_on_exception(self, f):
        outcome = _execute_job(f, _FailAlgo())
        assert outcome.ok is False

    def test_payload_contains_error_key(self, f):
        outcome = _execute_job(f, _FailAlgo())
        assert "error" in outcome.payload

    def test_payload_error_includes_type(self, f):
        outcome = _execute_job(f, _FailAlgo())
        assert "ValueError" in outcome.payload["error"]

    def test_payload_error_includes_message(self, f):
        outcome = _execute_job(f, _FailAlgo())
        assert "fallo controlado" in outcome.payload["error"]

    def test_failure_in_before_run_captured(self, f):
        outcome = _execute_job(f, _FailBeforeRun())
        assert outcome.ok is False
        assert "error en before_run" in outcome.payload["error"]

    def test_failure_in_after_run_captured(self, f):
        outcome = _execute_job(f, _FailAfterRun())
        assert outcome.ok is False
        assert "error en after_run" in outcome.payload["error"]


# ===========================================================================
# Orden de los hooks
# ===========================================================================

class TestExecuteJobHooks:
    def test_hooks_called_in_order(self, f):
        algo = _HookRecorder()
        _execute_job(f, algo)
        assert algo.calls == ["before", "run", "after"]

    def test_after_run_not_called_on_failure(self, f):
        """Si run() falla, after_run no debe ejecutarse."""
        class _FailRun(Algorithm):
            def __init__(self):
                self.after_called = False
            def run(self, file_path):
                raise RuntimeError("boom")
            def after_run(self, file_path, result):
                self.after_called = True

        algo = _FailRun()
        _execute_job(f, algo)
        assert algo.after_called is False

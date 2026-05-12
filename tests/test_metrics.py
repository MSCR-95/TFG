"""
Tests de métricas del framework RunnerV2.

Cubre los seis contadores no triviales:
  - algorithm_errors
  - cancelled  /  queue_skipped_cancelled
  - process_expired
  - framework_errors  (ver nota al final del módulo)
  - retries_scheduled
  - timeouts  (verificación de sanidad)
"""
from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from framework import RunnerV2, RetryPolicy, RETRY_ERROR, RETRY_EXPIRED, RETRY_TIMEOUT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _runner(**kwargs) -> RunnerV2:
    """Crea un RunnerV2 con defaults cómodos para tests."""
    defaults = dict(max_workers=1, buffer_factor=1)
    defaults.update(kwargs)
    return RunnerV2(**defaults)


@pytest.fixture()
def f(tmp_path: Path) -> Path:
    """Fichero de texto mínimo reutilizable."""
    p = tmp_path / "sample.txt"
    p.write_text("contenido de prueba\n")
    return p


@pytest.fixture()
def slow_f(tmp_path: Path) -> Path:
    """Fichero cuyo nombre contiene 'slow' → mixed_demo duerme 6 s."""
    p = tmp_path / "slow_test.txt"
    p.write_text("contenido lento\n")
    return p


# ===========================================================================
# algorithm_errors
# ===========================================================================

class TestAlgorithmErrors:
    def test_increments_on_single_failure(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo")

        records = runner.run()

        assert runner.metrics.algorithm_errors == 1
        assert runner.metrics.completed_ok == 0
        assert records[0].error is True
        assert records[0].timed_out is False
        assert records[0].cancelled is False

    def test_increments_once_per_attempt(self, f):
        """Con 3 intentos (1 original + 2 reintentos) debe contar 3 errores."""
        policy = RetryPolicy(retries=2, retry_on=frozenset({RETRY_ERROR}))
        runner = _runner(retry_policy=policy)
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo")

        records = runner.run()

        assert runner.metrics.algorithm_errors == 3
        assert runner.metrics.completed_ok == 0


# ===========================================================================
# retries_scheduled
# ===========================================================================

class TestRetriesScheduled:
    def test_retry_on_error(self, f):
        policy = RetryPolicy(retries=2, retry_on=frozenset({RETRY_ERROR}))
        runner = _runner(retry_policy=policy)
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo")

        records = runner.run()

        assert runner.metrics.retries_scheduled == 2
        # Emite 3 records: intentos 1 y 2 con will_retry=True, intento 3 con will_retry=False
        assert len(records) == 3
        assert records[0].will_retry is True
        assert records[0].attempt == 1
        assert records[1].will_retry is True
        assert records[1].attempt == 2
        assert records[2].will_retry is False
        assert records[2].attempt == 3

    def test_retry_on_timeout(self, slow_f):
        policy = RetryPolicy(retries=1, retry_on=frozenset({RETRY_TIMEOUT}))
        runner = _runner(default_timeout=0.1, retry_policy=policy)
        runner.submit(job_id="j1", file_path=slow_f, algorithm="mixed_demo")

        records = runner.run()

        assert runner.metrics.retries_scheduled == 1
        assert runner.metrics.timeouts == 2
        assert len(records) == 2
        assert all(r.timed_out for r in records)

    def test_retry_on_process_expired(self, f):
        policy = RetryPolicy(retries=2, retry_on=frozenset({RETRY_EXPIRED}))
        runner = _runner(retry_policy=policy)
        runner.submit(job_id="j1", file_path=f, algorithm="process_killer")

        records = runner.run()

        assert runner.metrics.retries_scheduled == 2
        assert runner.metrics.process_expired == 3
        assert len(records) == 3

    def test_no_retry_without_policy(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo")

        runner.run()

        assert runner.metrics.retries_scheduled == 0

    def test_no_retry_if_reason_not_in_policy(self, f):
        """retry_on=timeout no debe reintentar errores de algoritmo."""
        policy = RetryPolicy(retries=3, retry_on=frozenset({RETRY_TIMEOUT}))
        runner = _runner(retry_policy=policy)
        runner.submit(job_id="j1", file_path=f, algorithm="error_demo")

        runner.run()

        assert runner.metrics.retries_scheduled == 0
        assert runner.metrics.algorithm_errors == 1


# ===========================================================================
# cancelled / queue_skipped_cancelled
# ===========================================================================

class TestCancelled:
    def test_queue_skipped_when_cancelled_before_run(self, f):
        """Job cancelado antes de que corra: se salta desde la cola pendiente."""
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        runner.submit(job_id="j2", file_path=f, algorithm="word_count")

        runner.cancel("j2")
        records = runner.run()

        assert runner.metrics.queue_skipped_cancelled == 1
        assert runner.metrics.cancelled == 1

        cancelled = [r for r in records if r.cancelled]
        assert len(cancelled) == 1
        assert cancelled[0].job_id == "j2"
        assert cancelled[0].will_retry is False

    def test_cancelled_job_never_executed(self, f):
        """El job cancelado no debe producir ningún resultado exitoso."""
        runner = _runner()
        runner.submit(job_id="ok", file_path=f, algorithm="word_count")
        runner.submit(job_id="no", file_path=f, algorithm="word_count")

        runner.cancel("no")
        records = runner.run()

        ok_records = [r for r in records if not r.cancelled]
        assert len(ok_records) == 1
        assert ok_records[0].job_id == "ok"

    def test_multiple_cancellations(self, f):
        runner = _runner()
        for i in range(5):
            runner.submit(job_id=f"j{i}", file_path=f, algorithm="word_count")

        runner.cancel("j1")
        runner.cancel("j3")
        records = runner.run()

        assert runner.metrics.queue_skipped_cancelled == 2
        assert runner.metrics.cancelled == 2
        assert runner.metrics.completed_ok == 3

    def test_inflight_cancelled_via_future(self, f, slow_f):
        """
        Job en vuelo en Pebble (no ejecutándose aún): future.cancel() → CancelledError.
        Usa buffer_factor=2 para que ambos jobs queden en _inflight a la vez,
        mientras el slow_f ocupa el único worker.
        """
        runner = RunnerV2(max_workers=1, buffer_factor=2)
        # El job lento ocupa el único worker durante ~6 s
        runner.submit(job_id="slow", file_path=slow_f, algorithm="mixed_demo")
        # Este queda encolado dentro de Pebble (worker ocupado)
        runner.submit(job_id="to_cancel", file_path=f, algorithm="word_count")

        records: list = []

        def _run():
            for r in runner.run_stream():
                records.append(r)

        t = threading.Thread(target=_run, daemon=True)
        t.start()

        # Esperamos a que _fill_buffer haya encolado ambos jobs en Pebble
        time.sleep(0.5)
        runner.cancel("to_cancel")

        t.join(timeout=15)
        assert not t.is_alive(), "El runner no terminó a tiempo"

        assert runner.metrics.cancelled >= 1
        cancelled = [r for r in records if r.cancelled and r.job_id == "to_cancel"]
        assert len(cancelled) == 1


# ===========================================================================
# process_expired
# ===========================================================================

class TestProcessExpired:
    def test_increments_on_worker_crash(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="process_killer")

        records = runner.run()

        assert runner.metrics.process_expired == 1
        assert runner.metrics.completed_ok == 0
        assert records[0].error is True
        assert records[0].timed_out is False
        assert records[0].cancelled is False

    def test_multiple_crashes(self, f):
        runner = _runner(max_workers=2)
        runner.submit(job_id="j1", file_path=f, algorithm="process_killer")
        runner.submit(job_id="j2", file_path=f, algorithm="process_killer")

        runner.run()

        assert runner.metrics.process_expired == 2

    def test_no_retry_without_policy(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="process_killer")

        runner.run()

        assert runner.metrics.retries_scheduled == 0
        assert runner.metrics.process_expired == 1


# ===========================================================================
# Metrics.summary()
# ===========================================================================

class TestMetricsSummary:
    def test_returns_dict(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        runner.run()
        assert isinstance(runner.metrics.summary(), dict)

    def test_all_keys_present(self):
        runner = _runner()
        keys = set(runner.metrics.summary().keys())
        expected = {
            "submitted_jobs", "scheduled_attempts", "completed_ok",
            "algorithm_errors", "timeouts", "cancelled", "process_expired",
            "framework_errors", "retries_scheduled", "queue_skipped_cancelled",
        }
        assert keys == expected

    def test_values_match_counters(self, f):
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        runner.submit(job_id="j2", file_path=f, algorithm="error_demo")
        runner.run()
        s = runner.metrics.summary()
        assert s["submitted_jobs"] == 2
        assert s["completed_ok"] == 1
        assert s["algorithm_errors"] == 1

    def test_summary_reflects_live_state(self, f):
        """summary() debe reflejar el estado actual, no una snapshot fija."""
        runner = _runner()
        runner.submit(job_id="j1", file_path=f, algorithm="word_count")
        before = runner.metrics.summary()["submitted_jobs"]
        runner.submit(job_id="j2", file_path=f, algorithm="word_count")
        after = runner.metrics.summary()["submitted_jobs"]
        assert after == before + 1


# ===========================================================================
# Nota sobre framework_errors
# ===========================================================================
#
# `framework_errors` es el contador catch-all del bloque `except Exception`
# en `_finalize_future`. Se activa ante fallos inesperados de infraestructura
# (p.ej. error de pickle al deserializar el resultado del worker, bug en Pebble,
# etc.) que no encajan en timeout, CancelledError ni ProcessExpired.
#
# No tiene un test directo aquí porque reproducirlo de forma fiable requiere
# mockear las entrañas de Pebble o el protocolo de serialización, lo que
# acoplaría los tests a detalles de implementación internos.
# Si en el futuro se identifica un caso reproducible, añadir aquí.

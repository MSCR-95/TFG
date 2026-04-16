"""
framework.runner
================
Motor de ejecución concurrente: ``RunnerV2``.

``RunnerV2`` es el componente central del framework. Gestiona la planificación,
ejecución, reintento, cancelación y emisión de resultados de todos los jobs.

Arquitectura interna
--------------------
::

    ┌─────────────────────────────────────────────────────────┐
    │                       RunnerV2                          │
    │                                                         │
    │  _pending (heapq)          _inflight (dict)             │
    │  ─────────────────         ────────────────             │
    │  (prio, seq, Job) ──►  future → RuntimeMeta             │
    │                                    │                    │
    │                          Pebble ProcessPool             │
    │                                    │                    │
    │                          worker process(es)             │
    │                                    │                    │
    │                          _execute_job()                 │
    │                                    │                    │
    │                          JobOutcome ──► ResultRecord    │
    └─────────────────────────────────────────────────────────┘

Ciclo del scheduler (``run_stream``)
-------------------------------------
1. Emitir records pendientes (``_ready_records``).
2. Filtrar jobs cancelados al frente del heap (``_drain_cancelled_pending``).
3. Rellenar la ventana de ejecución (``_fill_buffer``).
4. Esperar al primer future que termine (``wait(FIRST_COMPLETED)``).
5. Para cada future terminado: clasificar el resultado, aplicar retry si
   corresponde, emitir el ``ResultRecord``.
6. Repetir hasta que no haya pendientes, en vuelo ni records listos.

Backpressure
------------
El runner nunca mete todos los jobs en el pool a la vez. Mantiene como
máximo ``max_workers × buffer_factor`` futures en vuelo simultáneamente.
Esto evita que Pebble acumule miles de tareas en su cola interna cuando el
lote tiene millones de jobs.

Modelo de errores y retry
--------------------------
Cuando un intento falla, ``_finalize_future`` clasifica la causa::

    FuturesTimeoutError → RETRY_TIMEOUT  → metrics.timeouts
    CancelledError      → (no retry)     → metrics.cancelled
    ProcessExpired      → RETRY_EXPIRED  → metrics.process_expired
    Exception genérica  → RETRY_FRAMEWORK→ metrics.framework_errors
    outcome.ok=False    → RETRY_ERROR    → metrics.algorithm_errors

Si la causa está en ``retry_policy.retry_on`` y quedan intentos, el job
se reencola con ``attempt+1``.
"""

from __future__ import annotations

import datetime as dt
import heapq
import logging
from collections import deque
from collections.abc import Generator, Sequence
from concurrent.futures import (
    CancelledError,
    FIRST_COMPLETED,
    TimeoutError as FuturesTimeoutError,
    wait,
)
from dataclasses import replace
from itertools import count
from pathlib import Path
from typing import Any

from pebble import ProcessPool
from pebble.common import ProcessExpired

from framework.core import (
    Algorithm,
    Job,
    JobOutcome,
    Metrics,
    ResultRecord,
    RetryPolicy,
    RuntimeMeta,
    RETRY_ERROR,
    RETRY_EXPIRED,
    RETRY_FRAMEWORK,
    RETRY_TIMEOUT,
)
from framework.registry import build_algorithm
from framework.worker import _execute_job


# ============================================================================
# Runner V2
# ============================================================================

class RunnerV2:
    """
    Motor de ejecución concurrente para lotes de jobs sobre ficheros.

    Combina una cola de prioridad, un pool de procesos Pebble, backpressure,
    política de reintentos configurable, cancelación best-effort y emisión de
    resultados en streaming.

    Parameters
    ----------
    max_workers:
        Número de procesos worker simultáneos. Se clampea a mínimo 1.
    default_timeout:
        Timeout por defecto en segundos para los jobs que no especifiquen
        el suyo. ``None`` significa sin límite.
    retry_policy:
        Política de reintentos global. Puede sobreescribirse por job
        individual en ``submit(retry_policy=...)``.
        Default: ``RetryPolicy()`` (0 reintentos).
    buffer_factor:
        Multiplicador sobre ``max_workers`` para calcular el máximo de
        futures en vuelo. Un valor de 2 mantiene hasta ``2 × max_workers``
        tareas en el pool. Se clampea a mínimo 1.
    logger:
        Logger Python a usar. Si es ``None`` se usa el logger del módulo.

    Examples
    --------
    Uso básico::

        from pathlib import Path
        from framework import RunnerV2, RetryPolicy, JSONLResultSink, RETRY_ERROR

        runner = RunnerV2(
            max_workers=4,
            default_timeout=30.0,
            retry_policy=RetryPolicy(retries=2, retry_on=frozenset({RETRY_ERROR})),
        )
        runner.submit_directory(
            directory=Path("data/"),
            algorithms=["word_count", "sha256"],
            pattern="*.txt",
        )
        sink = JSONLResultSink(Path("output/results.jsonl"))
        n = sink.write_all(runner.run_stream())
        print(runner.metrics.summary())

    Uso de la política por job::

        runner = RunnerV2(max_workers=2)
        runner.submit(
            job_id="critico",
            file_path=Path("important.cnf"),
            algorithm="maxsat_brute",
            retry_policy=RetryPolicy(retries=3, retry_on=frozenset({RETRY_ERROR})),
        )
    """

    def __init__(
        self,
        *,
        max_workers: int = 1,
        default_timeout: float | None = None,
        retry_policy: RetryPolicy | None = None,
        buffer_factor: int = 2,
        logger: logging.Logger | None = None,
    ) -> None:
        self.max_workers = max(1, max_workers)
        self.default_timeout = default_timeout
        self.retry_policy = retry_policy or RetryPolicy()
        self.buffer_factor = max(1, buffer_factor)
        self.logger = logger or logging.getLogger(__name__)

        # Heap de prioridad: (priority, seq, job)
        self._pending: list[tuple[int, int, Job]] = []
        self._seq = count()

        # Futures en vuelo
        self._inflight: dict[Any, RuntimeMeta] = {}

        # Jobs cancelados por id
        self._cancelled_job_ids: set[str] = set()

        # Job ids conocidos para evitar colisiones en cancel() y reportes
        self._known_job_ids: set[str] = set()

        # Cola interna de registros ya listos para emitir
        self._ready_records: deque[ResultRecord] = deque()

        # Pool de Pebble, creado durante run_stream()
        self._pool: ProcessPool | None = None

        self.metrics = Metrics()

    # ------------------------------------------------------------------
    # API publica de entrada de jobs
    # ------------------------------------------------------------------

    def submit(
        self,
        *,
        job_id: str,
        file_path: Path,
        algorithm: str | Algorithm,
        priority: int = 100,
        timeout: float | None = None,
        max_attempts: int | None = None,
        retry_policy: RetryPolicy | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Anade un job a la cola pendiente.

        retry_policy:
        - si se pasa, sobrescribe la política del runner para este job
        max_attempts:
        - si no se pasa, se calcula a partir de la política efectiva.retries + 1
        """
        if job_id in self._known_job_ids:
            raise ValueError(f"job_id duplicado: '{job_id}'")

        algo_obj = build_algorithm(algorithm) if isinstance(algorithm, str) else algorithm
        effective_policy = retry_policy if retry_policy is not None else self.retry_policy
        attempts = max_attempts if max_attempts is not None else effective_policy.retries + 1

        job = Job(
            job_id=job_id,
            file_path=file_path,
            algorithm=algo_obj,
            priority=priority,
            timeout=timeout if timeout is not None else self.default_timeout,
            attempt=1,
            max_attempts=max(1, attempts),
            metadata=metadata,
            retry_policy=retry_policy,
        )

        heapq.heappush(self._pending, (job.priority, next(self._seq), job))
        self._known_job_ids.add(job.job_id)
        self.metrics.submitted_jobs += 1
        return job.job_id

    def submit_directory(
        self,
        *,
        directory: Path,
        algorithms: Sequence[str | Algorithm],
        pattern: str = "*",
        recursive: bool = False,
        priority: int = 100,
        timeout: float | None = None,
        max_attempts: int | None = None,
        skip_duplicates: bool = False,
    ) -> list[str]:
        """
        Descubre ficheros y crea un job por cada combinacion (fichero, algoritmo).

        skip_duplicates:
        - False (default): lanza ValueError si el job_id ya existe (comportamiento original)
        - True: omite silenciosamente los jobs cuyo job_id ya está registrado
        """
        glob_fn = directory.rglob if recursive else directory.glob
        files = sorted(f for f in glob_fn(pattern) if f.is_file())

        created: list[str] = []
        for f in files:
            for algo in algorithms:
                algo_name = algo if isinstance(algo, str) else algo.name
                job_id = f"{algo_name}:{f.as_posix()}"
                if skip_duplicates and job_id in self._known_job_ids:
                    continue
                created.append(
                    self.submit(
                        job_id=job_id,
                        file_path=f,
                        algorithm=algo,
                        priority=priority,
                        timeout=timeout,
                        max_attempts=max_attempts,
                    )
                )
        return created

    def cancel(self, job_id: str) -> bool:
        """
        Cancela un job de la mejor forma posible.

        Semantica:
        - Si esta pendiente, no llegara a ejecutarse.
        - Si esta en vuelo, se solicita cancelacion al Future.
          Esto puede cancelar jobs en cola, pero no garantiza abortar
          inmediatamente uno que ya este ejecutandose.

        Devuelve:
        - True  si se marco para cancelacion o future.cancel() acepto la peticion.
        - False si estaba en vuelo y la cancelacion no fue aceptada.
        """
        self._cancelled_job_ids.add(job_id)

        for future, meta in list(self._inflight.items()):
            if meta.job.job_id == job_id:
                self.logger.info("Solicitando cancelacion de job en vuelo: %s", job_id)
                return bool(future.cancel())

        return True

    # ------------------------------------------------------------------
    # API de ejecucion
    # ------------------------------------------------------------------

    def run(self) -> list[ResultRecord]:
        """Modo batch: consume el generador y devuelve todos los resultados."""
        return list(self.run_stream())

    def run_stream(self) -> Generator[ResultRecord, None, None]:
        """
        Ejecuta el scheduler y va emitiendo ResultRecord en streaming.

        Backpressure:
        - no se mete todo en vuelo de golpe
        - se mantiene una ventana de tamano max_workers * buffer_factor
        """
        max_inflight = self.max_workers * self.buffer_factor

        with ProcessPool(max_workers=self.max_workers) as pool:
            self._pool = pool

            while self._pending or self._inflight or self._ready_records:
                while self._ready_records:
                    yield self._ready_records.popleft()

                self._drain_cancelled_pending()
                self._fill_buffer(max_inflight=max_inflight)

                if not self._inflight:
                    continue

                done, _ = wait(self._inflight.keys(), return_when=FIRST_COMPLETED)

                for future in done:
                    yield self._finalize_future(future)

            self._pool = None

    # ------------------------------------------------------------------
    # Internos del scheduler
    # ------------------------------------------------------------------

    def _fill_buffer(self, *, max_inflight: int) -> None:
        """Programa nuevos jobs hasta llenar la ventana de ejecucion."""
        assert self._pool is not None

        while self._pending and len(self._inflight) < max_inflight:
            _, _, job = heapq.heappop(self._pending)

            if job.job_id in self._cancelled_job_ids:
                self._cancel_pending(job)
                continue

            scheduled_at = dt.datetime.now(dt.timezone.utc)

            future = self._pool.schedule(
                _execute_job,
                args=(job.file_path, job.algorithm),
                timeout=job.timeout,
            )
            self._inflight[future] = RuntimeMeta(job=job, scheduled_at=scheduled_at)
            self.metrics.scheduled_attempts += 1

            self.logger.debug(
                "SCHEDULE job=%s algo=%s attempt=%d/%d priority=%d timeout=%s",
                job.job_id,
                job.algorithm.name,
                job.attempt,
                job.max_attempts,
                job.priority,
                job.timeout,
            )

    def _drain_cancelled_pending(self) -> None:
        """
        No podemos eliminar eficientemente del heap cualquier job arbitrario.
        Asi que los cancelados pendientes se filtran cuando llegan arriba.
        """
        while self._pending and self._pending[0][2].job_id in self._cancelled_job_ids:
            _, _, job = heapq.heappop(self._pending)
            self._cancel_pending(job)

    def _cancel_pending(self, job: Job) -> None:
        """Contabiliza y emite el record de un job cancelado antes de ejecutarse."""
        self.metrics.queue_skipped_cancelled += 1
        self._ready_records.append(self._make_cancelled_record(job))

    def _finalize_future(self, future: Any) -> ResultRecord:
        """
        Convierte un future ya terminado en un ResultRecord.
        Si aplica retry, reencola un nuevo intento y marca will_retry=True.
        """
        meta = self._inflight.pop(future)
        job = meta.job
        ended_at = dt.datetime.now(dt.timezone.utc)
        scheduled_at_iso = meta.scheduled_at.isoformat()
        elapsed = round((ended_at - meta.scheduled_at).total_seconds(), 6)

        try:
            outcome: JobOutcome = future.result()

            self.logger.debug(
                "DONE job=%s algo=%s attempt=%d/%d ok=%s worker_duration=%.3f wall_clock=%.3f pid=%d",
                job.job_id,
                job.algorithm.name,
                job.attempt,
                job.max_attempts,
                outcome.ok,
                outcome.duration_s,
                elapsed,
                outcome.pid,
            )

            if outcome.ok:
                self.metrics.completed_ok += 1
                self.logger.debug(
                    "PAYLOAD job=%s algo=%s payload=%s",
                    job.job_id,
                    job.algorithm.name,
                    outcome.payload,
                )
                return self._make_record(
                    job=job,
                    ended_at=ended_at,
                    started_at=scheduled_at_iso,
                    duration_s=elapsed,
                    timed_out=False,
                    cancelled=False,
                    error=False,
                    will_retry=False,
                    result=outcome.payload,
                )

            self.metrics.algorithm_errors += 1
            return self._failure_record(
                job=job,
                reason=RETRY_ERROR,
                ended_at=ended_at,
                started_at=scheduled_at_iso,
                duration_s=elapsed,
                timed_out=False,
                cancelled=False,
                result=outcome.payload,
            )

        except FuturesTimeoutError:
            self.metrics.timeouts += 1
            self.logger.warning(
                "TIMEOUT job=%s algo=%s attempt=%d/%d limit=%.3f wall_clock=%.3f",
                job.job_id,
                job.algorithm.name,
                job.attempt,
                job.max_attempts,
                job.timeout,
                elapsed,
            )
            return self._failure_record(
                job=job,
                reason=RETRY_TIMEOUT,
                ended_at=ended_at,
                started_at=scheduled_at_iso,
                duration_s=elapsed,
                timed_out=True,
                cancelled=False,
                result={"error": f"TimeoutError: superado el limite de {job.timeout} s"},
            )

        except CancelledError:
            self.metrics.cancelled += 1
            return self._make_record(
                job=job,
                ended_at=ended_at,
                started_at=scheduled_at_iso,
                duration_s=elapsed,
                timed_out=False,
                cancelled=True,
                error=True,
                will_retry=False,
                result={"error": "CancelledError: job cancelado"},
            )

        except ProcessExpired as exc:
            self.metrics.process_expired += 1
            self.logger.error(
                "PROCESS EXPIRED job=%s algo=%s attempt=%d/%d: %s",
                job.job_id,
                job.algorithm.name,
                job.attempt,
                job.max_attempts,
                exc,
            )
            return self._failure_record(
                job=job,
                reason=RETRY_EXPIRED,
                ended_at=ended_at,
                started_at=scheduled_at_iso,
                duration_s=elapsed,
                timed_out=False,
                cancelled=False,
                result={"error": f"ProcessExpired: {exc}"},
            )

        except Exception as exc:
            self.metrics.framework_errors += 1
            self.logger.exception(
                "FRAMEWORK ERROR job=%s algo=%s attempt=%d/%d",
                job.job_id,
                job.algorithm.name,
                job.attempt,
                job.max_attempts,
            )
            return self._failure_record(
                job=job,
                reason=RETRY_FRAMEWORK,
                ended_at=ended_at,
                started_at=scheduled_at_iso,
                duration_s=elapsed,
                timed_out=False,
                cancelled=False,
                result={"error": f"{type(exc).__name__}: {exc}"},
            )

    def _failure_record(
        self,
        *,
        job: Job,
        reason: str,
        ended_at: dt.datetime,
        started_at: str,
        duration_s: float,
        timed_out: bool,
        cancelled: bool,
        result: dict[str, Any],
    ) -> ResultRecord:
        """Gestiona retry y construye el ResultRecord para cualquier fallo."""
        will_retry = self._should_retry(job, reason=reason)
        if will_retry:
            self._requeue(job)
        return self._make_record(
            job=job,
            ended_at=ended_at,
            started_at=started_at,
            duration_s=duration_s,
            timed_out=timed_out,
            cancelled=cancelled,
            error=True,
            will_retry=will_retry,
            result=result,
        )

    def _should_retry(self, job: Job, *, reason: str) -> bool:
        """Decide si un job debe reintentarse."""
        if job.job_id in self._cancelled_job_ids:
            return False
        policy = job.retry_policy if job.retry_policy is not None else self.retry_policy
        if reason not in policy.retry_on:
            return False
        return job.attempt < job.max_attempts

    def _requeue(self, job: Job) -> None:
        """Reencola el job con el siguiente numero de intento."""
        next_job = replace(job, attempt=job.attempt + 1)
        heapq.heappush(self._pending, (next_job.priority, next(self._seq), next_job))
        self.metrics.retries_scheduled += 1

        self.logger.info(
            "RETRY job=%s algo=%s next_attempt=%d/%d",
            next_job.job_id,
            next_job.algorithm.name,
            next_job.attempt,
            next_job.max_attempts,
        )

    def _make_record(
        self,
        *,
        job: Job,
        ended_at: dt.datetime,
        started_at: str,
        duration_s: float,
        timed_out: bool,
        cancelled: bool,
        error: bool,
        will_retry: bool,
        result: dict[str, Any],
    ) -> ResultRecord:
        return ResultRecord(
            job_id=job.job_id,
            file=str(job.file_path),
            algorithm=job.algorithm.name,
            priority=job.priority,
            attempt=job.attempt,
            max_attempts=job.max_attempts,
            duration_s=round(duration_s, 6),
            started_at=started_at,
            ended_at=ended_at.isoformat(),
            timed_out=timed_out,
            cancelled=cancelled,
            error=error,
            will_retry=will_retry,
            result=result,
        )

    def _make_cancelled_record(self, job: Job) -> ResultRecord:
        """Record para jobs cancelados antes de llegar a ejecutarse."""
        now = dt.datetime.now(dt.timezone.utc)
        self.metrics.cancelled += 1
        return self._make_record(
            job=job,
            ended_at=now,
            started_at=now.isoformat(),
            duration_s=0.0,
            timed_out=False,
            cancelled=True,
            error=True,
            will_retry=False,
            result={"error": "CancelledError: job cancelado antes de ejecutarse"},
        )

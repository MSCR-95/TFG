"""
framework.core
==============
Contratos y modelos de datos del framework de ejecución de algoritmos.

Este módulo define:

- Las constantes de razón de reintento (RETRY_*).
- ``Algorithm``: clase base abstracta que todo algoritmo debe implementar.
- ``Job``: unidad lógica que el scheduler gestiona.
- ``JobOutcome``: payload interno que devuelve el worker al proceso principal.
- ``ResultRecord``: registro público emitido en streaming por el runner.
- ``Metrics``: contadores de observabilidad del runtime.
- ``RetryPolicy``: política de reintentos configurable por razón.
- ``RuntimeMeta``: metadatos de scheduling por future en vuelo.

Ningún componente de este módulo tiene estado global ni efectos laterales.
"""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


# ============================================================================
# Constantes de retry
# ============================================================================

RETRY_TIMEOUT = "timeout"
"""El intento agotó su límite de tiempo (``job.timeout``)."""

RETRY_ERROR = "error"
"""El algoritmo lanzó una excepción no capturada dentro de ``run()``."""

RETRY_EXPIRED = "expired"
"""El proceso worker murió inesperadamente (``os._exit``, SIGKILL, OOM…)."""

RETRY_FRAMEWORK = "framework"
"""Error interno del runtime no clasificable en las categorías anteriores."""

VALID_RETRY_REASONS = {
    RETRY_TIMEOUT,
    RETRY_ERROR,
    RETRY_EXPIRED,
    RETRY_FRAMEWORK,
}
"""Conjunto de razones válidas para ``RetryPolicy.retry_on``."""


# ============================================================================
# Contrato base de algoritmos
# ============================================================================

class Algorithm(ABC):
    """
    Interfaz base de todos los algoritmos del framework.

    Para registrar un algoritmo nuevo decora la subclase con
    ``@register_algorithm`` de ``framework.registry``::

        @register_algorithm("mi_algo", family="mi_familia")
        class MiAlgoritmo(Algorithm):
            def run(self, file_path: Path) -> dict[str, Any]:
                ...

    Ciclo de vida de un intento
    ---------------------------
    El worker ejecuta los tres métodos en este orden:

    1. ``before_run(file_path)`` — preparación opcional.
    2. ``run(file_path)``        — lógica principal (obligatoria).
    3. ``after_run(file_path, result)`` — post-proceso opcional,
       sólo se llama si ``run()`` terminó sin excepción.

    Si cualquiera de los tres lanza una excepción el worker la captura,
    marca el intento como fallido y no llama a los métodos posteriores.

    Requisitos de implementación
    ----------------------------
    - La clase debe definirse a nivel de módulo (no como clase interna ni
      lambda) para ser serializable con ``pickle`` en Windows/spawn.
    - ``run()`` debe devolver un ``dict`` con valores serializables a JSON.
    - No uses la clave ``"error"`` en resultados exitosos; está reservada
      para el framework cuando el intento falla.
    """

    __algo_name__: str = ""
    """Sobrescrito automáticamente por ``@register_algorithm``."""

    @property
    def name(self) -> str:
        """Nombre canónico del algoritmo (minúsculas, sin espacios)."""
        return self.__algo_name__ or self.__class__.__name__.lower()

    def before_run(self, file_path: Any) -> None:
        """
        Hook opcional ejecutado antes de ``run()``.

        Útil para validaciones ligeras, preparación de recursos o logging
        específico del algoritmo. La implementación por defecto no hace nada.
        """

    @abstractmethod
    def run(self, file_path: Any) -> dict[str, Any]:
        """
        Ejecuta el algoritmo sobre ``file_path`` y devuelve el resultado.

        El valor de retorno debe ser un ``dict`` cuyos valores sean
        serializables a JSON (str, int, float, bool, list, dict anidados
        con tipos básicos).

        Parameters
        ----------
        file_path:
            Ruta al fichero de entrada proporcionada por el scheduler.

        Returns
        -------
        dict[str, Any]
            Resultado del algoritmo. Se almacenará en ``ResultRecord.result``.

        Raises
        ------
        Exception
            Cualquier excepción no capturada aquí es interceptada por el
            worker, que marca el intento como fallido (``ok=False``) y
            serializa el tipo y mensaje en ``payload["error"]``.
        """

    def after_run(self, file_path: Any, result: dict[str, Any]) -> None:
        """
        Hook opcional ejecutado después de ``run()`` cuando éste tuvo éxito.

        Recibe el resultado devuelto por ``run()`` por si necesita
        enriquecerlo o registrar algo. La implementación por defecto no
        hace nada.

        Note
        ----
        Si ``run()`` lanzó una excepción, este método **no** se llama.
        """


# ============================================================================
# Modelos de datos
# ============================================================================

@dataclass(frozen=True)
class Job:
    """
    Unidad lógica que el scheduler gestiona internamente.

    Inmutable (``frozen=True``). Cuando el runner reencola un reintento
    crea una nueva instancia con ``attempt`` incrementado usando
    ``dataclasses.replace()``.

    Attributes
    ----------
    job_id:
        Identificador único dentro de un ``RunnerV2``. Requerido.
    file_path:
        Ruta al fichero de entrada que se pasará al algoritmo.
    algorithm:
        Instancia del algoritmo a ejecutar.
    priority:
        Prioridad en la cola. Menor valor = mayor prioridad. Default: 100.
        Jobs con la misma prioridad se despachan en orden FIFO.
    timeout:
        Límite de tiempo en segundos para este intento. ``None`` = sin límite.
    attempt:
        Número del intento actual (empieza en 1).
    max_attempts:
        Número máximo de intentos permitidos (1 = sin reintentos).
    metadata:
        Datos adicionales arbitrarios del llamante. No los usa el runtime.
    retry_policy:
        Política de reintentos específica para este job. Si es ``None``
        se usa la política global del runner.
    """

    job_id: str
    file_path: Path
    algorithm: Algorithm
    priority: int = 100
    timeout: float | None = None
    attempt: int = 1
    max_attempts: int = 1
    metadata: dict[str, Any] | None = None
    retry_policy: RetryPolicy | None = None


@dataclass(frozen=True)
class JobOutcome:
    """
    Resultado interno devuelto por el worker al proceso principal.

    Este objeto es un detalle de implementación del runtime. El contrato
    público de salida hacia el usuario es ``ResultRecord``.

    Attributes
    ----------
    ok:
        ``True`` si el algoritmo completó sin excepción; ``False`` si lanzó
        alguna que fue capturada dentro del worker.
    payload:
        Resultado del algoritmo si ``ok=True``, o dict con clave ``"error"``
        describiendo la excepción si ``ok=False``.
    started_at:
        ISO-8601 del momento en que el worker comenzó el intento (UTC).
    ended_at:
        ISO-8601 del momento en que el worker terminó el intento (UTC).
    duration_s:
        Tiempo de ejecución medido dentro del worker en segundos.
        Difiere de ``ResultRecord.duration_s``, que mide el wall-clock
        completo incluyendo latencia de scheduling y comunicación IPC.
    pid:
        PID del proceso worker que ejecutó este intento.
    """

    ok: bool
    payload: dict[str, Any]
    started_at: str
    ended_at: str
    duration_s: float
    pid: int


@dataclass(frozen=True)
class ResultRecord:
    """
    Registro público emitido por el runner en streaming.

    Representa **un intento** (no un job completo). Si un job se reintenta
    tres veces, el runner emite tres ``ResultRecord`` con ``attempt`` 1, 2
    y 3 respectivamente. Los dos primeros tendrán ``will_retry=True``.

    Attributes
    ----------
    job_id:
        Identificador del job lógico al que pertenece este intento.
    file:
        Ruta al fichero de entrada (como string).
    algorithm:
        Nombre canónico del algoritmo.
    priority:
        Prioridad con la que el job fue enviado.
    attempt:
        Número de este intento (1-based).
    max_attempts:
        Total de intentos permitidos para este job.
    duration_s:
        Tiempo wall-clock del intento completo en segundos, desde que el
        runner lo programa hasta que observa su finalización. Consistente
        para éxito, error, timeout, cancelación y errores de infraestructura.
    started_at:
        ISO-8601 del momento en que el runner despachó este intento (UTC).
    ended_at:
        ISO-8601 del momento en que el runner observó la finalización (UTC).
    timed_out:
        ``True`` si el intento terminó por timeout.
    cancelled:
        ``True`` si el intento fue cancelado (antes o durante la ejecución).
    error:
        ``True`` si el intento terminó con cualquier tipo de fallo
        (incluye timeout y cancelación).
    will_retry:
        ``True`` si este intento falló pero se ha reencolado un nuevo intento.
        Implica ``error=True``.
    result:
        Payload del algoritmo si tuvo éxito, o dict con clave ``"error"``
        describiendo el fallo.
    """

    job_id: str
    file: str
    algorithm: str
    priority: int
    attempt: int
    max_attempts: int
    duration_s: float
    started_at: str
    ended_at: str
    timed_out: bool
    cancelled: bool
    error: bool
    will_retry: bool
    result: dict[str, Any]


@dataclass
class Metrics:
    """
    Contadores de observabilidad del runtime.

    Se actualizan en tiempo real durante ``run_stream()``. Accede a ellos
    después de completar la ejecución o en cualquier punto intermedio.

    Attributes
    ----------
    submitted_jobs:
        Total de jobs enviados con ``submit()`` o ``submit_directory()``.
    scheduled_attempts:
        Total de intentos despachados al pool (incluye reintentos).
    completed_ok:
        Intentos que terminaron correctamente (``ok=True``).
    algorithm_errors:
        Intentos fallidos por excepción del algoritmo.
    timeouts:
        Intentos terminados por superar ``job.timeout``.
    cancelled:
        Intentos cancelados, ya sea desde la cola o desde el pool.
    process_expired:
        Workers que murieron inesperadamente (``ProcessExpired``).
    framework_errors:
        Errores internos del runtime no clasificados en las otras categorías.
    retries_scheduled:
        Reintentos reencolados (siempre ≤ ``scheduled_attempts``).
    queue_skipped_cancelled:
        Jobs descartados desde la cola pendiente antes de llegar al worker.

    Examples
    --------
    Interpretar un resultado::

        m = runner.metrics
        print(m.summary())
        # {'submitted_jobs': 100, 'completed_ok': 95, 'timeouts': 5, ...}

        # Si scheduled_attempts > submitted_jobs, hubo reintentos:
        extra = m.scheduled_attempts - m.submitted_jobs
    """

    submitted_jobs: int = 0
    scheduled_attempts: int = 0
    completed_ok: int = 0
    algorithm_errors: int = 0
    timeouts: int = 0
    cancelled: int = 0
    process_expired: int = 0
    framework_errors: int = 0
    retries_scheduled: int = 0
    queue_skipped_cancelled: int = 0

    def summary(self) -> dict[str, int]:
        """
        Devuelve todos los contadores como ``dict``.

        El diccionario refleja el estado actual de las métricas en el momento
        de la llamada. Útil para logging estructurado o para mostrar un
        resumen al final de una ejecución::

            print(runner.metrics.summary())
        """
        return asdict(self)


@dataclass(frozen=True)
class RetryPolicy:
    """
    Política de reintentos para un runner o para un job individual.

    Attributes
    ----------
    retries:
        Número de reintentos **adicionales** al intento original.

        - ``0`` → sin reintentos (sólo 1 intento en total).
        - ``1`` → un reintento (2 intentos en total).
        - ``n`` → n reintentos (n+1 intentos en total).

    retry_on:
        Conjunto de razones que habilitan el reintento. Sólo se reintenta
        si el fallo del intento coincide con al menos una razón de este
        conjunto.

        Valores válidos (usar las constantes ``RETRY_*``):

        - ``"timeout"``    — el intento agotó su tiempo límite.
        - ``"error"``      — el algoritmo lanzó una excepción.
        - ``"expired"``    — el worker murió inesperadamente.
        - ``"framework"``  — error interno del runtime.

    Examples
    --------
    Reintentar sólo timeouts, máximo 2 veces adicionales::

        from framework import RetryPolicy, RETRY_TIMEOUT
        policy = RetryPolicy(retries=2, retry_on=frozenset({RETRY_TIMEOUT}))

    Reintentar timeouts y errores de algoritmo::

        from framework import RetryPolicy, RETRY_TIMEOUT, RETRY_ERROR
        policy = RetryPolicy(
            retries=1,
            retry_on=frozenset({RETRY_TIMEOUT, RETRY_ERROR}),
        )

    Política específica por job::

        runner.submit(
            job_id="j1", file_path=f, algorithm="mi_algo",
            retry_policy=RetryPolicy(retries=3, retry_on=frozenset({RETRY_ERROR})),
        )
    """

    retries: int = 0
    retry_on: frozenset[str] = frozenset({RETRY_TIMEOUT})

    def __post_init__(self) -> None:
        invalid = set(self.retry_on) - VALID_RETRY_REASONS
        if invalid:
            raise ValueError(
                f"retry_on contiene valores no válidos: {sorted(invalid)}. "
                f"Permitidos: {sorted(VALID_RETRY_REASONS)}"
            )


@dataclass(frozen=True)
class RuntimeMeta:
    """
    Metadatos de scheduling que el runner mantiene por cada future en vuelo.

    Sólo se usa internamente en ``RunnerV2._inflight``. Permite calcular
    el wall-clock del intento al observar la finalización del future.

    Attributes
    ----------
    job:
        El job asociado a este future.
    scheduled_at:
        Momento (UTC) en que el runner despachó el intento al pool.
    """

    job: Job
    scheduled_at: dt.datetime


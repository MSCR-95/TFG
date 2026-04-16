"""
framework.worker
================
Función de ejecución aislada que corre dentro del proceso worker.

Por qué es una función de módulo y no un método
------------------------------------------------
Python en Windows usa el modo de inicio ``spawn`` para los procesos hijo:
el intérprete arranca desde cero y necesita importar el módulo para
reconstruir el callable antes de ejecutarlo. Las funciones definidas a nivel
de módulo son directamente serializables con ``pickle``; los métodos de
instancia no lo son si la instancia contiene referencias no serializables.

Ciclo de vida dentro del worker
--------------------------------
::

    before_run(file_path)
         │
         ▼
      run(file_path) ──── excepción ──► JobOutcome(ok=False, payload={"error": ...})
         │
         ▼
    after_run(file_path, resultado)
         │
         ▼
    JobOutcome(ok=True, payload=resultado)

Cualquier excepción en cualquiera de las tres fases produce
``ok=False``. ``after_run`` sólo se invoca si ``run()`` tuvo éxito.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

from framework.core import Algorithm, JobOutcome


# ============================================================================
# Worker function
# ============================================================================

def _execute_job(file_path: Path, algorithm: Algorithm) -> JobOutcome:
    """
    Ejecuta un intento de algoritmo de forma aislada dentro del worker process.

    Esta función es el único punto de entrada del proceso hijo. Llama a los
    tres métodos del ciclo de vida del algoritmo en orden y captura cualquier
    excepción para devolverla serializada como ``JobOutcome``.

    Parameters
    ----------
    file_path:
        Ruta al fichero de entrada, tal como la proporcionó el scheduler.
    algorithm:
        Instancia del algoritmo a ejecutar. Debe ser serializable con
        ``pickle`` (es decir, definida a nivel de módulo).

    Returns
    -------
    JobOutcome
        Resultado del intento. ``ok=True`` si ``run()`` y ambos hooks
        completaron sin excepción; ``ok=False`` en cualquier otro caso.
        El campo ``duration_s`` mide el tiempo interno del worker; el
        wall-clock completo incluyendo scheduling lo mide el runner.

    Notes
    -----
    - El PID devuelto permite rastrear qué proceso ejecutó cada intento.
    - Los timestamps usan UTC para coherencia entre máquinas con zonas
      horarias distintas.
    """
    started_at = dt.datetime.now(dt.timezone.utc)
    t0 = started_at.timestamp()

    try:
        algorithm.before_run(file_path)
        payload = algorithm.run(file_path)
        algorithm.after_run(file_path, payload)
        ok = True
    except Exception as exc:
        ok = False
        payload = {"error": f"{type(exc).__name__}: {exc}"}

    ended_at = dt.datetime.now(dt.timezone.utc)
    return JobOutcome(
        ok=ok,
        payload=payload,
        started_at=started_at.isoformat(),
        ended_at=ended_at.isoformat(),
        duration_s=round(ended_at.timestamp() - t0, 6),
        pid=os.getpid(),
    )

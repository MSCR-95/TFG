"""
framework.registry
==================
Registro dinámico de algoritmos por nombre y familia.

El registro es un par de dicts globales que se populan mediante el decorador
``@register_algorithm`` cuando Python importa los módulos de algoritmos.

Patrones
--------
- **Registry / Plugin**: los algoritmos se auto-registran al ser importados,
  sin necesidad de mantener un catálogo centralizado en el código del runtime.
- **Factory**: ``build_algorithm`` y ``build_algorithms_by_family`` actúan
  como factories que instancian algoritmos a partir de sus nombres.

Normalización
-------------
Todos los nombres de algoritmo y familia se normalizan a minúsculas y sin
espacios iniciales/finales antes de almacenarse o buscarse. Esto hace el
sistema insensible a mayúsculas en el CLI y en la API programática.
"""

from __future__ import annotations

from collections.abc import Sequence

from framework.core import Algorithm


# ============================================================================
# Registro de algoritmos
# ============================================================================

_ALGO_REGISTRY: dict[str, type[Algorithm]] = {}
"""Mapa nombre_normalizado → clase de algoritmo."""

_FAMILY_REGISTRY: dict[str, list[str]] = {}
"""Mapa familia_normalizada → lista de nombres de algoritmos."""


def register_algorithm(name: str, *, family: str | None = None):
    """
    Decorador que registra una clase de algoritmo por nombre y familia.

    Debe aplicarse a una subclase de ``Algorithm`` definida a nivel de módulo.
    Puede usarse como decorador de clase o llamarse explícitamente.

    Parameters
    ----------
    name:
        Identificador único del algoritmo (se normaliza a minúsculas).
        Es el valor que se usa en ``--algos`` en el CLI y en
        ``RunnerV2.submit(algorithm="nombre")``.
    family:
        Grupo lógico al que pertenece el algoritmo. Opcional. Permite
        seleccionar todos los algoritmos de una familia con ``--family``
        o con ``build_algorithms_by_family()``.

    Returns
    -------
    type[Algorithm]
        La misma clase decorada, sin modificar su comportamiento.

    Raises
    ------
    ValueError
        Si el nombre (normalizado) ya está registrado.

    Examples
    --------
    Como decorador::

        @register_algorithm("word_count", family="prueba")
        class WordCountAlgorithm(Algorithm):
            def run(self, file_path: Path) -> dict[str, Any]:
                ...

    Llamada explícita (útil en tests)::

        register_algorithm("test_algo")(MyAlgorithm)
    """

    def _wrap(cls: type[Algorithm]):
        key = name.strip().lower()
        if key in _ALGO_REGISTRY:
            raise ValueError(f"Algorithm '{name}' ya está registrado")
        _ALGO_REGISTRY[key] = cls
        cls.__algo_name__ = key  # type: ignore[attr-defined]

        if family is not None:
            fkey = family.strip().lower()
            cls.__algo_family__ = fkey  # type: ignore[attr-defined]
            _FAMILY_REGISTRY.setdefault(fkey, []).append(key)

        return cls

    return _wrap


def build_algorithm(name: str) -> Algorithm:
    """
    Instancia un algoritmo a partir de su nombre registrado.

    Parameters
    ----------
    name:
        Nombre del algoritmo. Insensible a mayúsculas y espacios laterales.

    Returns
    -------
    Algorithm
        Nueva instancia del algoritmo. Cada llamada devuelve una instancia
        diferente.

    Raises
    ------
    KeyError
        Si el nombre no está registrado. El mensaje incluye la lista de
        nombres disponibles.
    """
    key = name.strip().lower()
    cls = _ALGO_REGISTRY.get(key)
    if cls is None:
        raise KeyError(
            f"Algoritmo desconocido: '{name}'. Registrados: {list(_ALGO_REGISTRY)}"
        )
    return cls()


def build_algorithms(names: Sequence[str]) -> list[Algorithm]:
    """
    Instancia múltiples algoritmos a partir de una lista de nombres.

    Parameters
    ----------
    names:
        Secuencia de nombres. Puede estar vacía.

    Returns
    -------
    list[Algorithm]
        Lista de instancias en el mismo orden que ``names``.

    Raises
    ------
    KeyError
        Si alguno de los nombres no está registrado.
    """
    return [build_algorithm(name) for name in names]


def build_algorithms_by_family(family: str) -> list[Algorithm]:
    """
    Instancia todos los algoritmos registrados bajo una familia.

    Parameters
    ----------
    family:
        Nombre de la familia. Insensible a mayúsculas y espacios laterales.

    Returns
    -------
    list[Algorithm]
        Una instancia de cada algoritmo de la familia, en el orden en que
        fueron registrados.

    Raises
    ------
    KeyError
        Si la familia no existe o no tiene algoritmos. El mensaje incluye
        las familias conocidas.
    """
    fkey = family.strip().lower()
    names = _FAMILY_REGISTRY.get(fkey)
    if not names:
        known = list(_FAMILY_REGISTRY)
        raise KeyError(
            f"Familia desconocida: '{family}'. Familias registradas: {known}"
        )
    return [build_algorithm(n) for n in names]


def list_families() -> list[str]:
    """
    Devuelve la lista de familias registradas, ordenada alfabéticamente.

    Returns
    -------
    list[str]
        Familias disponibles en orden lexicográfico.
    """
    return sorted(_FAMILY_REGISTRY)

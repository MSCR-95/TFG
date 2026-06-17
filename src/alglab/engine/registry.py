"""Algorithm registry: decorator-based registration and instantiation.

The registry maps string names to :class:`~alglab.engine.core.Algorithm`
subclasses.  Algorithms register themselves at import time via
``@register_algorithm("name")``.  The worker subprocess populates the registry
by importing ``alglab.algorithms`` inside its initialiser, which triggers all
``@register_algorithm`` decorators in that package.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from .core import Algorithm

_ALGO_REGISTRY: dict[str, type[Algorithm[Any]]] = {}


def register_algorithm(name: str):
    """Class decorator that registers an :class:`~alglab.engine.core.Algorithm` subclass.

    Associates the decorated class with *name* in the global registry and sets
    ``cls.__algo_name__`` to the normalised key so that
    :attr:`~alglab.engine.core.Algorithm.name` returns the correct value.

    Args:
        name: Registry key (case-insensitive, leading/trailing whitespace
            stripped).  Must be unique across the process; re-registering the
            same key raises :class:`ValueError`.

    Returns:
        A class decorator that registers the class and returns it unchanged.

    Raises:
        ValueError: If *name* is already registered.

    Example:
        >>> from alglab.engine.core import Algorithm
        >>> from alglab.engine.registry import register_algorithm
        >>> from typing import Any
        >>>
        >>> @register_algorithm("my_algo")
        ... class MyAlgo(Algorithm[None]):
        ...     def run(self, context: None, *, seed=None) -> dict[str, Any]:
        ...         return {}
    """

    def _wrap(cls: type[Algorithm[Any]]):
        key = name.strip().lower()
        if key in _ALGO_REGISTRY:
            raise ValueError(f"Algorithm '{name}' is already registered")
        _ALGO_REGISTRY[key] = cls
        cls.__algo_name__ = key
        logger.debug("Registered algorithm: {}", key)
        return cls

    return _wrap


def build_algorithm(name: str, **kwargs: Any) -> Algorithm[Any]:
    """Instantiate a registered algorithm by name.

    Args:
        name: Registry key.  Case-insensitive; leading/trailing whitespace
            is stripped.
        **kwargs: Constructor arguments forwarded to the algorithm class.
            When empty, the class is instantiated with no arguments.

    Returns:
        A new instance of the algorithm class associated with *name*.

    Raises:
        KeyError: If *name* is not in the registry.

    Example:
        >>> algo = build_algorithm("maxsat_brute")
        >>> algo = build_algorithm("maxsat_qubo_sa", num_reads=500)
    """
    key = name.strip().lower()
    cls = _ALGO_REGISTRY.get(key)
    if cls is None:
        raise KeyError(f"Unknown algorithm: '{name}'. Registered: {list(_ALGO_REGISTRY)}")
    if kwargs:
        return cls(**kwargs)
    return cls()


def parse_algo_spec(spec: str) -> tuple[str, dict[str, Any]]:
    """Parse an algorithm spec string into a ``(name, kwargs)`` pair.

    Accepts two formats:

    - ``"name"`` — plain algorithm name, no parameters.
    - ``"name:key=value,key=value"`` — name with one or more ``key=value``
      parameter pairs separated by commas.

    Values are coerced to ``bool`` (``true``/``false``), ``int``, or
    ``float`` in that order; values that do not match any numeric format
    remain as ``str``.

    Args:
        spec: Algorithm spec string from the CLI or YAML config.

    Returns:
        A tuple of the algorithm name and a dict of parsed keyword arguments.

    Raises:
        ValueError: If the spec is malformed (empty name, missing ``=``,
            empty key or value, empty parameter block after ``:``)

    Example:
        >>> parse_algo_spec("maxsat_qubo_sa")
        ('maxsat_qubo_sa', {})
        >>> parse_algo_spec("maxsat_qubo_sa:num_reads=10,include_assignment=true")
        ('maxsat_qubo_sa', {'num_reads': 10, 'include_assignment': True})
    """
    if ":" not in spec:
        return spec.strip(), {}
    name, rest = spec.split(":", 1)
    name = name.strip()
    if not name:
        raise ValueError(f"Invalid algorithm spec '{spec}': missing algorithm name")
    if not rest.strip():
        raise ValueError(f"Invalid algorithm spec '{spec}': missing key=value parameters")

    kwargs: dict[str, Any] = {}
    for kv in rest.split(","):
        kv = kv.strip()
        if not kv or "=" not in kv:
            raise ValueError(
                f"Invalid algorithm spec '{spec}': parameters must use key=value syntax"
            )
        k, v = kv.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            raise ValueError(f"Invalid algorithm spec '{spec}': empty parameter name")
        if not v:
            raise ValueError(f"Invalid algorithm spec '{spec}': empty value for '{k}'")
        v_lower = v.lower()
        if v_lower == "true":
            kwargs[k] = True
            continue
        if v_lower == "false":
            kwargs[k] = False
            continue
        try:
            kwargs[k] = int(v)
        except ValueError:
            try:
                kwargs[k] = float(v)
            except ValueError:
                kwargs[k] = v
    return name, kwargs

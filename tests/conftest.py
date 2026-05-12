import multiprocessing

# Necesario en Windows (spawn) para que los worker processes arranquen correctamente
multiprocessing.freeze_support()

# Activa todos los @register_algorithm antes de que corran los tests
import algorithms  # noqa: F401, E402

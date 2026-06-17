import multiprocessing

# Required on Windows (spawn mode) so worker processes start correctly
multiprocessing.freeze_support()

# Activate max2sat algorithms in the registry
import alglab.algorithms  # noqa: F401, E402

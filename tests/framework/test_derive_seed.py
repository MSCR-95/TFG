from __future__ import annotations

from pathlib import Path

from alglab.engine import derive_seed


class TestDeriveSeed:
    def test_deterministic(self):
        p = Path("/tmp/a.cnf")
        assert derive_seed(42, p) == derive_seed(42, p)

    def test_unique_per_file(self):
        assert derive_seed(42, Path("/tmp/a.cnf")) != derive_seed(42, Path("/tmp/b.cnf"))

    def test_sensitive_to_global_seed(self):
        p = Path("/tmp/a.cnf")
        assert derive_seed(1, p) != derive_seed(2, p)

    def test_returns_nonnegative_int(self):
        result = derive_seed(0, Path("/tmp/x.cnf"))
        assert isinstance(result, int) and result >= 0

    def test_global_seed_zero_works(self):
        assert derive_seed(0, Path("/tmp/a.cnf")) != derive_seed(0, Path("/tmp/b.cnf"))

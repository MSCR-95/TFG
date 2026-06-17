"""Shared fixtures for the study module tests."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import pandas as pd
import pytest


@pytest.fixture
def sample_records() -> list[dict]:
    """3 instances × 2 algorithms × 2 seeds — minimal reproducible JSONL."""
    return [
        {
            "file": "f1.cnf",
            "algorithm": "rs",
            "status": "ok",
            "result": {"satisfaction_ratio": 0.95, "verification_mismatch": False},
            "seed": 42,
            "run_duration_s": 0.02,
        },
        {
            "file": "f1.cnf",
            "algorithm": "rs",
            "status": "ok",
            "result": {"satisfaction_ratio": 0.90, "verification_mismatch": False},
            "seed": 43,
            "run_duration_s": 0.03,
        },
        {
            "file": "f1.cnf",
            "algorithm": "sds",
            "status": "ok",
            "result": {"satisfaction_ratio": 1.0, "verification_mismatch": False},
            "seed": 42,
            "run_duration_s": 0.05,
        },
        {
            "file": "f1.cnf",
            "algorithm": "sds",
            "status": "ok",
            "result": {"satisfaction_ratio": 1.0, "verification_mismatch": False},
            "seed": 43,
            "run_duration_s": 0.04,
        },
        {
            "file": "f2.cnf",
            "algorithm": "rs",
            "status": "ok",
            "result": {"satisfaction_ratio": 0.80, "verification_mismatch": False},
            "seed": 42,
            "run_duration_s": 0.03,
        },
        {
            "file": "f2.cnf",
            "algorithm": "rs",
            "status": "timeout",
            "result": None,
            "seed": 43,
            "run_duration_s": None,
            "error_message": "timeout",
        },
        {
            "file": "f2.cnf",
            "algorithm": "sds",
            "status": "ok",
            "result": {"satisfaction_ratio": 0.95, "verification_mismatch": False},
            "seed": 42,
            "run_duration_s": 0.06,
        },
        {
            "file": "f3.cnf",
            "algorithm": "rs",
            "status": "ok",
            "result": {"satisfaction_ratio": 0.70, "verification_mismatch": False},
            "seed": 42,
            "run_duration_s": 0.01,
        },
        {
            "file": "f3.cnf",
            "algorithm": "sds",
            "status": "ok",
            "result": {"satisfaction_ratio": 0.85, "verification_mismatch": False},
            "seed": 42,
            "run_duration_s": 0.02,
        },
    ]


@pytest.fixture
def jsonl_path(tmp_path: Path, sample_records: list[dict]) -> Path:
    """Write sample_records to a temporary JSONL file."""
    path = tmp_path / "test_results.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for rec in sample_records:
            fh.write(json.dumps(rec) + "\n")
    return path


@pytest.fixture
def flat_df(jsonl_path: Path) -> pd.DataFrame:
    """Flat DataFrame read from the test JSONL."""
    from alglab.study.io import read_jsonl

    return read_jsonl(jsonl_path)


@pytest.fixture
def value_matrix(flat_df: pd.DataFrame) -> pd.DataFrame:
    """file × algorithm matrix for satisfaction_ratio (clean values)."""
    from alglab.study.io import prepare_matrix
    from alglab.study.schema import MeasureSpec

    spec = MeasureSpec("result.satisfaction_ratio")
    return prepare_matrix(flat_df, spec)


@pytest.fixture
def rank_matrix_fixture(value_matrix: pd.DataFrame) -> pd.DataFrame:
    """Rank matrix derived from value_matrix."""
    from alglab.study.stats import rank_matrix

    return rank_matrix(value_matrix)


@pytest.fixture
def value_matrix_3algo() -> pd.DataFrame:
    """10 instances × 3 algorithms matrix for tests requiring more data."""
    import numpy as np

    np.random.seed(42)
    return pd.DataFrame(
        {
            "algo_a": np.random.uniform(0.7, 0.95, 10),
            "algo_b": np.random.uniform(0.75, 0.98, 10),
            "algo_c": np.random.uniform(0.72, 0.92, 10),
        },
        index=[f"f{i}.cnf" for i in range(10)],
    )

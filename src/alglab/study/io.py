"""Read, clean, and transform experimental JSONL data into matrices.

Canonical pipeline::

    df_raw  = read_jsonl("results.jsonl")
    df_ok   = filter_ok(df_raw)
    df_m    = extract_measure(df_ok, MeasureSpec("result.satisfaction_ratio"))
    df_agg  = aggregate_seeds(df_m, spec)
    matrix  = pivot_matrix(df_agg)
    matrix  = impute_nan(matrix, spec)
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from .schema import MeasureSpec


def read_jsonl(path: str | Path) -> pd.DataFrame:
    """Read a JSONL file and return a flat DataFrame.

    Each line is normalized with ``pd.json_normalize``, which flattens nested
    dicts using ``'.'`` as separator.  Fields under ``result.*`` appear as
    ``result.field`` columns.

    Args:
        path: Path to the JSONL file.

    Returns:
        Flat DataFrame with one row per JSONL record.

    Raises:
        ValueError: If the file is empty or contains no valid records.
    """
    with Path(path).open(encoding="utf-8") as fh:
        records = [json.loads(line) for line in fh if line.strip()]
    if not records:
        raise ValueError(f"{path}: JSONL file is empty")
    return pd.json_normalize(records)


def filter_ok(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows with ``status == "ok"``.

    Dropped rows (timeout, error) reappear as NaN after the pivot and are
    penalized by :func:`impute_nan` using the configured strategy (by default
    ``"column_worst"``).

    Use ``filter_status=False`` in :func:`prepare_matrix` for measures where
    a non-ok record still carries a numeric value.

    Args:
        df: DataFrame returned by :func:`read_jsonl`.

    Returns:
        Copy of *df* filtered to rows where ``status == "ok"``.

    Raises:
        ValueError: If no rows remain after filtering.
        KeyError: If the ``status`` column is absent.
    """
    _check_columns(df, {"status"})
    filtered = df[df["status"] == "ok"].copy()
    if filtered.empty:
        raise ValueError("No runs have status='ok' after filtering")
    return filtered


def extract_measure(df: pd.DataFrame, spec: MeasureSpec) -> pd.DataFrame:
    """Extract a measure from the DataFrame and return ``(file, algorithm, seed, value)``.

    Args:
        df: DataFrame with at least ``file``, ``algorithm``, and the column
            named by ``spec.path``.  Typically the output of :func:`filter_ok`.
        spec: Measure specification describing which column to extract.

    Returns:
        DataFrame with columns ``file``, ``algorithm``, ``seed``, and ``value``.

    Raises:
        KeyError: If ``spec.path`` does not exist as a column, or if ``file``
            or ``algorithm`` are absent.
    """
    _check_columns(df, {"file", "algorithm"})

    if spec.path not in df.columns:
        raise KeyError(
            f"Measure '{spec.path}' not found in JSONL. "
            f"Available columns: {sorted(df.columns.tolist())}"
        )

    result = df[["file", "algorithm"]].copy()
    result["seed"] = df["seed"] if "seed" in df.columns else np.nan
    result["value"] = df[spec.path]

    if spec.cast_bool_to_int:
        result["value"] = result["value"].astype(int)

    return result


def aggregate_seeds(df: pd.DataFrame, spec: MeasureSpec) -> pd.DataFrame:
    """Aggregate runs with different seeds for each ``(instance, algorithm)`` pair.

    Strategy is determined by ``spec.aggregation``:

    ==========  ======================================
    mean        arithmetic mean
    median      median
    best        max if higher_is_better, min otherwise
    worst       min if higher_is_better, max otherwise
    ==========  ======================================

    If there is no ``seed`` column or all values are NaN, aggregation
    collapses without error (equivalent to no grouping).

    Args:
        df: DataFrame with columns ``file``, ``algorithm``, and ``value``,
            as returned by :func:`extract_measure`.
        spec: Measure specification controlling the aggregation function
            and ranking direction.

    Returns:
        DataFrame with columns ``file``, ``algorithm``, and ``value``
        (one row per ``(file, algorithm)`` pair).
    """
    if spec.aggregation == "best":
        agg_func = "max" if spec.higher_is_better else "min"
    elif spec.aggregation == "worst":
        agg_func = "min" if spec.higher_is_better else "max"
    else:
        agg_func = spec.aggregation

    return df.groupby(["file", "algorithm"], as_index=False).agg(value=("value", agg_func))


def pivot_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot to a file × algorithm matrix with ``value`` as cell contents.

    Args:
        df: DataFrame with columns ``file``, ``algorithm``, and ``value``,
            as returned by :func:`aggregate_seeds`.

    Returns:
        DataFrame indexed by ``file`` with one column per algorithm and
        cell values from the ``value`` column.
    """
    return df.pivot(index="file", columns="algorithm", values="value")


def impute_nan(
    matrix: pd.DataFrame,
    spec: MeasureSpec,
    strategy: Literal["worst", "row_worst", "column_worst", "drop", "warn"] = "column_worst",
) -> pd.DataFrame:
    """Handle NaN values in the pivoted matrix.

    ==========  ===========================================================
    column_worst
                NaN → worst observed value in that column/algorithm.
                higher_is_better=True  → min(column)
                higher_is_better=False → max(column)
    worst       compatibility alias for row_worst.
    row_worst   NaN → worst observed value in that row/benchmark.
                higher_is_better=True  → min(row)
                higher_is_better=False → max(row)
    drop        remove rows (instances) that contain any NaN.
    warn        emit a warning but do not modify data.
    ==========  ===========================================================
    """
    nan_mask = matrix.isna()
    if not nan_mask.any().any():
        return matrix

    if strategy == "drop":
        return matrix.dropna()

    if strategy == "warn":
        warnings.warn(
            f"Matrix contains {int(nan_mask.sum().sum())} NaN(s). "
            f"Consider nan_strategy='column_worst' or 'drop'.",
            stacklevel=2,
        )
        return matrix

    if strategy in {"worst", "row_worst"}:
        return _impute_row_worst(matrix, spec)

    if strategy != "column_worst":
        raise ValueError(f"Unknown imputation strategy: {strategy}")

    # strategy == "column_worst"
    result = matrix.copy()
    for col in result.columns:
        col_nan = result[col].isna()
        if not col_nan.any():
            continue
        fill_val = result[col].min() if spec.higher_is_better else result[col].max()
        if pd.isna(fill_val):
            raise ValueError(
                f"Column '{col}' has no non-NaN values; cannot impute with strategy 'column_worst'"
            )
        result.loc[col_nan, col] = fill_val
    return result


def _impute_row_worst(matrix: pd.DataFrame, spec: MeasureSpec) -> pd.DataFrame:
    result = matrix.copy()
    for idx in result.index:
        row_nan = result.loc[idx].isna()
        if not row_nan.any():
            continue
        observed = result.loc[idx, ~row_nan]
        fill_val = observed.min() if spec.higher_is_better else observed.max()
        if pd.isna(fill_val):
            raise ValueError(
                f"Row '{idx}' has no non-NaN values; cannot impute with strategy 'row_worst'"
            )
        result.loc[idx, row_nan] = fill_val
    return result


def _check_columns(df: pd.DataFrame, required: set[str]) -> None:
    missing = required - set(df.columns)
    if missing:
        raise KeyError(
            f"Missing required columns: {missing}. Available columns: {list(df.columns)}"
        )


def build_matrix(
    jsonl_path: Path,
    spec: MeasureSpec,
    *,
    nan_strategy: Literal["worst", "row_worst", "column_worst", "drop", "warn"] = "column_worst",
) -> pd.DataFrame:
    """Read JSONL and return a file × algorithm matrix for *spec*.

    Convenience wrapper for ``read_jsonl → prepare_matrix``.
    Resolves relative paths against the current working directory.

    Args:
        jsonl_path: Path to the JSONL results file produced by ``alglab run``.
        spec: Measure specification describing the metric to extract.
        nan_strategy: Imputation strategy for NaN values after the pivot.
            Defaults to ``"column_worst"``.

    Returns:
        File × algorithm value matrix ready for statistical analysis.
    """
    df = read_jsonl(Path(jsonl_path))
    return prepare_matrix(df, spec, filter_status=True, nan_strategy=nan_strategy)


def prepare_matrix(
    df: pd.DataFrame,
    spec: MeasureSpec,
    *,
    filter_status: bool = True,
    nan_strategy: Literal["worst", "row_worst", "column_worst", "drop", "warn"] = "column_worst",
) -> pd.DataFrame:
    """Full pipeline: filter → extract → aggregate → pivot → impute.

    Args:
        df: Raw DataFrame from :func:`read_jsonl`.
        spec: Measure specification describing the metric to extract.
        filter_status: If ``True``, discard runs with ``status != "ok"``
            (timeouts, errors).  Gaps are penalized later by imputation.
            Use ``False`` for measures where a non-ok record carries a numeric
            value.
        nan_strategy: Imputation strategy for NaN values after the pivot.
            Defaults to ``"column_worst"``; ``"worst"`` remains a compatibility
            alias for row-wise worst imputation.

    Returns:
        A file × algorithm matrix ready for statistical analysis.
    """
    if filter_status:
        df = filter_ok(df)
    df = extract_measure(df, spec)
    df = aggregate_seeds(df, spec)
    matrix = pivot_matrix(df)
    return impute_nan(matrix, spec, strategy=nan_strategy)

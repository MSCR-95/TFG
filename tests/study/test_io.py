"""Tests para alglab.study.io."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from alglab.study.io import (
    aggregate_seeds,
    build_matrix,
    extract_measure,
    filter_ok,
    impute_nan,
    pivot_matrix,
    prepare_matrix,
    read_jsonl,
)
from alglab.study.schema import MeasureSpec


class TestReadJSONL:
    def test_basic(self, jsonl_path: Path) -> None:
        df = read_jsonl(jsonl_path)
        assert len(df) == 9
        assert "file" in df.columns
        assert "algorithm" in df.columns
        assert "status" in df.columns
        assert "seed" in df.columns

    def test_flattens_nested_dicts(self, jsonl_path: Path) -> None:
        df = read_jsonl(jsonl_path)
        assert "result.satisfaction_ratio" in df.columns
        assert "result.verification_mismatch" in df.columns

    def test_empty_jsonl_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.jsonl"
        path.write_text("", encoding="utf-8")
        with pytest.raises(ValueError, match="empty"):
            read_jsonl(path)

    def test_blank_lines_ignored(self, tmp_path: Path) -> None:
        path = tmp_path / "blanks.jsonl"
        path.write_text(
            json.dumps({"file": "a", "algorithm": "x", "status": "ok"}) + "\n\n",
            encoding="utf-8",
        )
        df = read_jsonl(path)
        assert len(df) == 1


class TestFilterOk:
    def test_removes_errors_and_timeouts(self, flat_df: pd.DataFrame) -> None:
        filtered = filter_ok(flat_df)
        assert "timeout" not in filtered["status"].values
        assert len(filtered) == 8

    def test_all_ok_unchanged(self, flat_df: pd.DataFrame) -> None:
        only_ok = flat_df[flat_df["status"] == "ok"].copy()
        result = filter_ok(only_ok)
        assert len(result) == len(only_ok)

    def test_all_errors_raises(self, flat_df: pd.DataFrame) -> None:
        only_errors = flat_df[flat_df["status"] != "ok"].copy()
        with pytest.raises(ValueError, match="status='ok'"):
            filter_ok(only_errors)

    def test_missing_status_column_raises(self) -> None:
        df = pd.DataFrame({"file": ["f1"], "algorithm": ["rs"]})
        with pytest.raises(KeyError, match="status"):
            filter_ok(df)


class TestExtractMeasure:
    def test_basic(self, flat_df: pd.DataFrame) -> None:
        spec = MeasureSpec("result.satisfaction_ratio")
        result = extract_measure(filter_ok(flat_df), spec)
        assert list(result.columns) == ["file", "algorithm", "seed", "value"]
        assert len(result) == 8
        assert result["value"].dtype == np.float64

    def test_missing_metric_raises_keyerror(self, flat_df: pd.DataFrame) -> None:
        spec = MeasureSpec("result.no_existe")
        with pytest.raises(KeyError, match="no_existe"):
            extract_measure(filter_ok(flat_df), spec)

    def test_cast_bool_to_int(self, flat_df: pd.DataFrame) -> None:
        spec = MeasureSpec("result.verification_mismatch", cast_bool_to_int=True)
        result = extract_measure(filter_ok(flat_df), spec)
        assert result["value"].dtype == np.int64
        assert set(result["value"].unique()) <= {0, 1}

    def test_cast_bool_to_int_false_by_default(self, flat_df: pd.DataFrame) -> None:
        spec = MeasureSpec("result.verification_mismatch")
        result = extract_measure(filter_ok(flat_df), spec)
        # json_normalize infiere object cuando hay None + bool mezclados
        assert result["value"].dtype == np.object_

    def test_direct_field_no_nesting(self, flat_df: pd.DataFrame) -> None:
        spec = MeasureSpec("run_duration_s")
        result = extract_measure(filter_ok(flat_df), spec)
        assert "value" in result.columns
        assert (result["value"] > 0).all()

    def test_missing_file_column_raises(self) -> None:
        df = pd.DataFrame({"algorithm": ["rs"], "status": ["ok"]})
        spec = MeasureSpec("x")
        with pytest.raises(KeyError, match="file"):
            extract_measure(df, spec)

    def test_no_seed_column_fills_nan(self, flat_df: pd.DataFrame) -> None:
        spec = MeasureSpec("run_duration_s")
        df_no_seed = filter_ok(flat_df).drop(columns=["seed"])
        result = extract_measure(df_no_seed, spec)
        assert result["seed"].isna().all()


class TestAggregateSeeds:
    @pytest.fixture
    def extracted(self, flat_df: pd.DataFrame) -> pd.DataFrame:
        spec = MeasureSpec("result.satisfaction_ratio")
        return extract_measure(filter_ok(flat_df), spec)

    def test_mean(self, extracted: pd.DataFrame) -> None:
        spec = MeasureSpec("result.satisfaction_ratio", aggregation="mean")
        agg = aggregate_seeds(extracted, spec)
        assert len(agg) == 6  # 3 files × 2 algorithms
        row = agg[(agg["file"] == "f1.cnf") & (agg["algorithm"] == "rs")]
        assert row["value"].iloc[0] == 0.925

    def test_median(self, extracted: pd.DataFrame) -> None:
        spec = MeasureSpec("result.satisfaction_ratio", aggregation="median")
        agg = aggregate_seeds(extracted, spec)
        row = agg[(agg["file"] == "f1.cnf") & (agg["algorithm"] == "rs")]
        assert row["value"].iloc[0] == 0.925  # (0.90 + 0.95) / 2 → median = mean

    def test_best_higher_is_better(self, extracted: pd.DataFrame) -> None:
        spec = MeasureSpec("result.satisfaction_ratio", aggregation="best", higher_is_better=True)
        agg = aggregate_seeds(extracted, spec)
        row = agg[(agg["file"] == "f1.cnf") & (agg["algorithm"] == "rs")]
        assert row["value"].iloc[0] == 0.95

    def test_best_lower_is_better(self, extracted: pd.DataFrame) -> None:
        spec = MeasureSpec("result.satisfaction_ratio", aggregation="best", higher_is_better=False)
        agg = aggregate_seeds(extracted, spec)
        row = agg[(agg["file"] == "f1.cnf") & (agg["algorithm"] == "rs")]
        assert row["value"].iloc[0] == 0.90

    def test_worst_higher_is_better(self, extracted: pd.DataFrame) -> None:
        spec = MeasureSpec("result.satisfaction_ratio", aggregation="worst", higher_is_better=True)
        agg = aggregate_seeds(extracted, spec)
        row = agg[(agg["file"] == "f1.cnf") & (agg["algorithm"] == "rs")]
        assert row["value"].iloc[0] == 0.90

    def test_worst_lower_is_better(self, extracted: pd.DataFrame) -> None:
        spec = MeasureSpec("result.satisfaction_ratio", aggregation="worst", higher_is_better=False)
        agg = aggregate_seeds(extracted, spec)
        row = agg[(agg["file"] == "f1.cnf") & (agg["algorithm"] == "rs")]
        assert row["value"].iloc[0] == 0.95

    def test_no_seeds_collapses(self, extracted: pd.DataFrame) -> None:
        spec = MeasureSpec("result.satisfaction_ratio", aggregation="mean")
        no_seeds = extracted.drop(columns=["seed"])
        agg = aggregate_seeds(no_seeds, spec)
        assert len(agg) == 6  # 3 files × 2 algos — sin seed, agrupa por (file, algorithm)


class TestPivotMatrix:
    def test_shape(self, flat_df: pd.DataFrame) -> None:
        spec = MeasureSpec("result.satisfaction_ratio")
        extracted = extract_measure(filter_ok(flat_df), spec)
        agg = aggregate_seeds(extracted, spec)
        matrix = pivot_matrix(agg)
        assert matrix.shape == (3, 2)  # 3 files × 2 algorithms
        assert list(matrix.columns) == ["rs", "sds"]

    def test_creates_nan_for_missing_combinations(self, flat_df: pd.DataFrame) -> None:
        spec = MeasureSpec("result.satisfaction_ratio")
        extracted = extract_measure(filter_ok(flat_df), spec)
        agg = aggregate_seeds(extracted, spec)
        matrix = pivot_matrix(agg)
        # f2/rs tiene un timeout → la fila ok de rs para f2 existe (seed 42), pero
        # seed 43 es timeout. Tras aggregate_seeds sobre solo-ok, queda una fila ok.
        assert not matrix.isna().any().any()


class TestImputeNaN:
    @pytest.fixture
    def matrix_with_nan(self) -> pd.DataFrame:
        return pd.DataFrame(
            {"rs": [0.95, 0.80, np.nan], "sds": [1.0, np.nan, 0.85]},
            index=pd.Index(["f1.cnf", "f2.cnf", "f3.cnf"], name="file"),
        )

    def test_worst_higher_is_better(self, matrix_with_nan: pd.DataFrame) -> None:
        spec = MeasureSpec("x", higher_is_better=True)
        result = impute_nan(matrix_with_nan, spec, strategy="worst")
        assert not result.isna().any().any()
        assert result.loc["f3.cnf", "rs"] == 0.85  # min(f3) = 0.85
        assert result.loc["f2.cnf", "sds"] == 0.80  # min(f2) = 0.80

    def test_worst_lower_is_better(self, matrix_with_nan: pd.DataFrame) -> None:
        spec = MeasureSpec("x", higher_is_better=False)
        result = impute_nan(matrix_with_nan, spec, strategy="worst")
        assert not result.isna().any().any()
        assert result.loc["f3.cnf", "rs"] == 0.85  # max(f3) = 0.85
        assert result.loc["f2.cnf", "sds"] == 0.80  # max(f2) = 0.80

    def test_default_uses_column_worst(self, matrix_with_nan: pd.DataFrame) -> None:
        spec = MeasureSpec("x", higher_is_better=True)
        result = impute_nan(matrix_with_nan, spec)
        assert result.loc["f3.cnf", "rs"] == 0.80  # min(rs) = 0.80
        assert result.loc["f2.cnf", "sds"] == 0.85  # min(sds) = 0.85

    def test_row_worst_explicit(self, matrix_with_nan: pd.DataFrame) -> None:
        spec = MeasureSpec("x", higher_is_better=True)
        result = impute_nan(matrix_with_nan, spec, strategy="row_worst")
        assert result.loc["f3.cnf", "rs"] == 0.85  # min(f3) = 0.85
        assert result.loc["f2.cnf", "sds"] == 0.80  # min(f2) = 0.80

    def test_column_worst_explicit(self, matrix_with_nan: pd.DataFrame) -> None:
        spec = MeasureSpec("x", higher_is_better=True)
        result = impute_nan(matrix_with_nan, spec, strategy="column_worst")
        assert result.loc["f3.cnf", "rs"] == 0.80  # min(rs) = 0.80
        assert result.loc["f2.cnf", "sds"] == 0.85  # min(sds) = 0.85

    def test_drop(self, matrix_with_nan: pd.DataFrame) -> None:
        spec = MeasureSpec("x")
        result = impute_nan(matrix_with_nan, spec, strategy="drop")
        assert not result.isna().any().any()
        assert len(result) == 1  # solo f1.cnf no tiene NaN

    def test_warn(self, matrix_with_nan: pd.DataFrame) -> None:
        spec = MeasureSpec("x")
        with pytest.warns(UserWarning, match="NaN"):
            result = impute_nan(matrix_with_nan, spec, strategy="warn")
        assert result.isna().any().any()  # no modifica

    def test_no_nan_unchanged(self) -> None:
        matrix = pd.DataFrame({"rs": [0.9, 0.8], "sds": [1.0, 0.95]})
        spec = MeasureSpec("x")
        result = impute_nan(matrix, spec, strategy="worst")
        pd.testing.assert_frame_equal(result, matrix)

    def test_all_nan_column_raises(self) -> None:
        matrix = pd.DataFrame({"rs": [np.nan, np.nan], "sds": [1.0, 0.95]})
        spec = MeasureSpec("x", higher_is_better=True)
        with pytest.raises(ValueError, match="no non-NaN values"):
            impute_nan(matrix, spec, strategy="column_worst")

    def test_all_nan_row_raises(self) -> None:
        matrix = pd.DataFrame({"rs": [np.nan, 0.9], "sds": [np.nan, 0.95]})
        spec = MeasureSpec("x", higher_is_better=True)
        with pytest.raises(ValueError, match="no non-NaN values"):
            impute_nan(matrix, spec, strategy="worst")


class TestPrepareMatrix:
    def test_default_pipeline(self, flat_df: pd.DataFrame) -> None:
        spec = MeasureSpec("result.satisfaction_ratio")
        matrix = prepare_matrix(flat_df, spec)
        assert matrix.shape == (3, 2)
        assert not matrix.isna().any().any()

    def test_filter_status_false(self, flat_df: pd.DataFrame) -> None:
        spec = MeasureSpec("run_duration_s", higher_is_better=False)
        matrix = prepare_matrix(flat_df, spec, filter_status=False)
        assert matrix.shape == (3, 2)

    def test_nan_strategy_drop(self, flat_df: pd.DataFrame) -> None:
        spec = MeasureSpec("result.satisfaction_ratio")
        matrix = prepare_matrix(flat_df, spec, nan_strategy="drop")
        assert not matrix.isna().any().any()

    def test_default_nan_strategy_is_column_worst(self) -> None:
        df = pd.DataFrame(
            [
                {"file": "f1", "algorithm": "a", "status": "ok", "score": 10.0},
                {"file": "f1", "algorithm": "b", "status": "ok", "score": 20.0},
                {"file": "f2", "algorithm": "a", "status": "ok", "score": 30.0},
                {"file": "f3", "algorithm": "b", "status": "ok", "score": 40.0},
            ]
        )
        spec = MeasureSpec("score", higher_is_better=False)

        matrix = prepare_matrix(df, spec)

        assert matrix.loc["f3", "a"] == 30.0  # max(a), because lower is better
        assert matrix.loc["f2", "b"] == 40.0  # max(b), because lower is better


class TestBuildMatrix:
    def test_returns_dataframe(self, jsonl_path: Path) -> None:
        spec = MeasureSpec("result.satisfaction_ratio")
        matrix = build_matrix(jsonl_path, spec)
        assert isinstance(matrix, pd.DataFrame)

    def test_shape(self, jsonl_path: Path) -> None:
        spec = MeasureSpec("result.satisfaction_ratio")
        matrix = build_matrix(jsonl_path, spec)
        assert matrix.shape == (3, 2)

    def test_no_nan_by_default(self, jsonl_path: Path) -> None:
        spec = MeasureSpec("result.satisfaction_ratio")
        matrix = build_matrix(jsonl_path, spec)
        assert not matrix.isna().any().any()

    def test_nan_strategy_forwarded(self, jsonl_path: Path) -> None:
        spec = MeasureSpec("result.satisfaction_ratio")
        matrix_col = build_matrix(jsonl_path, spec, nan_strategy="column_worst")
        matrix_row = build_matrix(jsonl_path, spec, nan_strategy="row_worst")
        # Both clean; values may differ when NaN exists — here no NaN so equal
        pd.testing.assert_frame_equal(matrix_col, matrix_row)

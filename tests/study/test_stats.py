"""Tests for alglab.study.stats — scmamp-compatible statistical functions."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from alglab.study.stats import (
    correct_control_matrix,
    correct_pairwise,
    correction_bergmann_hommel_vector,
    correction_shaffer_vector,
    count_recursively,
    exhaustive_sets,
    iman_davenport_test,
    levene_diagnostic,
    p_adjust_hommel,
    pairs_for,
    rank_matrix,
    raw_wilcoxon_pvalues,
    shapiro_diagnostics,
    summary_table,
    wilcoxon_signed_test,
)


class TestRankMatrix:
    def test_higher_is_better_best_gets_rank1(self) -> None:
        m = pd.DataFrame({"a": [1.0, 2.0], "b": [2.0, 1.0]})
        r = rank_matrix(m, higher_is_better=True)
        assert r.loc[0, "a"] == 2.0
        assert r.loc[0, "b"] == 1.0

    def test_lower_is_better_best_gets_rank1(self) -> None:
        m = pd.DataFrame({"a": [1.0, 2.0], "b": [2.0, 1.0]})
        r = rank_matrix(m, higher_is_better=False)
        assert r.loc[0, "a"] == 1.0
        assert r.loc[0, "b"] == 2.0

    def test_ties_get_average_rank(self) -> None:
        m = pd.DataFrame({"a": [1.0], "b": [1.0]})
        r = rank_matrix(m)
        assert r.iloc[0, 0] == pytest.approx(1.5)
        assert r.iloc[0, 1] == pytest.approx(1.5)

    def test_columns_preserved(self) -> None:
        m = pd.DataFrame({"x": [3.0], "y": [1.0], "z": [2.0]})
        r = rank_matrix(m)
        assert list(r.columns) == ["x", "y", "z"]


class TestSummaryTable:
    def test_columns(self) -> None:
        m = pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
        s = summary_table(m, higher_is_better=True)
        assert list(s.columns) == ["RANK", "MIN", "MAX", "MEAN", "STDEV"]

    def test_best_algo_has_lowest_rank(self) -> None:
        m = pd.DataFrame({"a": [1.0, 1.0], "b": [2.0, 2.0]})
        s = summary_table(m, higher_is_better=True)
        assert s.loc["b", "RANK"] < s.loc["a", "RANK"]

    def test_min_max_mean(self) -> None:
        m = pd.DataFrame({"a": [1.0, 3.0]})
        s = summary_table(m, higher_is_better=True)
        assert s.loc["a", "MIN"] == pytest.approx(1.0)
        assert s.loc["a", "MAX"] == pytest.approx(3.0)
        assert s.loc["a", "MEAN"] == pytest.approx(2.0)


class TestWilcoxonSignedTest:
    def test_identical_samples_t_is_n_n1_over_4(self) -> None:
        # All d=0; half-rank gives rp=rn=N*(N+1)/4 per scmamp convention.
        x = [1.0, 2.0, 3.0]
        t, _ = wilcoxon_signed_test(x, x)
        assert t == pytest.approx(3.0)  # 3*4/4 = 3.0

    def test_p_in_unit_interval(self) -> None:
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [2.0, 3.0, 4.0, 5.0, 6.0]
        _, p = wilcoxon_signed_test(x, y)
        assert 0.0 <= p <= 1.0

    def test_mismatched_lengths_raises(self) -> None:
        with pytest.raises(ValueError, match="equal length"):
            wilcoxon_signed_test([1.0, 2.0], [1.0])

    def test_clear_difference_gives_small_p(self) -> None:
        x = list(range(1, 21))
        y = [v + 10 for v in x]
        _, p = wilcoxon_signed_test(x, y)
        assert p < 0.05

    def test_symmetric_t(self) -> None:
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [2.0, 3.0, 4.0, 5.0, 6.0]
        t1, _ = wilcoxon_signed_test(x, y)
        t2, _ = wilcoxon_signed_test(y, x)
        assert t1 == pytest.approx(t2)


class TestImanDavenportTest:
    def test_identical_columns_high_pvalue(self) -> None:
        m = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [1.0, 2.0, 3.0], "c": [1.0, 2.0, 3.0]})
        _, p, _, _ = iman_davenport_test(m)
        assert p > 0.5

    def test_clear_difference_low_pvalue(self) -> None:
        m = pd.DataFrame(
            {"best": list(range(10, 20)), "mid": list(range(5, 15)), "worst": list(range(0, 10))}
        )
        _, p, _, _ = iman_davenport_test(m)
        assert p < 0.05

    def test_degrees_of_freedom(self) -> None:
        m = pd.DataFrame(
            {"a": [1.0, 2.0, 3.0, 4.0], "b": [2.0, 3.0, 4.0, 5.0], "c": [3.0, 4.0, 5.0, 6.0]}
        )
        _, _, df1, df2 = iman_davenport_test(m)
        assert df1 == 2
        assert df2 == 6

    def test_invariant_to_direction(self) -> None:
        m = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [3.0, 2.0, 1.0], "c": [2.0, 2.0, 2.0]})
        s_hi, p_hi, _, _ = iman_davenport_test(m, higher_is_better=True)
        s_lo, p_lo, _, _ = iman_davenport_test(m, higher_is_better=False)
        assert s_hi == pytest.approx(s_lo, rel=1e-9)
        assert p_hi == pytest.approx(p_lo, rel=1e-9)


class TestShapiroDiagnostics:
    def test_columns(self) -> None:
        m = pd.DataFrame({"a": [1.0, 2.0, 3.0, 4.0, 5.0]})
        df = shapiro_diagnostics(m)
        assert list(df.columns) == ["W", "p_value", "normality"]

    def test_constant_column(self) -> None:
        m = pd.DataFrame({"a": [1.0, 1.0, 1.0, 1.0]})
        df = shapiro_diagnostics(m)
        assert df.loc["a", "p_value"] == pytest.approx(1.0)
        assert df.loc["a", "normality"] == "not_rejected"

    def test_short_column_nan(self) -> None:
        m = pd.DataFrame({"a": [1.0, 2.0]})
        df = shapiro_diagnostics(m)
        assert pd.isna(df.loc["a", "p_value"])


class TestLeveneDiagnostic:
    def test_returns_three_items(self) -> None:
        m = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]})
        assert len(levene_diagnostic(m)) == 3

    def test_not_enough_data(self) -> None:
        m = pd.DataFrame({"a": [1.0]})
        _, _, conclusion = levene_diagnostic(m)
        assert conclusion == "not_enough_data"


class TestPairsFor:
    def test_all_pairs_count(self) -> None:
        assert len(pairs_for(4)) == 6

    def test_control_count(self) -> None:
        assert len(pairs_for(4, control=0)) == 3

    def test_control_always_first(self) -> None:
        for p in pairs_for(4, control=2):
            assert p[0] == 2


class TestRawWilcoxonPvalues:
    def test_all_pairs_symmetric(self) -> None:
        m = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [2.0, 3.0, 4.0], "c": [1.5, 2.5, 3.5]})
        raw = raw_wilcoxon_pvalues(m)
        assert raw.loc["a", "b"] == pytest.approx(raw.loc["b", "a"])

    def test_control_gives_1_row(self) -> None:
        m = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [2.0, 3.0, 4.0], "c": [1.5, 2.5, 3.5]})
        raw = raw_wilcoxon_pvalues(m, control=0)
        assert raw.shape == (1, 3)
        assert pd.isna(raw.iloc[0]["a"])


class TestPAdjustHommel:
    def test_preserves_nan(self) -> None:
        out = p_adjust_hommel([0.01, float("nan"), 0.05])
        assert pd.isna(out[1])

    def test_corrected_ge_original(self) -> None:
        values = [0.01, 0.02, 0.03]
        out = p_adjust_hommel(values)
        for orig, adj in zip(values, out, strict=True):
            assert adj >= orig - 1e-12


class TestCorrectControlMatrix:
    def test_wrong_method_raises(self) -> None:
        raw = pd.DataFrame([[0.01, 0.05]], columns=["a", "b"])
        with pytest.raises(ValueError):
            correct_control_matrix(raw, method="bonferroni")


class TestCountRecursively:
    def test_sorted_unique(self) -> None:
        sk = count_recursively(4)
        assert sk == sorted(set(sk))

    def test_k2_contains_1(self) -> None:
        assert 1 in count_recursively(2)


class TestCorrectionShafferVector:
    def test_values_le_1(self) -> None:
        adj = correction_shaffer_vector([0.001, 0.01, 0.5], k_algorithms=3)
        assert (adj <= 1.0 + 1e-12).all()


class TestExhaustiveSets:
    def test_k1_empty(self) -> None:
        assert exhaustive_sets((0,)) == set()

    def test_k2_one_set(self) -> None:
        assert len(exhaustive_sets((0, 1))) == 1

    def test_cached(self) -> None:
        exhaustive_sets.cache_clear()
        exhaustive_sets((0, 1, 2))
        assert exhaustive_sets.cache_info().currsize >= 1


class TestCorrectionBergmannHommelVector:
    def test_length(self) -> None:
        pairs = [(0, 1), (0, 2), (1, 2)]
        out = correction_bergmann_hommel_vector([0.01, 0.05, 0.2], pairs, k_algorithms=3)
        assert len(out) == 3

    def test_values_le_1(self) -> None:
        pairs = [(0, 1), (0, 2), (1, 2)]
        out = correction_bergmann_hommel_vector([0.01, 0.05, 0.3], pairs, k_algorithms=3)
        assert (out <= 1.0 + 1e-12).all()


class TestCorrectPairwise:
    def _matrix(self) -> pd.DataFrame:
        return pd.DataFrame(
            np.array([[np.nan, 0.05, 0.01], [0.05, np.nan, 0.03], [0.01, 0.03, np.nan]]),
            columns=["a", "b", "c"],
            index=["a", "b", "c"],
        )

    def test_shape_preserved(self) -> None:
        out = correct_pairwise(self._matrix())
        assert out.shape == (3, 3)

    def test_symmetric(self) -> None:
        out = correct_pairwise(self._matrix())
        assert out.loc["a", "b"] == pytest.approx(out.loc["b", "a"])

    def test_non_square_raises(self) -> None:
        with pytest.raises(ValueError):
            correct_pairwise(pd.DataFrame({"a": [0.01], "b": [0.05]}))

    def test_unknown_method_raises(self) -> None:
        with pytest.raises(ValueError):
            correct_pairwise(self._matrix(), method="bonferroni")

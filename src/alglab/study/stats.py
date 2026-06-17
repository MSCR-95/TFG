"""Statistical functions for algorithm comparison.

Port of the R/scmamp functions used in 2-WAY-ANALYSIS.R and N-WAY-ANALYSIS.R.
All tests mirror scmamp behaviour (Demsar 2006 conventions) so that results
produced here are directly comparable to the R reference implementation.

Typical usage
-------------
k == 2:
    :func:`wilcoxon_signed_test` on raw values.

k >= 3:
    :func:`iman_davenport_test` → :func:`correct_control_matrix` (Hommel 1×N)
    → :func:`correct_pairwise` (Bergmann-Hommel N×N) on the rank matrix.
"""

from __future__ import annotations

import functools
import itertools
import math
import warnings
from collections.abc import Iterable, Sequence

import numpy as np
import pandas as pd
from scipy.stats import f as f_dist
from scipy.stats import levene, norm, rankdata, shapiro
from statsmodels.stats.multitest import multipletests

# ── ranking ─────────────────────────────────────────────────────────────


def rank_matrix(data: pd.DataFrame, higher_is_better: bool = True) -> pd.DataFrame:
    """Rank each row across algorithms.  Best algorithm = rank 1.  Ties averaged.

    Mirrors ``scmamp::rankMatrix(decreasing=higher_is_better)``.

    Args:
        data: File × algorithm value matrix.
        higher_is_better: When ``True``, larger values receive lower (better)
            ranks.  When ``False``, smaller values receive lower ranks.

    Returns:
        DataFrame of the same shape as *data* with rank values.

    Example:
        >>> import pandas as pd
        >>> m = pd.DataFrame({"A": [0.9, 0.8], "B": [0.7, 0.95]})
        >>> rank_matrix(m, higher_is_better=True)
             A    B
        0  1.0  2.0
        1  2.0  1.0
    """
    values = -data.to_numpy(dtype=float) if higher_is_better else data.to_numpy(dtype=float)
    ranks = np.apply_along_axis(lambda row: rankdata(row, method="average"), 1, values)
    return pd.DataFrame(ranks, columns=data.columns, index=data.index)


def summary_table(data: pd.DataFrame, higher_is_better: bool) -> pd.DataFrame:
    """Compute RANK, MIN, MAX, MEAN, and STDEV per algorithm.

    Args:
        data: File × algorithm value matrix.
        higher_is_better: Ranking direction passed to :func:`rank_matrix`.

    Returns:
        DataFrame indexed by algorithm name with columns
        ``["RANK", "MIN", "MAX", "MEAN", "STDEV"]``.
    """
    ranks = rank_matrix(data, higher_is_better)
    summary = pd.DataFrame(index=data.columns)
    summary["RANK"] = ranks.mean(axis=0)
    summary["MIN"] = data.min(axis=0)
    summary["MAX"] = data.max(axis=0)
    summary["MEAN"] = data.mean(axis=0)
    summary["STDEV"] = data.std(axis=0, ddof=1)
    return summary


# ── wilcoxon signed-rank ─────────────────────────────────────────────────


def wilcoxon_signed_test(
    x: Sequence[float] | np.ndarray, y: Sequence[float] | np.ndarray
) -> tuple[float, float]:
    r"""Paired Wilcoxon signed-rank test — scmamp ``wilcoxonSignedTest`` port.

    Uses the Demsar (2006) normal approximation:

    .. math::

        z = \\frac{T - n(n+1)/4}{\\sqrt{n(n+1)(2n+1)/24}}, \\quad p = \\Phi(z)

    Zero differences receive half the tied rank (same as scmamp).  All *n*
    pairs are included, regardless of zero differences.

    Args:
        x: First sample.
        y: Second sample.  Must have the same length as *x*.

    Returns:
        A ``(T, p)`` tuple where *T* is the smaller of the positive and
        negative rank sums, and *p* is the one-sided normal approximation
        p-value.

    Raises:
        ValueError: If *x* and *y* have different lengths.
    """
    xa = np.asarray(x, dtype=float)
    ya = np.asarray(y, dtype=float)
    if xa.shape != ya.shape:
        raise ValueError("Paired test requires vectors of equal length")
    d = xa - ya
    ranks = rankdata(np.abs(d), method="average")
    rp = float(ranks[d > 0].sum() + 0.5 * ranks[d == 0].sum())
    rn = float(ranks[d < 0].sum() + 0.5 * ranks[d == 0].sum())
    t = min(rp, rn)
    n = len(d)
    z = (t - 0.25 * n * (n + 1)) / math.sqrt(n * (n + 1) * (2 * n + 1) / 24)
    return t, float(norm.cdf(z))


# ── iman-davenport ───────────────────────────────────────────────────────


def iman_davenport_test(
    data: pd.DataFrame, higher_is_better: bool = True
) -> tuple[float, float, int, int]:
    """Iman-Davenport F-distribution correction of Friedman's test.

    Mirrors ``scmamp::imanDavenportTest`` (Demsar 2006, p. 11).

    Note:
        Friedman's chi-squared statistic is invariant to ranking direction, so
        *higher_is_better* does not affect the numeric result.  The parameter
        is accepted for API consistency with the rest of the module.

    Args:
        data: File × algorithm value matrix (raw values, not ranks).
        higher_is_better: Ranking direction; see note above.

    Returns:
        A ``(F, p, df1, df2)`` tuple.  When the denominator is ≤ 0 (perfect
        rank separation), returns ``(inf, 0.0, df1, df2)``.
    """
    n, k = data.shape
    mean_ranks = rank_matrix(data, higher_is_better=higher_is_better).mean(axis=0)
    friedman_stat = 12 * n / (k * (k + 1)) * (float((mean_ranks**2).sum()) - (k * (k + 1) ** 2) / 4)
    denom = n * (k - 1) - friedman_stat
    if denom <= 0:
        return math.inf, 0.0, k - 1, (k - 1) * (n - 1)
    statistic = (n - 1) * friedman_stat / denom
    p_value = float(f_dist.sf(statistic, k - 1, (k - 1) * (n - 1)))
    return statistic, p_value, k - 1, (k - 1) * (n - 1)


# ── diagnostics ─────────────────────────────────────────────────────────


def shapiro_diagnostics(data: pd.DataFrame, alpha: float = 0.05) -> pd.DataFrame:
    """Shapiro-Wilk normality test per algorithm column.

    These are diagnostics only; the analysis pipeline uses non-parametric
    tests regardless of the outcome.

    Args:
        data: File × algorithm value matrix.
        alpha: Significance level for the ``normality`` conclusion column.

    Returns:
        DataFrame indexed by algorithm name with columns ``["W", "p_value",
        "normality"]``.  ``normality`` is ``"reject"`` when p ≤ *alpha* and
        ``"not_rejected"`` otherwise.
    """
    rows = []
    for col in data.columns:
        values = data[col].dropna().to_numpy(dtype=float)
        if len(values) < 3:
            stat, p = float("nan"), float("nan")
        elif np.allclose(values, values[0]):
            stat, p = float("nan"), 1.0
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                res = shapiro(values)
            stat, p = float(res.statistic), float(res.pvalue)
        rows.append(
            {
                "algorithm": col,
                "W": stat,
                "p_value": p,
                "normality": "reject" if not pd.isna(p) and p <= alpha else "not_rejected",
            }
        )
    return pd.DataFrame(rows).set_index("algorithm")


def levene_diagnostic(data: pd.DataFrame, alpha: float = 0.05) -> tuple[float, float, str]:
    """Levene test for equal variances (Brown-Forsythe variant, center=median).

    These are diagnostics only; the analysis pipeline uses non-parametric
    tests regardless of the outcome.

    Args:
        data: File × algorithm value matrix.
        alpha: Significance level for the conclusion string.

    Returns:
        A ``(statistic, p_value, conclusion)`` tuple.  *conclusion* is
        ``"reject_equal_variances"`` when p ≤ *alpha*, ``"not_rejected"``
        otherwise, or ``"not_enough_data"`` when fewer than two columns have
        more than one non-NaN value.
    """
    samples = [data[col].dropna().to_numpy(dtype=float) for col in data.columns]
    samples = [s for s in samples if len(s) > 1]
    if len(samples) < 2:
        return float("nan"), float("nan"), "not_enough_data"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = levene(*samples, center="median")
    statistic = float(result.statistic)
    p_value = float(result.pvalue)
    conclusion = (
        "reject_equal_variances" if not pd.isna(p_value) and p_value <= alpha else "not_rejected"
    )
    return statistic, p_value, conclusion


# ── pairwise p-values ────────────────────────────────────────────────────


def pairs_for(k: int, control: int | None = None) -> list[tuple[int, int]]:
    """Return the list of ``(i, j)`` index pairs for all-vs-all or one-vs-all.

    Args:
        k: Total number of algorithms.
        control: If given, returns pairs where one element is *control*.
            If ``None``, returns all upper-triangle pairs.

    Returns:
        For ``control=None``: ``k*(k-1)/2`` pairs from the upper triangle.
        For a given *control*: ``k-1`` pairs (control vs every other index).
    """
    if control is None:
        return [(i, j) for i in range(k - 1) for j in range(i + 1, k)]
    return [(control, j) for j in range(k) if j != control]


def raw_wilcoxon_pvalues(data: pd.DataFrame, control: int | None = None) -> pd.DataFrame:
    """Compute raw (uncorrected) Wilcoxon signed-rank p-values.

    Args:
        data: File × algorithm value or rank matrix.
        control: Column index of the control algorithm.  When ``None``,
            computes all pairwise p-values.

    Returns:
        When *control* is ``None``: a symmetric k × k DataFrame with NaN on
        the diagonal.
        When *control* is given: a 1 × k DataFrame with NaN in the control
        column.
    """
    names = list(data.columns)
    k = len(names)
    if control is None:
        result = pd.DataFrame(np.nan, index=names, columns=names, dtype=float)
        for i in range(k - 1):
            for j in range(i + 1, k):
                _, p = wilcoxon_signed_test(data.iloc[:, i].to_numpy(), data.iloc[:, j].to_numpy())
                result.iloc[i, j] = result.iloc[j, i] = p
    else:
        result = pd.DataFrame(np.nan, index=[0], columns=names, dtype=float)
        for j in range(k):
            if j != control:
                _, p = wilcoxon_signed_test(
                    data.iloc[:, control].to_numpy(), data.iloc[:, j].to_numpy()
                )
                result.iloc[0, j] = p
    return result


# ── corrections ──────────────────────────────────────────────────────────


def p_adjust_hommel(values: Sequence[float] | np.ndarray, alpha: float = 0.05) -> np.ndarray:
    """Apply Hommel correction to a vector of p-values, preserving NaN positions.

    Args:
        values: Raw p-values.  NaN entries are preserved in the output.
        alpha: Significance level passed to ``statsmodels.multipletests``.

    Returns:
        Array of corrected p-values with the same length and NaN positions as
        *values*.
    """
    arr = np.asarray(values, dtype=float)
    valid = ~np.isnan(arr)
    out = np.full_like(arr, np.nan, dtype=float)
    if valid.any():
        _, corrected, _, _ = multipletests(arr[valid], alpha=alpha, method="hommel")
        out[valid] = corrected
    return out


def correct_control_matrix(raw: pd.DataFrame, method: str = "hommel") -> pd.DataFrame:
    """Apply Hommel correction to a 1 × k raw p-value matrix (1×N comparison).

    Args:
        raw: 1 × k DataFrame of raw p-values as returned by
            :func:`raw_wilcoxon_pvalues` with a *control* argument.
        method: Correction method.  Only ``"hommel"`` is supported.

    Returns:
        Corrected 1 × k DataFrame.

    Raises:
        ValueError: If *method* is not ``"hommel"``.
    """
    if method != "hommel":
        raise ValueError("Only 'hommel' is supported for 1×n comparisons")
    corrected = raw.copy()
    corrected.iloc[0] = p_adjust_hommel(raw.iloc[0].to_numpy(dtype=float))
    return corrected


def count_recursively(k: int) -> list[int]:
    """Return S(k): the sorted set of possible true hypothesis counts for *k* algorithms.

    Port of scmamp's ``countRecursively`` (Shaffer 1986).  Used to determine
    the maximum number of simultaneously true pairwise hypotheses when applying
    Shaffer's static correction.

    Args:
        k: Number of algorithms being compared.

    Returns:
        Sorted list of distinct integers that can arise as the count of
        simultaneously true null hypotheses among the ``k*(k-1)/2`` pairwise
        comparisons of *k* algorithms.
    """
    values = [0]
    if k > 1:
        values.extend(count_recursively(k - 1))
        for j in range(2, k + 1):
            values.extend(
                x + math.factorial(j) // (2 * math.factorial(j - 2))
                for x in count_recursively(k - j)
            )
    return sorted(set(values))


def correction_shaffer_vector(p_values: Sequence[float], k_algorithms: int) -> np.ndarray:
    """Apply Shaffer's static correction to a vector of pairwise p-values.

    Each p-value is multiplied by the maximum number of simultaneously true
    hypotheses *t_i* for its rank position, then accumulated to ensure
    monotonicity.

    Args:
        p_values: Raw pairwise p-values in upper-triangle order.
        k_algorithms: Total number of algorithms; determines the Shaffer S(k)
            sequence via :func:`count_recursively`.

    Returns:
        Array of adjusted p-values, clipped to [0, 1].
    """
    raw = np.asarray(p_values, dtype=float)
    sk = count_recursively(k_algorithms)[1:]
    t_i: list[int] = []
    for current, nxt in zip(sk[:-1], sk[1:], strict=False):
        t_i.extend([current] * (nxt - current))
    t_i.append(sk[-1])
    t_i_arr = np.asarray(list(reversed(t_i)), dtype=float)
    order = np.argsort(raw)
    adjusted_sorted = np.minimum(raw[order] * t_i_arr[: len(raw)], 1.0)
    adjusted_sorted = np.maximum.accumulate(adjusted_sorted)
    adjusted = np.empty_like(raw)
    adjusted[order] = adjusted_sorted
    return adjusted


# ── Bergmann-Hommel ──────────────────────────────────────────────────────
# _compute_subdivisions, _partitions_for, and _canonical_set are internal
# helpers for exhaustive_sets.  Their O(2^k) cost is acceptable for k ≤ 9
# and is amortised by the lru_cache on exhaustive_sets.


def _compute_subdivisions(
    values: tuple[int, ...],
) -> list[tuple[tuple[int, ...], tuple[int, ...]]]:
    if len(values) == 1:
        return [(values, tuple())]
    last = values[-1:]
    base = values[:-1]
    result: list[tuple[tuple[int, ...], tuple[int, ...]]] = []
    for s1, s2 in _compute_subdivisions(base):
        result.append((s1 + last, s2))
        result.append((s1, s2 + last))
    result.append((last, base))
    return result


def _partitions_for(
    values: tuple[int, ...],
) -> list[tuple[tuple[int, ...], tuple[int, ...]]]:
    if len(values) == 1:
        return _compute_subdivisions(values)
    last = values[-1]
    return [(s1, s2 + (last,)) for s1, s2 in _compute_subdivisions(values[:-1])]


def _canonical_set(edges: Iterable[tuple[int, int]]) -> tuple[tuple[int, ...], ...]:
    return tuple(sorted(tuple(sorted(edge)) for edge in edges))


@functools.lru_cache(maxsize=16)
def exhaustive_sets(values: tuple[int, ...]) -> set[tuple[tuple[int, ...], ...]]:
    """Compute the complete collection of exhaustive hypothesis sets (Garcia & Herrera 2008).

    An exhaustive set is a maximal set of pairwise hypotheses that can all be
    simultaneously true.  The collection is used by
    :func:`correction_bergmann_hommel_vector` to determine the tightest valid
    multiplier for each hypothesis.

    Results are cached per unique *values* tuple to avoid repeated O(2^k)
    computation across multiple calls with the same number of algorithms.

    Args:
        values: Tuple of algorithm indices, typically ``tuple(range(k))``.

    Returns:
        Set of exhaustive hypothesis sets.  Each set is represented as a
        canonical tuple of sorted ``(i, j)`` pairs.
    """
    if len(values) == 1:
        return set()
    if len(values) == 2:
        return {_canonical_set([(values[0], values[1])])}
    result: set[tuple[tuple[int, ...], ...]] = {_canonical_set(itertools.combinations(values, 2))}
    for s1, s2 in _partitions_for(values):
        e1 = exhaustive_sets(s1)
        e2 = exhaustive_sets(s2)
        result.update(e1)
        result.update(e2)
        for left in e1:
            for right in e2:
                result.add(_canonical_set((*left, *right)))  # type: ignore[arg-type]
    return result


def correction_bergmann_hommel_vector(
    p_values: Sequence[float],
    pairs: Sequence[tuple[int, int]],
    k_algorithms: int,
) -> np.ndarray:
    """Apply Bergmann-Hommel dynamic correction to a vector of pairwise p-values.

    For each hypothesis H_{ij}, finds every exhaustive set that contains it and
    uses the smallest adjusted p-value across those sets as the candidate.
    The final correction applies step-up monotonisation.

    Args:
        p_values: Raw pairwise p-values in the same order as *pairs*.
        pairs: ``(i, j)`` index pairs corresponding to each p-value.
        k_algorithms: Total number of algorithms; used to compute the exhaustive
            sets via :func:`exhaustive_sets`.

    Returns:
        Array of adjusted p-values, clipped to [0, 1] and monotonised.
    """
    raw = np.asarray(p_values, dtype=float)
    p_by_pair = {_canonical_set([pair])[0]: value for pair, value in zip(pairs, raw, strict=True)}
    exhaustive = exhaustive_sets(tuple(range(k_algorithms)))
    adjusted = np.zeros_like(raw)
    for idx, pair in enumerate(pairs):
        pair_key = _canonical_set([pair])[0]
        candidates = [
            min(len(h) * min(p_by_pair[p] for p in h), 1.0) for h in exhaustive if pair_key in h
        ]
        adjusted[idx] = max(candidates) if candidates else raw[idx]
    order = np.argsort(raw)
    adjusted_sorted = np.maximum.accumulate(np.minimum(adjusted[order], 1.0))
    out = np.empty_like(raw)
    out[order] = adjusted_sorted
    return out


def correct_pairwise(raw: pd.DataFrame, method: str = "bergmann") -> pd.DataFrame:
    """Apply pairwise multiple-comparison correction to a symmetric k × k p-value matrix.

    Uses Bergmann-Hommel for k ≤ 9 and Shaffer for k > 9, matching the
    threshold used in the original R script.  Pass *method* explicitly to
    override.

    Args:
        raw: Symmetric k × k DataFrame of raw p-values with NaN on the diagonal,
            as returned by :func:`raw_wilcoxon_pvalues`.
        method: ``"bergmann"`` (default) or ``"shaffer"``.

    Returns:
        Corrected k × k DataFrame in the same shape as *raw*.

    Raises:
        ValueError: If *raw* is not square or *method* is not recognised.

    Example:
        >>> raw = raw_wilcoxon_pvalues(rank_matrix(matrix, higher_is_better=True))
        >>> corrected = correct_pairwise(raw)
    """
    if raw.shape[0] != raw.shape[1]:
        raise ValueError("Pairwise correction requires a square matrix")
    k = raw.shape[0]
    pairs = pairs_for(k)
    raw_np = raw.to_numpy(dtype=float)
    p_values = [float(raw_np[i, j]) for i, j in pairs]
    if method == "bergmann":
        adjusted = correction_bergmann_hommel_vector(p_values, pairs, k)
    elif method == "shaffer":
        adjusted = correction_shaffer_vector(p_values, k)
    else:
        raise ValueError("method must be 'bergmann' or 'shaffer'")
    result = raw.copy()
    for (i, j), value in zip(pairs, adjusted, strict=True):
        result.iloc[i, j] = value
        result.iloc[j, i] = value
    return result

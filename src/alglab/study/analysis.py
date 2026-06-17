"""Orchestration for the statistical study workflow.

Entry point: :func:`run_study`.

The analysis branches on the number of algorithms *k*:

- **k == 2**: Summary, diagnostics, Wilcoxon signed-rank test.
  Plots: boxplot, density, Q-Q.
- **k ≥ 3**: Summary, diagnostics, Iman-Davenport global test,
  Hommel 1×N post-hoc (each algorithm as control), Bergmann-Hommel N×N
  pairwise correction.  Plots: boxplot, density, Q-Q, ranking diagram,
  corrected p-value heatmap.

All post-hoc Wilcoxon tests use the rank matrix (matching scmamp's
``postHocTest(use.rank=TRUE)`` default).
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd

from ._formatting import format_number
from .io import build_matrix
from .plots import FIGURE_FORMATS, plot_all
from .schema import MeasureSpec, StudyConfig
from .stats import (
    correct_control_matrix,
    correct_pairwise,
    iman_davenport_test,
    levene_diagnostic,
    rank_matrix,
    raw_wilcoxon_pvalues,
    shapiro_diagnostics,
    summary_table,
    wilcoxon_signed_test,
)


def run_study(config: StudyConfig, output_dir: Path) -> list[Path]:
    """Run the full statistical analysis for all measures in *config*.

    Creates *output_dir* if it does not exist, then for each measure:

    1. Builds the file × algorithm value matrix from the JSONL data.
    2. Saves the matrix to ``matrices/<label>.csv``.
    3. Writes the text analysis to ``analysis.txt`` (appending per measure).
    4. Generates PDF/SVG/PNG plots under ``plots/``.

    Args:
        config: Study configuration specifying the JSONL source, measures,
            and significance level.
        output_dir: Directory where all output is written.  Created
            automatically if it does not exist.

    Returns:
        Sorted list of all generated file paths (``analysis.txt``, CSV
        matrices, and all plot files).

    Example:
        >>> from alglab.study import run_study, StudyConfig, MeasureSpec
        >>> from pathlib import Path
        >>>
        >>> config = StudyConfig(
        ...     input_jsonl=Path("output/results.jsonl"),
        ...     measures=[MeasureSpec("result.satisfaction_ratio", label="sat",
        ...                           higher_is_better=True)],
        ...     alpha=0.05,
        ... )
        >>> paths = run_study(config, Path("report/"))
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    matrices_dir = output_dir / "matrices"
    plots_dir = output_dir / "plots"
    matrices_dir.mkdir(exist_ok=True)
    plots_dir.mkdir(exist_ok=True)

    analysis_path = output_dir / "analysis.txt"
    generated: list[Path] = [analysis_path]
    analysis_parts: list[str] = []

    for spec in config.measures:
        matrix = build_matrix(config.input_jsonl, spec)
        matrix.to_csv(matrices_dir / f"{spec.label}.csv")
        generated.append(matrices_dir / f"{spec.label}.csv")

        summary = summary_table(matrix, spec.higher_is_better)
        text, corrected_pairwise = _analyse_measure(matrix, spec, summary, config.alpha)
        analysis_parts.append(text)
        generated.extend(
            plot_all(
                matrix,
                summary,
                corrected_pairwise,
                spec.label,
                plots_dir,
                alpha=config.alpha,
                formats=FIGURE_FORMATS,
                include_nway_plots=(matrix.shape[1] >= 3),
            )
        )

    analysis_path.write_text("".join(analysis_parts), encoding="utf-8")
    return sorted(generated)


# ── internal helpers ─────────────────────────────────────────────────────


def _fmt_table(data: pd.DataFrame) -> str:
    """Render *data* as a fixed-width string using :func:`format_number` for all cells.

    Args:
        data: DataFrame to render.

    Returns:
        A multi-line string suitable for inclusion in the text report.
    """
    return data.to_string(formatters={col: format_number for col in data.columns})


def _analyse_measure(
    matrix: pd.DataFrame,
    spec: MeasureSpec,
    summary: pd.DataFrame,
    alpha: float,
) -> tuple[str, pd.DataFrame | None]:
    """Build the full statistical analysis text for one measure.

    Formats the summary table, assumption diagnostics (Shapiro-Wilk and
    Levene), and either a Wilcoxon test (k == 2) or the full N-way suite
    (k ≥ 3).

    Args:
        matrix: File × algorithm value matrix.
        spec: Measure specification (used for labels and ranking direction).
        summary: Precomputed summary table from :func:`~alglab.study.stats.summary_table`.
        alpha: Significance level for all tests.

    Returns:
        A ``(text, corrected_pairwise)`` tuple.  *text* is the formatted
        analysis string.  *corrected_pairwise* is the k × k adjusted p-value
        matrix (k ≥ 3) or ``None`` (k == 2).
    """
    buf = io.StringIO()

    k = matrix.shape[1]
    sorted_summary = summary.sort_values("RANK")
    ranking_sep = " >= " if spec.higher_is_better else " <= "

    print("\n\n** STATISTIC ANALYSIS ***********************************************\n", file=buf)
    print(">> Summary\n", file=buf)
    print(f"Variable:\t{spec.label}", file=buf)
    print(f"Goodness:\t{'Positive' if spec.higher_is_better else 'Negative'}", file=buf)
    print(f"Ranking:\t{ranking_sep.join(list(sorted_summary.index))}", file=buf)
    if k >= 3:
        print("Base test:\twilcoxon", file=buf)
    print(file=buf)
    print(_fmt_table(summary), file=buf)

    print("\n>> Assumption diagnostics\n", file=buf)
    print("Shapiro-Wilk normality per algorithm (H0: normal sample)", file=buf)
    print(_fmt_table(shapiro_diagnostics(matrix, alpha=alpha)), file=buf)
    stat, p_value, conclusion = levene_diagnostic(matrix, alpha=alpha)
    print("\nLevene homoscedasticity test (H0: equal variances)", file=buf)
    print(f"Statistic = {format_number(stat)}", file=buf)
    print(f"P-value   = {format_number(p_value)}", file=buf)
    print(f"Conclusion = {conclusion}", file=buf)
    print(
        "\nDecision: non-parametric paired/rank tests are used; diagnostics document "
        "normality/variance assumptions rather than selecting a parametric path.",
        file=buf,
    )

    if k == 2:
        print("\n>> Wilcoxon 's test\n", file=buf)
        statistic, p_value = wilcoxon_signed_test(
            matrix.iloc[:, 0].to_numpy(), matrix.iloc[:, 1].to_numpy()
        )
        print(f"Statistic = {format_number(statistic)}", file=buf)
        print(f"P-value   = {format_number(p_value)}", file=buf)
        return buf.getvalue(), None

    n_way_text, corrected_pw = _analyse_n_way(matrix, spec.higher_is_better, sorted_summary, alpha)
    buf.write(n_way_text)
    return buf.getvalue(), corrected_pw


def _analyse_n_way(
    matrix: pd.DataFrame,
    higher_is_better: bool,
    sorted_summary: pd.DataFrame,
    alpha: float,
) -> tuple[str, pd.DataFrame]:
    """Build the N-way test suite analysis text.

    Executes Iman-Davenport, Hommel 1×N (each algorithm as control), and
    Bergmann-Hommel N×N on the *rank matrix* — matching scmamp's
    ``postHocTest(use.rank=TRUE)`` default.

    Args:
        matrix: File × algorithm raw value matrix.
        higher_is_better: Ranking direction passed to
            :func:`~alglab.study.stats.rank_matrix`.
        sorted_summary: Summary table sorted by ascending mean rank.
        alpha: Significance level.

    Returns:
        A ``(text, corrected_pairwise)`` tuple where *corrected_pairwise* is
        the k × k adjusted p-value matrix (diagonal NaN).
    """
    buf = io.StringIO()
    ranks = rank_matrix(matrix, higher_is_better)

    print("\n>> Iman-Davenport's test\n", file=buf)
    statistic, p_value, df1, df2 = iman_davenport_test(matrix, higher_is_better)
    print(f"Statistic F({df1}, {df2}) = {format_number(statistic)}", file=buf)
    print(f"P-Value = {format_number(p_value)}", file=buf)

    print("\n>> Hommel's test (1 x n) [To be read per column!]\n", file=buf)
    hpv = pd.DataFrame(index=matrix.columns)
    for control_name in sorted_summary.index:
        control_idx = int(matrix.columns.get_loc(control_name))  # type: ignore[arg-type]
        raw = raw_wilcoxon_pvalues(ranks, control=control_idx)
        hpv[control_name] = correct_control_matrix(raw).iloc[0].reindex(matrix.columns)
    print(_fmt_table(hpv), file=buf)

    print("\n>> Bergmann-Hommel's test (n x n)\n", file=buf)
    if matrix.shape[1] <= 9:
        method = "bergmann"
    else:
        method = "shaffer"
        print(
            "WARNING! Using Shaffer correction since there are more than 9 techniques! :/\n",
            file=buf,
        )
    raw_pw = raw_wilcoxon_pvalues(ranks)
    corrected_pw = correct_pairwise(raw_pw, method=method)
    print(_fmt_table(corrected_pw), file=buf)

    print("\n>> Critical difference diagram\n", file=buf)
    print("See the plots panel.", file=buf)
    print("\n>> Boxplot\n", file=buf)
    print("See the plots panel.", file=buf)

    return buf.getvalue(), corrected_pw

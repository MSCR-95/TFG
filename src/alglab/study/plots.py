"""Matplotlib figures for statistical study output.

All public functions save files to disk in every format listed in
:data:`FIGURE_FORMATS` and return the list of generated paths.

Figures follow a publication-friendly style: DejaVu Sans font, no top/right
spines, muted grid, and a colour-blind-aware palette.  PDF and SVG outputs
embed fonts as outlines (``pdf.fonttype=42``, ``svg.fonttype="none"``) so they
render correctly in LaTeX workflows.

Matplotlib is imported lazily inside each function to keep module-level import
time low and to avoid forcing a non-``Agg`` backend when the module is imported
in headless environments (e.g. tests).
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

import numpy as np
import pandas as pd

from ._formatting import format_number

FIGURE_FORMATS: tuple[str, ...] = ("pdf", "svg", "png")
FIGURE_DPI = 160
FIGURE_STYLE = {
    "font.family": "DejaVu Sans",
    "font.size": 9,
    "axes.titlesize": 11,
    "axes.labelsize": 9,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.fontsize": 8,
    "figure.titlesize": 12,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.color": "#d9d9d9",
    "grid.linewidth": 0.6,
    "grid.alpha": 0.8,
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
    "svg.fonttype": "none",
}
PALETTE = (
    "#4E79A7",
    "#F28E2B",
    "#59A14F",
    "#E15759",
    "#B07AA1",
    "#76B7B2",
    "#EDC948",
    "#9C755F",
    "#BAB0AC",
)


def _color(index: int) -> str:
    return PALETTE[index % len(PALETTE)]


def _save(fig: Figure, stem: str, output_dir: Path, formats: Sequence[str]) -> list[Path]:
    import matplotlib.pyplot as plt

    paths = []
    for fmt in formats:
        path = output_dir / f"{stem}.{fmt}"
        fig.savefig(path, dpi=FIGURE_DPI, bbox_inches="tight")
        paths.append(path)
    plt.close(fig)
    return paths


def _style_axes(ax: Axes, title: str, xlabel: str = "", ylabel: str = "") -> None:
    ax.set_title(title, pad=10, fontweight="bold")
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)


def plot_boxplot(
    data: pd.DataFrame,
    summary: pd.DataFrame,
    variable: str,
    output_dir: Path,
    formats: Sequence[str] = FIGURE_FORMATS,
) -> list[Path]:
    """Generate a boxplot of algorithm performance, ordered by mean rank.

    Algorithms are sorted from best to worst rank (left to right).  Each box
    is filled with a distinct palette colour.

    Args:
        data: File × algorithm value matrix.
        summary: Precomputed summary table from
            :func:`~alglab.study.stats.summary_table`, used for rank ordering.
        variable: Display name for the measure (used in the plot title and
            output file stem).
        output_dir: Directory where output files are written.
        formats: Sequence of file format extensions (e.g. ``("pdf", "png")``).

    Returns:
        List of paths to the generated files, one per format.
    """
    import matplotlib.pyplot as plt

    ordered = list(summary.sort_values("RANK").index)
    ordered_data = data[ordered]

    with plt.rc_context(FIGURE_STYLE):
        fig, ax = plt.subplots(figsize=(max(8, len(data.columns) * 1.2), 4.5))
        box = ax.boxplot(
            [ordered_data[col].dropna().to_numpy(dtype=float) for col in ordered],
            tick_labels=ordered,
            patch_artist=True,
            medianprops={"color": "#222222", "linewidth": 1.2},
            boxprops={"linewidth": 0.9},
            whiskerprops={"linewidth": 0.9},
            capprops={"linewidth": 0.9},
            flierprops={
                "marker": "o",
                "markersize": 2.5,
                "markerfacecolor": "#666666",
                "markeredgecolor": "#666666",
                "alpha": 0.55,
            },
        )
        for idx, patch in enumerate(box["boxes"]):
            patch.set_facecolor(_color(idx))
            patch.set_alpha(0.72)
        _style_axes(ax, variable)
        ax.tick_params(axis="x", labelrotation=35)
        fig.tight_layout()
    return _save(fig, f"{variable}-boxplot", output_dir, formats)


def plot_density(
    data: pd.DataFrame,
    summary: pd.DataFrame,
    variable: str,
    output_dir: Path,
    formats: Sequence[str] = FIGURE_FORMATS,
) -> list[Path]:
    """Generate a kernel density estimate plot, one curve per algorithm.

    Algorithms are ordered best-to-worst by mean rank.  Algorithms with fewer
    than two unique values are skipped (KDE is undefined).

    Args:
        data: File × algorithm value matrix.
        summary: Precomputed summary table used for rank ordering.
        variable: Display name for the measure.
        output_dir: Directory where output files are written.
        formats: Sequence of file format extensions.

    Returns:
        List of paths to the generated files, one per format.
    """
    import matplotlib.pyplot as plt

    ordered = list(summary.sort_values("RANK").index)
    ordered_data = data[ordered]

    with plt.rc_context(FIGURE_STYLE):
        fig, ax = plt.subplots(figsize=(max(8, len(data.columns) * 1.2), 4.5))
        for idx, algo in enumerate(ordered):
            values = ordered_data[algo].dropna()
            if len(values) < 2 or values.nunique(dropna=True) < 2:
                continue
            values.plot.kde(ax=ax, linewidth=1.6, label=algo, color=_color(idx))
        _style_axes(ax, f"{variable} density", ylabel="density")
        handles, labels = ax.get_legend_handles_labels()
        if handles:
            ax.legend(handles, labels, frameon=False)
        fig.tight_layout()
    return _save(fig, f"{variable}-density", output_dir, formats)


def plot_qq(
    data: pd.DataFrame,
    summary: pd.DataFrame,
    variable: str,
    output_dir: Path,
    formats: Sequence[str] = FIGURE_FORMATS,
) -> list[Path]:
    """Generate a grid of Normal Q-Q plots, one subplot per algorithm.

    Subplots are arranged in rows of up to three columns, ordered best-to-worst
    by mean rank.  Algorithms with fewer than three values or no variation
    display a placeholder text.

    Args:
        data: File × algorithm value matrix.
        summary: Precomputed summary table used for rank ordering.
        variable: Display name for the measure.
        output_dir: Directory where output files are written.
        formats: Sequence of file format extensions.

    Returns:
        List of paths to the generated files, one per format.
    """
    import matplotlib.pyplot as plt
    from scipy.stats import probplot

    ordered = list(summary.sort_values("RANK").index)
    ordered_data = data[ordered]
    ncols = min(3, max(1, len(ordered)))
    nrows = math.ceil(len(ordered) / ncols)

    with plt.rc_context(FIGURE_STYLE):
        fig, axes = plt.subplots(nrows, ncols, figsize=(ncols * 3.5, nrows * 3))
        axes_flat = np.atleast_1d(axes).flatten()
        for idx, algo in enumerate(ordered):
            ax = axes_flat[idx]
            values = ordered_data[algo].dropna().to_numpy(dtype=float)
            if len(values) < 3 or np.allclose(values, values[0]):
                ax.text(0.5, 0.5, "insufficient variation", ha="center", va="center")
                ax.set_axis_off()
            else:
                probplot(values, dist="norm", plot=ax)
                ax.get_lines()[0].set_markerfacecolor(_color(idx))
                ax.get_lines()[0].set_markeredgecolor(_color(idx))
                ax.get_lines()[0].set_alpha(0.75)
                ax.get_lines()[1].set_color("#222222")
                ax.get_lines()[1].set_linewidth(1.1)
            _style_axes(ax, algo, xlabel="theoretical quantiles", ylabel="ordered values")
        for idx in range(len(ordered), len(axes_flat)):
            axes_flat[idx].set_visible(False)
        fig.suptitle(f"{variable} Q-Q plots", fontweight="bold")
        fig.tight_layout()
    return _save(fig, f"{variable}-qq", output_dir, formats)


def plot_ranking(
    summary: pd.DataFrame,
    corrected_pairwise: pd.DataFrame,
    variable: str,
    output_dir: Path,
    alpha: float = 0.05,
    formats: Sequence[str] = FIGURE_FORMATS,
) -> list[Path]:
    """Generate a critical-difference–style mean rank diagram.

    Algorithms are placed on a horizontal axis at their mean rank position.
    Non-significant pairs (corrected p-value > *alpha*) are connected by a
    thick horizontal line at a fixed vertical offset.  Only available for k ≥ 3.

    Args:
        summary: Precomputed summary table with a ``"RANK"`` column.
        corrected_pairwise: Corrected k × k p-value matrix from
            :func:`~alglab.study.stats.correct_pairwise`.
        variable: Display name for the measure.
        output_dir: Directory where output files are written.
        alpha: Significance threshold; pairs with corrected p > *alpha* are
            connected.
        formats: Sequence of file format extensions.

    Returns:
        List of paths to the generated files, one per format.
    """
    import matplotlib.pyplot as plt

    ordered = summary.sort_values("RANK")

    with plt.rc_context(FIGURE_STYLE):
        fig, ax = plt.subplots(figsize=(max(8, len(ordered) * 1.2), 2.8))
        ax.set_xlim(0.5, len(ordered) + 0.5)
        ax.set_ylim(0, 1)
        ax.set_yticks([])
        ax.set_xlabel("Mean rank")
        ax.grid(False)
        ax.scatter(ordered["RANK"], [0.6] * len(ordered), color="#222222", zorder=3)
        for name, rank in ordered["RANK"].items():
            ax.text(rank, 0.74, str(name), rotation=35, ha="right", va="bottom", fontsize=8)
        idx_list = list(ordered.index)
        for i, name_i in enumerate(idx_list):
            for name_j in idx_list[i + 1 :]:
                if corrected_pairwise.loc[name_i, name_j] > alpha:
                    ax.plot(
                        [ordered.loc[name_i, "RANK"], ordered.loc[name_j, "RANK"]],
                        [0.42, 0.42],
                        lw=4,
                        color="#4E79A7",
                        alpha=0.7,
                        solid_capstyle="round",
                    )
        _style_axes(ax, f"{variable} ranking", xlabel="Mean rank")
        fig.tight_layout()
    return _save(fig, f"{variable}-ranking", output_dir, formats)


def plot_pvalues(
    corrected_pairwise: pd.DataFrame,
    summary: pd.DataFrame,
    variable: str,
    output_dir: Path,
    formats: Sequence[str] = FIGURE_FORMATS,
) -> list[Path]:
    """Generate a heatmap of corrected pairwise p-values.

    Rows and columns are ordered best-to-worst by mean rank.  Diagonal cells
    (self-comparison) are masked.  Only available for k ≥ 3.

    Args:
        corrected_pairwise: Corrected k × k p-value matrix from
            :func:`~alglab.study.stats.correct_pairwise`.
        summary: Precomputed summary table used for rank ordering.
        variable: Display name for the measure.
        output_dir: Directory where output files are written.
        formats: Sequence of file format extensions.

    Returns:
        List of paths to the generated files, one per format.
    """
    import matplotlib.pyplot as plt

    ordered_cols = list(summary.sort_values("RANK").index)
    ordered_p = corrected_pairwise.loc[ordered_cols, ordered_cols]
    matrix = ordered_p.to_numpy(dtype=float)

    with plt.rc_context(FIGURE_STYLE):
        fig, ax = plt.subplots(
            figsize=(max(7, len(ordered_cols) * 1.1), max(5, len(ordered_cols) * 0.9))
        )
        masked = np.ma.masked_invalid(matrix)
        im = ax.imshow(masked, vmin=0, vmax=1, cmap="Blues_r")
        ax.set_xticks(range(len(ordered_p.columns)), labels=ordered_p.columns)
        ax.set_yticks(range(len(ordered_p.index)), labels=ordered_p.index)
        ax.tick_params(axis="x", labelrotation=45)
        ax.grid(False)
        for i in range(matrix.shape[0]):
            for j in range(matrix.shape[1]):
                v = matrix[i, j]
                if not np.isnan(v):
                    ax.text(
                        j, i, format_number(v, precision=4), ha="center", va="center", fontsize=7
                    )
        _style_axes(ax, f"{variable} corrected p-values")
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="p-value")
        fig.tight_layout()
    return _save(fig, f"{variable}-pvalues", output_dir, formats)


def plot_all(
    data: pd.DataFrame,
    summary: pd.DataFrame,
    corrected_pairwise: pd.DataFrame | None,
    variable: str,
    output_dir: Path,
    alpha: float = 0.05,
    formats: Sequence[str] = FIGURE_FORMATS,
    include_nway_plots: bool = False,
) -> list[Path]:
    """Generate all applicable plots for one measure and return their paths.

    Always produces boxplot, density, and Q-Q plots.  Ranking and p-value
    heatmap are added when ``include_nway_plots=True`` and
    *corrected_pairwise* is not ``None``.

    Args:
        data: File × algorithm value matrix.
        summary: Precomputed summary table from
            :func:`~alglab.study.stats.summary_table`.
        corrected_pairwise: Corrected k × k p-value matrix, or ``None`` for
            two-algorithm comparisons.
        variable: Display name for the measure (used in titles and file stems).
        output_dir: Directory where plots are written.  Created if absent.
        alpha: Significance threshold for the ranking diagram.
        formats: File format extensions to generate.
        include_nway_plots: When ``True`` and *corrected_pairwise* is provided,
            also generate the ranking diagram and p-value heatmap.

    Returns:
        Sorted list of all generated plot file paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    generated: list[Path] = []
    generated.extend(plot_boxplot(data, summary, variable, output_dir, formats))
    generated.extend(plot_density(data, summary, variable, output_dir, formats))
    generated.extend(plot_qq(data, summary, variable, output_dir, formats))
    if include_nway_plots and corrected_pairwise is not None:
        generated.extend(
            plot_ranking(summary, corrected_pairwise, variable, output_dir, alpha, formats)
        )
        generated.extend(plot_pvalues(corrected_pairwise, summary, variable, output_dir, formats))
    return generated

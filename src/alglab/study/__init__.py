"""Public API for building matrices and running statistical studies."""

from .analysis import run_study
from .io import (
    aggregate_seeds,
    build_matrix,
    extract_measure,
    filter_ok,
    impute_nan,
    pivot_matrix,
    prepare_matrix,
    read_jsonl,
)
from .schema import MeasureSpec, StudyConfig
from .stats import (
    correct_control_matrix,
    correct_pairwise,
    correction_bergmann_hommel_vector,
    correction_shaffer_vector,
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

__all__ = [
    # schema
    "MeasureSpec",
    "StudyConfig",
    # io
    "aggregate_seeds",
    "build_matrix",
    "extract_measure",
    "filter_ok",
    "impute_nan",
    "pivot_matrix",
    "prepare_matrix",
    "read_jsonl",
    # stats
    "correct_control_matrix",
    "correct_pairwise",
    "correction_bergmann_hommel_vector",
    "correction_shaffer_vector",
    "exhaustive_sets",
    "iman_davenport_test",
    "levene_diagnostic",
    "p_adjust_hommel",
    "pairs_for",
    "rank_matrix",
    "raw_wilcoxon_pvalues",
    "shapiro_diagnostics",
    "summary_table",
    "wilcoxon_signed_test",
    # analysis
    "run_study",
]

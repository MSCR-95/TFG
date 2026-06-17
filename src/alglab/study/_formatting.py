"""Shared number-formatting utility for study text and plot outputs."""

from __future__ import annotations

import numpy as np
import pandas as pd


def format_number(
    value: float | int | str | np.integer | None,
    *,
    precision: int = 6,
) -> str:
    """Format a numeric value as a compact string for reports and plot annotations.

    Args:
        value: The value to format.  ``str`` values are returned as-is.
        precision: Number of significant digits in the mantissa for both
            decimal and scientific notation output.

    Returns:
        A compact string: ``""`` for ``None`` / NaN, ``str(value)`` for
        integers, ``"0"`` for exact zero, scientific notation for very small
        (< 1e-4) or very large (≥ 1e5) magnitudes, and a trimmed decimal
        string otherwise.
    """
    if isinstance(value, str):
        return value
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, int | np.integer):
        return str(value)
    fv = float(value)
    if fv == 0:
        return "0"
    abs_v = abs(fv)
    if abs_v < 1e-4 or abs_v >= 1e5:
        return f"{fv:.{precision}e}"
    return f"{fv:.{precision}f}".rstrip("0").rstrip(".")

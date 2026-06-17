"""Shared configuration types for the study module.

:class:`MeasureSpec` describes a single metric to extract from the JSONL
output, and :class:`StudyConfig` groups one or more measures with the input
file path and significance level for a complete analysis run.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class MeasureSpec:
    """Specification for extracting and aggregating one metric from JSONL results.

    The ``path`` attribute uses the same dot-notation convention as
    ``pd.json_normalize``: dots separate nesting levels in the original JSON.

    Example paths:

    - ``"result.satisfaction_ratio"`` — nested field under the ``result`` dict.
    - ``"run_duration_s"`` — top-level field of the result record.

    Attributes:
        path: Dot-separated field path to the metric in the flattened DataFrame.
        label: Human-readable display name.  Defaults to the last path segment
            (e.g. ``"satisfaction_ratio"`` for
            ``"result.satisfaction_ratio"``).
        higher_is_better: Ranking direction.  ``True`` means larger values are
            better (e.g. accuracy); ``False`` means smaller values are better
            (e.g. runtime).
        aggregation: How to collapse multiple seeds for the same
            ``(file, algorithm)`` pair:

            - ``"mean"`` — arithmetic mean.
            - ``"median"`` — median.
            - ``"best"`` — max if ``higher_is_better``, min otherwise.
            - ``"worst"`` — min if ``higher_is_better``, max otherwise.

        cast_bool_to_int: When ``True``, boolean column values are cast to
            ``int`` before analysis.  Useful for metrics like
            ``verification_mismatch`` that are stored as booleans.
    """

    path: str
    label: str = ""
    higher_is_better: bool = True
    aggregation: Literal["mean", "median", "best", "worst"] = "mean"
    cast_bool_to_int: bool = False

    def __post_init__(self) -> None:
        """Fill the label from the metric path when omitted."""
        if not self.label:
            object.__setattr__(self, "label", self.path.rsplit(".", 1)[-1])


@dataclass
class StudyConfig:
    """Top-level configuration for a single study run.

    Bundles the input data path, one or more measure specifications, and the
    significance level used by all statistical tests.

    Attributes:
        input_jsonl: Path to the JSONL file produced by ``alglab run``.
        measures: List of metrics to analyse.  Must contain at least one entry.
        alpha: Significance level for all statistical tests (default: 0.05).
    """

    input_jsonl: Path
    measures: list[MeasureSpec]
    alpha: float = 0.05

    @classmethod
    def from_yaml(cls, path: Path) -> StudyConfig:
        """Load a :class:`StudyConfig` from a YAML file.

        The ``input_jsonl`` path in the YAML is resolved relative to the
        directory that contains the YAML file, so configs are portable as long
        as the relative path from config to data stays the same.

        Args:
            path: Path to the YAML configuration file.

        Returns:
            A fully populated :class:`StudyConfig` instance.

        Raises:
            ValueError: If the YAML root is not a mapping, or if no measures
                are defined.
            KeyError: If the required ``input_jsonl`` key is missing.

        Example:
            >>> config = StudyConfig.from_yaml(Path("study.yaml"))
            >>> config.alpha
            0.05
        """
        import yaml

        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Study config must be a YAML mapping")
        jsonl_path = Path(data["input_jsonl"])
        if not jsonl_path.is_absolute():
            jsonl_path = path.parent / jsonl_path
        measures = [
            MeasureSpec(
                path=raw["path"],
                label=raw.get("label", ""),
                higher_is_better=raw.get("higher_is_better", True),
                aggregation=raw.get("aggregation", "mean"),
                cast_bool_to_int=raw.get("cast_bool_to_int", False),
            )
            for raw in data.get("measures", [])
        ]
        if not measures:
            raise ValueError("Study config must include at least one measure")
        return cls(
            input_jsonl=jsonl_path,
            measures=measures,
            alpha=float(data.get("alpha", 0.05)),
        )

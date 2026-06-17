"""Backing logic for ``alglab study``. Separated from the Typer glue."""

from __future__ import annotations

from pathlib import Path


def run_from_config(
    config_path: Path,
    out_dir: Path,
    *,
    alpha_override: float | None = None,
) -> list[Path]:
    """Load a StudyConfig YAML and run the full study.

    *alpha_override* replaces the alpha in the YAML when provided explicitly
    via CLI (the CLI default is 0.05, so callers should pass None if they want
    the YAML value to win; passing an explicit float always overrides).
    """
    from alglab.study.analysis import run_study
    from alglab.study.schema import StudyConfig

    config = StudyConfig.from_yaml(config_path)
    if alpha_override is not None:
        config = StudyConfig(
            input_jsonl=config.input_jsonl,
            measures=config.measures,
            alpha=alpha_override,
        )
    return run_study(config, out_dir)

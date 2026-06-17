"""``alglab study`` — statistical analysis of experiment results."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

study_app = typer.Typer(name="study", help="Analyse experiment results.", no_args_is_help=True)


@study_app.command()
def run(
    config: Annotated[
        str,
        typer.Option(
            "--config",
            "-c",
            help="Path to YAML study configuration.",
        ),
    ],
    output_dir: Annotated[
        str,
        typer.Option(
            "--output",
            "-o",
            help="Output directory for reports.",
        ),
    ] = "report",
    alpha: Annotated[
        float | None,
        typer.Option(help="Significance level (overrides config value; default: use config)."),
    ] = None,
) -> None:
    """Run statistical analysis from a YAML config and write reports."""
    from rich.console import Console

    from ._study import run_from_config

    console = Console()
    config_path = Path(config)
    out = Path(output_dir)
    console.print(f"[bold]Loading config from[/] {config_path}")
    generated = run_from_config(config_path, out, alpha_override=alpha)
    console.print(f"[green]Done.[/] {len(generated)} files written to {out}")

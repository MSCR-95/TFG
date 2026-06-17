"""CLI entry point: ``alglab`` with ``run``, ``study`` and ``experiment`` commands."""

from __future__ import annotations

from typing import Annotated

import typer

from .experiment import experiment_app as experiment_app_ref
from .run import run_experiment
from .study import study_app as study_app_ref

app = typer.Typer(
    name="alglab",
    help="Algorithm lab: run experiments and analyse results.",
    no_args_is_help=True,
)
app.add_typer(experiment_app_ref, name="experiment")
app.add_typer(study_app_ref, name="study")


@app.command()
def run(
    directory: Annotated[
        str,
        typer.Option(
            "--dir",
            help="Input directory with problem instances.",
        ),
    ],
    algorithms: Annotated[
        list[str],
        typer.Option(
            "--algos",
            "-A",
            help="Algorithm names to run (repeatable).",
        ),
    ],
    pattern: Annotated[
        str,
        typer.Option(help="Glob pattern for instance files."),
    ] = "*",
    recursive: Annotated[
        bool,
        typer.Option(help="Scan input directory recursively."),
    ] = False,
    sort_files: Annotated[
        bool,
        typer.Option(
            "--sort-files/--no-sort-files",
            help="Sort instance files by path before creating jobs.",
        ),
    ] = False,
    n_jobs: Annotated[
        int,
        typer.Option(help="Number of workers (-1 = all CPUs)."),
    ] = 1,
    timeout: Annotated[
        float | None,
        typer.Option(help="Per-job timeout in seconds."),
    ] = None,
    out_path: Annotated[
        str,
        typer.Option(help="Output JSONL file."),
    ] = "output/results.jsonl",
    append: Annotated[
        bool,
        typer.Option(help="Append to output file instead of overwriting."),
    ] = False,
    seed: Annotated[
        int | None,
        typer.Option(help="Seed for all jobs in this run."),
    ] = None,
    log_level: Annotated[
        str,
        typer.Option(help="Log level."),
    ] = "INFO",
    log_dir: Annotated[
        str | None,
        typer.Option(help="Directory for rotating log files."),
    ] = None,
) -> None:
    """Run algorithms on problem instances."""
    raise SystemExit(
        run_experiment(
            directory=directory,
            algorithms=algorithms,
            pattern=pattern,
            recursive=recursive,
            sort_files=sort_files,
            n_jobs=n_jobs,
            timeout=timeout,
            out_path=out_path,
            append=append,
            seed=seed,
            log_level=log_level,
            log_dir=log_dir,
        )
    )


def cli() -> None:
    """Thin wrapper called by the ``alglab`` console script."""
    import multiprocessing

    multiprocessing.freeze_support()
    app()

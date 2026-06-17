"""``alglab experiment`` — reproducible experiment orchestration."""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
import yaml
from loguru import logger

from alglab._logging import setup

from .run import run_experiment

experiment_app = typer.Typer(
    name="experiment",
    help="Create and run reproducible experiment directories.",
    no_args_is_help=True,
)

_VALID_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


@experiment_app.command()
def run(
    name: Annotated[
        str,
        typer.Option(
            "--name",
            help="Experiment directory name under --root.",
        ),
    ],
    config: Annotated[
        str,
        typer.Option(
            "--config",
            "-c",
            help="Path to experiment YAML configuration.",
        ),
    ],
    root: Annotated[
        str,
        typer.Option(
            "--root",
            help="Root directory for experiments.",
        ),
    ] = "experiments",
    overwrite: Annotated[
        bool,
        typer.Option(
            "--overwrite",
            help="Delete an existing experiment directory before running.",
        ),
    ] = False,
) -> None:
    """Run generation, engine and study into ``experiments/<name>/``."""
    from rich.console import Console

    console = Console()
    try:
        exit_code = run_experiment_workflow(
            name=name,
            config_path=Path(config),
            root=Path(root),
            overwrite=overwrite,
        )
    except ValueError as exc:
        console.print(f"[red]Error:[/] {exc}")
        raise typer.Exit(2) from exc
    except Exception as exc:
        console.print(f"[red]Error:[/] {type(exc).__name__}: {exc}")
        raise typer.Exit(1) from exc

    raise typer.Exit(exit_code)


def run_experiment_workflow(
    *,
    name: str,
    config_path: Path,
    root: Path,
    overwrite: bool = False,
) -> int:
    _validate_experiment_name(name)
    if not config_path.is_file():
        raise ValueError(f"--config does not exist or is not a file: {config_path}")

    raw_config = _load_config(config_path)
    exp_dir = root / name
    paths = _ExperimentPaths(
        root=exp_dir,
        config=exp_dir / "config",
        generation=exp_dir / "generation",
        data=exp_dir / "data",
        engine=exp_dir / "engine",
        study=exp_dir / "study",
    )
    _prepare_directories(paths, overwrite=overwrite)

    copied_config = paths.config / "experiment.yaml"
    shutil.copy2(config_path, copied_config)

    manifest = _base_manifest(name, config_path, paths)
    try:
        setup(
            level=str(raw_config.get("generation", {}).get("log_level", "INFO")),
            log_dir=paths.generation / "logs",
        )
        logger.info("Starting generation phase")
        generated = _run_generation(raw_config["generation"], paths.data)
        logger.info("Generation phase completed: instances={}", len(generated))
        logger.info("Generation logs: {}", paths.generation / "logs")
        manifest["phases"]["generation"] = {
            "status": "ok",
            "instances": len(generated),
        }

        logger.info("Starting engine phase")
        engine_exit_code = _run_engine(raw_config["engine"], paths)
        result_count = _count_jsonl_rows(paths.engine / "results.jsonl")
        logger.info(
            "Engine phase completed: exit_code={} results={}",
            engine_exit_code,
            result_count,
        )
        logger.info("Engine logs: {}", paths.engine / "logs")
        manifest["phases"]["engine"] = {
            "status": "ok" if engine_exit_code == 0 else "failed",
            "exit_code": engine_exit_code,
            "results": result_count,
        }

        if engine_exit_code != 0:
            manifest["phases"]["study"] = {"status": "skipped"}
            return engine_exit_code

        setup(
            level=str(raw_config.get("study", {}).get("log_level", "INFO")),
            log_dir=paths.study / "logs",
        )
        logger.info("Starting study phase")
        reports = _run_study(raw_config["study"], paths)
        logger.info("Study phase completed: files={}", len(reports))
        logger.info("Study logs: {}", paths.study / "logs")
        manifest["phases"]["study"] = {
            "status": "ok",
            "reports": [str(path.relative_to(paths.root)) for path in reports],
        }
        return 0
    finally:
        _write_manifest(paths.config / "manifest.json", manifest)


@dataclass
class _ExperimentPaths:
    root: Path
    config: Path
    generation: Path
    data: Path
    engine: Path
    study: Path


def _validate_experiment_name(name: str) -> None:
    if not name or not _VALID_NAME_RE.fullmatch(name):
        raise ValueError(
            "--name must start with a letter or digit and contain only letters, digits, '.', '_', or '-'"
        )


def _load_config(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Config must be a YAML dictionary")
    for section in ["generation", "engine", "study"]:
        if section not in data or not isinstance(data[section], dict):
            raise ValueError(f"Config must include a '{section}' mapping")
    _validate_engine_config(data["engine"])
    _validate_study_config(data["study"])
    return data


def _validate_engine_config(engine: dict[str, Any]) -> None:
    algorithms = engine.get("algorithms")
    if not isinstance(algorithms, list) or not algorithms:
        raise ValueError("engine.algorithms must be a non-empty list")
    if not all(isinstance(algo, str) and algo.strip() for algo in algorithms):
        raise ValueError("engine.algorithms must contain non-empty strings")


def _validate_study_config(study: dict[str, Any]) -> None:
    measures = study.get("measures")
    if not isinstance(measures, list) or not measures:
        raise ValueError("study.measures must be a non-empty list")
    for raw in measures:
        if not isinstance(raw, dict) or not isinstance(raw.get("path"), str):
            raise ValueError("Each study measure must include a string 'path'")


def _prepare_directories(paths: _ExperimentPaths, *, overwrite: bool) -> None:
    if paths.root.exists():
        if not overwrite:
            raise ValueError(f"Experiment already exists: {paths.root}")
        shutil.rmtree(paths.root)
    for path in [
        paths.config,
        paths.generation / "logs",
        paths.data,
        paths.engine / "logs",
        paths.study,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def _base_manifest(name: str, config_path: Path, paths: _ExperimentPaths) -> dict[str, Any]:
    return {
        "name": name,
        "created_at": datetime.now(UTC).isoformat(),
        "source_config": str(config_path),
        "paths": {
            "root": str(paths.root),
            "config": str(paths.config),
            "generation": str(paths.generation),
            "data": str(paths.data),
            "engine": str(paths.engine),
            "study": str(paths.study),
        },
        "phases": {
            "generation": {"status": "pending"},
            "engine": {"status": "pending"},
            "study": {"status": "pending"},
        },
    }


def _run_generation(generation: dict[str, Any], data_dir: Path) -> list[Path]:
    from alglab.algorithms.max2sat.generator import (
        generate_instances,
        parse_float_spec,
        parse_int_spec,
    )

    try:
        vars_values = parse_int_spec(str(generation["vars"]))
        densities = parse_float_spec(str(generation["densities"]))
        n_files = int(generation["n_files"])
        k = int(generation["k"])
    except KeyError as exc:
        raise ValueError(f"generation.{exc.args[0]} is required") from exc

    return generate_instances(
        n_files=n_files,
        vars_values=vars_values,
        densities=densities,
        k=k,
        out=data_dir,
        seed_base=int(generation.get("seed_base", 0)),
        jobs=int(generation.get("jobs", 1)),
        clean=False,
        dry_run=False,
    )


def _run_engine(engine: dict[str, Any], paths: _ExperimentPaths) -> int:
    return run_experiment(
        directory=str(paths.data),
        algorithms=list(engine["algorithms"]),
        pattern=str(engine.get("pattern", "*.cnf")),
        recursive=bool(engine.get("recursive", True)),
        sort_files=bool(engine.get("sort_files", False)),
        n_jobs=int(engine.get("n_jobs", 1)),
        timeout=_optional_float(engine.get("timeout")),
        out_path=str(paths.engine / "results.jsonl"),
        append=False,
        seed=_optional_int(engine.get("seed")),
        log_level=str(engine.get("log_level", "INFO")),
        log_dir=str(paths.engine / "logs"),
    )


def _run_study(study: dict[str, Any], paths: _ExperimentPaths) -> list[Path]:
    from alglab.study.analysis import run_study
    from alglab.study.schema import MeasureSpec, StudyConfig

    alpha = float(study.get("alpha", 0.05))
    measures = [
        MeasureSpec(
            path=raw["path"],
            label=raw.get("label", ""),
            higher_is_better=raw.get("higher_is_better", True),
            aggregation=raw.get("aggregation", "mean"),
            cast_bool_to_int=raw.get("cast_bool_to_int", False),
        )
        for raw in study["measures"]
    ]
    config = StudyConfig(
        input_jsonl=paths.engine / "results.jsonl",
        measures=measures,
        alpha=alpha,
    )
    return run_study(config, paths.study)


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, str | int | float):
        return float(value)
    raise ValueError(f"Expected float-compatible value, got {type(value).__name__}")


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, str | int | float):
        return int(value)
    raise ValueError(f"Expected int-compatible value, got {type(value).__name__}")


def _count_jsonl_rows(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

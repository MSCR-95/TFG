from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from alglab._cli.main import app

runner = CliRunner()


class TestCLI:
    def test_runs_named_algorithm_and_writes_jsonl(self, tmp_path: Path):
        input_dir = tmp_path / "inputs"
        input_dir.mkdir()
        (input_dir / "sample.cnf").write_text(
            "c test instance\np cnf 2 2\n1 0\n-1 2 0\n", encoding="utf-8"
        )

        output_path = tmp_path / "results.jsonl"

        result = runner.invoke(
            app,
            [
                "run",
                "--dir",
                str(input_dir),
                "--pattern",
                "*.cnf",
                "--algos",
                "maxsat_brute",
                "--n-jobs",
                "1",
                "--out-path",
                str(output_path),
            ],
        )

        assert result.exit_code == 0, result.output
        assert output_path.exists()

        lines = [line for line in output_path.read_text(encoding="utf-8").splitlines() if line]
        assert len(lines) == 1

        record = json.loads(lines[0])
        assert record["algorithm"] == "maxsat_brute"
        assert record["status"] == "ok"
        assert record["result"]["num_vars"] == 2
        assert record["result"]["num_clauses"] == 2
        assert record["result"]["satisfied_clauses"] == 2
        assert record["result"]["satisfaction_ratio"] == 1.0
        assert record["result"]["verification_mismatch"] is False
        assert "assignment" not in record["result"]
        assert record["pre_run_duration_s"] is not None
        assert record["run_duration_s"] is not None
        assert record["post_run_duration_s"] is not None

    def test_seed_derives_unique_per_file(self, tmp_path: Path):
        input_dir = tmp_path / "inputs"
        input_dir.mkdir()
        cnf = "c test\np cnf 2 2\n1 0\n-1 2 0\n"
        (input_dir / "a.cnf").write_text(cnf, encoding="utf-8")
        (input_dir / "b.cnf").write_text(cnf, encoding="utf-8")

        output_path = tmp_path / "results.jsonl"
        result = runner.invoke(
            app,
            [
                "run",
                "--dir",
                str(input_dir),
                "--pattern",
                "*.cnf",
                "--algos",
                "maxsat_brute",
                "--n-jobs",
                "1",
                "--out-path",
                str(output_path),
                "--seed",
                "42",
            ],
        )
        assert result.exit_code == 0, result.output

        records = [
            json.loads(line)
            for line in output_path.read_text(encoding="utf-8").splitlines()
            if line
        ]
        seeds = [r["seed"] for r in records]
        assert len(seeds) == 2
        assert seeds[0] != seeds[1], "different files must get different derived seeds"
        assert all(s != 42 for s in seeds), "derived seeds must differ from global seed"

    def test_seed_is_reproducible(self, tmp_path: Path):
        input_dir = tmp_path / "inputs"
        input_dir.mkdir()
        (input_dir / "x.cnf").write_text("c test\np cnf 2 2\n1 0\n-1 2 0\n", encoding="utf-8")

        def run_once(out: Path) -> int:
            res = runner.invoke(
                app,
                [
                    "run",
                    "--dir",
                    str(input_dir),
                    "--pattern",
                    "*.cnf",
                    "--algos",
                    "maxsat_brute",
                    "--n-jobs",
                    "1",
                    "--out-path",
                    str(out),
                    "--seed",
                    "7",
                ],
            )
            assert res.exit_code == 0, res.output
            return json.loads(out.read_text(encoding="utf-8").strip())["seed"]

        seed_a = run_once(tmp_path / "r1.jsonl")
        seed_b = run_once(tmp_path / "r2.jsonl")
        assert seed_a == seed_b

    @pytest.mark.skipif(
        __import__("importlib.util", fromlist=["find_spec"]).find_spec("dimod") is None,
        reason="dwave-ocean-sdk not installed",
    )
    def test_runs_multiple_algos_and_writes_jsonl(self, tmp_path: Path):
        input_dir = tmp_path / "cnf"
        input_dir.mkdir()
        (input_dir / "small.cnf").write_text(
            "c instancia minima\np cnf 2 2\n1 2 0\n-1 2 0\n",
            encoding="utf-8",
        )

        output_path = tmp_path / "maxsat.jsonl"

        result = runner.invoke(
            app,
            [
                "run",
                "--dir",
                str(input_dir),
                "--pattern",
                "*.cnf",
                "--algos",
                "maxsat_brute",
                "--algos",
                "maxsat_qubo_sa",
                "--n-jobs",
                "1",
                "--out-path",
                str(output_path),
            ],
        )

        assert result.exit_code == 0, result.output
        assert output_path.exists()

        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]

        assert len(rows) == 2
        brute_row = next(r for r in rows if r["algorithm"] == "maxsat_brute")
        assert brute_row["status"] == "ok"

        payload = brute_row["result"]
        assert payload["num_vars"] == 2
        assert payload["num_clauses"] == 2
        assert payload["satisfied_clauses"] == 2

    def test_fails_when_directory_is_invalid(self, tmp_path: Path):
        missing_dir = tmp_path / "missing"

        result = runner.invoke(
            app,
            [
                "run",
                "--dir",
                str(missing_dir),
                "--algos",
                "maxsat_brute",
            ],
        )

        assert result.exit_code == 2
        assert "--dir is not a valid directory" in result.output

    def test_fails_when_algorithm_is_unknown(self, tmp_path: Path):
        input_dir = tmp_path / "inputs"
        input_dir.mkdir()
        (input_dir / "sample.txt").write_text("hola", encoding="utf-8")

        result = runner.invoke(
            app,
            [
                "run",
                "--dir",
                str(input_dir),
                "--algos",
                "algoritmo_inexistente_xyz",
            ],
        )

        assert result.exit_code != 0
        assert "desconocido" in result.output.lower() or "error" in result.output.lower()

    @pytest.mark.parametrize("n_jobs", ["0", "-2"])
    def test_fails_when_n_jobs_is_invalid(self, tmp_path: Path, n_jobs: str):
        input_dir = tmp_path / "inputs"
        input_dir.mkdir()
        (input_dir / "sample.cnf").write_text("p cnf 1 1\n1 0\n", encoding="utf-8")

        result = runner.invoke(
            app,
            [
                "run",
                "--dir",
                str(input_dir),
                "--algos",
                "maxsat_brute",
                "--n-jobs",
                n_jobs,
            ],
        )

        assert result.exit_code == 2
        assert "--n-jobs must be -1 or a positive integer" in result.output

    @pytest.mark.parametrize(
        ("sort_flag", "expected"),
        [
            ([], False),
            (["--sort-files"], True),
        ],
    )
    def test_run_forwards_sort_files_option(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        sort_flag: list[str],
        expected: bool,
    ):
        captured: dict[str, Any] = {}

        def fake_run_experiment(**kwargs: Any) -> int:
            captured.update(kwargs)
            return 0

        monkeypatch.setattr("alglab._cli.main.run_experiment", fake_run_experiment)

        result = runner.invoke(
            app,
            [
                "run",
                "--dir",
                str(tmp_path),
                "--algos",
                "maxsat_brute",
                *sort_flag,
            ],
        )

        assert result.exit_code == 0, result.output
        assert captured["sort_files"] is expected

    def test_returns_one_when_job_fails(self, tmp_path: Path):
        input_dir = tmp_path / "inputs"
        input_dir.mkdir()
        (input_dir / "empty.cnf").write_text("p cnf 1 0\n", encoding="utf-8")
        output_path = tmp_path / "results.jsonl"

        result = runner.invoke(
            app,
            [
                "run",
                "--dir",
                str(input_dir),
                "--algos",
                "maxsat_brute",
                "--out-path",
                str(output_path),
            ],
        )

        assert result.exit_code == 1
        rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
        assert rows[0]["status"] == "algorithm_error"

    def test_experiment_run_creates_reproducible_layout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        config_path = _write_experiment_config(tmp_path)
        root = tmp_path / "experiments"

        def fake_generation(generation: dict[str, Any], data_dir: Path) -> list[Path]:
            data_dir.mkdir(parents=True, exist_ok=True)
            instance = data_dir / "instance_0001.cnf"
            instance.write_text("p cnf 1 1\n1 0\n", encoding="utf-8")
            return [instance]

        def fake_engine(engine: dict[str, Any], paths: Any) -> int:
            results = paths.engine / "results.jsonl"
            results.write_text('{"status": "ok"}\n', encoding="utf-8")
            return 0

        def fake_study(study: dict[str, Any], paths: Any) -> list[Path]:
            report = paths.study / "analysis.txt"
            report.write_text("analysis\n", encoding="utf-8")
            return [report]

        monkeypatch.setattr("alglab._cli.experiment._run_generation", fake_generation)
        monkeypatch.setattr("alglab._cli.experiment._run_engine", fake_engine)
        monkeypatch.setattr("alglab._cli.experiment._run_study", fake_study)

        result = runner.invoke(
            app,
            [
                "experiment",
                "run",
                "--name",
                "exp_001",
                "--config",
                str(config_path),
                "--root",
                str(root),
            ],
        )

        assert result.exit_code == 0, result.output
        exp_dir = root / "exp_001"
        assert (exp_dir / "config" / "experiment.yaml").exists()
        assert (exp_dir / "generation" / "logs").is_dir()
        assert (exp_dir / "data" / "instance_0001.cnf").exists()
        assert (exp_dir / "engine" / "results.jsonl").exists()
        assert (exp_dir / "engine" / "logs").is_dir()
        assert (exp_dir / "study" / "analysis.txt").exists()

        manifest = json.loads((exp_dir / "config" / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["name"] == "exp_001"
        assert manifest["phases"]["generation"]["instances"] == 1
        assert manifest["phases"]["engine"]["exit_code"] == 0
        assert manifest["phases"]["study"]["reports"] == ["study/analysis.txt"]

    def test_experiment_run_fails_when_name_exists(self, tmp_path: Path):
        config_path = _write_experiment_config(tmp_path)
        root = tmp_path / "experiments"
        (root / "exp_001").mkdir(parents=True)

        result = runner.invoke(
            app,
            [
                "experiment",
                "run",
                "--name",
                "exp_001",
                "--config",
                str(config_path),
                "--root",
                str(root),
            ],
        )

        assert result.exit_code == 2
        assert "Experiment already exists" in result.output

    def test_experiment_run_overwrite_recreates_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        config_path = _write_experiment_config(tmp_path)
        root = tmp_path / "experiments"
        stale_file = root / "exp_001" / "data" / "stale.cnf"
        stale_file.parent.mkdir(parents=True)
        stale_file.write_text("stale", encoding="utf-8")

        monkeypatch.setattr("alglab._cli.experiment._run_generation", lambda *_: [])
        monkeypatch.setattr("alglab._cli.experiment._run_engine", lambda *_: 0)
        monkeypatch.setattr("alglab._cli.experiment._run_study", lambda *_: [])

        result = runner.invoke(
            app,
            [
                "experiment",
                "run",
                "--name",
                "exp_001",
                "--config",
                str(config_path),
                "--root",
                str(root),
                "--overwrite",
            ],
        )

        assert result.exit_code == 0, result.output
        assert not stale_file.exists()
        assert (root / "exp_001" / "config" / "experiment.yaml").exists()

    @pytest.mark.parametrize("name", ["", "../bad", "bad/name"])
    def test_experiment_run_rejects_bad_name(self, tmp_path: Path, name: str):
        config_path = _write_experiment_config(tmp_path)

        result = runner.invoke(
            app,
            [
                "experiment",
                "run",
                "--name",
                name,
                "--config",
                str(config_path),
                "--root",
                str(tmp_path / "experiments"),
            ],
        )

        assert result.exit_code == 2

    def test_experiment_run_rejects_missing_sections(self, tmp_path: Path):
        config_path = tmp_path / "bad.yaml"
        config_path.write_text("generation: {}\nengine: {}\n", encoding="utf-8")

        result = runner.invoke(
            app,
            [
                "experiment",
                "run",
                "--name",
                "exp_001",
                "--config",
                str(config_path),
                "--root",
                str(tmp_path / "experiments"),
            ],
        )

        assert result.exit_code == 2
        assert "study" in result.output

    def test_experiment_run_returns_one_when_engine_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        config_path = _write_experiment_config(tmp_path)
        root = tmp_path / "experiments"

        monkeypatch.setattr("alglab._cli.experiment._run_generation", lambda *_: [])

        def fake_engine(engine: dict[str, Any], paths: Any) -> int:
            (paths.engine / "results.jsonl").write_text(
                '{"status": "algorithm_error"}\n', encoding="utf-8"
            )
            return 1

        monkeypatch.setattr("alglab._cli.experiment._run_engine", fake_engine)

        result = runner.invoke(
            app,
            [
                "experiment",
                "run",
                "--name",
                "exp_001",
                "--config",
                str(config_path),
                "--root",
                str(root),
            ],
        )

        assert result.exit_code == 1
        manifest = json.loads(
            (root / "exp_001" / "config" / "manifest.json").read_text(encoding="utf-8")
        )
        assert manifest["phases"]["engine"]["status"] == "failed"
        assert manifest["phases"]["study"]["status"] == "skipped"

    @pytest.mark.parametrize(
        ("sort_files", "expected"),
        [
            (None, False),
            (True, True),
        ],
    )
    def test_experiment_engine_forwards_sort_files(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        sort_files: bool | None,
        expected: bool,
    ):
        from alglab._cli.experiment import _ExperimentPaths, _run_engine

        captured: dict[str, Any] = {}

        def fake_run_experiment(**kwargs: Any) -> int:
            captured.update(kwargs)
            return 0

        monkeypatch.setattr("alglab._cli.experiment.run_experiment", fake_run_experiment)
        engine: dict[str, Any] = {"algorithms": ["maxsat_brute"]}
        if sort_files is not None:
            engine["sort_files"] = sort_files
        paths = _ExperimentPaths(
            root=tmp_path,
            config=tmp_path / "config",
            generation=tmp_path / "generation",
            data=tmp_path / "data",
            engine=tmp_path / "engine",
            study=tmp_path / "study",
        )

        assert _run_engine(engine, paths) == 0
        assert captured["sort_files"] is expected


def _write_experiment_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "experiment.yaml"
    config_path.write_text(
        """
generation:
  n_files: 1
  vars: "1"
  densities: "1.0"
  k: 1
engine:
  algorithms:
    - maxsat_brute
study:
  measures:
    - path: run_duration_s
      higher_is_better: false
""".strip(),
        encoding="utf-8",
    )
    return config_path

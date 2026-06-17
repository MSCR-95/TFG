"""Tests for the experiment orchestrator: _cli/experiment.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from alglab._cli.experiment import (
    _ExperimentPaths,
    _load_config,
    _prepare_directories,
    _validate_experiment_name,
    run_experiment_workflow,
)

# ---------------------------------------------------------------------------
# _validate_experiment_name
# ---------------------------------------------------------------------------


class TestValidateExperimentName:
    def test_simple_name_passes(self):
        _validate_experiment_name("my_experiment")

    def test_name_with_dots_and_dashes(self):
        _validate_experiment_name("exp-2024.01")

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="--name"):
            _validate_experiment_name("")

    def test_name_starting_with_dash_raises(self):
        with pytest.raises(ValueError):
            _validate_experiment_name("-bad")

    def test_name_with_spaces_raises(self):
        with pytest.raises(ValueError):
            _validate_experiment_name("bad name")


# ---------------------------------------------------------------------------
# _load_config
# ---------------------------------------------------------------------------

_VALID_YAML = {
    "generation": {"n_files": 5, "vars": "10", "densities": "2.5", "k": 2},
    "engine": {"algorithms": ["maxsat_brute"]},
    "study": {"measures": [{"path": "result.satisfaction_ratio"}]},
}


def _write_yaml(path: Path, data: object) -> Path:
    path.write_text(yaml.dump(data), encoding="utf-8")
    return path


class TestLoadConfig:
    def test_valid_config_returns_dict(self, tmp_path):
        cfg = _write_yaml(tmp_path / "exp.yaml", _VALID_YAML)
        result = _load_config(cfg)
        assert "generation" in result
        assert "engine" in result
        assert "study" in result

    def test_missing_generation_section_raises(self, tmp_path):
        data = {k: v for k, v in _VALID_YAML.items() if k != "generation"}
        cfg = _write_yaml(tmp_path / "exp.yaml", data)
        with pytest.raises(ValueError, match="generation"):
            _load_config(cfg)

    def test_missing_engine_section_raises(self, tmp_path):
        data = {k: v for k, v in _VALID_YAML.items() if k != "engine"}
        cfg = _write_yaml(tmp_path / "exp.yaml", data)
        with pytest.raises(ValueError, match="engine"):
            _load_config(cfg)

    def test_missing_study_section_raises(self, tmp_path):
        data = {k: v for k, v in _VALID_YAML.items() if k != "study"}
        cfg = _write_yaml(tmp_path / "exp.yaml", data)
        with pytest.raises(ValueError, match="study"):
            _load_config(cfg)

    def test_empty_algorithms_raises(self, tmp_path):
        data = {**_VALID_YAML, "engine": {"algorithms": []}}
        cfg = _write_yaml(tmp_path / "exp.yaml", data)
        with pytest.raises(ValueError, match="algorithms"):
            _load_config(cfg)

    def test_algorithms_not_list_raises(self, tmp_path):
        data = {**_VALID_YAML, "engine": {"algorithms": "maxsat_brute"}}
        cfg = _write_yaml(tmp_path / "exp.yaml", data)
        with pytest.raises(ValueError, match="algorithms"):
            _load_config(cfg)

    def test_measures_not_list_raises(self, tmp_path):
        data = {**_VALID_YAML, "study": {"measures": "not_a_list"}}
        cfg = _write_yaml(tmp_path / "exp.yaml", data)
        with pytest.raises(ValueError, match="measures"):
            _load_config(cfg)

    def test_measure_without_path_raises(self, tmp_path):
        data = {**_VALID_YAML, "study": {"measures": [{"label": "no_path"}]}}
        cfg = _write_yaml(tmp_path / "exp.yaml", data)
        with pytest.raises(ValueError, match="path"):
            _load_config(cfg)

    def test_non_dict_yaml_raises(self, tmp_path):
        cfg = tmp_path / "exp.yaml"
        cfg.write_text("- not\n- a\n- dict\n", encoding="utf-8")
        with pytest.raises(ValueError, match="dictionary"):
            _load_config(cfg)


# ---------------------------------------------------------------------------
# _prepare_directories
# ---------------------------------------------------------------------------


class TestPrepareDirectories:
    def _make_paths(self, root: Path) -> _ExperimentPaths:
        return _ExperimentPaths(
            root=root,
            config=root / "config",
            generation=root / "generation",
            data=root / "data",
            engine=root / "engine",
            study=root / "study",
        )

    def test_creates_all_directories(self, tmp_path):
        root = tmp_path / "exp"
        paths = self._make_paths(root)
        _prepare_directories(paths, overwrite=False)
        assert (root / "config").is_dir()
        assert (root / "data").is_dir()
        assert (root / "engine" / "logs").is_dir()
        assert (root / "study").is_dir()

    def test_existing_dir_without_overwrite_raises(self, tmp_path):
        root = tmp_path / "exp"
        root.mkdir()
        paths = self._make_paths(root)
        with pytest.raises(ValueError, match="already exists"):
            _prepare_directories(paths, overwrite=False)

    def test_existing_dir_with_overwrite_recreates(self, tmp_path):
        root = tmp_path / "exp"
        root.mkdir()
        sentinel = root / "old_file.txt"
        sentinel.write_text("old")
        paths = self._make_paths(root)
        _prepare_directories(paths, overwrite=True)
        assert not sentinel.exists()
        assert (root / "config").is_dir()


# ---------------------------------------------------------------------------
# run_experiment_workflow — integration (mocked phases)
# ---------------------------------------------------------------------------


class TestRunExperimentWorkflow:
    def _write_config(self, path: Path) -> Path:
        cfg = path / "exp.yaml"
        _write_yaml(cfg, _VALID_YAML)
        return cfg

    def test_invalid_name_raises(self, tmp_path):
        cfg = self._write_config(tmp_path)
        with pytest.raises(ValueError, match="--name"):
            run_experiment_workflow(name="-bad", config_path=cfg, root=tmp_path / "experiments")

    def test_missing_config_raises(self, tmp_path):
        with pytest.raises(ValueError, match="does not exist"):
            run_experiment_workflow(
                name="exp",
                config_path=tmp_path / "nonexistent.yaml",
                root=tmp_path / "experiments",
            )

    def test_full_workflow_returns_zero_on_success(self, tmp_path):
        cfg = self._write_config(tmp_path)
        root = tmp_path / "experiments"

        with (
            patch(
                "alglab._cli.experiment._run_generation",
                return_value=[tmp_path / "fake.cnf"],
            ),
            patch("alglab._cli.experiment._run_engine", return_value=0),
            patch("alglab._cli.experiment._count_jsonl_rows", return_value=2),
            patch(
                "alglab._cli.experiment._run_study",
                side_effect=lambda cfg, paths: [paths.study / "analysis.txt"],
            ),
            patch("alglab._cli.experiment.setup"),
        ):
            code = run_experiment_workflow(name="my_exp", config_path=cfg, root=root)

        assert code == 0
        assert (root / "my_exp" / "config" / "experiment.yaml").is_file()
        assert (root / "my_exp" / "config" / "manifest.json").is_file()

    def test_engine_failure_skips_study(self, tmp_path):
        cfg = self._write_config(tmp_path)
        root = tmp_path / "experiments"

        study_mock = MagicMock()
        with (
            patch(
                "alglab._cli.experiment._run_generation",
                return_value=[tmp_path / "fake.cnf"],
            ),
            patch("alglab._cli.experiment._run_engine", return_value=1),
            patch("alglab._cli.experiment._count_jsonl_rows", return_value=0),
            patch("alglab._cli.experiment._run_study", study_mock),
            patch("alglab._cli.experiment.setup"),
        ):
            code = run_experiment_workflow(name="my_exp", config_path=cfg, root=root)

        assert code == 1
        study_mock.assert_not_called()

    def test_overwrite_flag_replaces_existing(self, tmp_path):
        cfg = self._write_config(tmp_path)
        root = tmp_path / "experiments"

        def _run_once(overwrite: bool = False):
            with (
                patch(
                    "alglab._cli.experiment._run_generation",
                    return_value=[tmp_path / "fake.cnf"],
                ),
                patch("alglab._cli.experiment._run_engine", return_value=0),
                patch("alglab._cli.experiment._count_jsonl_rows", return_value=1),
                patch(
                    "alglab._cli.experiment._run_study",
                    side_effect=lambda cfg, paths: [paths.study / "analysis.txt"],
                ),
                patch("alglab._cli.experiment.setup"),
            ):
                return run_experiment_workflow(
                    name="my_exp", config_path=cfg, root=root, overwrite=overwrite
                )

        _run_once()
        # Second run without overwrite must fail
        with pytest.raises(ValueError, match="already exists"):
            run_experiment_workflow(name="my_exp", config_path=cfg, root=root)

        # With overwrite=True it succeeds
        code = _run_once(overwrite=True)
        assert code == 0

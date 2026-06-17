"""Tests for alglab.study.analysis — run_study() integration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from alglab.study.analysis import run_study
from alglab.study.schema import MeasureSpec, StudyConfig


@pytest.fixture
def two_algo_config(tmp_path: Path, jsonl_path: Path) -> StudyConfig:
    """StudyConfig with 2 algorithms from conftest JSONL."""
    return StudyConfig(
        input_jsonl=jsonl_path,
        measures=[
            MeasureSpec("result.satisfaction_ratio", label="sat_ratio", higher_is_better=True)
        ],
        alpha=0.05,
    )


@pytest.fixture
def three_algo_jsonl(tmp_path: Path) -> Path:
    """JSONL with 3 algorithms × 8 instances (no seeds)."""
    records = []
    values = {
        "best": [0.95, 0.93, 0.97, 0.92, 0.96, 0.94, 0.98, 0.91],
        "mid": [0.80, 0.82, 0.79, 0.85, 0.81, 0.83, 0.78, 0.84],
        "worst": [0.60, 0.62, 0.58, 0.65, 0.61, 0.63, 0.57, 0.64],
    }
    for algo, vals in values.items():
        for i, v in enumerate(vals):
            records.append(
                {
                    "file": f"f{i}.cnf",
                    "algorithm": algo,
                    "status": "ok",
                    "result": {"score": v},
                    "seed": None,
                    "run_duration_s": 0.01,
                }
            )
    path = tmp_path / "three_algo.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    return path


@pytest.fixture
def three_algo_config(three_algo_jsonl: Path) -> StudyConfig:
    return StudyConfig(
        input_jsonl=three_algo_jsonl,
        measures=[MeasureSpec("result.score", label="score", higher_is_better=True)],
        alpha=0.05,
    )


class TestRunStudyTwoAlgo:
    def test_returns_sorted_list_of_paths(
        self, two_algo_config: StudyConfig, tmp_path: Path
    ) -> None:
        out = tmp_path / "study"
        paths = run_study(two_algo_config, out)
        assert isinstance(paths, list)
        assert len(paths) > 0
        assert paths == sorted(paths)

    def test_analysis_txt_created(self, two_algo_config: StudyConfig, tmp_path: Path) -> None:
        out = tmp_path / "study"
        paths = run_study(two_algo_config, out)
        analysis = out / "analysis.txt"
        assert analysis in paths
        assert analysis.exists()

    def test_matrix_csv_created(self, two_algo_config: StudyConfig, tmp_path: Path) -> None:
        out = tmp_path / "study"
        paths = run_study(two_algo_config, out)
        matrix_csv = out / "matrices" / "sat_ratio.csv"
        assert matrix_csv in paths
        assert matrix_csv.exists()

    def test_analysis_txt_has_summary_and_wilcoxon(
        self, two_algo_config: StudyConfig, tmp_path: Path
    ) -> None:
        out = tmp_path / "study"
        run_study(two_algo_config, out)
        text = (out / "analysis.txt").read_text(encoding="utf-8")
        assert "Summary" in text
        assert "Wilcoxon" in text
        assert "diagnostics" in text.lower()

    def test_analysis_txt_no_iman_hommel_bergmann(
        self, two_algo_config: StudyConfig, tmp_path: Path
    ) -> None:
        out = tmp_path / "study"
        run_study(two_algo_config, out)
        text = (out / "analysis.txt").read_text(encoding="utf-8")
        assert "Iman" not in text
        assert "Hommel" not in text
        assert "Bergmann" not in text

    def test_plots_boxplot_density_qq(self, two_algo_config: StudyConfig, tmp_path: Path) -> None:
        out = tmp_path / "study"
        paths = run_study(two_algo_config, out)
        plot_names = {p.name for p in paths if p.suffix == ".png"}
        assert any("boxplot" in n for n in plot_names)
        assert any("density" in n for n in plot_names)
        assert any("qq" in n for n in plot_names)

    def test_no_ranking_pvalue_plots(self, two_algo_config: StudyConfig, tmp_path: Path) -> None:
        out = tmp_path / "study"
        paths = run_study(two_algo_config, out)
        plot_names = {p.name for p in paths if p.suffix == ".png"}
        assert not any("ranking" in n for n in plot_names)
        assert not any("pvalue" in n for n in plot_names)


class TestRunStudyThreeAlgo:
    def test_analysis_txt_has_iman_hommel_bergmann(
        self, three_algo_config: StudyConfig, tmp_path: Path
    ) -> None:
        out = tmp_path / "study"
        run_study(three_algo_config, out)
        text = (out / "analysis.txt").read_text(encoding="utf-8")
        assert "Iman" in text
        assert "Hommel" in text
        assert "Bergmann" in text

    def test_ranking_and_pvalue_plots_exist(
        self, three_algo_config: StudyConfig, tmp_path: Path
    ) -> None:
        out = tmp_path / "study"
        paths = run_study(three_algo_config, out)
        plot_names = {p.name for p in paths if p.suffix == ".png"}
        assert any("ranking" in n for n in plot_names)
        assert any("pvalue" in n for n in plot_names)

    def test_all_plot_formats_generated(
        self, three_algo_config: StudyConfig, tmp_path: Path
    ) -> None:
        out = tmp_path / "study"
        paths = run_study(three_algo_config, out)
        suffixes = {p.suffix for p in paths if p.parent.name == "plots"}
        assert ".pdf" in suffixes
        assert ".svg" in suffixes
        assert ".png" in suffixes

    def test_matrix_csv_shape(self, three_algo_config: StudyConfig, tmp_path: Path) -> None:
        import pandas as pd

        out = tmp_path / "study"
        run_study(three_algo_config, out)
        df = pd.read_csv(out / "matrices" / "score.csv", index_col=0)
        assert df.shape == (8, 3)
        assert set(df.columns) == {"best", "mid", "worst"}


class TestRunStudyOutputDir:
    def test_creates_output_dir(self, two_algo_config: StudyConfig, tmp_path: Path) -> None:
        out = tmp_path / "nested" / "study"
        assert not out.exists()
        run_study(two_algo_config, out)
        assert out.exists()

    def test_multiple_measures(self, jsonl_path: Path, tmp_path: Path) -> None:
        config = StudyConfig(
            input_jsonl=jsonl_path,
            measures=[
                MeasureSpec("result.satisfaction_ratio", label="sat", higher_is_better=True),
                MeasureSpec("run_duration_s", label="runtime", higher_is_better=False),
            ],
            alpha=0.05,
        )
        out = tmp_path / "study"
        run_study(config, out)
        assert (out / "matrices" / "sat.csv").exists()
        assert (out / "matrices" / "runtime.csv").exists()

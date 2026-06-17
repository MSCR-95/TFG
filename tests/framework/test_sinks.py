"""
Tests for JSONL result sink.
"""

from __future__ import annotations

import json

import pytest

from alglab.engine.core import Result
from alglab.engine.sinks import JSONLResultSink


def _result(**overrides) -> Result:
    defaults = dict(
        file="/data/test.txt",
        algorithm="maxsat_brute",
        status="ok",
        result={"lines": 5, "words": 20},
        error_message=None,
        seed=None,
    )
    defaults.update(overrides)
    return Result(**defaults)


class TestJSONLResultSink:
    def test_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "nested" / "deep" / "out.jsonl"
        JSONLResultSink(path).write_all([_result()])
        assert path.exists()

    def test_one_line_per_record(self, tmp_path):
        path = tmp_path / "out.jsonl"
        records = [_result(algorithm=f"algo_{i}") for i in range(5)]
        JSONLResultSink(path).write_all(records)
        lines = [line for line in path.read_text().splitlines() if line.strip()]
        assert len(lines) == 5

    def test_each_line_is_valid_json(self, tmp_path):
        path = tmp_path / "out.jsonl"
        JSONLResultSink(path).write_all([_result(), _result(algorithm="maxsat_brute")])
        for line in path.read_text().splitlines():
            if line.strip():
                json.loads(line)

    def test_fields_round_trip(self, tmp_path):
        path = tmp_path / "out.jsonl"
        r = _result()
        JSONLResultSink(path).write_all([r])
        loaded = json.loads(path.read_text().strip())
        assert loaded["algorithm"] == r.algorithm
        assert loaded["status"] == r.status
        assert loaded["result"] == r.result
        assert "pre_run_duration_s" in loaded
        assert "run_duration_s" in loaded
        assert "post_run_duration_s" in loaded

    def test_overwrites_existing_file(self, tmp_path):
        path = tmp_path / "out.jsonl"
        path.write_text("old content\n")
        JSONLResultSink(path).write_all([_result()])
        lines = [line for line in path.read_text().splitlines() if line.strip()]
        assert len(lines) == 1
        assert "old content" not in path.read_text()

    def test_result_field_preserved_as_dict(self, tmp_path):
        path = tmp_path / "out.jsonl"
        r = _result(result={"nested": {"key": [1, 2, 3]}})
        JSONLResultSink(path).write_all([r])
        loaded = json.loads(path.read_text().strip())
        assert loaded["result"] == {"nested": {"key": [1, 2, 3]}}

    def test_non_ascii_characters(self, tmp_path):
        path = tmp_path / "out.jsonl"
        r = _result(file="/datos/ñoño.txt", result={"texto": "áéíóú"})
        JSONLResultSink(path).write_all([r])
        loaded = json.loads(path.read_text(encoding="utf-8").strip())
        assert loaded["file"] == "/datos/ñoño.txt"
        assert loaded["result"]["texto"] == "áéíóú"

    def test_append_mode(self, tmp_path):
        path = tmp_path / "out.jsonl"
        JSONLResultSink(path).write_all([_result()])
        JSONLResultSink(path, append=True).write_all([_result(algorithm="maxsat_brute")])
        lines = [line for line in path.read_text().splitlines() if line.strip()]
        assert len(lines) == 2

    def test_context_manager_write(self, tmp_path):
        path = tmp_path / "out.jsonl"
        with JSONLResultSink(path) as sink:
            sink.write(_result())
            sink.write(_result(algorithm="maxsat_brute"))
        lines = [line for line in path.read_text().splitlines() if line.strip()]
        assert len(lines) == 2

    def test_write_without_context_raises(self, tmp_path):
        path = tmp_path / "out.jsonl"
        sink = JSONLResultSink(path)
        with pytest.raises(RuntimeError):
            sink.write(_result())

    def test_reenter_open_sink_raises(self, tmp_path):
        path = tmp_path / "out.jsonl"
        sink = JSONLResultSink(path)
        with sink, pytest.raises(RuntimeError, match="already open"):
            sink.write_all([_result()])

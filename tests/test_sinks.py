"""
Tests de los sinks de resultados: JSONLResultSink y CSVResultSink.
"""
from __future__ import annotations

import csv
import dataclasses
import json
from pathlib import Path

import pytest

from framework.sinks import CSVResultSink, JSONLResultSink
from framework.core import ResultRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _record(**overrides) -> ResultRecord:
    defaults = dict(
        job_id="job1",
        file="/data/test.txt",
        algorithm="word_count",
        priority=100,
        attempt=1,
        max_attempts=1,
        duration_s=0.123,
        started_at="2024-01-01T00:00:00+00:00",
        ended_at="2024-01-01T00:00:00.123+00:00",
        timed_out=False,
        cancelled=False,
        error=False,
        will_retry=False,
        result={"lines": 5, "words": 20},
    )
    defaults.update(overrides)
    return ResultRecord(**defaults)


FIELDNAMES = [
    "job_id", "file", "algorithm", "priority", "attempt", "max_attempts",
    "duration_s", "started_at", "ended_at", "timed_out", "cancelled",
    "error", "will_retry", "result",
]


# ===========================================================================
# JSONLResultSink
# ===========================================================================

class TestJSONLResultSink:
    def test_creates_file(self, tmp_path):
        path = tmp_path / "out.jsonl"
        JSONLResultSink(path).write_all([_record()])
        assert path.exists()

    def test_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "nested" / "deep" / "out.jsonl"
        JSONLResultSink(path).write_all([_record()])
        assert path.exists()

    def test_one_line_per_record(self, tmp_path):
        path = tmp_path / "out.jsonl"
        records = [_record(job_id=f"j{i}") for i in range(5)]
        JSONLResultSink(path).write_all(records)
        lines = [l for l in path.read_text().splitlines() if l.strip()]
        assert len(lines) == 5

    def test_each_line_is_valid_json(self, tmp_path):
        path = tmp_path / "out.jsonl"
        JSONLResultSink(path).write_all([_record(), _record(job_id="j2")])
        for line in path.read_text().splitlines():
            if line.strip():
                json.loads(line)  # no debe lanzar

    def test_fields_round_trip(self, tmp_path):
        path = tmp_path / "out.jsonl"
        r = _record()
        JSONLResultSink(path).write_all([r])
        loaded = json.loads(path.read_text().strip())
        assert loaded["job_id"] == r.job_id
        assert loaded["algorithm"] == r.algorithm
        assert loaded["result"] == r.result
        assert loaded["duration_s"] == pytest.approx(r.duration_s)

    def test_empty_records_creates_empty_file(self, tmp_path):
        path = tmp_path / "empty.jsonl"
        JSONLResultSink(path).write_all([])
        assert path.read_text() == ""

    def test_overwrites_existing_file(self, tmp_path):
        path = tmp_path / "out.jsonl"
        path.write_text("old content\n")
        JSONLResultSink(path).write_all([_record()])
        lines = [l for l in path.read_text().splitlines() if l.strip()]
        assert len(lines) == 1
        assert "old content" not in path.read_text()

    def test_result_field_preserved_as_dict(self, tmp_path):
        path = tmp_path / "out.jsonl"
        r = _record(result={"nested": {"key": [1, 2, 3]}})
        JSONLResultSink(path).write_all([r])
        loaded = json.loads(path.read_text().strip())
        assert loaded["result"] == {"nested": {"key": [1, 2, 3]}}

    def test_non_ascii_characters(self, tmp_path):
        path = tmp_path / "out.jsonl"
        r = _record(file="/datos/ñoño.txt", result={"texto": "áéíóú"})
        JSONLResultSink(path).write_all([r])
        loaded = json.loads(path.read_text(encoding="utf-8").strip())
        assert loaded["file"] == "/datos/ñoño.txt"
        assert loaded["result"]["texto"] == "áéíóú"


# ===========================================================================
# CSVResultSink
# ===========================================================================

class TestCSVResultSink:
    def test_creates_file(self, tmp_path):
        path = tmp_path / "out.csv"
        CSVResultSink(path).write_all([_record()])
        assert path.exists()

    def test_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "nested" / "out.csv"
        CSVResultSink(path).write_all([_record()])
        assert path.exists()

    def test_has_header_row(self, tmp_path):
        path = tmp_path / "out.csv"
        CSVResultSink(path).write_all([_record()])
        with path.open(encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            assert set(reader.fieldnames) == set(FIELDNAMES)

    def test_one_data_row_per_record(self, tmp_path):
        path = tmp_path / "out.csv"
        records = [_record(job_id=f"j{i}") for i in range(3)]
        CSVResultSink(path).write_all(records)
        with path.open(encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 3

    def test_result_field_is_json_encoded(self, tmp_path):
        path = tmp_path / "out.csv"
        r = _record(result={"key": "val", "num": 42})
        CSVResultSink(path).write_all([r])
        with path.open(encoding="utf-8", newline="") as f:
            row = next(csv.DictReader(f))
        # result es string JSON, no dict
        parsed = json.loads(row["result"])
        assert parsed == {"key": "val", "num": 42}

    def test_boolean_fields_preserved(self, tmp_path):
        path = tmp_path / "out.csv"
        r = _record(timed_out=True, cancelled=False, error=True, will_retry=False)
        CSVResultSink(path).write_all([r])
        with path.open(encoding="utf-8", newline="") as f:
            row = next(csv.DictReader(f))
        # CSV serializa bools como strings "True"/"False"
        assert row["timed_out"] == "True"
        assert row["cancelled"] == "False"
        assert row["error"] == "True"

    def test_empty_records_writes_only_header(self, tmp_path):
        path = tmp_path / "out.csv"
        CSVResultSink(path).write_all([])
        with path.open(encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 0

    def test_fields_round_trip(self, tmp_path):
        path = tmp_path / "out.csv"
        r = _record(job_id="rtrip", algorithm="sha256", priority=50)
        CSVResultSink(path).write_all([r])
        with path.open(encoding="utf-8", newline="") as f:
            row = next(csv.DictReader(f))
        assert row["job_id"] == "rtrip"
        assert row["algorithm"] == "sha256"
        assert row["priority"] == "50"


def test_jsonl_write_all_returns_count(tmp_path):
    path = tmp_path / "count.jsonl"
    count = JSONLResultSink(path).write_all([_record(job_id="a"), _record(job_id="b")])
    assert count == 2


def test_csv_write_all_returns_count(tmp_path):
    path = tmp_path / "count.csv"
    count = CSVResultSink(path).write_all([_record(job_id="a"), _record(job_id="b"), _record(job_id="c")])
    assert count == 3

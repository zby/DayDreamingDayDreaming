"""Unit tests for CSV parsing helper with enriched error context."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from dagster import Failure

from daydreaming_dagster.utils.csv_reading import read_csv_with_context


pytestmark = [pytest.mark.unit]


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_read_csv_with_context_raises_failure_with_marked_line(tmp_path: Path) -> None:
    csv_path = tmp_path / "bad.csv"
    _write(
        csv_path,
        "header_a,header_b\n"
        "1,2\n"
        '"unterminated\n'
        "3,4\n",
    )

    with pytest.raises(Failure) as exc:
        read_csv_with_context(csv_path)

    message = str(exc.value)
    # Error message includes basic context and the marker on the failing line.
    assert str(csv_path) in message
    assert "unterminated" in message
    assert ">>>" in message
    assert "Line 2" in message


def test_read_csv_with_context_returns_dataframe_on_success(tmp_path: Path) -> None:
    csv_path = tmp_path / "good.csv"
    _write(csv_path, "header_a,header_b\n1,2\n3,4\n")

    df = read_csv_with_context(csv_path)

    assert list(df.columns) == ["header_a", "header_b"]
    assert df.to_dict(orient="list") == {"header_a": [1, 3], "header_b": [2, 4]}


def test_read_csv_with_context_handles_missing_error_line(tmp_path: Path, monkeypatch) -> None:
    csv_path = tmp_path / "incomplete.csv"
    _write(csv_path, "a,b\n1,2\n")

    def _raise_without_line(_path):
        raise pd.errors.ParserError("Parser error without location info")

    monkeypatch.setattr("daydreaming_dagster.utils.csv_reading.pd.read_csv", _raise_without_line)

    with pytest.raises(Failure) as exc:
        read_csv_with_context(csv_path)

    message = str(exc.value)
    assert "CSV parsing failed" in message
    assert ">>>" not in message

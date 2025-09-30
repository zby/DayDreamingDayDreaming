"""Unit tests for CSV parsing helper with enriched error context."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from daydreaming_dagster.utils.csv_reading import read_csv_with_context
from daydreaming_dagster.utils.errors import DDError, Err


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

    with pytest.raises(DDError) as exc:
        read_csv_with_context(csv_path)
    err = exc.value
    assert err.code is Err.PARSER_FAILURE
    ctx = err.ctx or {}
    assert ctx.get("path") == str(csv_path)
    assert ctx.get("reason") == "csv_parse_error"
    assert ctx.get("pandas_error")
    line_number = ctx.get("line_number")
    assert isinstance(line_number, int) and line_number >= 2
    snippet = ctx.get("snippet") or []
    assert any(item.get("is_error") for item in snippet)


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

    with pytest.raises(DDError) as exc:
        read_csv_with_context(csv_path)
    err = exc.value
    assert err.code is Err.PARSER_FAILURE
    ctx = err.ctx or {}
    assert ctx.get("reason") == "csv_parse_error"
    assert ctx.get("snippet") is None

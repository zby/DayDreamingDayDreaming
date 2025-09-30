import os
from pathlib import Path

import pytest

from daydreaming_dagster.utils.csv_reading import read_csv_with_context
from daydreaming_dagster.utils.errors import DDError, Err

pytestmark = [pytest.mark.unit]


def test_read_csv_with_context_reports_line_and_marker(tmp_path: Path):
    csv = tmp_path / "bad.csv"
    csv.write_text(
        """col1,col2
ok,1
bad, 1, 2
ok,3
"""
    )

    with pytest.raises(DDError) as exc:
        read_csv_with_context(csv)
    err = exc.value
    assert err.code is Err.PARSER_FAILURE
    ctx = err.ctx or {}
    assert ctx.get("path") == str(csv)
    assert ctx.get("reason") == "csv_parse_error"
    assert ctx.get("line_number") is not None
    snippet = ctx.get("snippet") or []
    assert any(item.get("is_error") for item in snippet)


def test_read_csv_with_context_success(tmp_path: Path):
    csv = tmp_path / "good.csv"
    csv.write_text("col1,col2\na,1\nb,2\n")

    df = read_csv_with_context(csv)
    assert list(df.columns) == ["col1", "col2"]
    assert len(df) == 2

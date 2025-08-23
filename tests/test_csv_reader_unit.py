import os
from pathlib import Path

import pytest
from dagster import Failure

from daydreaming_dagster.utils.csv_reading import read_csv_with_context

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

    with pytest.raises(Failure) as exc:
        read_csv_with_context(csv)

    msg = str(exc.value)
    # pandas error text varies, so assert generic parts
    assert "CSV parsing failed for" in msg
    assert str(csv) in msg
    assert ">>>" in msg  # marker
    assert "Line" in msg


def test_read_csv_with_context_success(tmp_path: Path):
    csv = tmp_path / "good.csv"
    csv.write_text("col1,col2\na,1\nb,2\n")

    df = read_csv_with_context(csv)
    assert list(df.columns) == ["col1", "col2"]
    assert len(df) == 2


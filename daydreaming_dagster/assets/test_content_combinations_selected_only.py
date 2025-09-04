import tempfile
from pathlib import Path
import pytest

from dagster import Failure, build_op_context

from daydreaming_dagster.assets.core import content_combinations


def test_content_combinations_requires_selected_file():
    with tempfile.TemporaryDirectory() as td:
        ctx = build_op_context(resources={"data_root": str(Path(td))})
        with pytest.raises(Failure):
            content_combinations(ctx)  # missing selected_combo_mappings.csv

import tempfile
from pathlib import Path

import pytest

from daydreaming_dagster.resources.io_managers import VersionedTextIOManager


class _OutCtx:
    def __init__(self, pk: str):
        self.partition_key = pk


class _InCtx:
    def __init__(self, pk: str):
        self.partition_key = pk


def test_reads_legacy_then_prefers_versioned_latest():
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        m = VersionedTextIOManager(base)
        pk = "doc_abc"

        # Legacy file present, no versions yet
        legacy = base / f"{pk}.txt"
        legacy.parent.mkdir(parents=True, exist_ok=True)
        legacy.write_text("legacy content")

        # Read falls back to legacy
        assert m.load_input(_InCtx(pk)) == "legacy content"

        # First write creates _v1 (legacy remains)
        m.handle_output(_OutCtx(pk), "v1 content")
        assert (base / f"{pk}_v1.txt").exists()
        # Now read prefers versioned file
        assert m.load_input(_InCtx(pk)) == "v1 content"

        # Second write creates _v2 and becomes latest
        m.handle_output(_OutCtx(pk), "v2 content")
        assert (base / f"{pk}_v2.txt").exists()
        assert m.load_input(_InCtx(pk)) == "v2 content"


def test_missing_raises_clear_error():
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        m = VersionedTextIOManager(base)
        with pytest.raises(FileNotFoundError):
            m.load_input(_InCtx("nope"))


def test_version_parsing_ignores_non_matching_files():
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        m = VersionedTextIOManager(base)
        pk = "task__x"
        base.mkdir(parents=True, exist_ok=True)
        # Non-matching files should be ignored
        (base / f"{pk}_vA.txt").write_text("bad version token")
        (base / f"{pk}_v1.txt").write_text("v1")
        (base / f"{pk}_v10.txt").write_text("v10")
        (base / f"{pk}_v2.txt").write_text("v2")

        # Read should pick highest numeric version
        assert m.load_input(_InCtx(pk)) == "v10"


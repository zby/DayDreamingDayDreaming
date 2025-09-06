from pathlib import Path

from daydreaming_dagster.utils.versioned_files import (
    latest_versioned_path,
    next_versioned_path,
    save_versioned_text,
)


def test_latest_and_next_when_empty(tmp_path: Path):
    d = tmp_path / "out"
    stem = "doc123"

    # Directory doesn't exist yet
    assert latest_versioned_path(d, stem) is None
    assert next_versioned_path(d, stem).name == f"{stem}_v1.txt"


def test_save_and_latest_progression(tmp_path: Path):
    d = tmp_path / "out"
    stem = "item"

    # First write -> v1
    p1 = save_versioned_text(d, stem, "hello")
    assert p1 is not None
    assert Path(p1).exists()
    assert Path(p1).name == f"{stem}_v1.txt"
    assert Path(p1).read_text(encoding="utf-8") == "hello"

    # Next versioned path should be v2
    assert next_versioned_path(d, stem).name == f"{stem}_v2.txt"

    # Second write -> v2
    p2 = save_versioned_text(d, stem, "world")
    assert p2 is not None
    assert Path(p2).exists()
    assert Path(p2).name == f"{stem}_v2.txt"
    assert Path(p2).read_text(encoding="utf-8") == "world"

    # Latest should be v2
    latest = latest_versioned_path(d, stem)
    assert latest is not None and latest.name == f"{stem}_v2.txt"


def test_custom_extension_handling(tmp_path: Path):
    d = tmp_path / "data"
    stem = "rec"

    # Write with custom extension
    from daydreaming_dagster.utils.versioned_files import save_versioned_text as sv
    p1 = sv(d, stem, "a", ext=".md")
    p2 = sv(d, stem, "b", ext="md")  # without leading dot
    assert Path(p1).name == f"{stem}_v1.md"
    assert Path(p2).name == f"{stem}_v2.md"

    # latest for .md should be v2; .txt should be none
    assert latest_versioned_path(d, stem, ".md").name == f"{stem}_v2.md"
    assert latest_versioned_path(d, stem, ".txt") is None

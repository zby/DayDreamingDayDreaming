from __future__ import annotations

from pathlib import Path
import os

from .paths import Paths


def test_generation_dir_and_files_basic():
    p = Paths.from_str("data")
    d = p.generation_dir("draft", "gid123")
    assert d == Path("data") / "gens" / "draft" / "gid123"
    assert p.prompt_path("draft", "gid123") == d / "prompt.txt"
    assert p.raw_path("draft", "gid123") == d / "raw.txt"
    assert p.parsed_path("draft", "gid123") == d / "parsed.txt"
    assert p.metadata_path("draft", "gid123") == d / "metadata.json"


def test_templates_root_and_file_env_override(monkeypatch):
    p = Paths.from_str("data")
    # default root
    assert p.templates_root() == Path("data") / "1_raw" / "templates"
    # env override
    monkeypatch.setenv("GEN_TEMPLATES_ROOT", "/tmp/custom_templates")
    try:
        assert p.templates_root() == Path("/tmp/custom_templates")
        assert p.template_file("draft", "tpl") == Path("/tmp/custom_templates/draft/tpl.txt")
    finally:
        monkeypatch.delenv("GEN_TEMPLATES_ROOT", raising=False)


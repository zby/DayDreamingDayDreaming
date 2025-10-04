from __future__ import annotations

from pathlib import Path

import pytest

import scripts.migrations.spec_wizard as spec_wizard
from scripts.migrations.spec_wizard import copy_spec_from_template, WizardError


def _write_spec(dir_path: Path, idx: int) -> None:
    (dir_path / "spec" / "items").mkdir(parents=True, exist_ok=True)
    (dir_path / "spec" / "config.yaml").write_text(f"config-{idx}\n", encoding="utf-8")
    (dir_path / "spec" / "items" / "rows.yaml").write_text(f"rows-{idx}\n", encoding="utf-8")


def test_copy_spec_from_template(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    template_dir = data_root / "cohorts" / "template-cohort"
    target_dir = data_root / "cohorts" / "new-cohort"
    _write_spec(template_dir, idx=1)

    created = copy_spec_from_template(data_root, "new-cohort", "template-cohort")

    assert created == target_dir / "spec"
    assert (created / "config.yaml").read_text(encoding="utf-8") == "config-1\n"
    assert (created / "items" / "rows.yaml").read_text(encoding="utf-8") == "rows-1\n"
    # ensure original unchanged
    assert (template_dir / "spec" / "config.yaml").read_text(encoding="utf-8") == "config-1\n"


def test_copy_spec_destination_exists(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    template_dir = data_root / "cohorts" / "template"
    target_dir = data_root / "cohorts" / "existing"
    _write_spec(template_dir, idx=2)
    _write_spec(target_dir, idx=3)

    with pytest.raises(WizardError):
        copy_spec_from_template(data_root, "existing", "template")


def test_copy_spec_missing_template(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    with pytest.raises(WizardError):
        copy_spec_from_template(data_root, "new", "missing")



def test_wizard_selects_most_recent_by_default(monkeypatch, tmp_path: Path, capsys) -> None:
    data_root = tmp_path / "data"
    cohorts_dir = data_root / "cohorts"
    older = cohorts_dir / "older"
    newer = cohorts_dir / "newer"
    _write_spec(older, idx=1)
    _write_spec(newer, idx=2)
    # ensure newer spec has later mtime
    for path in (older / "spec", newer / "spec"):
        path.touch()
    import time

    time.sleep(0.01)
    (newer / "spec" / "config.yaml").write_text("config-new\n", encoding="utf-8")
    # rebuild mtimes
    now = time.time()
    (older / "spec").touch()
    (newer / "spec").touch()
    # make newer more recent
    import os

    os.utime(newer / "spec", (now + 10, now + 10))
    os.utime(older / "spec", (now, now))

    responses = iter([""])

    def fake_prompt(_: str) -> str:
        return next(responses)

    monkeypatch.setattr(spec_wizard, "_prompt", fake_prompt)
    args = [
        "--cohort-id",
        "brand-new",
        "--data-root",
        str(data_root),
    ]
    spec_wizard.main(args)
    captured = capsys.readouterr()
    assert "Available cohorts" in captured.out
    new_spec = cohorts_dir / "brand-new" / "spec"
    assert (new_spec / "config.yaml").read_text(encoding="utf-8") == "config-new\n"

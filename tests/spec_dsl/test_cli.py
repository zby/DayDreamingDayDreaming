from __future__ import annotations

import json
from pathlib import Path

import pytest

from daydreaming_dagster.spec_dsl.cli import main


def write_spec(tmp_path: Path) -> Path:
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(
        json.dumps(
            {
                "axes": {
                    "draft_template": {
                        "levels": ["draft-A"],
                        "catalog_lookup": {"catalog": "draft_templates"},
                    },
                    "essay_template": ["essay-A"],
                },
                "rules": [
                    {
                        "pair": {
                            "left": "draft_template",
                            "right": "essay_template",
                            "name": "draft_essay",
                            "allowed": [["draft-A", "essay-A"]],
                        }
                    }
                ],
                "output": {
                    "keep_pair_axis": True,
                    "field_order": ["draft_template", "essay_template", "draft_essay"],
                },
            }
        ),
        encoding="utf-8",
    )
    return spec_path


def test_cli_writes_csv(tmp_path: Path) -> None:
    spec_path = write_spec(tmp_path)
    out_path = tmp_path / "rows.csv"
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"draft_templates": ["draft-A"]}), encoding="utf-8")

    exit_code = main([
        str(spec_path),
        "--out",
        str(out_path),
        "--format",
        "csv",
        "--catalog",
        str(catalog_path),
    ])

    assert exit_code == 0
    content = out_path.read_text(encoding="utf-8").splitlines()
    header = content[0].split(",")
    assert header == ["draft_template", "essay_template", "draft_essay"]


def test_cli_writes_jsonl(tmp_path: Path) -> None:
    spec_path = write_spec(tmp_path)
    out_path = tmp_path / "rows.jsonl"
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"draft_templates": ["draft-A"]}), encoding="utf-8")

    exit_code = main([
        str(spec_path),
        "--out",
        str(out_path),
        "--format",
        "jsonl",
        "--catalog",
        str(catalog_path),
    ])

    assert exit_code == 0
    lines = out_path.read_text(encoding="utf-8").strip().splitlines()
    payload = json.loads(lines[0])
    assert payload["draft_template"] == "draft-A"
    assert list(payload.keys()) == ["draft_template", "essay_template", "draft_essay"]


def test_cli_catalog_csv_support(tmp_path: Path) -> None:
    spec_path = write_spec(tmp_path)
    csv_path = tmp_path / "draft_templates.csv"
    csv_path.write_text("id\ndraft-A\n", encoding="utf-8")

    exit_code = main([
        str(spec_path),
        "--out",
        str(tmp_path / "rows.csv"),
        "--format",
        "csv",
        "--catalog-csv",
        f"draft_templates={csv_path}:id",
    ])

    assert exit_code == 0

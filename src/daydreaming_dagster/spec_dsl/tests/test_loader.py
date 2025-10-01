from __future__ import annotations

import json
from pathlib import Path

import pytest

from daydreaming_dagster.spec_dsl.errors import SpecDslError, SpecDslErrorCode
from daydreaming_dagster.spec_dsl.loader import load_spec


def write_spec(tmp_path: Path, payload: dict[str, object]) -> Path:
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_load_spec_round_trip(tmp_path: Path) -> None:
    spec_path = write_spec(
        tmp_path,
        {
            "axes": {
                "draft_template": {
                    "levels": ["draft-A", "draft-B"],
                    "catalog_lookup": {"catalog": "drafts"},
                },
                "essay_model": ["llm-1"],
            },
            "rules": [{"subset": {"axis": "draft_template", "keep": ["draft-A"]}}],
            "output": {"expand_pairs": False},
            "replicates": {
                "draft_template": {"count": 2, "column": "draft_rep"},
            },
        },
    )

    spec = load_spec(spec_path)

    assert list(spec.axes.keys()) == ["draft_template", "essay_model"]
    assert spec.axes["draft_template"].levels == ("draft-A", "draft-B")
    assert spec.axes["draft_template"].catalog_lookup == {"catalog": "drafts"}
    assert spec.rules == ({"subset": {"axis": "draft_template", "keep": ["draft-A"]}},)
    assert spec.output == {"expand_pairs": False}
    assert spec.replicates["draft_template"].count == 2
    assert spec.replicates["draft_template"].column == "draft_rep"


def test_load_spec_rejects_non_list_levels(tmp_path: Path) -> None:
    spec_path = write_spec(
        tmp_path,
        {
            "axes": {"draft_template": "not-a-list"},
        },
    )

    with pytest.raises(SpecDslError) as exc:
        load_spec(spec_path)

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert exc.value.ctx and exc.value.ctx["axis"] == "draft_template"


def test_load_spec_directory_support(tmp_path: Path) -> None:
    root = tmp_path / "spec"
    axes_dir = root / "axes"
    rules_dir = root / "rules"
    axes_dir.mkdir(parents=True)
    rules_dir.mkdir()

    (root / "config.yaml").write_text(
        """
axes:
  draft_template: [draft-A]
output:
  expand_pairs: false
""",
        encoding="utf-8",
    )
    (axes_dir / "essay_model.txt").write_text("llm-1\nllm-2\n", encoding="utf-8")
    (rules_dir / "01_subset.yaml").write_text(
        "subset:\n  axis: essay_model\n  keep:\n    - llm-2\n",
        encoding="utf-8",
    )

    spec = load_spec(root)

    assert list(spec.axes.keys()) == ["draft_template", "essay_model"]
    assert spec.axes["essay_model"].levels == ("llm-1", "llm-2")
    assert spec.rules[0]["subset"]["axis"] == "essay_model"
    assert spec.output == {"expand_pairs": False}


def test_load_spec_replicates_validation(tmp_path: Path) -> None:
    spec_path = write_spec(
        tmp_path,
        {
            "axes": {"draft_template": ["d1"]},
            "replicates": {"draft_template": {"count": 0}},
        },
    )

    with pytest.raises(SpecDslError) as exc:
        load_spec(spec_path)

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC

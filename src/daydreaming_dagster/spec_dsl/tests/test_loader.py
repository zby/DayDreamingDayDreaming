from __future__ import annotations

import json
from pathlib import Path

import pytest

from daydreaming_dagster.spec_dsl.errors import SpecDslError, SpecDslErrorCode
from daydreaming_dagster.spec_dsl.loader import load_spec

from .fixtures import ExperimentSpecFactory


def write_spec(tmp_path: Path, payload: dict[str, object]) -> Path:
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_load_spec_round_trip_file(tmp_path: Path) -> None:
    """Ensure disk-backed specs still parse correctly."""

    levels_csv = tmp_path / "draft_levels.csv"
    levels_csv.write_text("draft_template\ndraft-A\ndraft-B\n", encoding="utf-8")

    pairs_csv = tmp_path / "allowed_pairs.csv"
    pairs_csv.write_text("draft_template,essay_model\ndraft-A,llm-1\n", encoding="utf-8")

    spec_path = write_spec(
        tmp_path,
        {
            "axes": {
                "draft_template": "@file:draft_levels.csv",
                "essay_model": ["llm-1"],
            },
            "rules": {
                "subsets": {"draft_template": ["draft-A"]},
                "pairs": {
                    "bundle": {
                        "left": "draft_template",
                        "right": "essay_model",
                        "allowed": "@file:allowed_pairs.csv",
                    }
                },
            },
            "output": {
                "field_order": ["draft_template", "essay_model"],
            },
            "replicates": {
                "draft_template": 2,
            },
        },
    )

    spec = load_spec(spec_path)

    assert list(spec.axes.keys()) == ["draft_template", "essay_model"]
    assert spec.axes["draft_template"].levels == ("draft-A", "draft-B")
    assert spec.rules == (
        {"subset": {"axis": "draft_template", "keep": ["draft-A"]}},
        {
            "pair": {
                "left": "draft_template",
                "right": "essay_model",
                "name": "bundle",
                "allowed": [("draft-A", "llm-1")],
            }
        },
    )
    assert spec.output["field_order"] == ["draft_template", "essay_model"]
    assert spec.replicates["draft_template"].count == 2
    assert spec.replicates["draft_template"].column == "draft_template_replicate"


def test_parse_spec_round_trip(tmp_path: Path) -> None:
    factory = ExperimentSpecFactory(tmp_path)

    levels_csv = tmp_path / "draft_levels.csv"
    levels_csv.write_text("draft_template\ndraft-A\ndraft-B\n", encoding="utf-8")

    pairs_csv = tmp_path / "allowed_pairs.csv"
    pairs_csv.write_text("draft_template,essay_model\ndraft-A,llm-1\n", encoding="utf-8")

    spec = factory.parse(
        {
            "axes": {
                "draft_template": "@file:draft_levels.csv",
                "essay_model": ["llm-1"],
            },
            "rules": {
                "subsets": {"draft_template": ["draft-A"]},
                "pairs": {
                    "bundle": {
                        "left": "draft_template",
                        "right": "essay_model",
                        "allowed": "@file:allowed_pairs.csv",
                    }
                },
            },
            "output": {
                "field_order": ["draft_template", "essay_model"],
            },
            "replicates": {
                "draft_template": 2,
            },
        }
    )

    assert list(spec.axes.keys()) == ["draft_template", "essay_model"]
    assert spec.axes["draft_template"].levels == ("draft-A", "draft-B")


def test_parse_spec_rejects_non_list_levels(tmp_path: Path) -> None:
    factory = ExperimentSpecFactory(tmp_path)

    with pytest.raises(SpecDslError) as exc:
        factory.parse(
            {
                "axes": {"draft_template": "not-a-list"},
                "output": {"field_order": ["draft_template"]},
            }
        )

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert exc.value.ctx and exc.value.ctx["axis"] == "draft_template"


def test_load_spec_directory_support(tmp_path: Path) -> None:
    root = tmp_path / "spec"
    root.mkdir(parents=True)

    with pytest.raises(SpecDslError) as exc:
        load_spec(root)

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert exc.value.ctx and "deprecated" in exc.value.ctx.get("error", "")


def test_parse_spec_replicates_validation(tmp_path: Path) -> None:
    factory = ExperimentSpecFactory(tmp_path)

    with pytest.raises(SpecDslError) as exc:
        factory.parse(
            {
                "axes": {"draft_template": ["d1"]},
                "replicates": {"draft_template": 0},
                "output": {"field_order": ["draft_template"]},
            }
        )

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC


def test_parse_spec_rejects_deprecated_output_flags(tmp_path: Path) -> None:
    factory = ExperimentSpecFactory(tmp_path)

    with pytest.raises(SpecDslError) as exc:
        factory.parse(
            {
                "axes": {"draft_template": ["d1"]},
                "output": {
                    "field_order": ["draft_template"],
                    "expand_pairs": False,
                },
            }
        )

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC


def test_parse_spec_rejects_csv_without_header(tmp_path: Path) -> None:
    csv_path = tmp_path / "levels.csv"
    csv_path.write_text("draft-A\n", encoding="utf-8")

    factory = ExperimentSpecFactory(tmp_path)

    with pytest.raises(SpecDslError) as exc:
        factory.parse(
            {
                "axes": {"draft_template": "@file:levels.csv"},
                "output": {"field_order": ["draft_template"]},
            }
        )

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert exc.value.ctx and "CSV" in exc.value.ctx.get("error", "")


def test_parse_spec_requires_field_order(tmp_path: Path) -> None:
    factory = ExperimentSpecFactory(tmp_path)

    with pytest.raises(SpecDslError) as exc:
        factory.parse(
            {
                "axes": {"draft_template": ["d1"]},
                "output": {},
            }
        )

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert "field_order" in str(exc.value.ctx)


def test_parse_spec_rejects_legacy_rules_shape(tmp_path: Path) -> None:
    factory = ExperimentSpecFactory(tmp_path)

    with pytest.raises(SpecDslError) as exc:
        factory.parse(
            {
                "axes": {"draft_template": ["d1"]},
                "rules": [
                    {"subset": {"axis": "draft_template", "keep": ["d1"]}},
                ],
                "output": {"field_order": ["draft_template"]},
            }
        )

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC


def test_parse_spec_rejects_deprecated_output_order(tmp_path: Path) -> None:
    factory = ExperimentSpecFactory(tmp_path)

    with pytest.raises(SpecDslError) as exc:
        factory.parse(
            {
                "axes": {"draft_template": ["d1"]},
                "output": {"order": ["draft_template"], "field_order": ["draft_template"]},
            }
        )

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC


def test_parse_spec_pair_rule_shortcut(tmp_path: Path) -> None:
    factory = ExperimentSpecFactory(tmp_path)

    spec = factory.parse(
        {
            "axes": {"left": ["a"], "right": ["b"]},
            "rules": {
                "pairs": {
                    "bundle": {
                        "left": "left",
                        "right": "right",
                        "allowed": [["a", "b"]],
                    }
                }
            },
            "output": {"field_order": ["left", "right"]},
        }
    )

    assert spec.rules == (
        {
            "pair": {
                "left": "left",
                "right": "right",
                "name": "bundle",
                "allowed": [["a", "b"]],
            }
        },
    )

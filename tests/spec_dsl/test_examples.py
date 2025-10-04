from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from daydreaming_dagster.spec_dsl import compile_design, parse_spec_mapping

FIXTURES = Path(__file__).parent.parent / "fixtures" / "spec_dsl"

CATALOGS = {
    "draft_template": [
        "creative-synthesis-v2",
        "application-implementation-v2",
        "gwern_original",
    ],
    "essay_template": [
        "parsed-from-links-v1",
        "essay-copy-v1",
        "essay-llm-v1",
        "essay-theme-v16",
        "essay-theme-v17",
    ],
    "evaluation_template": [
        "creativity-metrics-v2",
        "daydreaming-verification-v3",
        "verification-eval-v1",
        "quality-eval-v1",
    ],
    "draft_llm": ["gemini_25_pro", "sonnet-4"],
    "essay_llm": ["sonnet-4", "gemini_25_pro"],
    "evaluation_llm": ["sonnet-4", "gemini_25_pro"],
}

EXAMPLES = {
    "baseline_cartesian": 12,
    "dual_llm_cartesian": 4,
    "paired_copy_mix": 2,
    "full_two_phase_cartesian": 64,
    "curated_essays": 2,
    "creative_alignment_tuple": 528,
}


@pytest.mark.parametrize(
    "example",
    "baseline_cartesian dual_llm_cartesian paired_copy_mix full_two_phase_cartesian curated_essays creative_alignment_tuple".split(),
)
def test_examples_compile(example: str) -> None:
    spec_path = FIXTURES / example / "config.yaml"
    payload = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    spec = parse_spec_mapping(payload, source=spec_path, base_dir=spec_path.parent)
    rows = compile_design(spec)
    assert len(rows) == EXAMPLES[example]

    if example == "dual_llm_cartesian":
        replicate_values = {row["draft_template_replicate"] for row in rows}
        assert replicate_values == {"1", "2"}
    if example == "curated_essays":
        for row in rows:
            assert row["evaluation_template"] == "verification-eval-v1"
    if example == "creative_alignment_tuple":
        assert {
            (row["draft_template"], row["essay_template"])
            for row in rows
        } == {
            ("creative-synthesis-v10", "creative-synthesis-v10"),
            ("creative-synthesis-v7", "parsed-from-links-v1"),
        }

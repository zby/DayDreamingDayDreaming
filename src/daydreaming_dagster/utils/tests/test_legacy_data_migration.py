from __future__ import annotations

from pathlib import Path

import json

import pytest

from daydreaming_dagster.utils.legacy_gens_migration import (
    parse_essay_filename,
    parse_evaluation_filename,
    migrate_legacy_gens,
)


def test_parse_essay_filename_extracts_parts():
    combos = {"combo_v1_133798aefe82"}
    templates = {"creative-synthesis-v10"}
    llms = {"gemini_2.5_flash", "deepseek_r1_p"}

    parts = parse_essay_filename(
        "combo_v1_133798aefe82_pair-explorer-recursive-v1_gemini_2.5_flash_creative-synthesis-v10_v3",
        combo_ids=combos,
        template_ids=templates,
        llm_ids=llms,
    )

    assert parts.combo_id == "combo_v1_133798aefe82"
    assert parts.template_id == "creative-synthesis-v10"
    assert parts.llm_model_id == "gemini_2.5_flash"
    assert parts.parent_template_id == "pair-explorer-recursive-v1"
    assert parts.parent_task_id == "combo_v1_133798aefe82_pair-explorer-recursive-v1_gemini_2.5_flash"
    assert parts.replicate == 3


def test_parse_evaluation_filename_extracts_parts():
    combos = {"combo_v1_133798aefe82"}
    eval_templates = {"gemini-prior-art-eval"}
    essay_templates = {"creative-synthesis-v12"}
    llms = {"gemini_25_pro", "deepseek_r1_p"}

    parts = parse_evaluation_filename(
        "combo_v1_133798aefe82_pair-explorer-recursive-v2_deepseek_r1_p_creative-synthesis-v12_gemini-prior-art-eval_gemini_25_pro",
        combo_ids=combos,
        evaluation_template_ids=eval_templates,
        essay_template_ids=essay_templates,
        llm_ids=llms,
    )

    assert parts.combo_id == "combo_v1_133798aefe82"
    assert parts.template_id == "gemini-prior-art-eval"
    assert parts.llm_model_id == "gemini_25_pro"
    assert parts.parent_template_id == "pair-explorer-recursive-v2"
    assert parts.essay_template_id == "creative-synthesis-v12"
    assert parts.parent_task_id == (
        "combo_v1_133798aefe82_pair-explorer-recursive-v2_deepseek_r1_p_creative-synthesis-v12"
    )
    assert parts.replicate is None


@pytest.fixture()
def _tmp_paths(tmp_path: Path) -> tuple[Path, Path]:
    legacy = tmp_path / "legacy"
    target = tmp_path / "target"
    (legacy / "3_generation" / "essay_prompts").mkdir(parents=True)
    (legacy / "3_generation" / "essay_responses").mkdir(parents=True)
    (legacy / "4_evaluation" / "evaluation_prompts").mkdir(parents=True)
    (legacy / "4_evaluation" / "evaluation_responses").mkdir(parents=True)

    (target / "1_raw").mkdir(parents=True)
    (target / "gens").mkdir()

    # Minimal metadata CSVs used by the migration filters
    (target / "1_raw" / "essay_templates.csv").write_text(
        "template_id\ncreative-synthesis-v10\ncreative-synthesis-v12\n",
        encoding="utf-8",
    )
    (target / "1_raw" / "evaluation_templates.csv").write_text(
        "template_id\ngemini-prior-art-eval\n",
        encoding="utf-8",
    )
    (target / "1_raw" / "llm_models.csv").write_text(
        "id\ngemini_2.5_flash\ngemini_25_pro\ndeepseek_r1_p\n",
        encoding="utf-8",
    )

    combos_csv = "combo_id\ncombo_v1_133798aefe82\n"
    (legacy / "combo_mappings.csv").write_text(combos_csv, encoding="utf-8")

    essay_stem = (
        "combo_v1_133798aefe82_pair-explorer-recursive-v1_gemini_2.5_flash_creative-synthesis-v10"
    )
    (legacy / "3_generation" / "essay_prompts" / f"{essay_stem}.txt").write_text(
        "PROMPT", encoding="utf-8"
    )
    (legacy / "3_generation" / "essay_responses" / f"{essay_stem}.txt").write_text(
        "ESSAY", encoding="utf-8"
    )

    eval_stem = (
        "combo_v1_133798aefe82_pair-explorer-recursive-v1_gemini_2.5_flash_creative-synthesis-v10_"
        "gemini-prior-art-eval_gemini_25_pro"
    )
    (legacy / "4_evaluation" / "evaluation_prompts" / f"{eval_stem}.txt").write_text(
        "EVAL PROMPT", encoding="utf-8"
    )
    (legacy / "4_evaluation" / "evaluation_responses" / f"{eval_stem}.txt").write_text(
        "**REASONING:** Ok\n\n**SCORE: 7**", encoding="utf-8"
    )

    return legacy, target


def test_migrate_legacy_gens_creates_expected_outputs(_tmp_paths: tuple[Path, Path]):
    legacy_root, target_root = _tmp_paths

    stats = migrate_legacy_gens(
        legacy_root=legacy_root,
        target_root=target_root,
        dry_run=False,
    )

    assert stats["essay"].converted == 1
    assert stats["evaluation"].converted == 1

    essay_dirs = list((target_root / "gens" / "essay").iterdir())
    assert len(essay_dirs) == 1
    essay_dir = essay_dirs[0]
    assert (essay_dir / "prompt.txt").read_text(encoding="utf-8") == "PROMPT"
    assert (essay_dir / "parsed.txt").read_text(encoding="utf-8") == "ESSAY"

    meta = json.loads((essay_dir / "metadata.json").read_text(encoding="utf-8"))
    assert meta["template_id"] == "creative-synthesis-v10"
    assert meta["llm_model_id"] == "gemini_2.5_flash"
    assert meta["combo_id"] == "combo_v1_133798aefe82"

    evaluation_dirs = list((target_root / "gens" / "evaluation").iterdir())
    assert len(evaluation_dirs) == 1
    eval_dir = evaluation_dirs[0]
    assert (eval_dir / "prompt.txt").read_text(encoding="utf-8") == "EVAL PROMPT"
    assert (eval_dir / "raw.txt").read_text(encoding="utf-8").startswith("**REASONING:**")
    assert (eval_dir / "parsed.txt").read_text(encoding="utf-8") == "7"

    eval_meta = json.loads((eval_dir / "metadata.json").read_text(encoding="utf-8"))
    assert eval_meta["template_id"] == "gemini-prior-art-eval"
    assert eval_meta["llm_model_id"] == "gemini_25_pro"
    assert eval_meta["parent_task_id"].startswith("combo_v1_133798aefe82_")
    assert eval_meta["parent_gen_id"] == essay_dir.name

    eval_raw_meta = json.loads((eval_dir / "raw_metadata.json").read_text(encoding="utf-8"))
    assert eval_raw_meta["parent_gen_id"] == essay_dir.name

    eval_parsed_meta = json.loads((eval_dir / "parsed_metadata.json").read_text(encoding="utf-8"))
    assert eval_parsed_meta["parent_gen_id"] == essay_dir.name

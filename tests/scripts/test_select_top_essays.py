from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "select_top_essays.py"
BUILD_SCRIPT = ROOT / "scripts" / "build_cohort_from_list.py"


def _write_metadata(root: Path, stage: str, gen_id: str, payload: dict) -> None:
    path = root / "gens" / stage / gen_id / "metadata.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_aggregated_scores(path: Path) -> None:
    rows = pd.DataFrame(
        [
            {
                "gen_id": "eval-1",
                "parent_gen_id": "essay-1",
                "evaluation_template": "novelty",
                "evaluation_llm_model": "sonnet-4",
                "score": 9.5,
            },
            {
                "gen_id": "eval-2",
                "parent_gen_id": "essay-1",
                "evaluation_template": "novelty",
                "evaluation_llm_model": "gemini_25_pro",
                "score": 9.0,
            },
            {
                "gen_id": "eval-ignored",
                "parent_gen_id": "essay-2",
                "evaluation_template": "novelty",
                "evaluation_llm_model": "sonnet-4",
                "score": 5.0,
            },
        ]
    )
    rows.to_csv(path, index=False)


def test_select_and_build_cohort_from_curated_list(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    aggregated_dir = data_root / "7_cross_experiment"
    aggregated_dir.mkdir(parents=True, exist_ok=True)
    aggregated_path = aggregated_dir / "aggregated_scores.csv"
    _write_aggregated_scores(aggregated_path)

    # Draft metadata (referenced by essay)
    _write_metadata(
        data_root,
        "draft",
        "draft-1",
        {
            "combo_id": "combo-1",
            "template_id": "draft-template",
            "llm_model_id": "draft-llm",
            "replicate": 1,
        },
    )

    # Essay metadata
    _write_metadata(
        data_root,
        "essay",
        "essay-1",
        {
            "template_id": "essay-template",
            "llm_model_id": "essay-llm",
            "parent_gen_id": "draft-1",
            "replicate": 1,
        },
    )

    # Evaluation metadata
    _write_metadata(
        data_root,
        "evaluation",
        "eval-1",
        {
            "template_id": "novelty",
            "llm_model_id": "sonnet-4",
            "parent_gen_id": "essay-1",
            "replicate": 1,
        },
    )
    _write_metadata(
        data_root,
        "evaluation",
        "eval-2",
        {
            "template_id": "novelty",
            "llm_model_id": "gemini_25_pro",
            "parent_gen_id": "essay-1",
            "replicate": 1,
        },
    )

    candidate_path = tmp_path / "top_candidates.csv"

    result_select = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--cohort-id",
            "novelty-top",
            "--template",
            "novelty",
            "--top-n",
            "1",
            "--aggregated-scores",
            str(aggregated_path),
            "--output",
            str(candidate_path),
            "--overwrite",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert candidate_path.exists(), result_select.stdout
    candidates = pd.read_csv(candidate_path)
    assert "essay_gen_id" in candidates.columns
    assert "selected" in candidates.columns
    assert candidates.loc[0, "selected"]

    result_build = subprocess.run(
        [
            sys.executable,
            str(BUILD_SCRIPT),
            "--cohort-id",
            "novelty-top",
            "--candidate-list",
            str(candidate_path),
            "--aggregated-scores",
            str(aggregated_path),
            "--data-root",
            str(data_root),
            "--overwrite",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    membership_path = data_root / "cohorts" / "novelty-top" / "membership.csv"
    assert membership_path.exists(), result_build.stdout
    membership = pd.read_csv(membership_path)
    assert set(membership["stage"]) == {"draft", "essay", "evaluation"}
    assert set(membership["gen_id"]) == {"draft-1", "essay-1", "eval-1", "eval-2"}
    assert (membership["origin_cohort_id"] == "novelty-top").all()

    spec_dir = data_root / "cohorts" / "novelty-top" / "spec"
    config_path = spec_dir / "config.yaml"
    assert config_path.exists()
    config_yaml = config_path.read_text(encoding="utf-8")
    assert "@file:items/cohort_rows.csv" in config_yaml

    items_path = spec_dir / "items" / "cohort_rows.csv"
    assert items_path.exists()
    items = pd.read_csv(items_path)
    assert "combo_id" in items.columns
    assert "novelty" in items["evaluation_template"].tolist()

    curated_copy = data_root / "cohorts" / "novelty-top" / "curation" / "essay_candidates.csv"
    assert curated_copy.exists()
    curated_df = pd.read_csv(curated_copy)
    assert "essay_gen_id" in curated_df.columns

    assert "Generated spec" in result_build.stdout

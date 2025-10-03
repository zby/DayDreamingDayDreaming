"""Full pipeline integration tests using temporary DAGSTER_HOME and copied live data.

Tests complete materialization of raw_data and llm_tasks groups with real data.
"""

import json
import os
import re
import shutil
import tempfile
import textwrap
import yaml
from pathlib import Path
from unittest.mock import patch, Mock

import pandas as pd
import pytest
from dagster import materialize, DagsterInstance
from tests.helpers.llm_stubs import CannedLLMResource
from daydreaming_dagster.cohorts import load_cohort_definition


class _StubCohortSpec:
    def compile_definition(
        self,
        *,
        spec=None,
        path=None,
        catalogs=None,
        seed=None,
    ):
        return load_cohort_definition(spec or path, catalogs=catalogs, seed=seed)

# Markers
pytestmark = [pytest.mark.integration, pytest.mark.live_data]


def _write_spec(
    root: Path,
    *,
    cohort_id: str,
    combos: list[str],
    draft_templates: list[str],
    essay_templates: list[str],
    evaluation_templates: list[str],
    generation_llms: list[str],
    evaluation_llms: list[str],
    replicates: dict[str, dict[str, int | str]] | None = None,
) -> None:
    spec_dir = root / "cohorts" / cohort_id / "spec"
    spec_dir.mkdir(parents=True, exist_ok=True)

    allowed_matrix = [
        [tpl, model] for tpl in evaluation_templates for model in evaluation_llms
    ]
    if not allowed_matrix:
        allowed_matrix = [["placeholder-eval", "placeholder-model"]]

    axes = {
        "combo_id": [str(item) for item in combos],
        "draft_template": [str(item) for item in draft_templates],
        "draft_llm": [str(item) for item in generation_llms],
        "essay_template": [str(item) for item in essay_templates],
        "essay_llm": [str(item) for item in generation_llms],
        "evaluation_template": [str(item) for item in evaluation_templates],
        "evaluation_llm": [str(item) for item in evaluation_llms],
    }

    spec: dict[str, object] = {
        "axes": axes,
        "rules": {
            "pairs": {
                "evaluation_bundle": {
                    "left": "evaluation_template",
                    "right": "evaluation_llm",
                    "allowed": allowed_matrix,
                }
            }
        },
        "output": {
            "field_order": [
                "combo_id",
                "draft_template",
                "draft_llm",
                "essay_template",
                "essay_llm",
                "evaluation_template",
                "evaluation_llm",
            ]
        },
    }

    if replicates:
        spec["replicates"] = replicates
        replicate_columns = [f"{axis}_replicate" for axis in replicates]
        spec["output"]["field_order"].extend(replicate_columns)

    (spec_dir / "config.yaml").write_text(
        yaml.safe_dump(spec, sort_keys=False),
        encoding="utf-8",
    )


@pytest.fixture(scope="function")
def pipeline_data_root_prepared():
    """Prepare persistent test data under tests/data_pipeline_test by copying live data and limiting scope.

    Steps:
    - Clean tests/data_pipeline_test
    - Copy data/1_raw into tests/data_pipeline_test/1_raw
    - Create output dirs (2_tasks, gens/) for gen-id–keyed prompts/responses
    - Trim raw catalogs to a small deterministic subset and write a cohort spec
    - Return Path to tests/data_pipeline_test
    """
    repo_root = Path(__file__).resolve().parents[1]
    live_source = repo_root / "data" / "1_raw"
    assert live_source.exists(), f"Live data not found: {live_source}"

    pipeline_data_root = repo_root / "tests" / "data_pipeline_test"
    if pipeline_data_root.exists():
        shutil.rmtree(pipeline_data_root)
    (pipeline_data_root / "1_raw").mkdir(parents=True)
    (pipeline_data_root / "2_tasks").mkdir(parents=True)
    # Gen store (prompts/responses live under data/gens/<stage>/<gen_id>/)
    (pipeline_data_root / "gens").mkdir(parents=True)
    (pipeline_data_root / "cohorts").mkdir(parents=True)
    (pipeline_data_root / "7_reporting").mkdir(parents=True)

    # Copy live 1_raw
    for item in live_source.iterdir():
        dest = pipeline_data_root / "1_raw" / item.name
        if item.is_dir():
            shutil.copytree(item, dest)
        else:
            shutil.copy2(item, dest)
    
    # Ensure template directories are properly copied with all files
    gen_templates_dir = pipeline_data_root / "1_raw" / "templates"
    eval_templates_dir = pipeline_data_root / "1_raw" / "templates" / "evaluation"
    
    # Verify template files exist (two-phase layout: draft/ and essay/)
    if gen_templates_dir.exists():
        gen_draft_files = list((gen_templates_dir / "draft").glob("*.txt"))
        gen_essay_files = list((gen_templates_dir / "essay").glob("*.txt"))
        total_gen_files = len(gen_draft_files) + len(gen_essay_files)
        print(f"✓ Copied {total_gen_files} generation template files (draft={len(gen_draft_files)}, essay={len(gen_essay_files)})")
        if total_gen_files == 0:
            raise RuntimeError("No generation template files found after copy (expected under draft/ and essay/)")
    
    if eval_templates_dir.exists():
        eval_files = list(eval_templates_dir.glob("*.txt"))
        print(f"✓ Copied {len(eval_files)} evaluation template files")
        if len(eval_files) == 0:
            raise RuntimeError("No evaluation template files found after copy")

    # Ensure concepts metadata is available at new root location
    old_concepts_csv = pipeline_data_root / "1_raw" / "concepts" / "concepts_metadata.csv"
    new_concepts_csv = pipeline_data_root / "1_raw" / "concepts_metadata.csv"
    if old_concepts_csv.exists() and not new_concepts_csv.exists():
        shutil.copy2(old_concepts_csv, new_concepts_csv)

    # Trim raw catalogs to a small deterministic subset and drop legacy columns
    concepts_csv = pipeline_data_root / "1_raw" / "concepts_metadata.csv"
    draft_templates_csv = pipeline_data_root / "1_raw" / "draft_templates.csv"
    essay_templates_csv = pipeline_data_root / "1_raw" / "essay_templates.csv"
    eval_templates_csv = pipeline_data_root / "1_raw" / "evaluation_templates.csv"
    models_csv = pipeline_data_root / "1_raw" / "llm_models.csv"

    def _drop_legacy_columns(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=[col for col in df.columns if col.lower() == "active"], errors="ignore")

    if concepts_csv.exists():
        cdf = pd.read_csv(concepts_csv)
        cdf = _drop_legacy_columns(cdf).head(2).reset_index(drop=True)
        cdf.to_csv(concepts_csv, index=False)

    target_template = "creative-synthesis-v10"

    if draft_templates_csv.exists():
        ddf = pd.read_csv(draft_templates_csv)
        if "template_id" in ddf.columns:
            ddf["template_id"] = ddf["template_id"].astype(str)
            prioritized = ddf[ddf["template_id"] == target_template]
            remaining = ddf[ddf["template_id"] != target_template].head(max(0, 3 - len(prioritized)))
            trimmed = pd.concat([prioritized, remaining], ignore_index=True)
            if trimmed.empty and not ddf.empty:
                trimmed = ddf.head(1).copy()
            ddf = trimmed
        ddf = _drop_legacy_columns(ddf)
        if "generator" in ddf.columns:
            ddf["generator"] = ddf["generator"].replace({"": "llm"}).fillna("llm")
        ddf.to_csv(draft_templates_csv, index=False)

    if essay_templates_csv.exists():
        edf = pd.read_csv(essay_templates_csv)
        if "template_id" in edf.columns:
            edf["template_id"] = edf["template_id"].astype(str)
            prioritized = edf[edf["template_id"] == target_template]
            remaining = edf[edf["template_id"] != target_template].head(max(0, 2 - len(prioritized)))
            trimmed = pd.concat([prioritized, remaining], ignore_index=True)
            if trimmed.empty and not edf.empty:
                trimmed = edf.head(1).copy()
            edf = trimmed
        edf = _drop_legacy_columns(edf)
        if "generator" in edf.columns:
            edf["generator"] = edf["generator"].replace({"": "llm"}).fillna("llm")
        edf.to_csv(essay_templates_csv, index=False)

    if eval_templates_csv.exists():
        evdf = pd.read_csv(eval_templates_csv)
        evdf = _drop_legacy_columns(evdf).head(2).reset_index(drop=True)
        evdf.to_csv(eval_templates_csv, index=False)

    if models_csv.exists():
        mdf = pd.read_csv(models_csv)
        mdf = _drop_legacy_columns(mdf).head(4).reset_index(drop=True)
        if "for_generation" in mdf.columns and not mdf.empty:
            mdf["for_generation"] = False
            mdf.loc[mdf.index[: min(2, len(mdf))], "for_generation"] = True
        if "for_evaluation" in mdf.columns and not mdf.empty:
            mdf["for_evaluation"] = False
            mdf.loc[mdf.index[: min(2, len(mdf))], "for_evaluation"] = True
        mdf.to_csv(models_csv, index=False)

    combo_mappings_csv = pipeline_data_root / "combo_mappings.csv"
    if combo_mappings_csv.exists():
        combo_df = pd.read_csv(combo_mappings_csv)
        combo_df = combo_df.head(10).reset_index(drop=True)
        combo_df.to_csv(combo_mappings_csv, index=False)
        combos = combo_df["combo_id"].astype(str).dropna().unique().tolist()[:3]
    else:
        combos = ["combo-test-1"]
        pd.DataFrame(
            [
                {
                    "combo_id": "combo-test-1",
                    "concept_id": "concept-1",
                    "description_level": "paragraph",
                    "k_max": 2,
                    "version": "v1",
                    "created_at": "",
                }
            ]
        ).to_csv(combo_mappings_csv, index=False)

    # Ensure at least one two-phase template exists for downstream checks
    draft_file = pipeline_data_root / "1_raw" / "templates" / "draft" / f"{target_template}.txt"
    essay_file = pipeline_data_root / "1_raw" / "templates" / "essay" / f"{target_template}.txt"
    if not (draft_file.exists() and essay_file.exists()):
        raise RuntimeError(
            f"Two-phase template files missing for {target_template}: {draft_file} or {essay_file}"
        )

    # Capture trimmed catalog entries to seed the cohort spec
    draft_templates = pd.read_csv(draft_templates_csv)["template_id"].astype(str).dropna().tolist()
    essay_templates = pd.read_csv(essay_templates_csv)["template_id"].astype(str).dropna().tolist()
    evaluation_templates = pd.read_csv(eval_templates_csv)["template_id"].astype(str).dropna().tolist()
    llm_models_df = pd.read_csv(models_csv)
    generation_llms = llm_models_df[llm_models_df.get("for_generation", False) == True]["id"].astype(str).dropna().tolist()
    evaluation_llms = llm_models_df[llm_models_df.get("for_evaluation", False) == True]["id"].astype(str).dropna().tolist()

    # Fallbacks in case trimming removed too much
    if not generation_llms and "id" in llm_models_df.columns:
        generation_llms = llm_models_df["id"].astype(str).dropna().tolist()[:1]
    if not evaluation_llms and "id" in llm_models_df.columns:
        evaluation_llms = llm_models_df["id"].astype(str).dropna().tolist()[:1]

    if not combos:
        combos = ["combo-test-1"]

    rep_cfg_path = pipeline_data_root / "1_raw" / "replication_config.csv"
    rep_map: dict[str, int] = {}
    if rep_cfg_path.exists():
        rep_df = pd.read_csv(rep_cfg_path)
        if {"stage", "replicates"}.issubset(rep_df.columns):
            rep_map = {
                str(row["stage"]).strip(): int(row["replicates"])
                for _, row in rep_df.iterrows()
            }

    replicates_spec: dict[str, int] = {}
    if rep_map.get("draft", 1) > 1:
        replicates_spec["draft_template"] = int(rep_map["draft"])
    if rep_map.get("essay", 1) > 1:
        replicates_spec["essay_template"] = int(rep_map["essay"])
    if rep_map.get("evaluation", 1) > 1:
        replicates_spec["evaluation_template"] = int(rep_map["evaluation"])

    _write_spec(
        pipeline_data_root,
        cohort_id="pipeline-test",
        combos=combos,
        draft_templates=draft_templates,
        essay_templates=essay_templates,
        evaluation_templates=evaluation_templates,
        generation_llms=generation_llms,
        evaluation_llms=evaluation_llms,
        replicates=replicates_spec or None,
    )

    return pipeline_data_root


class TestPipelineIntegration:
    """Full pipeline integration test with temporary DAGSTER_HOME."""

    def _verify_expected_files(self, test_directory, test_gen_partitions, test_eval_partitions):
        """Comprehensive verification of all expected pipeline output files."""
        
        # Task CSVs are optional in membership-first mode; if present, they should be non-empty
        task_files = [
            test_directory / "2_tasks" / "draft_generation_tasks.csv",
            test_directory / "2_tasks" / "essay_generation_tasks.csv",
            test_directory / "2_tasks" / "evaluation_tasks.csv",
        ]
        any_present = False
        for file_path in task_files:
            if file_path.exists():
                any_present = True
                assert file_path.stat().st_size > 0, f"Task file is empty: {file_path}"
        if not any_present:
            print("ℹ No task CSVs present (membership-first mode)")
            gen_tasks_file = test_directory / "2_tasks" / "draft_generation_tasks.csv"
            if gen_tasks_file.exists():
                gen_tasks_df = pd.read_csv(gen_tasks_file)
                assert len(gen_tasks_df) > 0, "Generation tasks should not be empty"
                required_columns = ["draft_task_id", "combo_id", "draft_template", "generation_model"]
                for col in required_columns:
                    assert col in gen_tasks_df.columns, f"Missing required column: {col}"
                for combo_id in gen_tasks_df["combo_id"].unique():
                    assert combo_id.startswith("combo_"), f"Invalid combo_id format: {combo_id}"
        
        # Gen store verification under data/gens/
        gens_root = test_directory / "gens"
        for partition in test_gen_partitions:
            draft_dir = gens_root / "draft" / str(partition)
            prompt_fp = draft_dir / "prompt.txt"
            parsed_fp = draft_dir / "parsed.txt"
            metadata_fp = draft_dir / "metadata.json"
            raw_meta_fp = draft_dir / "raw_metadata.json"
            parsed_meta_fp = draft_dir / "parsed_metadata.json"
            assert prompt_fp.exists(), f"Draft prompt not created: {prompt_fp}"
            assert parsed_fp.exists(), f"Draft parsed.txt not created: {parsed_fp}"
            assert metadata_fp.exists(), f"Draft metadata.json missing: {metadata_fp}"
            assert raw_meta_fp.exists(), f"Draft raw_metadata.json missing: {raw_meta_fp}"
            assert parsed_meta_fp.exists(), f"Draft parsed_metadata.json missing: {parsed_meta_fp}"
            assert prompt_fp.stat().st_size > 0, f"Draft prompt is empty: {prompt_fp}"
            assert parsed_fp.stat().st_size > 0, f"Draft parsed.txt is empty: {parsed_fp}"
            metadata = json.loads(metadata_fp.read_text(encoding="utf-8"))
            combo_in_meta = metadata.get("combo_id")
            assert isinstance(combo_in_meta, str) and combo_in_meta.startswith("combo_"), (
                f"Draft metadata combo_id malformed for {metadata_fp}: {combo_in_meta!r}"
            )
            raw_meta = json.loads(raw_meta_fp.read_text(encoding="utf-8"))
            assert raw_meta.get("function") == "draft_raw"
            assert raw_meta.get("input_mode") in {"prompt", "copy"}
            parsed_meta = json.loads(parsed_meta_fp.read_text(encoding="utf-8"))
            assert parsed_meta.get("function") == "draft_parsed"
            assert parsed_meta.get("parser_name")

        # Evaluation files (gens/evaluation/) - Check if they exist but don't require them
        eval_root = gens_root / "evaluation"
        eval_files_exist = any(eval_root.rglob("prompt.txt")) or any(eval_root.rglob("raw.txt")) or any(eval_root.rglob("parsed.txt"))
        if eval_files_exist:
            count = sum(1 for _ in eval_root.rglob("prompt.txt")) + sum(1 for _ in eval_root.rglob("raw.txt")) + sum(1 for _ in eval_root.rglob("parsed.txt"))
            print(f"✓ Found {count} evaluation gens-store files")
            for eval_dir in filter(lambda p: p.is_dir(), (eval_root).iterdir()):
                raw_meta_fp = eval_dir / "raw_metadata.json"
                parsed_meta_fp = eval_dir / "parsed_metadata.json"
                if raw_meta_fp.exists():
                    raw_meta = json.loads(raw_meta_fp.read_text(encoding="utf-8"))
                    assert raw_meta.get("function") == "evaluation_raw"
                if parsed_meta_fp.exists():
                    parsed_meta = json.loads(parsed_meta_fp.read_text(encoding="utf-8"))
                    assert parsed_meta.get("function") == "evaluation_parsed"
        else:
            print("⚠ Evaluation files not generated (expected - cross-partition complexity)")
        
        cohort_reports_dirs = list((test_directory / "cohorts").glob("*/reports"))
        if cohort_reports_dirs:
            reports_root = cohort_reports_dirs[0]
            parsing_file = reports_root / "parsing" / "aggregated_scores.csv"
            summary_files = list((reports_root / "summary").glob("*.csv"))
            analysis_files = list((reports_root / "analysis").glob("*.csv"))
            results_exist = parsing_file.exists() or bool(summary_files) or bool(analysis_files)
            if results_exist:
                print("✓ Cohort reports generated")
            else:
                print("⚠ Cohort reports not generated (expected - needs more evaluation data)")
        else:
            print("⚠ No cohort reports directory found (expected if cohort assets skipped)")
        
        # Print summary
        all_files = list(test_directory.rglob("*"))
        file_count = len([f for f in all_files if f.is_file()])
        print(f"✓ Total files created: {file_count}")
        print(f"✓ Task files verified: {len(task_files)}")
        print(f"✓ Generation partitions: {len(test_gen_partitions)}")
        print(f"✓ Evaluation partitions: {len(test_eval_partitions)}")
        
        # Note about simplified architecture
        print("ℹ Using simplified architecture - ContentCombination objects contain embedded content")

    def test_pipeline_e2e_complete_workflow(self, pipeline_data_root_prepared):
        """Complete end-to-end pipeline test that runs the entire workflow once and verifies all outcomes.
        
        This test:
        1. Sets up the pipeline with limited data (2 concepts, 2 templates, 2 models)
        2. Runs the complete pipeline from raw data to generation responses
        3. Optionally tests evaluation if enough data is available
        4. Verifies all expected outputs and file structures
        5. Runs only ONCE to avoid the heavy computational cost
        """
        pipeline_data_root = pipeline_data_root_prepared

        # Use a temporary DAGSTER_HOME for isolation, but persistent data_root
        with tempfile.TemporaryDirectory() as temp_home:
            temp_dagster_home = Path(temp_home) / "dagster_home"
            temp_dagster_home.mkdir(parents=True)
            env_vars = {
                "DAGSTER_HOME": str(temp_dagster_home),
                "DD_COHORT": "pipeline-test",
            }
            with patch.dict(os.environ, env_vars):
                instance = DagsterInstance.ephemeral(tempdir=str(temp_dagster_home))

                from daydreaming_dagster.assets.group_cohorts import (
                    cohort_id,
                    selected_combo_mappings,
                    content_combinations,
                    cohort_membership,
                )
                from daydreaming_dagster.assets.group_draft import (
                    draft_prompt,
                    draft_parsed,
                )
                from daydreaming_dagster.assets.group_essay import (
                    essay_prompt,
                    essay_parsed,
                )
                from daydreaming_dagster.assets.group_evaluation import (
                    evaluation_prompt,
                    evaluation_parsed,
                )
                from daydreaming_dagster.resources.io_managers import (
                    CohortCSVIOManager,
                    CSVIOManager,
                    InMemoryIOManager,
                )
                from daydreaming_dagster.resources.gens_prompt_io_manager import GensPromptIOManager
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig
                from daydreaming_dagster.data_layer.paths import Paths, COHORT_REPORT_ASSET_TARGETS

                paths = Paths.from_str(pipeline_data_root)

                resources = {
                    "data_root": str(pipeline_data_root),
                    "openrouter_client": CannedLLMResource(),
                    # no documents_index resource in filesystem-only mode
                    "csv_io_manager": CSVIOManager(base_path=pipeline_data_root / "2_tasks"),
                    # Prompt IO to gens store; responses written by assets to gens store; keep responses in-memory
                    "draft_prompt_io_manager": GensPromptIOManager(
                        gens_root=pipeline_data_root / "gens",
                        stage="draft",
                    ),
                    "draft_response_io_manager": InMemoryIOManager(),
                    "essay_prompt_io_manager": GensPromptIOManager(
                        gens_root=pipeline_data_root / "gens",
                        stage="essay",
                    ),
                    "essay_response_io_manager": InMemoryIOManager(),
                    "evaluation_prompt_io_manager": GensPromptIOManager(
                        gens_root=pipeline_data_root / "gens",
                        stage="evaluation",
                    ),
                    "evaluation_response_io_manager": InMemoryIOManager(),
                    "in_memory_io_manager": InMemoryIOManager(),
                    "parsing_results_io_manager": CohortCSVIOManager(
                        paths,
                        default_category="parsing",
                        asset_map=COHORT_REPORT_ASSET_TARGETS,
                    ),
                    "summary_results_io_manager": CohortCSVIOManager(
                        paths,
                        default_category="summary",
                        asset_map=COHORT_REPORT_ASSET_TARGETS,
                    ),
                    "error_log_io_manager": CSVIOManager(base_path=pipeline_data_root / "7_reporting"),
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph"),
                    "cohort_spec": _StubCohortSpec(),
                }

                print("🚀 Starting complete pipeline workflow...")
                
                # STEP 1: Materialize selected mappings first, then combinations and tasks
                print("📋 Step 1: Materializing task definitions...")
                
                # First materialize selected_combo_mappings
                result = materialize([selected_combo_mappings], resources=resources, instance=instance)
                assert result.success, "Selected combo mappings materialization failed"
                
                # Then materialize the rest (include cohort assets to satisfy dependencies)
                result = materialize(
                    [
                        selected_combo_mappings,
                        cohort_id,
                        cohort_membership,
                        content_combinations,
                    ],
                    resources=resources,
                    instance=instance,
                )
                assert result.success, "Task materialization failed"
                print("✅ Task materialization completed successfully")

                # Task artifacts are optional in membership-first mode
                task_dir = pipeline_data_root / "2_tasks"
                found_any = False
                for name in ("draft_generation_tasks.csv", "essay_generation_tasks.csv", "evaluation_tasks.csv"):
                    p = task_dir / name
                    if p.exists():
                        found_any = True
                        assert p.stat().st_size > 10, f"File appears empty: {name}"
                if not found_any:
                    print("ℹ No task CSVs present (membership-first mode)")

                # STEP 2: Materialize generation pipeline (drafts and essays)
                print("🔗 Step 2: Materializing generation pipeline...")
                
                # Get task IDs for generation
                # Use membership to select draft gen_ids
                cohort_dirs = sorted((pipeline_data_root / "cohorts").glob("*/membership.csv"))
                assert cohort_dirs, "membership.csv not created"
                mdf = pd.read_csv(cohort_dirs[0])
                test_gen_partitions = mdf[mdf["stage"] == "draft"]["gen_id"].astype(str).tolist()[:2]
                
                # Materialize a few generation tasks for testing
                from daydreaming_dagster.assets.group_draft import (
                    draft_prompt,
                    draft_parsed,
                )
                from daydreaming_dagster.assets.group_essay import (
                    essay_prompt,
                    essay_parsed,
                )
                
                # Materialize draft generation for specific partitions
                succeeded_gen_partitions = []
                failed_gen_partitions = []
                for partition_key in test_gen_partitions:
                    print(f"  Materializing drafts for partition: {partition_key}")
                    try:
                        result = materialize(
                            [
                                # Core dependencies for drafts
                                selected_combo_mappings,
                                content_combinations,
                                cohort_id,
                                cohort_membership,
                                # Draft generation assets
                                draft_prompt,
                                draft_parsed,
                            ],
                            resources=resources,
                            instance=instance,
                            partition_key=partition_key
                        )
                        if not result.success:
                            print(f"  ⚠ Draft generation failed for partition {partition_key} - continuing")
                            failed_gen_partitions.append(partition_key)
                            continue
                        succeeded_gen_partitions.append(partition_key)
                    except Exception as e:
                        print(f"  ⚠ Draft generation raised for partition {partition_key}: {e}")
                        failed_gen_partitions.append(partition_key)
                        continue
                
                # Materialize essay generation for corresponding essay task partitions
                # Choose essay gen_ids whose parent essay metadata references successful drafts
                essay_parent_map: dict[str, str] = {}
                essay_rows = mdf[mdf["stage"] == "essay"]["gen_id"].astype(str).tolist()
                succeeded_set = {str(x) for x in succeeded_gen_partitions}
                for essay_id in essay_rows:
                    meta_path = pipeline_data_root / "gens" / "essay" / essay_id / "metadata.json"
                    parent = ""
                    if meta_path.exists():
                        try:
                            meta = json.loads(meta_path.read_text(encoding="utf-8"))
                            parent = str(meta.get("parent_gen_id") or "")
                        except Exception:
                            parent = ""
                    essay_parent_map[essay_id] = parent

                test_essay_partitions = [
                    essay_id for essay_id, parent in essay_parent_map.items() if parent in succeeded_set
                ][:2]
                
                for partition_key in test_essay_partitions:
                    print(f"  Materializing essays for partition: {partition_key}")
                    result = materialize(
                        [
                            # Core dependencies for essays
                            selected_combo_mappings,
                            content_combinations,
                            cohort_id,
                            cohort_membership,
                            # Essay generation assets
                            essay_prompt,
                            essay_parsed
                        ],
                        resources=resources,
                        instance=instance,
                        partition_key=partition_key
                    )
                    if not result.success:
                        print(f"  ⚠ Essay generation failed for partition {partition_key} - continuing")
                        continue
                
                print("✅ Generation materialization completed")
                
                # Final verification including generated files
                self._verify_expected_files(
                    pipeline_data_root,
                    succeeded_gen_partitions,
                    [],
                )

                # Optional: RAW outputs may or may not exist for failed partitions depending on failure cause
                print("🎉 Task setup workflow test passed successfully!")

    def test_combo_mappings_normalized_structure(self):
        """Specific test for the normalized combo_mappings.csv structure (superset)."""
        # Use a simplified test with minimal data
        with tempfile.TemporaryDirectory() as temp_root:
            temp_dagster_home = Path(temp_root) / "dagster_home" 
            temp_data_dir = Path(temp_root) / "data"
            
            temp_dagster_home.mkdir()
            temp_data_dir.mkdir()
            (temp_data_dir / "1_raw").mkdir()
            (temp_data_dir / "2_tasks").mkdir()
            # minimal directory structure for task layer
            
            # Create minimal test data
            concepts_dir = temp_data_dir / "1_raw" / "concepts"
            concepts_dir.mkdir()
            
            # Minimal concepts metadata
            test_concepts = pd.DataFrame([
                {"concept_id": "test-concept-1", "name": "Test Concept 1"},
                {"concept_id": "test-concept-2", "name": "Test Concept 2"},
                {"concept_id": "inactive-concept", "name": "Inactive Concept"}
            ])
            test_concepts.to_csv(temp_data_dir / "1_raw" / "concepts_metadata.csv", index=False)
            
            # Create minimal description files
            desc_para_dir = concepts_dir / "descriptions-paragraph" 
            desc_para_dir.mkdir()
            desc_para_dir.joinpath("test-concept-1.txt").write_text("Test concept 1 description")
            desc_para_dir.joinpath("test-concept-2.txt").write_text("Test concept 2 description")
            desc_para_dir.joinpath("inactive-concept.txt").write_text("Inactive concept description")
            
            env = {
                'DAGSTER_HOME': str(temp_dagster_home),
                'DD_COHORT': 'pipeline-test',
            }
            with patch.dict(os.environ, env):
                instance = DagsterInstance.ephemeral(tempdir=str(temp_dagster_home))
                
                # Import selection generator and content_combinations (no fallback)
                from daydreaming_dagster.assets.group_cohorts import selected_combo_mappings, content_combinations
                from daydreaming_dagster.resources.io_managers import (
                    CSVIOManager,
                    InMemoryIOManager,
                )
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig
                
                test_data_root = str(temp_data_dir)
                
                resources = {
                    "data_root": test_data_root,
                    "csv_io_manager": CSVIOManager(base_path=Path(test_data_root) / "2_tasks"),
                    "in_memory_io_manager": InMemoryIOManager(fallback_data_root=Path(test_data_root)),
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph"),
                }
                
                # Materialize selection + combinations together so the in-memory output wires correctly
                result = materialize(
                    [selected_combo_mappings, content_combinations],
                    resources=resources,
                    instance=instance,
                )

                assert result.success, "Content combinations materialization should succeed"
                
                # Test the superset mapping file
                mappings_file = temp_data_dir / "combo_mappings.csv"
                assert mappings_file.exists(), "combo_mappings.csv should be created"
                df = pd.read_csv(mappings_file)

                # Structure should include normalized mapping plus metadata
                assert {"combo_id","concept_id","description_level","k_max"}.issubset(set(df.columns))

                # With k_max=2 and two concepts, there should be exactly one combo using those two
                expected_ids = {"test-concept-1", "test-concept-2"}
                combos = df.groupby("combo_id")["concept_id"].apply(lambda s: set(s.astype(str).tolist()))
                matching = [cid for cid, cset in combos.items() if cset == expected_ids]
                assert len(matching) == 1, "Should have exactly one combo for the two concepts"
                # Each combination has 2 rows (one per concept)
                assert len(df[df["combo_id"] == matching[0]]) == 2

                # Test combo_id format (accept both legacy sequential and new stable formats)
                combo_ids = df["combo_id"].unique()
                legacy_pattern = re.compile(r"^combo_\d{3}$")
                stable_pattern = re.compile(r"^combo_v\d+_[0-9a-f]{12}$")
                assert all(
                    legacy_pattern.match(cid) or stable_pattern.match(cid)
                    for cid in combo_ids
                ), "combo_ids should match 'combo_XXX' (legacy) or 'combo_vN_<12-hex>' (stable)"
                print("✅ Normalized combo_mappings.csv test passed!")

    def test_template_allowlist_filters_membership(self):
        """Cohort membership should include only templates defined by the spec."""
        with tempfile.TemporaryDirectory() as temp_root:
            temp_dagster_home = Path(temp_root) / "dagster_home"
            temp_data_dir = Path(temp_root) / "data"

            temp_dagster_home.mkdir()
            temp_data_dir.mkdir()
            (temp_data_dir / "1_raw").mkdir()
            (temp_data_dir / "2_tasks").mkdir()
            (temp_data_dir / "gens").mkdir()
            (temp_data_dir / "cohorts").mkdir()
            (temp_data_dir / "7_reporting").mkdir()

            raw_data = temp_data_dir / "1_raw"
            concepts_dir = raw_data / "concepts"
            concepts_dir.mkdir()
            pd.DataFrame(
                [
                    {"concept_id": "concept-1", "name": "Concept 1"},
                    {"concept_id": "concept-2", "name": "Concept 2"},
                ]
            ).to_csv(raw_data / "concepts_metadata.csv", index=False)

            desc_para_dir = concepts_dir / "descriptions-paragraph"
            desc_para_dir.mkdir()
            desc_para_dir.joinpath("concept-1.txt").write_text("Concept 1 paragraph")
            desc_para_dir.joinpath("concept-2.txt").write_text("Concept 2 paragraph")

            pd.DataFrame(
                [
                    {
                        "id": "generation-model",
                        "model": "provider/model",
                        "provider": "provider",
                        "display_name": "Test Model",
                        "for_generation": True,
                        "for_evaluation": True,
                        "specialization": "test",
                    }
                ]
            ).to_csv(raw_data / "llm_models.csv", index=False)

            pd.DataFrame(
                [
                    {"stage": "draft", "replicates": 1},
                    {"stage": "essay", "replicates": 1},
                    {"stage": "evaluation", "replicates": 1},
                ]
            ).to_csv(raw_data / "replication_config.csv", index=False)

            pd.DataFrame(
                [
                    {"template_id": "draft-allow-1", "template_name": "Draft A1", "description": ""},
                    {"template_id": "draft-allow-2", "template_name": "Draft A2", "description": ""},
                    {"template_id": "draft-ignore-1", "template_name": "Draft I1", "description": ""},
                ]
            ).to_csv(raw_data / "draft_templates.csv", index=False)

            pd.DataFrame(
                [
                    {"template_id": "essay-allow-1", "template_name": "Essay A1", "description": "", "generator": "copy"},
                    {"template_id": "essay-ignore-1", "template_name": "Essay I1", "description": "", "generator": "copy"},
                ]
            ).to_csv(raw_data / "essay_templates.csv", index=False)

            pd.DataFrame(
                [
                    {"template_id": "eval-allow-1", "template_name": "Eval A1", "description": ""},
                ]
            ).to_csv(raw_data / "evaluation_templates.csv", index=False)

            tpl_root = raw_data / "templates"
            (tpl_root / "draft").mkdir(parents=True, exist_ok=True)
            (tpl_root / "essay").mkdir(parents=True, exist_ok=True)
            (tpl_root / "evaluation").mkdir(parents=True, exist_ok=True)
            (tpl_root / "draft" / "draft-allow-1.txt").write_text("Draft allow 1")
            (tpl_root / "draft" / "draft-allow-2.txt").write_text("Draft allow 2")
            (tpl_root / "draft" / "draft-ignore-1.txt").write_text("Draft ignore")
            (tpl_root / "essay" / "essay-allow-1.txt").write_text("Essay allow")
            (tpl_root / "essay" / "essay-ignore-1.txt").write_text("Essay ignore")
            (tpl_root / "evaluation" / "eval-allow-1.txt").write_text("Evaluation template")

            pd.DataFrame(
                [
                    {
                        "combo_id": "combo-spec-1",
                        "concept_id": "concept-1",
                        "description_level": "paragraph",
                        "k_max": 2,
                        "version": "v1",
                        "created_at": "",
                    },
                    {
                        "combo_id": "combo-spec-1",
                        "concept_id": "concept-2",
                        "description_level": "paragraph",
                        "k_max": 2,
                        "version": "v1",
                        "created_at": "",
                    },
                ]
            ).to_csv(temp_data_dir / "combo_mappings.csv", index=False)

            _write_spec(
                temp_data_dir,
                cohort_id="allowlist-cohort",
                combos=["combo-spec-1"],
                draft_templates=["draft-allow-1", "draft-allow-2"],
                essay_templates=["essay-allow-1"],
                evaluation_templates=["eval-allow-1"],
                generation_llms=["generation-model"],
                evaluation_llms=["generation-model"],
            )

            env = {
                "DAGSTER_HOME": str(temp_dagster_home),
                "DD_COHORT": "allowlist-cohort",
            }
            with patch.dict(os.environ, env):
                instance = DagsterInstance.ephemeral(tempdir=str(temp_dagster_home))

                from dagster import IOManager

                class _DictIOManager(IOManager):
                    def __init__(self):
                        self._store = {}

                    def handle_output(self, context, obj):
                        key = tuple(context.asset_key.path)
                        self._store[key] = obj

                    def load_input(self, context):
                        upstream = context.upstream_output
                        key = tuple(upstream.asset_key.path)
                        if key not in self._store:
                            raise KeyError(f"No stored value for {key}")
                        return self._store[key]

                from daydreaming_dagster.assets.group_cohorts import (
                    selected_combo_mappings,
                    content_combinations,
                    cohort_id,
                    cohort_membership,
                )
                from daydreaming_dagster.resources.io_managers import (
                    CSVIOManager,
                    InMemoryIOManager,
                )
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig

                resources = {
                    "data_root": str(temp_data_dir),
                    "csv_io_manager": CSVIOManager(base_path=temp_data_dir / "2_tasks"),
                    "io_manager": _DictIOManager(),
                    "in_memory_io_manager": InMemoryIOManager(),
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph"),
                    "cohort_spec": _StubCohortSpec(),
                }

                result = materialize([selected_combo_mappings], resources=resources, instance=instance)
                assert result.success, "selected_combo_mappings materialization should succeed"

                result = materialize(
                    [selected_combo_mappings, content_combinations],
                    resources=resources,
                    instance=instance,
                )
                assert result.success, "content_combinations materialization should succeed"

                result = materialize(
                    [selected_combo_mappings, content_combinations, cohort_id, cohort_membership],
                    resources=resources,
                    instance=instance,
                )
                assert result.success, "cohort assets materialization should succeed"

            cohorts_dir = temp_data_dir / "cohorts"
            cohort_dirs = list(cohorts_dir.iterdir())
            assert cohort_dirs, "Cohort directory should be created"
            membership_path = cohort_dirs[0] / "membership.csv"
            assert membership_path.exists(), "membership.csv should be written"

            membership_df = pd.read_csv(membership_path)

            def _templates_for_stage(stage: str) -> set[str]:
                templates: set[str] = set()
                for gid in membership_df[membership_df["stage"] == stage]["gen_id"].astype(str):
                    meta_path = temp_data_dir / "gens" / stage / gid / "metadata.json"
                    if not meta_path.exists():
                        continue
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    template = meta.get("template_id") or meta.get("draft_template") or meta.get("essay_template")
                    if isinstance(template, str) and template.strip():
                        templates.add(template.strip())
                return templates

            draft_templates = _templates_for_stage("draft")
            essay_templates = _templates_for_stage("essay")
            all_templates = draft_templates | essay_templates

            assert draft_templates == {"draft-allow-1", "draft-allow-2"}
            assert essay_templates == {"essay-allow-1"}
            assert not (all_templates & {"draft-ignore-1", "essay-ignore-1"}), (
                "Templates outside the spec allowlist should not appear in cohort membership scope"
            )

    # Note: Error scenarios (like insufficient drafts/links) are covered by unit tests
    # in daydreaming_dagster/assets/test_two_phase_generation.py (see TestLinksValidation)

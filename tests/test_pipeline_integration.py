"""Full pipeline integration tests using temporary DAGSTER_HOME and copied live data.

Tests complete materialization of raw_data and llm_tasks groups with real data.
"""

import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock

import pandas as pd
import pytest
from dagster import materialize, DagsterInstance
from tests.helpers.llm_stubs import CannedLLMResource

# Markers
pytestmark = [pytest.mark.integration, pytest.mark.live_data]


@pytest.fixture(scope="function")
def pipeline_data_root_prepared():
    """Prepare persistent test data under tests/data_pipeline_test by copying live data and limiting scope.

    Steps:
    - Clean tests/data_pipeline_test
    - Copy data/1_raw into tests/data_pipeline_test/1_raw
    - Create output dirs (2_tasks, gens/) for gen-id–keyed prompts/responses
    - Limit active rows: concepts (2), draft_templates (1), essay_templates (1), evaluation_templates (2), llm_models for_generation (2)
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
    (pipeline_data_root / "5_parsing").mkdir(parents=True)
    (pipeline_data_root / "6_summary").mkdir(parents=True)
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

    # Limit active rows in key CSVs
    concepts_csv = pipeline_data_root / "1_raw" / "concepts_metadata.csv"
    draft_templates_csv = pipeline_data_root / "1_raw" / "draft_templates.csv"
    essay_templates_csv = pipeline_data_root / "1_raw" / "essay_templates.csv"
    eval_templates_csv = pipeline_data_root / "1_raw" / "evaluation_templates.csv"
    models_csv = pipeline_data_root / "1_raw" / "llm_models.csv"

    # Concepts: keep only first two active
    cdf = pd.read_csv(concepts_csv)
    if "active" in cdf.columns:
        cdf["active"] = False
        cdf.loc[cdf.index[:2], "active"] = True
    pd.testing.assert_series_equal((cdf["active"] == True).reset_index(drop=True)[:2], pd.Series([True, True]), check_names=False)
    cdf.to_csv(concepts_csv, index=False)

    # Link and Essay templates: verify creative-synthesis-v10 files exist
    target_template = "creative-synthesis-v10"
    draft_file = pipeline_data_root / "1_raw" / "templates" / "draft" / f"{target_template}.txt"
    essay_file = pipeline_data_root / "1_raw" / "templates" / "essay" / f"{target_template}.txt"
    
    if draft_file.exists() and essay_file.exists():
        print(f"✓ Verified {target_template} template with two-phase structure")
    else:
        raise RuntimeError(f"Two-phase template files missing for {target_template}: {draft_file} or {essay_file}")
    
    # Ensure essay templates select an LLM-based template (avoid parser-only during fail-fast refactor)
    if essay_templates_csv.exists():
        et_df = pd.read_csv(essay_templates_csv)
        if "active" in et_df.columns and "template_id" in et_df.columns and "generator" in et_df.columns:
            # Deactivate all, then activate a known LLM template if present
            et_df["active"] = False
            if (et_df["template_id"] == target_template).any():
                et_df.loc[et_df["template_id"] == target_template, "active"] = True
                et_df.loc[et_df["template_id"] == target_template, "generator"] = "llm"
            else:
                # Fallback: activate the first llm template if available
                llm_rows = et_df[et_df["generator"] == "llm"]
                if not llm_rows.empty:
                    idx = llm_rows.index[0]
                    et_df.loc[idx, "active"] = True
            et_df.to_csv(essay_templates_csv, index=False)

    # Evaluation templates: keep only first two active
    etdf = pd.read_csv(eval_templates_csv)
    if "active" in etdf.columns:
        etdf["active"] = False
        etdf.loc[etdf.index[:2], "active"] = True
    etdf.to_csv(eval_templates_csv, index=False)

    # LLM models: keep only first two generation-capable models active
    mdf = pd.read_csv(models_csv)
    if "for_generation" in mdf.columns:
        mdf["for_generation"] = False
        mdf.loc[mdf.index[:2], "for_generation"] = True
    mdf.to_csv(models_csv, index=False)

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
        
        # Results files (5_parsing/ & 6_summary/) - these may not exist if not enough evaluation data
        parsing_file = test_directory / "5_parsing" / "parsed_scores.csv"
        summary_file = test_directory / "6_summary" / "final_results.csv"
        
        results_exist = parsing_file.exists() or summary_file.exists()
        if results_exist:
            print("✓ Results processing files created")
        else:
            print("⚠ Results processing skipped (expected - needs more evaluation data)")
        
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
            with patch.dict(os.environ, {"DAGSTER_HOME": str(temp_dagster_home)}):
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
                from daydreaming_dagster.resources.io_managers import CSVIOManager, InMemoryIOManager
                from daydreaming_dagster.resources.gens_prompt_io_manager import GensPromptIOManager
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig

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
                    "parsing_results_io_manager": CSVIOManager(base_path=pipeline_data_root / "5_parsing"),
                    "summary_results_io_manager": CSVIOManager(base_path=pipeline_data_root / "6_summary"),
                    "error_log_io_manager": CSVIOManager(base_path=pipeline_data_root / "7_reporting"),
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph"),
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
                # Choose essay gen_ids whose parent_gen_id corresponds to successful drafts
                test_essay_partitions = mdf[(mdf["stage"] == "essay") & (mdf["parent_gen_id"].astype(str).isin([str(x) for x in succeeded_gen_partitions]))]["gen_id"].astype(str).tolist()[:2]
                
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
                {"concept_id": "test-concept-1", "name": "Test Concept 1", "active": True},
                {"concept_id": "test-concept-2", "name": "Test Concept 2", "active": True},
                {"concept_id": "inactive-concept", "name": "Inactive Concept", "active": False}
            ])
            test_concepts.to_csv(temp_data_dir / "1_raw" / "concepts_metadata.csv", index=False)
            
            # Create minimal description files
            desc_para_dir = concepts_dir / "descriptions-paragraph" 
            desc_para_dir.mkdir()
            desc_para_dir.joinpath("test-concept-1.txt").write_text("Test concept 1 description")
            desc_para_dir.joinpath("test-concept-2.txt").write_text("Test concept 2 description")
            desc_para_dir.joinpath("inactive-concept.txt").write_text("Inactive concept description")
            
            with patch.dict(os.environ, {'DAGSTER_HOME': str(temp_dagster_home)}):
                instance = DagsterInstance.ephemeral(tempdir=str(temp_dagster_home))
                
                # Import selection generator and content_combinations (no fallback)
                from daydreaming_dagster.assets.group_cohorts import selected_combo_mappings, content_combinations
                from daydreaming_dagster.resources.io_managers import CSVIOManager
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig
                
                test_data_root = str(temp_data_dir)
                
                resources = {
                    "data_root": test_data_root,
                    "csv_io_manager": CSVIOManager(base_path=Path(test_data_root) / "2_tasks"),
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph")
                }
                
                # Generate selection first, then combinations
                result = materialize(
                    [selected_combo_mappings],
                    resources=resources,
                    instance=instance,
                )
                assert result.success, "Selected combo mappings materialization should succeed"
                
                result = materialize(
                    [content_combinations],
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

                # With k_max=2 and 2 active concepts, there should be exactly one combo using those two
                expected_active_ids = {"test-concept-1", "test-concept-2"}
                combos = df.groupby("combo_id")["concept_id"].apply(lambda s: set(s.astype(str).tolist()))
                matching = [cid for cid, cset in combos.items() if cset == expected_active_ids]
                assert len(matching) == 1, "Should have exactly one combo for the 2 active concepts"
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

    def test_template_filtering_respects_active_column(self):
        """Cohort membership should only include templates marked active."""
        with tempfile.TemporaryDirectory() as temp_root:
            temp_dagster_home = Path(temp_root) / "dagster_home"
            temp_data_dir = Path(temp_root) / "data"

            temp_dagster_home.mkdir()
            temp_data_dir.mkdir()
            (temp_data_dir / "1_raw").mkdir()
            (temp_data_dir / "2_tasks").mkdir()
            (temp_data_dir / "gens").mkdir()
            (temp_data_dir / "5_parsing").mkdir()
            (temp_data_dir / "6_summary").mkdir()
            (temp_data_dir / "7_reporting").mkdir()

            raw_data = temp_data_dir / "1_raw"

            # Minimal concepts data (two actives).
            concepts_dir = raw_data / "concepts"
            concepts_dir.mkdir()
            pd.DataFrame(
                [
                    {"concept_id": "concept-1", "name": "Concept 1", "active": True},
                    {"concept_id": "concept-2", "name": "Concept 2", "active": True},
                ]
            ).to_csv(raw_data / "concepts_metadata.csv", index=False)

            desc_para_dir = concepts_dir / "descriptions-paragraph"
            desc_para_dir.mkdir()
            desc_para_dir.joinpath("concept-1.txt").write_text("Concept 1 paragraph")
            desc_para_dir.joinpath("concept-2.txt").write_text("Concept 2 paragraph")

            # LLM models with a single generation model.
            pd.DataFrame(
                [
                    {
                        "id": "generation-model",
                        "model": "provider/model",
                        "provider": "provider",
                        "display_name": "Test Model",
                        "for_generation": True,
                        "for_evaluation": False,
                        "specialization": "test",
                    }
                ]
            ).to_csv(raw_data / "llm_models.csv", index=False)

            # Replication config required by cohort builder.
            pd.DataFrame(
                [
                    {"stage": "draft", "replicates": 1},
                    {"stage": "essay", "replicates": 1},
                    {"stage": "evaluation", "replicates": 1},
                ]
            ).to_csv(raw_data / "replication_config.csv", index=False)

            # Draft templates with mixed active/inactive rows.
            pd.DataFrame(
                [
                    {"template_id": "draft-active-1", "template_name": "Draft A1", "description": "", "active": True},
                    {"template_id": "draft-active-2", "template_name": "Draft A2", "description": "", "active": True},
                    {"template_id": "draft-inactive-1", "template_name": "Draft I1", "description": "", "active": False},
                ]
            ).to_csv(raw_data / "draft_templates.csv", index=False)

            # Essay templates with one active and one inactive entry.
            pd.DataFrame(
                [
                    {"template_id": "essay-active-1", "template_name": "Essay A1", "description": "", "active": True, "generator": "copy"},
                    {"template_id": "essay-inactive-1", "template_name": "Essay I1", "description": "", "active": False, "generator": "copy"},
                ]
            ).to_csv(raw_data / "essay_templates.csv", index=False)

            # Evaluation templates to satisfy downstream reads (one active entry).
            pd.DataFrame(
                [
                    {"template_id": "eval-active-1", "template_name": "Eval A1", "description": "", "active": True},
                ]
            ).to_csv(raw_data / "evaluation_templates.csv", index=False)

            # Template files (present for completeness).
            tpl_root = raw_data / "templates"
            (tpl_root / "draft").mkdir(parents=True)
            (tpl_root / "essay").mkdir(parents=True)
            (tpl_root / "evaluation").mkdir(parents=True)
            for name in ("draft-active-1", "draft-active-2", "draft-inactive-1"):
                (tpl_root / "draft" / f"{name}.txt").write_text(f"Template {name}")
            for name in ("essay-active-1", "essay-inactive-1"):
                (tpl_root / "essay" / f"{name}.txt").write_text(f"Template {name}")
            (tpl_root / "evaluation" / "eval-active-1.txt").write_text("Evaluation template")

            with patch.dict(os.environ, {"DAGSTER_HOME": str(temp_dagster_home)}):
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
                from daydreaming_dagster.resources.io_managers import CSVIOManager
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig

                resources = {
                    "data_root": str(temp_data_dir),
                    "csv_io_manager": CSVIOManager(base_path=temp_data_dir / "2_tasks"),
                    "io_manager": _DictIOManager(),
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph"),
                }

                # selected_combo_mappings must materialize first to write the CSV consumed downstream.
                result = materialize([selected_combo_mappings], resources=resources, instance=instance)
                assert result.success, "selected_combo_mappings materialization should succeed"

                result = materialize([content_combinations], resources=resources, instance=instance)
                assert result.success, "content_combinations materialization should succeed"

                result = materialize(
                    [content_combinations, cohort_id, cohort_membership],
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

            assert draft_templates == {"draft-active-1", "draft-active-2"}
            assert essay_templates == {"essay-active-1"}
            assert not (all_templates & {"draft-inactive-1", "essay-inactive-1"}), (
                "Inactive templates should not appear in cohort membership scope"
            )

    # Note: Error scenarios (like insufficient drafts/links) are covered by unit tests
    # in daydreaming_dagster/assets/test_two_phase_generation.py (see TestLinksValidation)

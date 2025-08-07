"""Full pipeline integration tests using temporary DAGSTER_HOME and copied live data.

Tests complete materialization of raw_data and llm_tasks groups with real data.
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import shutil
import os
from unittest.mock import patch

from dagster import materialize, DagsterInstance


class TestPipelineIntegration:
    """Full pipeline integration test with temporary DAGSTER_HOME."""
    
    def test_full_pipeline_materialization_with_live_data(self):
        """Test complete pipeline materialization using copied live data in temporary environment."""
        # Create temporary directories for isolated test
        with tempfile.TemporaryDirectory() as temp_root:
            temp_dagster_home = Path(temp_root) / "dagster_home"
            temp_data_dir = Path(temp_root) / "data"
            
            # Set up directory structure
            temp_dagster_home.mkdir()
            temp_data_dir.mkdir()
            (temp_data_dir / "1_raw").mkdir()
            (temp_data_dir / "2_tasks").mkdir()
            
            # Copy live data to temporary location
            live_data_path = Path("data/1_raw")
            temp_raw_data = temp_data_dir / "1_raw"
            
            # Must fail if live data doesn't exist (per project guidelines)
            assert live_data_path.exists(), f"Live data directory not found: {live_data_path}"
            
            # Copy all live data subdirectories
            for item in live_data_path.iterdir():
                if item.is_dir():
                    shutil.copytree(item, temp_raw_data / item.name)
                else:
                    shutil.copy2(item, temp_raw_data / item.name)
            
            # Patch environment to use temporary paths
            with patch.dict(os.environ, {
                'DAGSTER_HOME': str(temp_dagster_home)
            }):
                # Create temporary Dagster instance
                instance = DagsterInstance.ephemeral(tempdir=str(temp_dagster_home))
                
                # Import definitions - need to patch paths to use temp data
                from daydreaming_dagster.definitions import defs
                
                # Create test-specific resources with temporary data root
                from daydreaming_dagster.resources.io_managers import CSVIOManager, PartitionedTextIOManager
                
                test_data_root = str(temp_data_dir)
                
                # Override only the resources that need different paths for testing
                patched_resources = {
                    # Keep existing resources that don't need path changes
                    "openrouter_client": defs.resources["openrouter_client"],
                    "experiment_config": defs.resources["experiment_config"],
                    
                    # Override data root for testing
                    "data_root": test_data_root,
                    
                    # Recreate simplified I/O managers with test data paths
                    "csv_io_manager": CSVIOManager(base_path=Path(test_data_root) / "2_tasks"),
                    "generation_prompt_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "3_generation" / "generation_prompts"),
                    "generation_response_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "3_generation" / "generation_responses"),
                    "evaluation_prompt_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "4_evaluation" / "evaluation_prompts"),
                    "evaluation_response_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "4_evaluation" / "evaluation_responses"),
                    "error_log_io_manager": CSVIOManager(base_path=Path(test_data_root) / "7_reporting"),
                    "parsing_results_io_manager": CSVIOManager(base_path=Path(test_data_root) / "5_parsing"),
                    "summary_results_io_manager": CSVIOManager(base_path=Path(test_data_root) / "6_summary")
                }
                
                # Import the assets we want to test
                from daydreaming_dagster.assets.raw_data import (
                    concepts, llm_models, 
                    generation_templates, evaluation_templates
                )
                from daydreaming_dagster.assets.core import (
                    content_combinations, content_combinations_csv, 
                    generation_tasks, evaluation_tasks
                )
                
                # Materialize all assets in correct dependency order
                all_assets = [
                    # Raw data assets
                    concepts, llm_models,
                    generation_templates, evaluation_templates,
                    # LLM task assets
                    content_combinations, content_combinations_csv,
                    generation_tasks, evaluation_tasks
                ]
                
                result = materialize(
                    all_assets,
                    resources=patched_resources,
                    instance=instance
                )
                
                assert result.success, "Full pipeline materialization should succeed"
                
                # Verify no empty files were created (our IO manager fix)
                task_dir = temp_data_dir / "2_tasks"
                empty_files = []
                for file_path in task_dir.rglob("*"):
                    if file_path.is_file() and file_path.stat().st_size <= 2:  # Empty or just whitespace
                        empty_files.append(file_path)
                
                assert len(empty_files) == 0, f"Found empty files that should have been skipped: {empty_files}"
                
                # Verify expected files are created
                expected_files = [
                    "generation_tasks.csv",
                    "evaluation_tasks.csv", 
                    "content_combinations_csv.csv"  # New normalized table
                ]
                
                for expected_file in expected_files:
                    file_path = task_dir / expected_file
                    assert file_path.exists(), f"Expected file not found: {expected_file}"
                    assert file_path.stat().st_size > 10, f"File appears to be empty: {expected_file}"
                
                # Test the new content_combinations_csv structure
                combinations_csv = pd.read_csv(task_dir / "content_combinations_csv.csv")
                
                # Verify normalized table structure
                assert "combo_id" in combinations_csv.columns, "Should have combo_id column"
                assert "concept_id" in combinations_csv.columns, "Should have concept_id column" 
                assert len(combinations_csv.columns) == 2, "Should have exactly 2 columns (normalized)"
                
                # Verify data integrity
                assert len(combinations_csv) > 0, "Should have combination rows"
                
                # Check that combo_ids follow expected format
                combo_ids = combinations_csv["combo_id"].unique()
                assert all(cid.startswith("combo_") for cid in combo_ids), "All combo_ids should start with 'combo_'"
                
                # Verify concept_ids match active concepts from live data
                active_concepts_df = pd.read_csv(temp_raw_data / "concepts" / "concepts_metadata.csv")
                expected_active_concept_ids = set(active_concepts_df[active_concepts_df["active"] == True]["concept_id"])
                actual_concept_ids = set(combinations_csv["concept_id"].unique())
                
                assert actual_concept_ids.issubset(expected_active_concept_ids), \
                    "All concept_ids in combinations should be from active concepts"
                
                # Test generation_tasks.csv structure
                gen_tasks_csv = pd.read_csv(task_dir / "generation_tasks.csv")
                assert "generation_task_id" in gen_tasks_csv.columns, "Should have generation_task_id column"
                assert "combo_id" in gen_tasks_csv.columns, "Should have combo_id column"
                assert "generation_template" in gen_tasks_csv.columns, "Should have generation_template column"
                assert "generation_model" in gen_tasks_csv.columns, "Should have generation_model column"
                
                # Verify our new essay-inventive-synthesis template is included
                templates_used = set(gen_tasks_csv["generation_template"].unique())
                assert "essay-inventive-synthesis" in templates_used, \
                    "New essay-inventive-synthesis template should be used in generation tasks"
                
                # Test template filtering: verify only active templates are used
                # Load generation template metadata to check which templates should be active
                gen_templates_metadata = pd.read_csv(temp_raw_data / "generation_templates.csv")
                expected_active_templates = set(gen_templates_metadata[gen_templates_metadata["active"] == True]["template_id"].tolist())
                
                # Verify only active templates are used in generation tasks
                assert templates_used == expected_active_templates, \
                    f"Only active templates should be used. Expected: {expected_active_templates}, Got: {templates_used}"
                
                # Verify template filtering reduces task count as expected
                all_templates_count = len(gen_templates_metadata)
                active_templates_count = len(expected_active_templates)
                combinations_count = len(combinations_csv["combo_id"].unique())
                
                # Load LLM models to calculate expected task count
                llm_models_df = pd.read_csv(temp_raw_data / "llm_models.csv")
                generation_models_count = len(llm_models_df[llm_models_df["for_generation"] == True])
                
                expected_task_count = combinations_count * active_templates_count * generation_models_count
                actual_task_count = len(gen_tasks_csv)
                
                assert actual_task_count == expected_task_count, \
                    f"Task count should match active templates. Expected: {expected_task_count} " \
                    f"({combinations_count} combos × {active_templates_count} active templates × {generation_models_count} models), " \
                    f"Got: {actual_task_count}"
                
                print(f"   Template filtering: {active_templates_count}/{all_templates_count} templates active")
                
                # Test evaluation_tasks.csv structure  
                eval_tasks_csv = pd.read_csv(task_dir / "evaluation_tasks.csv")
                assert "evaluation_task_id" in eval_tasks_csv.columns, "Should have evaluation_task_id column"
                assert "generation_task_id" in eval_tasks_csv.columns, "Should have generation_task_id column"
                
                # Test metadata consistency
                # Number of combinations should match across files
                unique_combos_in_combinations = len(combinations_csv["combo_id"].unique())
                unique_combos_in_gen_tasks = len(gen_tasks_csv["combo_id"].unique()) 
                
                assert unique_combos_in_combinations == unique_combos_in_gen_tasks, \
                    "Same combo_ids should appear in both content_combinations_csv and generation_tasks"
                
                print(f"✅ Integration test passed!")
                print(f"   Generated {len(combinations_csv)} combination rows")
                print(f"   Generated {len(gen_tasks_csv)} generation tasks") 
                print(f"   Generated {len(eval_tasks_csv)} evaluation tasks")
                print(f"   Used {len(templates_used)} templates including essay-inventive-synthesis")
                print(f"   Active concepts: {len(actual_concept_ids)}")
    
    def test_content_combinations_csv_normalized_structure(self):
        """Specific test for the normalized content_combinations_csv structure."""
        # Use a simplified test with minimal data
        with tempfile.TemporaryDirectory() as temp_root:
            temp_dagster_home = Path(temp_root) / "dagster_home" 
            temp_data_dir = Path(temp_root) / "data"
            
            temp_dagster_home.mkdir()
            temp_data_dir.mkdir()
            (temp_data_dir / "1_raw").mkdir()
            (temp_data_dir / "2_tasks").mkdir()
            
            # Create minimal test data
            concepts_dir = temp_data_dir / "1_raw" / "concepts"
            concepts_dir.mkdir()
            
            # Minimal concepts metadata
            test_concepts = pd.DataFrame([
                {"concept_id": "test-concept-1", "name": "Test Concept 1", "active": True},
                {"concept_id": "test-concept-2", "name": "Test Concept 2", "active": True},
                {"concept_id": "inactive-concept", "name": "Inactive Concept", "active": False}
            ])
            test_concepts.to_csv(concepts_dir / "concepts_metadata.csv", index=False)
            
            # Create minimal description files
            desc_para_dir = concepts_dir / "descriptions-paragraph" 
            desc_para_dir.mkdir()
            desc_para_dir.joinpath("test-concept-1.txt").write_text("Test concept 1 description")
            desc_para_dir.joinpath("test-concept-2.txt").write_text("Test concept 2 description")
            desc_para_dir.joinpath("inactive-concept.txt").write_text("Inactive concept description")
            
            with patch.dict(os.environ, {'DAGSTER_HOME': str(temp_dagster_home)}):
                instance = DagsterInstance.ephemeral(tempdir=str(temp_dagster_home))
                
                # Import and test just the content_combinations_csv asset
                from daydreaming_dagster.assets.raw_data import concepts
                from daydreaming_dagster.assets.core import content_combinations, content_combinations_csv
                from daydreaming_dagster.resources.io_managers import CSVIOManager
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig
                
                test_data_root = str(temp_data_dir)
                
                resources = {
                    "data_root": test_data_root,
                    "csv_io_manager": CSVIOManager(base_path=Path(test_data_root) / "2_tasks"),
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph")
                }
                
                result = materialize(
                    [concepts, content_combinations, content_combinations_csv],
                    resources=resources,
                    instance=instance
                )
                
                assert result.success, "Content combinations materialization should succeed"
                
                # Test the normalized table
                combinations_file = temp_data_dir / "2_tasks" / "content_combinations_csv.csv"
                assert combinations_file.exists(), "content_combinations_csv.csv should be created"
                
                df = pd.read_csv(combinations_file)
                
                # Test exact structure
                assert list(df.columns) == ["combo_id", "concept_id"], \
                    "Should have exactly 2 columns: combo_id, concept_id"
                
                # Test data relationships - with k_max=2 and 2 active concepts, should have 1 combination  
                # Each combination has 2 rows (one per concept)
                assert len(df) == 2, "Should have 2 rows for 1 combination of 2 concepts"
                assert len(df["combo_id"].unique()) == 1, "Should have 1 unique combination"
                
                # Test only active concepts are included
                concept_ids = set(df["concept_id"].unique())
                expected_active_ids = {"test-concept-1", "test-concept-2"}
                assert concept_ids == expected_active_ids, "Should only include active concepts"
                
                # Test combo_id format
                combo_ids = df["combo_id"].unique()
                assert all(cid.startswith("combo_") and len(cid) == 9 for cid in combo_ids), \
                    "combo_ids should follow format combo_XXX"
                
                print("✅ Normalized content_combinations_csv test passed!")

    def test_template_filtering_respects_active_column(self):
        """Specific test for template filtering based on active column in metadata CSV."""
        with tempfile.TemporaryDirectory() as temp_root:
            temp_dagster_home = Path(temp_root) / "dagster_home"
            temp_data_dir = Path(temp_root) / "data"
            
            temp_dagster_home.mkdir()
            temp_data_dir.mkdir()
            (temp_data_dir / "1_raw").mkdir()
            (temp_data_dir / "2_tasks").mkdir()
            
            # Create test data with explicit template filtering scenario
            raw_data = temp_data_dir / "1_raw"
            
            # Create minimal concepts data
            concepts_dir = raw_data / "concepts"
            concepts_dir.mkdir()
            
            test_concepts = pd.DataFrame([
                {"concept_id": "test-concept-1", "name": "Test Concept 1", "active": True},
                {"concept_id": "test-concept-2", "name": "Test Concept 2", "active": True},
            ])
            test_concepts.to_csv(concepts_dir / "concepts_metadata.csv", index=False)
            
            desc_para_dir = concepts_dir / "descriptions-paragraph"
            desc_para_dir.mkdir()
            desc_para_dir.joinpath("test-concept-1.txt").write_text("Test concept 1 description")
            desc_para_dir.joinpath("test-concept-2.txt").write_text("Test concept 2 description")
            
            # Create LLM models data (1 generation model for simplicity)
            test_models = pd.DataFrame([
                {"id": "test_gen_model", "model": "test/model", "provider": "test", "display_name": "Test Model", "for_generation": True, "for_evaluation": False, "specialization": "test"}
            ])
            test_models.to_csv(raw_data / "llm_models.csv", index=False)
            
            # Create generation templates metadata with mixed active/inactive
            test_gen_templates_metadata = pd.DataFrame([
                {"template_id": "active-template-1", "template_name": "Active Template 1", "description": "Test active template 1", "active": True},
                {"template_id": "active-template-2", "template_name": "Active Template 2", "description": "Test active template 2", "active": True},
                {"template_id": "inactive-template-1", "template_name": "Inactive Template 1", "description": "Test inactive template", "active": False},
                {"template_id": "inactive-template-2", "template_name": "Inactive Template 2", "description": "Test inactive template", "active": False},
            ])
            test_gen_templates_metadata.to_csv(raw_data / "generation_templates.csv", index=False)
            
            # Create actual template files (all of them, including inactive ones)
            gen_templates_dir = raw_data / "generation_templates"
            gen_templates_dir.mkdir()
            gen_templates_dir.joinpath("active-template-1.txt").write_text("Active template 1 content")
            gen_templates_dir.joinpath("active-template-2.txt").write_text("Active template 2 content")  
            gen_templates_dir.joinpath("inactive-template-1.txt").write_text("Inactive template 1 content")
            gen_templates_dir.joinpath("inactive-template-2.txt").write_text("Inactive template 2 content")
            
            with patch.dict(os.environ, {'DAGSTER_HOME': str(temp_dagster_home)}):
                instance = DagsterInstance.ephemeral(tempdir=str(temp_dagster_home))
                
                from daydreaming_dagster.assets.raw_data import (
                    concepts, llm_models, 
                    generation_templates
                )
                from daydreaming_dagster.assets.core import (
                    content_combinations, generation_tasks
                )
                from daydreaming_dagster.resources.io_managers import CSVIOManager
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig
                
                test_data_root = str(temp_data_dir)
                
                resources = {
                    "data_root": test_data_root,
                    "csv_io_manager": CSVIOManager(base_path=Path(test_data_root) / "2_tasks"),
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph")
                }
                
                result = materialize([
                    concepts, llm_models,
                    generation_templates, content_combinations, generation_tasks
                ], resources=resources, instance=instance)
                
                assert result.success, "Template filtering test materialization should succeed"
                
                # Load results and verify filtering
                gen_tasks_file = temp_data_dir / "2_tasks" / "generation_tasks.csv"
                assert gen_tasks_file.exists(), "generation_tasks.csv should be created"
                
                gen_tasks_df = pd.read_csv(gen_tasks_file)
                
                # Check that only active templates are used
                templates_used = set(gen_tasks_df["generation_template"].unique())
                expected_active_templates = {"active-template-1", "active-template-2"}
                
                assert templates_used == expected_active_templates, \
                    f"Only active templates should be used. Expected: {expected_active_templates}, Got: {templates_used}"
                
                # Verify expected task count:
                # 1 concept combination (k_max=2, we have 2 concepts) × 2 active templates × 1 model = 2 tasks
                expected_task_count = 1 * 2 * 1  
                actual_task_count = len(gen_tasks_df)
                
                assert actual_task_count == expected_task_count, \
                    f"Expected {expected_task_count} tasks (1 combo × 2 active templates × 1 model), got {actual_task_count}"
                
                # Verify inactive templates are not used
                assert "inactive-template-1" not in templates_used, "Inactive templates should not be used"
                assert "inactive-template-2" not in templates_used, "Inactive templates should not be used"
                
                print(f"✅ Template filtering test passed!")
                print(f"   Used {len(templates_used)} active templates out of {len(test_gen_templates_metadata)} total")
                print(f"   Generated {actual_task_count} tasks as expected")

    def test_prompt_generation_with_consolidated_templates(self):
        """Test that prompt generation works correctly with consolidated template DataFrames."""
        with tempfile.TemporaryDirectory() as temp_root:
            temp_dagster_home = Path(temp_root) / "dagster_home"
            temp_data_dir = Path(temp_root) / "data"
            
            temp_dagster_home.mkdir()
            temp_data_dir.mkdir()
            (temp_data_dir / "1_raw").mkdir()
            (temp_data_dir / "2_tasks").mkdir()
            (temp_data_dir / "3_generation").mkdir()
            (temp_data_dir / "3_generation" / "generation_prompts").mkdir()
            (temp_data_dir / "4_evaluation").mkdir()
            (temp_data_dir / "4_evaluation" / "evaluation_prompts").mkdir()
            
            # Create test data
            raw_data = temp_data_dir / "1_raw"
            
            # Create minimal concepts data
            concepts_dir = raw_data / "concepts"
            concepts_dir.mkdir()
            
            test_concepts = pd.DataFrame([
                {"concept_id": "test-concept-1", "name": "Test Concept 1", "active": True},
                {"concept_id": "test-concept-2", "name": "Test Concept 2", "active": True},
            ])
            test_concepts.to_csv(concepts_dir / "concepts_metadata.csv", index=False)
            
            desc_para_dir = concepts_dir / "descriptions-paragraph"
            desc_para_dir.mkdir()
            desc_para_dir.joinpath("test-concept-1.txt").write_text("Test concept 1 description for daydreaming")
            desc_para_dir.joinpath("test-concept-2.txt").write_text("Test concept 2 description for daydreaming")
            
            # Create LLM models data
            test_models = pd.DataFrame([
                {"id": "test_gen_model", "model": "test/generation-model", "provider": "test", "display_name": "Test Gen Model", "for_generation": True, "for_evaluation": False, "specialization": "test"},
                {"id": "test_eval_model", "model": "test/evaluation-model", "provider": "test", "display_name": "Test Eval Model", "for_generation": False, "for_evaluation": True, "specialization": "test"}
            ])
            test_models.to_csv(raw_data / "llm_models.csv", index=False)
            
            # Create generation templates with Jinja2 template content
            test_gen_templates_metadata = pd.DataFrame([
                {"template_id": "test-generation-template", "template_name": "Test Generation Template", "description": "Test template for generation", "active": True}
            ])
            test_gen_templates_metadata.to_csv(raw_data / "generation_templates.csv", index=False)
            
            gen_templates_dir = raw_data / "generation_templates"
            gen_templates_dir.mkdir()
            # Template with Jinja2 syntax that should render concepts
            template_content = """Generate creative ideas combining these concepts:

{% for concept in concepts %}
**{{ concept.name }}**: {{ concept.content }}
{% endfor %}

Please create an innovative synthesis."""
            gen_templates_dir.joinpath("test-generation-template.txt").write_text(template_content)
            
            # Create evaluation templates
            test_eval_templates_metadata = pd.DataFrame([
                {"template_id": "test-evaluation-template", "template_name": "Test Evaluation Template", "description": "Test template for evaluation", "active": True}
            ])
            test_eval_templates_metadata.to_csv(raw_data / "evaluation_templates.csv", index=False)
            
            eval_templates_dir = raw_data / "evaluation_templates"
            eval_templates_dir.mkdir()
            eval_content = """Rate this response on a scale of 1-10:

{{ generation_response }}

SCORE: """
            eval_templates_dir.joinpath("test-evaluation-template.txt").write_text(eval_content)
            
            with patch.dict(os.environ, {'DAGSTER_HOME': str(temp_dagster_home)}):
                instance = DagsterInstance.ephemeral(tempdir=str(temp_dagster_home))
                
                from daydreaming_dagster.assets.raw_data import (
                    concepts, llm_models, 
                    generation_templates, evaluation_templates
                )
                from daydreaming_dagster.assets.core import (
                    content_combinations, generation_tasks, evaluation_tasks
                )
                from daydreaming_dagster.assets.llm_generation import generation_prompt
                from daydreaming_dagster.assets.llm_evaluation import evaluation_prompt
                from daydreaming_dagster.resources.io_managers import CSVIOManager, PartitionedTextIOManager
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig
                
                test_data_root = str(temp_data_dir)
                
                resources = {
                    "data_root": test_data_root,
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph"),
                    "csv_io_manager": CSVIOManager(base_path=Path(test_data_root) / "2_tasks"),
                    "generation_prompt_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "3_generation" / "generation_prompts"),
                    "generation_response_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "3_generation" / "generation_responses"),
                    "evaluation_prompt_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "4_evaluation" / "evaluation_prompts"),
                }
                
                # First materialize all dependencies up to generation_tasks
                result = materialize([
                    concepts, llm_models,
                    generation_templates, evaluation_templates,
                    content_combinations, generation_tasks, evaluation_tasks
                ], resources=resources, instance=instance)
                
                assert result.success, "Dependency materialization should succeed"
                
                # Load the generation tasks to get partition keys
                gen_tasks_file = temp_data_dir / "2_tasks" / "generation_tasks.csv"
                gen_tasks_df = pd.read_csv(gen_tasks_file)
                
                # Get the first generation task partition for testing
                test_partition = gen_tasks_df.iloc[0]["generation_task_id"]
                
                # Test generation prompt asset with specific partition
                from dagster import build_asset_context
                
                # Build context with the test partition
                context = build_asset_context(
                    resources=resources,
                    partition_key=test_partition,
                    instance=instance
                )
                
                # Manually materialize the generation_prompt asset with a specific partition
                # This tests the actual consolidated template functionality
                
                # Get first task partition to test with
                test_partition = gen_tasks_df.iloc[0]["generation_task_id"]
                
                # Materialize the generation_prompt asset for this partition
                # We need to include all its dependencies
                from daydreaming_dagster.assets.llm_generation import generation_prompt
                
                prompt_result = materialize(
                    [concepts, llm_models, generation_templates, evaluation_templates,
                     content_combinations, generation_tasks, generation_prompt],
                    resources=resources,
                    instance=instance,
                    partition_key=test_partition
                )
                
                # Verify the prompt generation succeeded
                assert prompt_result.success, "Generation prompt materialization should succeed"
                
                # Read the generated prompt file to verify content
                prompt_file = temp_data_dir / "3_generation" / "generation_prompts" / f"{test_partition}.txt"
                assert prompt_file.exists(), f"Prompt file should exist: {prompt_file}"
                
                generated_prompt = prompt_file.read_text()
                
                # Verify the prompt was generated correctly
                assert isinstance(generated_prompt, str), "Prompt should be a string"
                assert len(generated_prompt) > 0, "Prompt should not be empty"
                
                # Verify template rendering worked (concepts were injected)
                assert "Test Concept 1" in generated_prompt, "Should contain concept 1 name"
                assert "Test Concept 2" in generated_prompt, "Should contain concept 2 name"
                assert "Test concept 1 description" in generated_prompt, "Should contain concept 1 content"
                assert "Test concept 2 description" in generated_prompt, "Should contain concept 2 content"
                assert "Generate creative ideas" in generated_prompt, "Should contain template content"
                
                # Test evaluation templates conversion
                eval_templates_content = []
                eval_templates_metadata = pd.read_csv(raw_data / "evaluation_templates.csv")
                for _, row in eval_templates_metadata[eval_templates_metadata["active"]].iterrows():
                    template_path = eval_templates_dir / f"{row['template_id']}.txt"
                    content = template_path.read_text()
                    eval_templates_content.append({
                        "template_id": row["template_id"],
                        "template_name": row["template_name"],
                        "description": row["description"], 
                        "active": row["active"],
                        "content": content
                    })
                
                evaluation_templates_df = pd.DataFrame(eval_templates_content)
                
                # Test DataFrame to dict conversion works
                eval_templates_dict = evaluation_templates_df.set_index('template_id')['content'].to_dict()
                
                assert isinstance(eval_templates_dict, dict), "Should convert to dict"
                assert "test-evaluation-template" in eval_templates_dict, "Should contain template ID as key"
                assert "Rate this response" in eval_templates_dict["test-evaluation-template"], "Should contain template content"
                
                print("✅ Prompt generation test passed!")
                print(f"   Generated prompt length: {len(generated_prompt)} characters")
                print(f"   Template rendering correctly injected concepts")
                print(f"   Evaluation template conversion: {len(eval_templates_dict)} templates")
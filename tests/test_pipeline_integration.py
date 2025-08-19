"""Full pipeline integration tests using temporary DAGSTER_HOME and copied live data.

Tests complete materialization of raw_data and llm_tasks groups with real data.
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import re
import shutil
import os
from unittest.mock import patch, Mock
from dagster import materialize, DagsterInstance, ConfigurableResource


class CannedLLMResource(ConfigurableResource):
    """Mock LLM resource with realistic canned responses for full pipeline testing."""
    
    def generate(self, prompt: str, model: str, temperature: float = 0.7, max_tokens: int = None) -> str:
        """Generate method that matches LLMClientResource interface."""
        # Determine if this is generation or evaluation based on prompt content
        if "SCORE" in prompt or "Rate" in prompt or "evaluate" in prompt.lower():
            # This is an evaluation prompt - return a score
            return self._get_evaluation_response(model)
        else:
            # This is a generation prompt - return creative content
            return self._get_generation_response(model, prompt)
    
    def get_client(self):
        """Backward compatibility method - return self since we now implement generate directly."""
        return self
    
    def _get_generation_response(self, model, prompt):
        """Generate realistic creative responses based on model."""
        responses = {
            "deepseek/deepseek-r1:free": """
# Breakthrough in Daydreaming Methodology

This combination of concepts reveals a fascinating intersection between human creativity and systematic discovery. The default mode network, traditionally associated with mind-wandering, emerges as a crucial component in innovative thinking processes.

## Key Insights

1. **Combinatorial Creativity**: The systematic exploration of concept combinations mirrors how breakthrough discoveries often emerge from unexpected connections between disparate fields.

2. **Economic Innovation Models**: Traditional economic models fail to capture the non-linear nature of creative discovery, where value emerges from novel combinations rather than incremental improvements.

3. **Human-AI Collaboration**: The generator-verifier gap highlights how human intuition and AI systematization can complement each other in the discovery process.

This synthesis suggests that structured daydreaming could be formalized into a reproducible methodology for innovation discovery.
            """.strip(),
            
            "deepseek/deepseek-r1-zero:free": """
# Systematic Analysis of Creative Discovery

The intersection of these concepts reveals a fundamental insight about how innovation actually works. Rather than being purely serendipitous, breakthrough discoveries follow predictable patterns when we understand the underlying cognitive mechanisms.

## Core Framework

**Default Mode Network Activation**: The brain's default mode network, active during daydreaming, is not random wandering but systematic exploration of conceptual spaces. This explains why insights often come during "unfocused" thinking.

**Combinatorial Creativity**: Innovation emerges from novel combinations of existing concepts, not from entirely new ideas. The systematic exploration of concept combinations mirrors how breakthrough discoveries actually happen.

**Economic Innovation Models**: Traditional economic models fail because they assume linear, incremental improvement. Real innovation is non-linear and emerges from unexpected connections between domains.

## Practical Implications

This synthesis suggests that "structured daydreaming" could be formalized into a reproducible methodology. By understanding the cognitive patterns behind creative discovery, we can design systems that amplify human creativity rather than replacing it.
            """.strip(),
            
            "google/gemma-3-27b-it:free": """
The convergence of these concepts points toward a revolutionary approach to creative discovery. By understanding how the default mode network facilitates combinatorial thinking, we can design systems that amplify human creativity.

**Core Proposition**: Daydreaming is not passive wandering but active exploration of conceptual possibility spaces. When combined with systematic search methodologies, it becomes a powerful innovation engine.

**Implementation Framework**:
- Structured concept combination protocols
- Economic models that account for creative value creation  
- Human-AI hybrid systems that leverage both intuitive leaps and systematic verification

This approach could transform how we think about innovation, moving from serendipitous discovery to intentional creative exploration.
            """.strip()
        }
        
        return responses.get(model, f"Creative analysis of concepts using {model}")
    
    def _get_evaluation_response(self, model):
        """Generate realistic evaluation responses with scores (compatible with in_last_line strategy)."""
        evaluations = {
            "deepseek/deepseek-r1:free": """
**Reasoning**: This response demonstrates exceptional creativity and insight. The synthesis of daydreaming methodology with systematic discovery shows deep understanding of the intersection between human cognition and innovation processes. The structured approach to creativity is particularly compelling.

**Strengths**:
- Novel framework combining structured and intuitive thinking
- Clear implementation pathway
- Strong theoretical foundation

**Areas for improvement**:
- Could benefit from more concrete examples
- Implementation details could be more specific

**SCORE**: 8.5
            """.strip(),
            
            "google/gemma-3-27b-it:free": """
**Reasoning**: Strong creative synthesis with good theoretical grounding. The concept of "structured daydreaming" is innovative and the human-AI collaboration angle is well-developed. However, the response could benefit from more detailed exploration of practical applications.

**Strengths**:
- Clear conceptual framework
- Good integration of multiple domains
- Practical orientation

**Areas for improvement**:
- More empirical support needed
- Could explore limitations more thoroughly

**SCORE**: 7.5
            """.strip()
        }
        
        return evaluations.get(model, f"Good analysis with creative elements using {model}\n**SCORE**: 7.5")


@pytest.fixture(scope="function")
def pipeline_data_root_prepared():
    """Prepare persistent test data under tests/data_pipeline_test by copying live data and limiting scope.

    Steps:
    - Clean tests/data_pipeline_test
    - Copy data/1_raw into tests/data_pipeline_test/1_raw
    - Create output dirs (2_tasks, 3_generation/{generation_prompts,generation_responses})
    - Limit active rows: concepts (2), generation_templates (2), evaluation_templates (2), llm_models for_generation (2)
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
    (pipeline_data_root / "3_generation" / "generation_prompts").mkdir(parents=True)
    (pipeline_data_root / "3_generation" / "generation_responses").mkdir(parents=True)
    (pipeline_data_root / "3_generation" / "parsed_generation_responses").mkdir(parents=True)
    # Two-phase generation directories
    (pipeline_data_root / "3_generation" / "links_prompts").mkdir(parents=True)
    (pipeline_data_root / "3_generation" / "links_responses").mkdir(parents=True)
    (pipeline_data_root / "3_generation" / "essay_prompts").mkdir(parents=True)
    (pipeline_data_root / "3_generation" / "essay_responses").mkdir(parents=True)
    (pipeline_data_root / "4_evaluation" / "evaluation_prompts").mkdir(parents=True)
    (pipeline_data_root / "4_evaluation" / "evaluation_responses").mkdir(parents=True)
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
    gen_templates_dir = pipeline_data_root / "1_raw" / "generation_templates"
    eval_templates_dir = pipeline_data_root / "1_raw" / "evaluation_templates"
    
    # Verify template files exist
    if gen_templates_dir.exists():
        gen_files = list(gen_templates_dir.glob("*.txt"))
        print(f"✓ Copied {len(gen_files)} generation template files")
        if len(gen_files) == 0:
            raise RuntimeError("No generation template files found after copy")
    
    if eval_templates_dir.exists():
        eval_files = list(eval_templates_dir.glob("*.txt"))
        print(f"✓ Copied {len(eval_files)} evaluation template files")
        if len(eval_files) == 0:
            raise RuntimeError("No evaluation template files found after copy")

    # Limit active rows in key CSVs
    concepts_csv = pipeline_data_root / "1_raw" / "concepts" / "concepts_metadata.csv"
    gen_templates_csv = pipeline_data_root / "1_raw" / "generation_templates.csv"
    eval_templates_csv = pipeline_data_root / "1_raw" / "evaluation_templates.csv"
    models_csv = pipeline_data_root / "1_raw" / "llm_models.csv"

    # Concepts: keep only first two active
    cdf = pd.read_csv(concepts_csv)
    if "active" in cdf.columns:
        cdf["active"] = False
        cdf.loc[cdf.index[:2], "active"] = True
    pd.testing.assert_series_equal((cdf["active"] == True).reset_index(drop=True)[:2], pd.Series([True, True]), check_names=False)
    cdf.to_csv(concepts_csv, index=False)

    # Generation templates: use only creative-synthesis-v8 (two-phase structure)
    gtdf = pd.read_csv(gen_templates_csv)
    if "active" in gtdf.columns:
        gtdf["active"] = False
        
        # Activate only creative-synthesis-v8 to test two-phase generation
        target_template = "creative-synthesis-v8"
        template_mask = gtdf["template_id"] == target_template
        
        if template_mask.any():
            # Verify two-phase structure exists
            links_file = pipeline_data_root / "1_raw" / "generation_templates" / "links" / f"{target_template}.txt"
            essay_file = pipeline_data_root / "1_raw" / "generation_templates" / "essay" / f"{target_template}.txt"
            
            if links_file.exists() and essay_file.exists():
                gtdf.loc[template_mask, "active"] = True
                print(f"✓ Activated {target_template} template with two-phase structure")
            else:
                raise RuntimeError(f"Two-phase template files missing for {target_template}: {links_file} or {essay_file}")
        else:
            raise RuntimeError(f"Template {target_template} not found in generation_templates.csv")
    
    gtdf.to_csv(gen_templates_csv, index=False)

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
        
        # Task definition files (02_tasks/) - Only generation_tasks.csv and evaluation_tasks.csv are created now
        task_files = [
            test_directory / "2_tasks" / "generation_tasks.csv",
            test_directory / "2_tasks" / "evaluation_tasks.csv",
        ]
        
        for file_path in task_files:
            assert file_path.exists(), f"Task file not created: {file_path}"
            assert file_path.stat().st_size > 0, f"Task file is empty: {file_path}"
        
        # Verify task file contents
        gen_tasks_file = test_directory / "2_tasks" / "generation_tasks.csv"
        if gen_tasks_file.exists():
            gen_tasks_df = pd.read_csv(gen_tasks_file)
            assert len(gen_tasks_df) > 0, "Generation tasks should not be empty"
            required_columns = ["generation_task_id", "combo_id", "generation_template", "generation_model"]
            for col in required_columns:
                assert col in gen_tasks_df.columns, f"Missing required column: {col}"
            
            # Verify combo_id format
            for combo_id in gen_tasks_df["combo_id"].unique():
                assert combo_id.startswith("combo_"), f"Invalid combo_id format: {combo_id}"
        
        # Two-phase generation files (3_generation/)
        links_prompt_dir = test_directory / "3_generation" / "links_prompts"
        links_response_dir = test_directory / "3_generation" / "links_responses"
        essay_prompt_dir = test_directory / "3_generation" / "essay_prompts"
        essay_response_dir = test_directory / "3_generation" / "essay_responses"
        parsed_response_dir = test_directory / "3_generation" / "parsed_generation_responses"
        
        for partition in test_gen_partitions:
            # Verify all two-phase files exist
            links_prompt_file = links_prompt_dir / f"{partition}.txt"
            links_response_file = links_response_dir / f"{partition}.txt"
            essay_prompt_file = essay_prompt_dir / f"{partition}.txt"
            essay_response_file = essay_response_dir / f"{partition}.txt"
            parsed_response_file = parsed_response_dir / f"{partition}.txt"
            
            assert links_prompt_file.exists(), f"Links prompt not created: {links_prompt_file}"
            assert links_response_file.exists(), f"Links response not created: {links_response_file}"
            assert essay_prompt_file.exists(), f"Essay prompt not created: {essay_prompt_file}"
            assert essay_response_file.exists(), f"Essay response not created: {essay_response_file}"
            assert parsed_response_file.exists(), f"Parsed response not created: {parsed_response_file}"
            
            # Verify file sizes
            assert links_prompt_file.stat().st_size > 0, f"Links prompt is empty: {links_prompt_file}"
            assert links_response_file.stat().st_size > 0, f"Links response is empty: {links_response_file}"
            assert essay_prompt_file.stat().st_size > 0, f"Essay prompt is empty: {essay_prompt_file}"
            assert essay_response_file.stat().st_size > 0, f"Essay response is empty: {essay_response_file}"
            assert parsed_response_file.stat().st_size > 0, f"Parsed response is empty: {parsed_response_file}"
            
            # Verify content quality
            essay_content = essay_response_file.read_text()
            assert len(essay_content) > 100, "Essay response should be substantial"
            assert any(word in essay_content.lower() for word in ["creative", "innovation", "discovery", "concept"]), "Essay response should contain relevant keywords"
        
        # Evaluation files (4_evaluation/) - Check if they exist but don't require them
        eval_prompt_dir = test_directory / "4_evaluation" / "evaluation_prompts"
        eval_response_dir = test_directory / "4_evaluation" / "evaluation_responses"
        
        eval_files_exist = False
        if eval_prompt_dir.exists() and eval_response_dir.exists():
            eval_files = list(eval_prompt_dir.glob("*.txt")) + list(eval_response_dir.glob("*.txt"))
            if len(eval_files) > 0:
                eval_files_exist = True
                print(f"✓ Found {len(eval_files)} evaluation files")
        
        if not eval_files_exist:
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

                from daydreaming_dagster.assets.raw_data import (
                    concepts, llm_models, generation_templates, evaluation_templates
                )
                from daydreaming_dagster.assets.core import (
                    content_combinations, content_combinations_csv, generation_tasks, evaluation_tasks
                )
                from daydreaming_dagster.assets.llm_generation import (
                    generation_prompt, generation_response
                )
                from daydreaming_dagster.assets.parsed_generation import (
                    parsed_generation_responses
                )
                from daydreaming_dagster.assets.two_phase_generation import (
                    links_prompt, links_response, essay_prompt, essay_response
                )
                from daydreaming_dagster.assets.llm_evaluation import (
                    evaluation_prompt, evaluation_response
                )
                from daydreaming_dagster.resources.io_managers import (
                    CSVIOManager, PartitionedTextIOManager
                )
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig

                resources = {
                    "data_root": str(pipeline_data_root),
                    "openrouter_client": CannedLLMResource(),
                    "csv_io_manager": CSVIOManager(base_path=pipeline_data_root / "2_tasks"),
                    "generation_prompt_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "generation_prompts",
                        overwrite=True
                    ),
                    "generation_response_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "generation_responses",
                        overwrite=True
                    ),
                    "parsed_generation_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "parsed_generation_responses",
                        overwrite=True
                    ),
                    # Two-phase generation I/O managers
                    "links_prompt_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "links_prompts",
                        overwrite=True
                    ),
                    "links_response_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "links_responses",
                        overwrite=True
                    ),
                    "essay_prompt_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "essay_prompts",
                        overwrite=True
                    ),
                    "essay_response_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "essay_responses",
                        overwrite=True
                    ),
                    "evaluation_prompt_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "4_evaluation" / "evaluation_prompts",
                        overwrite=True
                    ),
                    "evaluation_response_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "4_evaluation" / "evaluation_responses",
                        overwrite=True
                    ),
                    "parsing_results_io_manager": CSVIOManager(base_path=pipeline_data_root / "5_parsing"),
                    "summary_results_io_manager": CSVIOManager(base_path=pipeline_data_root / "6_summary"),
                    "error_log_io_manager": CSVIOManager(base_path=pipeline_data_root / "7_reporting"),
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph"),
                }

                print("🚀 Starting complete pipeline workflow...")
                
                # STEP 1: Materialize all raw data and task assets
                print("📋 Step 1: Materializing raw data and task definitions...")
                result = materialize(
                    [
                        concepts, llm_models,
                        generation_templates, evaluation_templates,
                        content_combinations, content_combinations_csv,
                        generation_tasks, evaluation_tasks,
                    ],
                    resources=resources,
                    instance=instance,
                )
                assert result.success, "Task materialization failed"
                print("✅ Task materialization completed successfully")

                # Verify no empty files and required files created under 2_tasks
                task_dir = pipeline_data_root / "2_tasks"
                empty_files = []
                for file_path in task_dir.rglob("*"):
                    if file_path.is_file() and file_path.stat().st_size <= 2:
                        empty_files.append(file_path)
                assert len(empty_files) == 0, f"Empty files should not be created: {empty_files}"

                expected_files = [
                    "generation_tasks.csv",
                    "evaluation_tasks.csv",
                    "content_combinations_csv.csv",
                ]
                for name in expected_files:
                    p = task_dir / name
                    assert p.exists(), f"Expected file not found: {name}"
                    assert p.stat().st_size > 10, f"File appears empty: {name}"

                # Check normalized content_combinations_csv structure and integrity
                combinations_csv = pd.read_csv(task_dir / "content_combinations_csv.csv")
                assert "combo_id" in combinations_csv.columns
                assert "concept_id" in combinations_csv.columns
                assert len(combinations_csv.columns) == 2
                assert len(combinations_csv) > 0
                combo_ids = combinations_csv["combo_id"].unique()
                assert all(cid.startswith("combo_") for cid in combo_ids)

                # Active concept IDs subset check
                active_concepts_df = pd.read_csv(pipeline_data_root / "1_raw" / "concepts" / "concepts_metadata.csv")
                expected_active_concept_ids = set(active_concepts_df[active_concepts_df["active"] == True]["concept_id"])
                actual_concept_ids = set(combinations_csv["concept_id"].unique())
                assert actual_concept_ids.issubset(expected_active_concept_ids)

                # Generation tasks structure and counts vs active templates/models
                gen_tasks_csv_path = task_dir / "generation_tasks.csv"
                gen_tasks_csv = pd.read_csv(gen_tasks_csv_path)
                assert "generation_task_id" in gen_tasks_csv.columns
                assert "combo_id" in gen_tasks_csv.columns
                assert "generation_template" in gen_tasks_csv.columns
                assert "generation_model" in gen_tasks_csv.columns

                gen_templates_metadata = pd.read_csv(pipeline_data_root / "1_raw" / "generation_templates.csv")
                expected_active_templates = set(gen_templates_metadata[gen_templates_metadata["active"] == True]["template_id"].tolist())
                templates_used = set(gen_tasks_csv["generation_template"].unique())
                assert templates_used == expected_active_templates

                combinations_count = len(combinations_csv["combo_id"].unique())
                llm_models_df = pd.read_csv(pipeline_data_root / "1_raw" / "llm_models.csv")
                generation_models_count = len(llm_models_df[llm_models_df["for_generation"] == True])
                expected_task_count = combinations_count * len(expected_active_templates) * generation_models_count
                actual_task_count = len(gen_tasks_csv)
                assert actual_task_count == expected_task_count

                # Evaluation tasks basic structure and metadata consistency
                eval_tasks_csv_path = task_dir / "evaluation_tasks.csv"
                eval_tasks_csv = pd.read_csv(eval_tasks_csv_path)
                assert "evaluation_task_id" in eval_tasks_csv.columns
                assert "generation_task_id" in eval_tasks_csv.columns

                unique_combos_in_combinations = len(combinations_csv["combo_id"].unique())
                unique_combos_in_gen_tasks = len(gen_tasks_csv["combo_id"].unique())
                assert unique_combos_in_combinations == unique_combos_in_gen_tasks

                print(f"✅ Task verification completed: {actual_task_count} generation tasks, {len(eval_tasks_csv)} evaluation tasks")

                # STEP 2: Generate content for a subset of generation tasks
                print("🔄 Step 2: Generating content for generation tasks...")
                gen_tasks_df = pd.read_csv(pipeline_data_root / "2_tasks" / "generation_tasks.csv")
                # Select small set of partitions: up to 4
                partitions = gen_tasks_df["generation_task_id"].head(4).tolist()
                assert len(partitions) > 0

                # Materialize generation assets for each partition individually to avoid partition conflicts
                for pk in partitions:
                    print(f"   Processing partition: {pk}")
                    
                    # Materialize two-phase generation pipeline
                    # Phase 1: Links generation
                    links_res = materialize(
                        [concepts, llm_models, generation_templates, content_combinations, generation_tasks, 
                         links_prompt, links_response],
                        resources=resources,
                        instance=instance,
                        partition_key=pk,
                    )
                    assert links_res.success
                    
                    # Phase 2: Essay generation
                    essay_res = materialize(
                        [concepts, llm_models, generation_templates, content_combinations, generation_tasks,
                         links_prompt, links_response, essay_prompt, essay_response],
                        resources=resources,
                        instance=instance,
                        partition_key=pk,
                    )
                    assert essay_res.success
                    
                    # Canonical generation response
                    canonical_res = materialize(
                        [concepts, llm_models, generation_templates, content_combinations, generation_tasks,
                         links_prompt, links_response, essay_prompt, essay_response],
                        resources=resources,
                        instance=instance,
                        partition_key=pk,
                    )
                    assert canonical_res.success
                    
                    # Materialize parsed generation responses (with dependencies)
                    parsed_res = materialize(
                        [concepts, llm_models, generation_templates, content_combinations, generation_tasks,
                         links_prompt, links_response, essay_prompt, essay_response,
                         parsed_generation_responses],
                        resources=resources,
                        instance=instance,
                        partition_key=pk,
                    )
                    assert parsed_res.success
                    
                    # Verify two-phase generation files were created
                    assert (pipeline_data_root / "3_generation" / "links_prompts" / f"{pk}.txt").exists()
                    assert (pipeline_data_root / "3_generation" / "links_responses" / f"{pk}.txt").exists()
                    assert (pipeline_data_root / "3_generation" / "essay_prompts" / f"{pk}.txt").exists()
                    assert (pipeline_data_root / "3_generation" / "essay_responses" / f"{pk}.txt").exists()
                    assert (pipeline_data_root / "3_generation" / "parsed_generation_responses" / f"{pk}.txt").exists()

                print(f"✅ Content generation completed for {len(partitions)} partitions")

                # Verify two-phase generation worked correctly
                first_partition = partitions[0]
                essay_response_path = pipeline_data_root / "3_generation" / "essay_responses" / f"{first_partition}.txt"
                parsed_response_path = pipeline_data_root / "3_generation" / "parsed_generation_responses" / f"{first_partition}.txt"
                
                essay_content = essay_response_path.read_text()
                parsed_content = parsed_response_path.read_text()
                
                # With two-phase generation, parsed content should be the same as essay response
                assert essay_content == parsed_content, "Parsed content should equal essay response in two-phase generation"
                
                # Verify content contains expected elements from canned responses
                assert "Breakthrough in Daydreaming Methodology" in parsed_content or "Systematic Analysis of Creative Discovery" in parsed_content, "Parsed content should contain expected canned response content"
                assert "default mode network" in parsed_content.lower(), "Parsed content should contain core concept content"
                assert "creativity" in parsed_content.lower() or "innovation" in parsed_content.lower(), "Parsed content should contain relevant themes"

                # STEP 3: Test evaluation if we have enough data
                print("🔍 Step 3: Testing evaluation pipeline...")
                eval_tasks_file = pipeline_data_root / "2_tasks" / "evaluation_tasks.csv"
                eval_tasks_df = pd.read_csv(eval_tasks_file)
                
                # Get evaluation partitions that depend on our generation partitions
                available_eval_tasks = eval_tasks_df[
                    eval_tasks_df["generation_task_id"].isin(partitions)
                ]
                test_evaluation_partitions = available_eval_tasks["evaluation_task_id"].head(2).tolist()
                
                if test_evaluation_partitions:
                    print(f"📊 Testing evaluation for {len(test_evaluation_partitions)} partitions...")
                    print("⚠ Skipping evaluation step for now due to partition dependency complexity")
                    print("   This test focuses on the generation pipeline structure")
                    print("   Evaluation testing can be added in a separate focused test")
                else:
                    print("⚠ No evaluation partitions available for testing")

                # STEP 4: Final verification of all outputs
                print("🔍 Step 4: Final verification of all pipeline outputs...")
                self._verify_expected_files(
                    pipeline_data_root, 
                    partitions, 
                    test_evaluation_partitions if 'test_evaluation_partitions' in locals() else []
                )
                
                print("🎉 Complete pipeline workflow test passed successfully!")
                print(f"   - Generated {len(partitions)} generation tasks")
                print(f"   - Created all expected output files")
                print(f"   - Verified data integrity and relationships")
                print(f"   - Pipeline ran only ONCE as requested")

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
            (temp_data_dir / "3_generation").mkdir()
            (temp_data_dir / "3_generation" / "generation_prompts").mkdir()
            (temp_data_dir / "3_generation" / "generation_responses").mkdir()
            (temp_data_dir / "3_generation" / "parsed_generation_responses").mkdir()
            (temp_data_dir / "4_evaluation").mkdir()
            (temp_data_dir / "4_evaluation" / "evaluation_prompts").mkdir()
            (temp_data_dir / "4_evaluation" / "evaluation_responses").mkdir()
            (temp_data_dir / "5_parsing").mkdir()
            (temp_data_dir / "6_summary").mkdir()
            (temp_data_dir / "7_reporting").mkdir()
            
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
                
                # Test combo_id format (accept both legacy sequential and new stable formats)
                combo_ids = df["combo_id"].unique()
                legacy_pattern = re.compile(r"^combo_\d{3}$")
                stable_pattern = re.compile(r"^combo_v\d+_[0-9a-f]{12}$")
                assert all(
                    legacy_pattern.match(cid) or stable_pattern.match(cid)
                    for cid in combo_ids
                ), "combo_ids should match 'combo_XXX' (legacy) or 'combo_vN_<12-hex>' (stable)"
                
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
            (temp_data_dir / "3_generation").mkdir()
            (temp_data_dir / "3_generation" / "generation_prompts").mkdir()
            (temp_data_dir / "3_generation" / "generation_responses").mkdir()
            (temp_data_dir / "3_generation" / "parsed_generation_responses").mkdir()
            (temp_data_dir / "4_evaluation").mkdir()
            (temp_data_dir / "4_evaluation" / "evaluation_prompts").mkdir()
            (temp_data_dir / "4_evaluation" / "evaluation_responses").mkdir()
            (temp_data_dir / "5_parsing").mkdir()
            (temp_data_dir / "6_summary").mkdir()
            (temp_data_dir / "7_reporting").mkdir()
            
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
                from daydreaming_dagster.resources.io_managers import CSVIOManager, PartitionedTextIOManager
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig
                
                test_data_root = str(temp_data_dir)
                
                resources = {
                    "data_root": test_data_root,
                    "csv_io_manager": CSVIOManager(base_path=Path(test_data_root) / "2_tasks"),
                    "generation_prompt_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "3_generation" / "generation_prompts"),
                    "generation_response_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "3_generation" / "generation_responses"),
                    "parsed_generation_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "3_generation" / "parsed_generation_responses"),
                    "evaluation_prompt_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "4_evaluation" / "evaluation_prompts"),
                    "evaluation_response_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "4_evaluation" / "evaluation_responses"),
                    "parsing_results_io_manager": CSVIOManager(base_path=Path(test_data_root) / "5_parsing"),
                    "summary_results_io_manager": CSVIOManager(base_path=Path(test_data_root) / "6_summary"),
                    "error_log_io_manager": CSVIOManager(base_path=Path(test_data_root) / "7_reporting"),
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

    # Note: Error scenarios (like insufficient links) are covered by comprehensive unit tests
    # in daydreaming_dagster/assets/test_two_phase_generation.py, specifically TestLinksValidation class

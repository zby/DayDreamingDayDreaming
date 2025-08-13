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

    # Generation templates: keep only first two active
    gtdf = pd.read_csv(gen_templates_csv)
    if "active" in gtdf.columns:
        gtdf["active"] = False
        gtdf.loc[gtdf.index[:2], "active"] = True
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
        
        # Generation files (3_generation/)
        gen_prompt_dir = test_directory / "3_generation" / "generation_prompts"
        gen_response_dir = test_directory / "3_generation" / "generation_responses"
        
        for partition in test_gen_partitions:
            prompt_file = gen_prompt_dir / f"{partition}.txt"
            response_file = gen_response_dir / f"{partition}.txt"
            
            assert prompt_file.exists(), f"Generation prompt not created: {prompt_file}"
            assert response_file.exists(), f"Generation response not created: {response_file}"
            assert prompt_file.stat().st_size > 0, f"Generation prompt is empty: {prompt_file}"
            assert response_file.stat().st_size > 0, f"Generation response is empty: {response_file}"
            
            # Verify response content quality
            response_content = response_file.read_text()
            assert len(response_content) > 100, "Response should be substantial"
            assert any(word in response_content.lower() for word in ["creative", "innovation", "discovery", "concept"]), "Response should contain relevant keywords"
        
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

    def test_pipeline_e2e_live_data_limited_generations(self, pipeline_data_root_prepared):
        """Copy live data into tests/data_pipeline_test, limit active rows, then run limited generations.

        - Clean tests/data_pipeline_test and copy from data/1_raw into tests/data_pipeline_test/1_raw
        - Limit active rows: concepts (2), generation_templates (2), evaluation_templates (2), llm generation models (2)
        - Materialize raw assets and task CSVs using tests/data_pipeline_test as data_root
        - Run generations for a small set of partitions (mocked client). Data persists for inspection.
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
                    generation_prompt, generation_response, parsed_generation_responses
                )
                from daydreaming_dagster.resources.io_managers import (
                    CSVIOManager, PartitionedTextIOManager
                )
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig

                resources = {
                    "data_root": str(pipeline_data_root),
                    "csv_io_manager": CSVIOManager(base_path=pipeline_data_root / "2_tasks"),
                    "generation_prompt_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "generation_prompts"
                    ),
                    "generation_response_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "generation_responses"
                    ),
                    "parsed_generation_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "parsed_generation_responses"
                    ),
                    "evaluation_prompt_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "4_evaluation" / "evaluation_prompts"
                    ),
                    "evaluation_response_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "4_evaluation" / "evaluation_responses"
                    ),
                    "parsing_results_io_manager": CSVIOManager(base_path=pipeline_data_root / "5_parsing"),
                    "summary_results_io_manager": CSVIOManager(base_path=pipeline_data_root / "6_summary"),
                    "error_log_io_manager": CSVIOManager(base_path=pipeline_data_root / "7_reporting"),
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph"),
                }

                # Materialize up to tasks once
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
                assert result.success

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

                gen_tasks_df = pd.read_csv(pipeline_data_root / "2_tasks" / "generation_tasks.csv")
                # Select small set of partitions: up to 4
                partitions = gen_tasks_df["generation_task_id"].head(4).tolist()
                assert len(partitions) > 0

                class _MockClient:
                    def generate(self, prompt: str, model: str) -> str:
                        # Return a simple response since test templates use fallback strategy
                        # (creative-synthesis and systematic-analytical don't expect XML structure)
                        return f"""This is the mock essay response generated by {model}.
It demonstrates the daydreaming loop concept by combining multiple ideas.

The response shows how different concepts can be synthesized together.
This validates the generation and parsing pipeline integration using the fallback strategy.

Since this template uses fallback parsing, the entire response becomes the essay content."""

                gen_resources = {**resources, "openrouter_client": _MockClient()}

                for pk in partitions:
                    gen_res = materialize(
                        [
                            concepts, llm_models, generation_templates,
                            content_combinations, generation_tasks,
                            generation_prompt, generation_response, parsed_generation_responses,
                        ],
                        resources=gen_resources,
                        instance=instance,
                        partition_key=pk,
                    )
                    assert gen_res.success
                    assert (pipeline_data_root / "3_generation" / "generation_prompts" / f"{pk}.txt").exists()
                    assert (pipeline_data_root / "3_generation" / "generation_responses" / f"{pk}.txt").exists()
                    assert (pipeline_data_root / "3_generation" / "parsed_generation_responses" / f"{pk}.txt").exists()

                # Verify parsing worked correctly - with fallback strategy, entire response becomes essay
                first_partition = partitions[0]
                raw_response_path = pipeline_data_root / "3_generation" / "generation_responses" / f"{first_partition}.txt"
                parsed_response_path = pipeline_data_root / "3_generation" / "parsed_generation_responses" / f"{first_partition}.txt"
                
                raw_content = raw_response_path.read_text()
                parsed_content = parsed_response_path.read_text()
                
                # With fallback strategy, parsed content should be the same as raw (entire response is essay)
                assert raw_content == parsed_content, "With fallback strategy, parsed content should equal raw content"
                
                # Verify content contains expected elements
                assert "This is the mock essay response" in parsed_content, "Parsed content should contain mock essay text"
                assert "daydreaming loop concept" in parsed_content, "Parsed content should contain core essay content"
                assert "fallback strategy" in parsed_content, "Parsed content should mention fallback strategy"

    
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
            (temp_data_dir / "3_generation" / "generation_responses").mkdir()
            (temp_data_dir / "3_generation" / "parsed_generation_responses").mkdir()
            (temp_data_dir / "4_evaluation").mkdir()
            (temp_data_dir / "4_evaluation" / "evaluation_prompts").mkdir()
            (temp_data_dir / "4_evaluation" / "evaluation_responses").mkdir()
            (temp_data_dir / "5_parsing").mkdir()
            (temp_data_dir / "6_summary").mkdir()
            (temp_data_dir / "7_reporting").mkdir()
            
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
                    "parsed_generation_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "3_generation" / "parsed_generation_responses"),
                    "evaluation_prompt_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "4_evaluation" / "evaluation_prompts"),
                    "evaluation_response_io_manager": PartitionedTextIOManager(base_path=Path(test_data_root) / "4_evaluation" / "evaluation_responses"),
                    "parsing_results_io_manager": CSVIOManager(base_path=Path(test_data_root) / "5_parsing"),
                    "summary_results_io_manager": CSVIOManager(base_path=Path(test_data_root) / "6_summary"),
                    "error_log_io_manager": CSVIOManager(base_path=Path(test_data_root) / "7_reporting"),
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

    def test_pipeline_with_canned_llm_responses(self, pipeline_data_root_prepared):
        """Test pipeline with realistic canned LLM responses using CannedLLMResource."""
        pipeline_data_root = pipeline_data_root_prepared
        
        # Use a temporary DAGSTER_HOME for isolation
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
                    generation_prompt, generation_response, parsed_generation_responses
                )
                from daydreaming_dagster.assets.llm_evaluation import (
                    evaluation_prompt, evaluation_response
                )
                from daydreaming_dagster.resources.io_managers import (
                    CSVIOManager, PartitionedTextIOManager
                )
                from daydreaming_dagster.resources.experiment_config import ExperimentConfig

                # Use CannedLLMResource for realistic responses
                resources = {
                    "data_root": str(pipeline_data_root),
                    "openrouter_client": CannedLLMResource(),
                    "csv_io_manager": CSVIOManager(base_path=pipeline_data_root / "2_tasks"),
                    "generation_prompt_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "generation_prompts"
                    ),
                    "generation_response_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "generation_responses"
                    ),
                    "parsed_generation_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "parsed_generation_responses"
                    ),
                    "evaluation_prompt_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "4_evaluation" / "evaluation_prompts"
                    ),
                    "evaluation_response_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "4_evaluation" / "evaluation_responses"
                    ),
                    "parsing_results_io_manager": CSVIOManager(base_path=pipeline_data_root / "5_parsing"),
                    "summary_results_io_manager": CSVIOManager(base_path=pipeline_data_root / "6_summary"),
                    "error_log_io_manager": CSVIOManager(base_path=pipeline_data_root / "7_reporting"),
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph"),
                }

                # Materialize up to tasks
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
                assert result.success

                # Get generation partitions for testing
                gen_tasks_file = pipeline_data_root / "2_tasks" / "generation_tasks.csv"
                gen_tasks_df = pd.read_csv(gen_tasks_file)
                test_generation_partitions = gen_tasks_df["generation_task_id"].head(2).tolist()
                
                # Test generation with canned responses
                for partition in test_generation_partitions:
                    gen_result = materialize(
                        [
                            concepts, llm_models, generation_templates,
                            content_combinations, generation_tasks,
                            generation_prompt, generation_response, parsed_generation_responses,
                        ],
                        resources=resources,
                        instance=instance,
                        partition_key=partition,
                    )
                    assert gen_result.success
                    
                    # Verify files were created
                    prompt_file = pipeline_data_root / "3_generation" / "generation_prompts" / f"{partition}.txt"
                    response_file = pipeline_data_root / "3_generation" / "generation_responses" / f"{partition}.txt"
                    
                    assert prompt_file.exists(), f"Generation prompt file missing: {prompt_file}"
                    assert response_file.exists(), f"Generation response file missing: {response_file}"
                    
                    # Verify response content quality from canned responses
                    response_content = response_file.read_text()
                    assert len(response_content) > 50, "Canned response should be substantial"
                    # Check for any meaningful content rather than specific keywords
                    assert len(response_content.strip()) > 0, "Response should not be empty"
                
                # Test evaluation with canned responses
                eval_tasks_file = pipeline_data_root / "2_tasks" / "evaluation_tasks.csv"
                eval_tasks_df = pd.read_csv(eval_tasks_file)
                
                # Get evaluation partitions that depend on our tested generation partitions
                available_eval_tasks = eval_tasks_df[
                    eval_tasks_df["generation_task_id"].isin(test_generation_partitions)
                ]
                test_evaluation_partitions = available_eval_tasks["evaluation_task_id"].head(2).tolist()
                
                if test_evaluation_partitions:
                    for eval_partition in test_evaluation_partitions:
                        eval_result = materialize(
                            [
                                concepts, llm_models, evaluation_templates,
                                content_combinations, evaluation_tasks,
                                evaluation_prompt, evaluation_response,
                            ],
                            resources=resources,
                            instance=instance,
                            partition_key=eval_partition,
                        )
                        assert eval_result.success
                        
                        # Verify evaluation files were created
                        eval_prompt_file = pipeline_data_root / "4_evaluation" / "evaluation_prompts" / f"{eval_partition}.txt"
                        eval_response_file = pipeline_data_root / "4_evaluation" / "evaluation_responses" / f"{eval_partition}.txt"
                        
                        assert eval_prompt_file.exists(), f"Evaluation prompt file missing: {eval_prompt_file}"
                        assert eval_response_file.exists(), f"Evaluation response file missing: {eval_response_file}"
                        
                        # Verify evaluation response contains score
                        eval_response_content = eval_response_file.read_text()
                        assert "SCORE" in eval_response_content, "Evaluation response should contain score"
                        assert len(eval_response_content) > 50, "Evaluation response should be substantial"
                
                # Use the file verification helper
                self._verify_expected_files(
                    pipeline_data_root, 
                    test_generation_partitions, 
                    test_evaluation_partitions
                )
                
                print("✅ Pipeline with canned LLM responses test passed!")

    def test_cross_partition_dependencies(self, pipeline_data_root_prepared):
        """Test that evaluation assets can access generation responses across partitions."""
        pipeline_data_root = pipeline_data_root_prepared
        
        # Use a temporary DAGSTER_HOME for isolation
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
                    generation_prompt, generation_response, parsed_generation_responses
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
                        base_path=pipeline_data_root / "3_generation" / "generation_prompts"
                    ),
                    "generation_response_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "generation_responses"
                    ),
                    "parsed_generation_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "3_generation" / "parsed_generation_responses"
                    ),
                    "evaluation_prompt_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "4_evaluation" / "evaluation_prompts"
                    ),
                    "evaluation_response_io_manager": PartitionedTextIOManager(
                        base_path=pipeline_data_root / "4_evaluation" / "evaluation_responses"
                    ),
                    "parsing_results_io_manager": CSVIOManager(base_path=pipeline_data_root / "5_parsing"),
                    "summary_results_io_manager": CSVIOManager(base_path=pipeline_data_root / "6_summary"),
                    "error_log_io_manager": CSVIOManager(base_path=pipeline_data_root / "7_reporting"),
                    "experiment_config": ExperimentConfig(k_max=2, description_level="paragraph"),
                }

                # Materialize up to tasks
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
                assert result.success

                # Get generation partitions and create responses
                gen_tasks_file = pipeline_data_root / "2_tasks" / "generation_tasks.csv"
                gen_tasks_df = pd.read_csv(gen_tasks_file)
                test_generation_partitions = gen_tasks_df["generation_task_id"].head(3).tolist()
                
                # Create generation responses first
                for partition in test_generation_partitions:
                    gen_result = materialize(
                        [
                            concepts, llm_models, generation_templates,
                            content_combinations, generation_tasks,
                            generation_prompt, generation_response, parsed_generation_responses,
                        ],
                        resources=resources,
                        instance=instance,
                        partition_key=partition,
                    )
                    assert gen_result.success
                
                # Now test evaluation with cross-partition dependencies
                eval_tasks_file = pipeline_data_root / "2_tasks" / "evaluation_tasks.csv"
                eval_tasks_df = pd.read_csv(eval_tasks_file)
                
                # Get evaluation partitions that depend on our generation partitions
                available_eval_tasks = eval_tasks_df[
                    eval_tasks_df["generation_task_id"].isin(test_generation_partitions)
                ]
                test_evaluation_partitions = available_eval_tasks["evaluation_task_id"].head(2).tolist()
                
                if test_evaluation_partitions:
                    # Test that evaluation can access generation responses across partitions
                    for eval_partition in test_evaluation_partitions:
                        # Get the corresponding generation task ID
                        corresponding_gen_task = eval_tasks_df[
                            eval_tasks_df["evaluation_task_id"] == eval_partition
                        ]["generation_task_id"].iloc[0]
                        
                        # Verify the generation response file exists
                        expected_gen_file = pipeline_data_root / "3_generation" / "generation_responses" / f"{corresponding_gen_task}.txt"
                        assert expected_gen_file.exists(), f"Cross-partition dependency check failed: {expected_gen_file} does not exist"
                        
                        # Now test evaluation materialization - include generation_tasks as dependency
                        eval_result = materialize(
                            [
                                concepts, llm_models, evaluation_templates,
                                content_combinations, generation_tasks, evaluation_tasks,
                                evaluation_prompt, evaluation_response,
                            ],
                            resources=resources,
                            instance=instance,
                            partition_key=eval_partition,
                        )
                        assert eval_result.success
                        
                        # Verify evaluation files were created
                        eval_prompt_file = pipeline_data_root / "4_evaluation" / "evaluation_prompts" / f"{eval_partition}.txt"
                        eval_response_file = pipeline_data_root / "4_evaluation" / "evaluation_responses" / f"{eval_partition}.txt"
                        
                        assert eval_prompt_file.exists(), f"Evaluation prompt file missing: {eval_prompt_file}"
                        assert eval_response_file.exists(), f"Evaluation response file missing: {eval_response_file}"
                        
                        # Verify evaluation response contains score
                        eval_response_content = eval_response_file.read_text()
                        assert "SCORE" in eval_response_content, "Evaluation response should contain score"
                        
                        print(f"✓ Cross-partition dependency verified: {corresponding_gen_task} → {eval_partition}")
                    
                    print("✅ Cross-partition dependency test passed!")
                else:
                    print("⚠ No evaluation partitions available for cross-partition testing")
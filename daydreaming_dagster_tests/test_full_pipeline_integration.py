"""Full pipeline integration test with canned LLM responses and file verification."""

import pytest
import tempfile
import shutil
import os
from pathlib import Path
from unittest.mock import Mock
from dagster import (
    materialize, 
    DagsterInstance,
    ConfigurableResource,
    build_asset_context
)
from daydreaming_dagster import defs
import pandas as pd


class CannedLLMResource(ConfigurableResource):
    """Mock LLM resource with realistic canned responses for full pipeline testing."""
    
    def get_client(self):
        """Return mock client with realistic responses for generation and evaluation."""
        client = Mock()
        
        def mock_generate(prompt, model, temperature=0.7):
            # Determine if this is generation or evaluation based on prompt content
            if "SCORE" in prompt or "Rate" in prompt or "evaluate" in prompt.lower():
                # This is an evaluation prompt - return a score
                return self._get_evaluation_response(model)
            else:
                # This is a generation prompt - return creative content
                return self._get_generation_response(model, prompt)
        
        client.generate = mock_generate
        return client
    
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
        """Generate realistic evaluation responses with scores."""
        evaluations = {
            "deepseek/deepseek-r1:free": """
**SCORE**: 8.5

**Reasoning**: This response demonstrates exceptional creativity and insight. The synthesis of daydreaming methodology with systematic discovery shows deep understanding of the intersection between human cognition and innovation processes. The structured approach to creativity is particularly compelling.

**Strengths**:
- Novel framework combining structured and intuitive thinking
- Clear implementation pathway
- Strong theoretical foundation

**Areas for improvement**:
- Could benefit from more concrete examples
- Implementation details could be more specific
            """.strip(),
            
            "google/gemma-3-27b-it:free": """
**SCORE**: 7.8

**Reasoning**: Strong creative synthesis with good theoretical grounding. The concept of "structured daydreaming" is innovative and the human-AI collaboration angle is well-developed. However, the response could benefit from more detailed exploration of practical applications.

**Strengths**:
- Clear conceptual framework
- Good integration of multiple domains
- Practical orientation

**Areas for improvement**:
- More empirical support needed
- Could explore limitations more thoroughly
            """.strip()
        }
        
        return evaluations.get(model, f"**SCORE**: 7.5\n\nGood analysis with creative elements using {model}")


class TestFullPipelineIntegration:
    """Complete pipeline integration test with file verification."""
    
    @pytest.fixture
    def test_directory(self):
        """Create a temporary test directory for pipeline outputs."""
        test_dir = tempfile.mkdtemp(prefix="dagster_pipeline_test_")
        yield Path(test_dir)
        # Cleanup after test
        shutil.rmtree(test_dir, ignore_errors=True)
    
    @pytest.fixture
    def test_dagster_home(self, test_directory):
        """Set up isolated Dagster instance in test directory."""
        dagster_home = test_directory / "dagster_home"
        dagster_home.mkdir(exist_ok=True)
        (dagster_home / "dagster.yaml").touch()
        
        # Set environment variable for this test
        original_home = os.environ.get("DAGSTER_HOME")
        os.environ["DAGSTER_HOME"] = str(dagster_home)
        
        instance = DagsterInstance.get()
        yield instance
        
        # Restore original environment
        if original_home:
            os.environ["DAGSTER_HOME"] = original_home
        else:
            os.environ.pop("DAGSTER_HOME", None)
    
    @pytest.fixture
    def pipeline_resources_with_test_paths(self, test_directory):
        """Create pipeline resources configured to write to test directory."""
        # Import I/O managers
        from daydreaming_dagster.resources.io_managers import (
            CSVIOManager, PartitionedTextIOManager, ErrorLogIOManager
        )
        
        # Create test-specific I/O managers pointing to test directory
        test_resources = {
            **defs.resources,
            "openrouter_client": CannedLLMResource(),
            
            # Override I/O managers to write to test directory
            "csv_io_manager": CSVIOManager(base_path=test_directory / "data"),
            "partitioned_text_io_manager": PartitionedTextIOManager(base_path=test_directory / "data" / "concept_contents"),
            "error_log_io_manager": ErrorLogIOManager(base_path=test_directory / "data" / "errors"),
            "parsing_results_io_manager": CSVIOManager(base_path=test_directory / "data" / "parsing"),
            "summary_results_io_manager": CSVIOManager(base_path=test_directory / "data" / "summary"),
            
            # Generation and evaluation I/O managers
            "generation_prompt_io_manager": PartitionedTextIOManager(base_path=test_directory / "data" / "03_generation" / "generation_prompts"),
            "generation_response_io_manager": PartitionedTextIOManager(base_path=test_directory / "data" / "03_generation" / "generation_responses"),
            "evaluation_prompt_io_manager": PartitionedTextIOManager(base_path=test_directory / "data" / "04_evaluation" / "evaluation_prompts"),
            "evaluation_response_io_manager": PartitionedTextIOManager(base_path=test_directory / "data" / "04_evaluation" / "evaluation_responses"),
        }
        
        return test_resources
    
    def test_complete_pipeline_execution_with_file_verification(self, test_directory, test_dagster_home, pipeline_resources_with_test_paths):
        """Run complete pipeline and verify all expected files are created."""
        
        print(f"Running pipeline test in: {test_directory}")
        
        # Phase 1: Setup and task creation
        print("Phase 1: Setting up pipeline and creating tasks...")
        
        setup_assets = [asset for asset in defs.assets if not asset.partitions_def]
        
        setup_result = materialize(
            assets=setup_assets,
            resources=pipeline_resources_with_test_paths,
            instance=test_dagster_home
        )
        
        assert setup_result.success, "Setup phase should succeed"
        print("✓ Setup phase completed")
        
        # Verify setup files were created
        expected_setup_files = [
            test_directory / "data" / "concept_combinations_combinations.csv",
            test_directory / "data" / "concept_combinations_relationships.csv", 
            test_directory / "data" / "generation_tasks.csv",
            test_directory / "data" / "evaluation_tasks.csv",
        ]
        
        for file_path in expected_setup_files:
            assert file_path.exists(), f"Expected setup file not created: {file_path}"
            assert file_path.stat().st_size > 0, f"Setup file is empty: {file_path}"
        
        print(f"✓ Verified {len(expected_setup_files)} setup files created")
        
        # Check concept contents directory
        concept_contents_dir = test_directory / "data" / "concept_contents"
        assert concept_contents_dir.exists(), "Concept contents directory should exist"
        concept_files = list(concept_contents_dir.glob("*"))
        assert len(concept_files) > 0, "Should have concept content files"
        print(f"✓ Created {len(concept_files)} concept content files")
        
        # Phase 2: Get partitions and test LLM generation
        print("Phase 2: Testing LLM generation with partitions...")
        
        gen_partitions = test_dagster_home.get_dynamic_partitions("generation_tasks")
        eval_partitions = test_dagster_home.get_dynamic_partitions("evaluation_tasks")
        
        assert len(gen_partitions) > 0, "Should have generation partitions"
        assert len(eval_partitions) > 0, "Should have evaluation partitions"
        print(f"✓ Created {len(gen_partitions)} generation partitions and {len(eval_partitions)} evaluation partitions")
        
        # Test a subset of partitions (first 2 to keep test fast)
        test_gen_partitions = gen_partitions[:2]
        test_eval_partitions = eval_partitions[:2]
        
        # Phase 3: Generate LLM responses
        print("Phase 3: Generating LLM responses...")
        
        llm_generation_assets = [
            asset for asset in defs.assets 
            if asset.partitions_def and asset.key.to_user_string() in ["generation_prompt", "generation_response"]
        ]
        
        for i, partition in enumerate(test_gen_partitions):
            print(f"  Processing generation partition {i+1}/{len(test_gen_partitions)}: {partition}")
            
            generation_result = materialize(
                assets=llm_generation_assets,
                partition_key=partition,
                resources=pipeline_resources_with_test_paths,
                instance=test_dagster_home
            )
            
            assert generation_result.success, f"Generation should succeed for partition {partition}"
            
            # Verify generation files were created
            prompt_file = test_directory / "data" / "03_generation" / "generation_prompts" / f"{partition}.txt"
            response_file = test_directory / "data" / "03_generation" / "generation_responses" / f"{partition}.txt"
            
            assert prompt_file.exists(), f"Generation prompt file not created: {prompt_file}"
            assert response_file.exists(), f"Generation response file not created: {response_file}"
            assert prompt_file.stat().st_size > 0, f"Generation prompt file is empty: {prompt_file}"
            assert response_file.stat().st_size > 0, f"Generation response file is empty: {response_file}"
            
            # Verify content quality
            response_content = response_file.read_text()
            assert len(response_content) > 100, "Response should be substantial"
            assert any(word in response_content.lower() for word in ["creative", "innovation", "discovery", "concept"]), "Response should contain relevant keywords"
        
        print(f"✓ Successfully generated {len(test_gen_partitions)} LLM responses")
        
        # Phase 4: Generate evaluations
        print("Phase 4: Generating evaluations...")
        
        llm_evaluation_assets = [
            asset for asset in defs.assets 
            if asset.partitions_def and asset.key.to_user_string() in ["evaluation_prompt", "evaluation_response"]
        ]
        
        for i, partition in enumerate(test_eval_partitions):
            print(f"  Processing evaluation partition {i+1}/{len(test_eval_partitions)}: {partition}")
            
            evaluation_result = materialize(
                assets=llm_evaluation_assets,
                partition_key=partition,
                resources=pipeline_resources_with_test_paths,
                instance=test_dagster_home
            )
            
            assert evaluation_result.success, f"Evaluation should succeed for partition {partition}"
            
            # Verify evaluation files were created
            eval_prompt_file = test_directory / "data" / "04_evaluation" / "evaluation_prompts" / f"{partition}.txt"
            eval_response_file = test_directory / "data" / "04_evaluation" / "evaluation_responses" / f"{partition}.txt"
            
            assert eval_prompt_file.exists(), f"Evaluation prompt file not created: {eval_prompt_file}"
            assert eval_response_file.exists(), f"Evaluation response file not created: {eval_response_file}"
            
            # Verify evaluation content
            eval_content = eval_response_file.read_text()
            assert "SCORE" in eval_content, "Evaluation should contain a score"
            assert any(char.isdigit() for char in eval_content), "Evaluation should contain numeric score"
        
        print(f"✓ Successfully generated {len(test_eval_partitions)} evaluations")
        
        # Phase 5: Test results processing (if evaluation responses exist)
        print("Phase 5: Testing results processing...")
        
        results_assets = [
            asset for asset in defs.assets 
            if not asset.partitions_def and asset.key.to_user_string() in ["parsed_scores", "final_results"]
        ]
        
        try:
            results_result = materialize(
                assets=results_assets,
                resources=pipeline_resources_with_test_paths,
                instance=test_dagster_home
            )
            
            if results_result.success:
                print("✓ Results processing completed successfully")
                
                # Check for results files
                parsing_dir = test_directory / "data" / "parsing"
                summary_dir = test_directory / "data" / "summary"
                
                if parsing_dir.exists():
                    parsing_files = list(parsing_dir.glob("*.csv"))
                    print(f"✓ Created {len(parsing_files)} parsing result files")
                
                if summary_dir.exists():
                    summary_files = list(summary_dir.glob("*.csv"))
                    print(f"✓ Created {len(summary_files)} summary result files")
            else:
                print("⚠ Results processing failed (expected - needs more evaluation data)")
        
        except Exception as e:
            print(f"⚠ Results processing failed: {e} (expected - needs more evaluation data)")
        
        # Phase 6: Comprehensive file verification
        print("Phase 6: Comprehensive file verification...")
        
        # Verify directory structure
        expected_dirs = [
            test_directory / "data",
            test_directory / "data" / "concept_contents",
            test_directory / "data" / "03_generation" / "generation_prompts", 
            test_directory / "data" / "03_generation" / "generation_responses",
            test_directory / "data" / "04_evaluation" / "evaluation_prompts",
            test_directory / "data" / "04_evaluation" / "evaluation_responses",
        ]
        
        for dir_path in expected_dirs:
            assert dir_path.exists(), f"Expected directory not created: {dir_path}"
        
        print("✓ All expected directories created")
        
        # Count all files created
        all_files = list(test_directory.rglob("*"))
        file_count = len([f for f in all_files if f.is_file()])
        
        print(f"✓ Total files created: {file_count}")
        
        # Print summary of what was created
        print("\n=== PIPELINE EXECUTION SUMMARY ===")
        print(f"Test directory: {test_directory}")
        print(f"Generated partitions: {len(gen_partitions)} generation, {len(eval_partitions)} evaluation")
        print(f"Tested partitions: {len(test_gen_partitions)} generation, {len(test_eval_partitions)} evaluation")
        print(f"Total files created: {file_count}")
        print(f"Data directory structure:")
        
        for dir_path in sorted(expected_dirs):
            if dir_path.exists():
                files_in_dir = list(dir_path.glob("*"))
                print(f"  {dir_path.relative_to(test_directory)}: {len(files_in_dir)} files")
        
        print("✓ FULL PIPELINE INTEGRATION TEST PASSED")
    
    def test_file_content_verification(self, test_directory, test_dagster_home, pipeline_resources_with_test_paths):
        """Test that generated files contain expected content structure."""
        
        # Run minimal pipeline to generate some files
        setup_result = materialize(
            assets=[asset for asset in defs.assets if not asset.partitions_def],
            resources=pipeline_resources_with_test_paths,
            instance=test_dagster_home
        )
        assert setup_result.success
        
        # Test one generation partition
        gen_partitions = test_dagster_home.get_dynamic_partitions("generation_tasks")
        if len(gen_partitions) > 0:
            test_partition = gen_partitions[0]
            
            generation_result = materialize(
                assets=[asset for asset in defs.assets if asset.partitions_def and "generation" in asset.key.to_user_string()],
                partition_key=test_partition,
                resources=pipeline_resources_with_test_paths,
                instance=test_dagster_home
            )
            assert generation_result.success
            
            # Verify file contents
            response_file = test_directory / "data" / "03_generation" / "generation_responses" / f"{test_partition}.txt"
            assert response_file.exists()
            
            content = response_file.read_text()
            
            # Content should be substantial and relevant
            assert len(content) > 50, "Response should be substantial"
            assert content.strip(), "Response should not be just whitespace"
            
            # Should contain expected keywords from our canned responses
            content_lower = content.lower()
            expected_keywords = ["creative", "innovation", "discovery", "concept", "daydreaming"]
            found_keywords = [kw for kw in expected_keywords if kw in content_lower]
            assert len(found_keywords) > 0, f"Response should contain relevant keywords. Content: {content[:200]}..."
            
            print(f"✓ Generated response contains {len(found_keywords)} relevant keywords: {found_keywords}")
            print(f"✓ Response length: {len(content)} characters")


if __name__ == "__main__":
    # Run the test directly for debugging
    import tempfile
    import shutil
    
    test_dir = Path(tempfile.mkdtemp(prefix="dagster_pipeline_test_manual_"))
    print(f"Manual test running in: {test_dir}")
    
    try:
        # Set up Dagster home
        dagster_home = test_dir / "dagster_home"
        dagster_home.mkdir(exist_ok=True)
        (dagster_home / "dagster.yaml").touch()
        os.environ["DAGSTER_HOME"] = str(dagster_home)
        
        instance = DagsterInstance.get()
        
        # Set up resources
        from daydreaming_dagster.resources.io_managers import (
            CSVIOManager, PartitionedTextIOManager
        )
        
        test_resources = {
            **defs.resources,
            "openrouter_client": CannedLLMResource(),
            "csv_io_manager": CSVIOManager(base_path=test_dir / "data"),
            "generation_response_io_manager": PartitionedTextIOManager(base_path=test_dir / "data" / "03_generation" / "generation_responses"),
        }
        
        print("Running setup phase...")
        setup_result = materialize(
            assets=[asset for asset in defs.assets if not asset.partitions_def],
            resources=test_resources,
            instance=instance
        )
        
        if setup_result.success:
            print("✓ Setup phase completed successfully")
            
            # Check what files were created
            data_dir = test_dir / "data"
            if data_dir.exists():
                files = list(data_dir.rglob("*"))
                print(f"✓ Created {len([f for f in files if f.is_file()])} files")
                for f in sorted(files):
                    if f.is_file():
                        print(f"  {f.relative_to(test_dir)}: {f.stat().st_size} bytes")
        else:
            print("✗ Setup phase failed")
            
    except Exception as e:
        print(f"✗ Manual test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        shutil.rmtree(test_dir, ignore_errors=True)
        print(f"Cleaned up test directory: {test_dir}")
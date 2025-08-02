"""
Tests for Dagster partition materialization issues.

This test suite validates that partitioned assets can be properly materialized
and that we handle the dynamic partition lifecycle correctly.
"""

import pytest
import pandas as pd
from dagster import (
    build_asset_context, 
    DagsterInstance, 
    materialize, 
    AssetSelection
)
from daydreaming_dagster import defs
from daydreaming_dagster.assets.partitions import (
    generation_tasks_partitions,
    evaluation_tasks_partitions,
    task_definitions
)


class TestPartitionMaterialization:
    """Test partition-related materialization issues."""
    
    def test_partitioned_assets_require_partition_specification(self):
        """
        Test that reproduces the error: partitioned assets need --partition option.
        
        This reproduces the CLI error:
        CheckError: Failure condition: Asset has partitions, but no '--partition' option was provided
        """
        # Get partitioned assets from our definitions
        partitioned_assets = []
        for asset_def in defs.assets:
            if asset_def.partitions_def is not None:
                partitioned_assets.append(asset_def.key.to_user_string())
        
        # We should have partitioned assets
        assert len(partitioned_assets) > 0, "Expected to find partitioned assets"
        print(f"Found partitioned assets: {partitioned_assets}")
        
        # Try to materialize a partitioned asset without specifying partitions
        # This should fail in the same way as the CLI command
        partitioned_assets_list = [
            asset_def for asset_def in defs.assets 
            if asset_def.partitions_def is not None
        ]
        
        # This simulates what happens when we try to materialize partitioned assets
        # without specifying partitions - it should fail
        with pytest.raises(Exception) as exc_info:
            # Try to materialize without partition specification
            materialize(
                assets=partitioned_assets_list,
                instance=DagsterInstance.ephemeral()
            )
        
        # The error should mention partitions
        error_message = str(exc_info.value)
        assert "partition" in error_message.lower(), f"Expected partition-related error, got: {error_message}"
    
    def test_dynamic_partitions_not_registered_initially(self):
        """
        Test that dynamic partitions don't exist until task_definitions asset is materialized.
        This is why we can't materialize partitioned assets immediately.
        """
        instance = DagsterInstance.ephemeral()
        
        # Check that our dynamic partitions don't exist initially
        gen_partitions = instance.get_dynamic_partitions(generation_tasks_partitions.name)
        eval_partitions = instance.get_dynamic_partitions(evaluation_tasks_partitions.name)
        
        assert len(gen_partitions) == 0, "Generation partitions should not exist initially"
        assert len(eval_partitions) == 0, "Evaluation partitions should not exist initially"
    
    def test_task_definitions_creates_partitions(self):
        """
        Test that materializing task_definitions creates the dynamic partitions.
        """
        from daydreaming_dagster.assets.core import generation_tasks, evaluation_tasks
        from daydreaming_dagster.resources.api_client import ExperimentConfig
        import pandas as pd
        
        # Create mock data for task creation
        mock_context = build_asset_context(
            resources={"experiment_config": ExperimentConfig()},
            instance=DagsterInstance.ephemeral()
        )
        
        # Mock the task data
        mock_generation_tasks = pd.DataFrame([
            {"generation_task_id": "test_gen_001", "combo_id": "combo_001", "generation_model": "test_model"},
            {"generation_task_id": "test_gen_002", "combo_id": "combo_002", "generation_model": "test_model"}
        ])
        
        mock_evaluation_tasks = pd.DataFrame([
            {"evaluation_task_id": "test_eval_001", "generation_task_id": "test_gen_001", "evaluation_model": "eval_model"},
            {"evaluation_task_id": "test_eval_002", "generation_task_id": "test_gen_002", "evaluation_model": "eval_model"}
        ])
        
        # Call task_definitions to register partitions
        result = task_definitions(mock_context, mock_generation_tasks, mock_evaluation_tasks)
        
        # Verify partitions were created
        instance = mock_context.instance
        gen_partitions = instance.get_dynamic_partitions(generation_tasks_partitions.name)
        eval_partitions = instance.get_dynamic_partitions(evaluation_tasks_partitions.name)
        
        assert len(gen_partitions) == 2, f"Expected 2 generation partitions, got {len(gen_partitions)}"
        assert len(eval_partitions) == 2, f"Expected 2 evaluation partitions, got {len(eval_partitions)}"
        
        assert "test_gen_001" in gen_partitions
        assert "test_gen_002" in gen_partitions
        assert "test_eval_001" in eval_partitions
        assert "test_eval_002" in eval_partitions
    
    def test_partition_materialization_workflow(self):
        """
        Test the correct workflow for materializing partitioned assets:
        1. Materialize non-partitioned assets first (including task_definitions)
        2. Then materialize specific partitions
        """
        # Step 1: Get non-partitioned assets (should work)
        non_partitioned_assets = [
            asset_def for asset_def in defs.assets 
            if asset_def.partitions_def is None
        ]
        
        assert len(non_partitioned_assets) > 0, "Expected to find non-partitioned assets"
        
        # These should be materializable without partition specification
        # (We won't actually materialize them here to keep test fast)
        
        # Step 2: Verify that partitioned assets exist
        partitioned_assets = [
            asset_def for asset_def in defs.assets 
            if asset_def.partitions_def is not None
        ]
        
        assert len(partitioned_assets) > 0, "Expected to find partitioned assets"
        
        # Step 3: Verify that partitioned assets are the LLM generation/evaluation assets
        partitioned_asset_names = [asset_def.key.to_user_string() for asset_def in partitioned_assets]
        expected_partitioned = ["generation_prompt", "generation_response", "evaluation_prompt", "evaluation_response"]
        
        for expected in expected_partitioned:
            assert expected in partitioned_asset_names, f"Expected {expected} to be partitioned"
    
    def test_group_selection_with_partitions_issue(self):
        """
        Test that reproduces the specific CLI issue with group selection.
        
        When we select "+group:llm_generation", it includes partitioned assets
        which require partition specification.
        """
        # Get assets in llm_generation group
        llm_generation_assets = [
            asset_def for asset_def in defs.assets 
            if hasattr(asset_def, 'group_names_by_key') and 
            any('llm_generation' in groups for groups in asset_def.group_names_by_key.values())
        ]
        
        # Alternative way to find assets by group
        if not llm_generation_assets:
            llm_generation_assets = [
                asset_def for asset_def in defs.assets
                if getattr(asset_def.op, 'tags', {}).get('dagster/group', {}) == 'llm_generation'
            ]
        
        # Manual check - we know these should be in llm_generation group
        llm_gen_asset_names = []
        for asset_def in defs.assets:
            asset_name = asset_def.key.to_user_string()
            if asset_name in ['generation_prompt', 'generation_response']:
                llm_gen_asset_names.append(asset_name)
                
                # Verify these are partitioned
                assert asset_def.partitions_def is not None, f"{asset_name} should be partitioned"
        
        assert len(llm_gen_asset_names) == 2, f"Expected 2 LLM generation assets, found: {llm_gen_asset_names}"
        
        # This demonstrates the issue: trying to materialize a group that contains
        # partitioned assets will fail without partition specification


class TestPartitionLifecycle:
    """Test the complete partition lifecycle."""
    
    def test_partition_creation_workflow(self):
        """
        Test the complete workflow of creating and using partitions.
        """
        instance = DagsterInstance.ephemeral()
        
        # Step 1: Verify no partitions exist initially
        assert len(instance.get_dynamic_partitions(generation_tasks_partitions.name)) == 0
        
        # Step 2: Create mock tasks (this would normally come from materializing core assets)
        mock_generation_tasks = pd.DataFrame([
            {"generation_task_id": "combo_001_template_model", "combo_id": "combo_001"}
        ])
        mock_evaluation_tasks = pd.DataFrame([
            {"evaluation_task_id": "eval_001", "generation_task_id": "combo_001_template_model"}
        ])
        
        # Step 3: Register partitions (this happens in task_definitions asset)
        instance.add_dynamic_partitions(
            generation_tasks_partitions.name,
            mock_generation_tasks["generation_task_id"].tolist()
        )
        
        # Step 4: Verify partitions were created
        gen_partitions = instance.get_dynamic_partitions(generation_tasks_partitions.name)
        assert len(gen_partitions) == 1
        assert "combo_001_template_model" in gen_partitions
        
        # Step 5: Now partitioned assets could be materialized with specific partitions
        # (We won't actually do this in the test to keep it fast)


if __name__ == "__main__":
    # Run tests directly for debugging
    test_instance = TestPartitionMaterialization()
    
    print("Testing partition materialization issues...")
    
    try:
        test_instance.test_partitioned_assets_require_partition_specification()
        print("✓ Partition specification requirement test passed")
    except Exception as e:
        print(f"✗ Partition specification test failed: {e}")
    
    try:
        test_instance.test_dynamic_partitions_not_registered_initially()
        print("✓ Dynamic partitions initial state test passed")
    except Exception as e:
        print(f"✗ Dynamic partitions initial test failed: {e}")
    
    try:
        test_instance.test_task_definitions_creates_partitions()
        print("✓ Task definitions creates partitions test passed")
    except Exception as e:
        print(f"✗ Task definitions test failed: {e}")
    
    try:
        test_instance.test_partition_materialization_workflow()
        print("✓ Partition materialization workflow test passed")
    except Exception as e:
        print(f"✗ Partition workflow test failed: {e}")
    
    try:
        test_instance.test_group_selection_with_partitions_issue()
        print("✓ Group selection with partitions test passed")
    except Exception as e:
        print(f"✗ Group selection test failed: {e}")
    
    lifecycle_test = TestPartitionLifecycle()
    try:
        lifecycle_test.test_partition_creation_workflow()
        print("✓ Partition creation workflow test passed")
    except Exception as e:
        print(f"✗ Partition creation workflow test failed: {e}")
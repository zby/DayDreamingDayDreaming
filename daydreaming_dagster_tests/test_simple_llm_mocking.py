"""Simple integration test demonstrating LLM mocking in Dagster pipeline."""

import pytest
import pandas as pd
from unittest.mock import Mock
from dagster import (
    build_asset_context, 
    materialize, 
    DagsterInstance,
    ConfigurableResource
)
from daydreaming_dagster import defs
from daydreaming_dagster.assets.llm_prompts_responses import generation_response


# Global shared mock client for tracking calls across instances
_SHARED_MOCK_CLIENT = None

class SimpleMockLLMResource(ConfigurableResource):
    """Simple mock LLM resource for testing."""
    
    mock_response: str = "Default mock response"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        global _SHARED_MOCK_CLIENT
        # Create or reuse the shared mock client
        if _SHARED_MOCK_CLIENT is None:
            _SHARED_MOCK_CLIENT = Mock()
        _SHARED_MOCK_CLIENT.generate.return_value = self.mock_response
    
    def get_client(self):
        """Return the shared mock client instance for tracking calls."""
        return _SHARED_MOCK_CLIENT
    
    @classmethod
    def reset_mock(cls):
        """Reset the shared mock for clean test isolation."""
        global _SHARED_MOCK_CLIENT
        _SHARED_MOCK_CLIENT = None


class TestSimpleLLMMocking:
    """Simple tests demonstrating LLM mocking patterns."""
    
    def test_asset_with_mock_llm_resource(self):
        """Test individual asset with mock LLM resource."""
        
        # Reset the shared mock for clean test
        SimpleMockLLMResource.reset_mock()
        
        # Create mock LLM resource with specific response
        mock_llm = SimpleMockLLMResource(
            mock_response="This is a creative analysis of the concept combination."
        )
        
        # Create asset context
        context = build_asset_context(
            resources={"openrouter_client": mock_llm},
            partition_key="test_task_001"
        )
        
        # Create mock inputs
        mock_prompt = "Analyze these concepts: A + B"
        mock_tasks = pd.DataFrame([{
            "generation_task_id": "test_task_001",
            "generation_model": "test-model"
        }])
        
        # Call the asset function directly
        result = generation_response(context, mock_prompt, mock_tasks)
        
        # Verify the mock response was returned
        assert result == "This is a creative analysis of the concept combination."
        
        # Verify the mock was called with correct parameters
        client = mock_llm.get_client()
        client.generate.assert_called_once_with(
            mock_prompt, 
            model="test-model"
        )
        
        print(f"✓ Asset returned mock response: {result}")
    
    def test_pipeline_with_mock_llm_resources(self):
        """Test pipeline components with mock LLM resources."""
        
        # Create mock resources
        creative_mock = SimpleMockLLMResource(
            mock_response="Creative response about daydreaming and innovation."
        )
        
        evaluator_mock = SimpleMockLLMResource(
            mock_response="**SCORE**: 8.5\n\nExcellent creativity demonstrated."
        )
        
        # Test the creative generator
        gen_context = build_asset_context(
            resources={"openrouter_client": creative_mock},
            partition_key="creative_test"
        )
        
        gen_tasks = pd.DataFrame([{
            "generation_task_id": "creative_test",
            "generation_model": "creative-model"
        }])
        
        gen_result = generation_response(gen_context, "Generate ideas", gen_tasks)
        assert "daydreaming" in gen_result
        print(f"✓ Creative generator: {gen_result}")
        
        # Test the evaluator (using same asset but different mock)
        eval_context = build_asset_context(
            resources={"openrouter_client": evaluator_mock},
            partition_key="eval_test" 
        )
        
        eval_tasks = pd.DataFrame([{
            "generation_task_id": "eval_test",
            "generation_model": "eval-model"
        }])
        
        eval_result = generation_response(eval_context, "Rate this response", eval_tasks)
        assert "SCORE" in eval_result
        assert "8.5" in eval_result
        print(f"✓ Evaluator: {eval_result}")
    
    def test_full_pipeline_with_override_resources(self):
        """Test materializing pipeline assets with overridden mock resources."""
        
        # Reset shared mock
        SimpleMockLLMResource.reset_mock()
        
        # Create mock LLM resource
        mock_llm = SimpleMockLLMResource(
            mock_response="Mock LLM generated this response for testing."
        )
        
        # Override the real LLM resource with mock
        test_resources = {
            **defs.resources,  # Keep all other resources
            "openrouter_client": mock_llm  # Override LLM with mock
        }
        
        # Create ephemeral instance
        instance = DagsterInstance.ephemeral()
        
        try:
            # Step 1: Materialize setup assets (should work without LLM)
            # Exclude assets that depend on LLM responses
            setup_assets = [
                asset for asset in defs.assets 
                if not asset.partitions_def 
                and asset.key.to_user_string() not in ["parsed_scores", "final_results"]
            ]
            
            setup_result = materialize(
                assets=setup_assets,
                resources=test_resources,
                instance=instance
            )
            
            assert setup_result.success, "Setup phase should succeed"
            print("✓ Setup phase completed successfully")
            
            # Step 2: Check that partitions were created
            gen_partitions = instance.get_dynamic_partitions("generation_tasks")
            assert len(gen_partitions) > 0, "Should create generation partitions"
            print(f"✓ Created {len(gen_partitions)} generation partitions")
            
            # For this test, we'll just verify the setup worked
            # Testing the partitioned assets is complex due to dependencies
            print("✓ Full pipeline setup test completed with mock LLM resources")
            
        except Exception as e:
            print(f"Pipeline test failed: {e}")
            raise
    
    def test_different_mock_responses_for_different_models(self):
        """Test that different models can have different mock responses."""
        
        model_responses = {
            "creative-model": "Highly creative and innovative response",
            "analytical-model": "Systematic and analytical response",
            "fast-model": "Quick response"
        }
        
        for model, expected_response in model_responses.items():
            mock_llm = SimpleMockLLMResource(mock_response=expected_response)
            
            context = build_asset_context(
                resources={"openrouter_client": mock_llm},
                partition_key=f"test_{model}"
            )
            
            tasks = pd.DataFrame([{
                "generation_task_id": f"test_{model}",
                "generation_model": model
            }])
            
            result = generation_response(context, "test prompt", tasks)
            assert result == expected_response
            print(f"✓ {model}: {result}")
    
    def test_mock_error_scenarios(self):
        """Test error handling with mock LLMs."""
        
        # Mock that raises an exception
        class FailingMockLLMResource(ConfigurableResource):
            def get_client(self):
                client = Mock()
                client.generate.side_effect = Exception("Mock API error")
                return client
        
        failing_mock = FailingMockLLMResource()
        
        context = build_asset_context(
            resources={"openrouter_client": failing_mock},
            partition_key="failing_test"
        )
        
        tasks = pd.DataFrame([{
            "generation_task_id": "failing_test",
            "generation_model": "failing-model"
        }])
        
        # Should propagate the exception
        with pytest.raises(Exception, match="Mock API error"):
            generation_response(context, "test prompt", tasks)
        
        print("✓ Error handling test passed")


if __name__ == "__main__":
    # Run tests directly
    test_instance = TestSimpleLLMMocking()
    
    print("Testing simple LLM mocking patterns...")
    
    test_methods = [
        "test_asset_with_mock_llm_resource",
        "test_pipeline_with_mock_llm_resources", 
        "test_full_pipeline_with_override_resources",
        "test_different_mock_responses_for_different_models",
        "test_mock_error_scenarios"
    ]
    
    for method_name in test_methods:
        try:
            method = getattr(test_instance, method_name)
            method()
            print(f"✓ {method_name} passed")
        except Exception as e:
            print(f"✗ {method_name} failed: {e}")
            import traceback
            traceback.print_exc()
        print()
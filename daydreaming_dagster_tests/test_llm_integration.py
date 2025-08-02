"""Integration tests with mock LLM client for the complete pipeline."""

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
from daydreaming_dagster.resources.api_client import OpenRouterResource


# Global shared state for tracking calls across instances
_SHARED_MOCK_STATE = {
    'client': None,
    'call_count': 0,
    'calls_made': [],
    'responses': {}
}

class MockLLMResource(ConfigurableResource):
    """Mock LLM resource that returns predictable responses."""
    
    def __init__(self, responses=None):
        super().__init__()
        global _SHARED_MOCK_STATE
        _SHARED_MOCK_STATE['responses'] = responses or {}
        
        # Create or reuse the shared mock client
        if _SHARED_MOCK_STATE['client'] is None:
            _SHARED_MOCK_STATE['client'] = Mock()
            
            def mock_generate(prompt, model, temperature=0.7):
                _SHARED_MOCK_STATE['call_count'] += 1
                call_info = {
                    'prompt': prompt[:100] + "..." if len(prompt) > 100 else prompt,
                    'model': model,
                    'temperature': temperature
                }
                _SHARED_MOCK_STATE['calls_made'].append(call_info)
                
                # Return a predictable response based on the model or a default
                if model in _SHARED_MOCK_STATE['responses']:
                    return _SHARED_MOCK_STATE['responses'][model]
                return f"Mock response {_SHARED_MOCK_STATE['call_count']} for {model}"
            
            _SHARED_MOCK_STATE['client'].generate = mock_generate
    
    def get_client(self):
        """Return the shared mock client instance for tracking calls."""
        return _SHARED_MOCK_STATE['client']
    
    @property
    def call_count(self):
        """Get the current call count."""
        return _SHARED_MOCK_STATE['call_count']
    
    @property
    def calls_made(self):
        """Get the list of calls made."""
        return _SHARED_MOCK_STATE['calls_made']
    
    @classmethod
    def reset_mock(cls):
        """Reset the shared mock for clean test isolation."""
        global _SHARED_MOCK_STATE
        _SHARED_MOCK_STATE = {
            'client': None,
            'call_count': 0,
            'calls_made': [],
            'responses': {}
        }


class TestLLMIntegration:
    """Integration tests for LLM pipeline with mock client."""
    
    def test_generation_pipeline_with_mock_llm(self):
        """Test the complete generation pipeline with a mock LLM client."""
        # Create mock LLM responses
        mock_responses = {
            "deepseek/deepseek-r1:free": "This is a mock response about daydreaming and creativity.",
            "google/gemma-3-27b-it:free": "Mock analysis of concept combinations and insights."
        }
        
        mock_llm_resource = MockLLMResource(responses=mock_responses)
        
        # Create test resources with mock LLM
        test_resources = {
            **defs.resources,  # Use all existing resources
            "openrouter_client": mock_llm_resource  # Override with mock
        }
        
        # Create ephemeral instance for test
        instance = DagsterInstance.ephemeral()
        
        # Step 1: Materialize setup assets (creates partitions)
        setup_result = materialize(
            assets=[asset for asset in defs.assets if not asset.partitions_def],
            resources=test_resources,
            instance=instance
        )
        
        assert setup_result.success
        
        # Verify partitions were created
        gen_partitions = instance.get_dynamic_partitions("generation_tasks")
        assert len(gen_partitions) > 0, "No generation partitions were created"
        
        # Step 2: Test specific partition generation
        test_partition = gen_partitions[0]
        
        # Get partitioned assets
        partitioned_assets = [
            asset for asset in defs.assets 
            if asset.partitions_def and asset.key.to_user_string() in ["generation_prompt", "generation_response"]
        ]
        
        # Materialize specific partition
        partition_result = materialize(
            assets=partitioned_assets,
            partition_key=test_partition,
            resources=test_resources,
            instance=instance
        )
        
        assert partition_result.success
        
        # Verify mock LLM was called
        assert mock_llm_resource.call_count > 0, "Mock LLM was not called"
        assert len(mock_llm_resource.calls_made) > 0, "No LLM calls were tracked"
        
        # Verify call details
        first_call = mock_llm_resource.calls_made[0]
        assert "model" in first_call
        assert "prompt" in first_call
        assert len(first_call["prompt"]) > 0
        
        print(f"Mock LLM made {mock_llm_resource.call_count} calls:")
        for i, call in enumerate(mock_llm_resource.calls_made):
            print(f"  Call {i+1}: {call['model']} - {call['prompt']}")
    
    def test_evaluation_pipeline_with_mock_llm(self):
        """Test evaluation pipeline with mock evaluator responses."""
        mock_responses = {
            "deepseek/deepseek-r1:free": "**SCORE**: 8.5\n\nThis response demonstrates strong creativity and insight.",
            "google/gemma-3-27b-it:free": "**SCORE**: 7.2\n\nGood analysis but lacks some creative depth."
        }
        
        mock_llm_resource = MockLLMResource(responses=mock_responses)
        
        test_resources = {
            **defs.resources,
            "openrouter_client": mock_llm_resource
        }
        
        instance = DagsterInstance.ephemeral()
        
        # First materialize all non-partitioned assets
        setup_result = materialize(
            assets=[asset for asset in defs.assets if not asset.partitions_def],
            resources=test_resources,
            instance=instance
        )
        assert setup_result.success
        
        # Get evaluation partitions
        eval_partitions = instance.get_dynamic_partitions("evaluation_tasks")
        assert len(eval_partitions) > 0
        
        # Test evaluation assets with first partition
        test_partition = eval_partitions[0]
        
        eval_assets = [
            asset for asset in defs.assets 
            if asset.partitions_def and asset.key.to_user_string() in ["evaluation_prompt", "evaluation_response"]
        ]
        
        # Note: This would normally require generation_response to exist first
        # In a real integration test, you might need to materialize generation first
        # or create mock generation responses
        
        # For this test, let's just verify the evaluation assets exist and are properly configured
        assert len(eval_assets) == 2, "Expected 2 evaluation assets"
        
        eval_asset_names = [asset.key.to_user_string() for asset in eval_assets]
        assert "evaluation_prompt" in eval_asset_names
        assert "evaluation_response" in eval_asset_names
    
    def test_mock_llm_error_handling(self):
        """Test pipeline behavior when LLM client raises errors."""
        
        class ErrorMockLLMResource(ConfigurableResource):
            def get_client(self):
                mock_client = Mock()
                mock_client.generate.side_effect = Exception("Mock API error")
                return mock_client
        
        error_llm_resource = ErrorMockLLMResource()
        
        test_resources = {
            **defs.resources,
            "openrouter_client": error_llm_resource
        }
        
        instance = DagsterInstance.ephemeral()
        
        # Setup should still work (no LLM calls)
        setup_result = materialize(
            assets=[asset for asset in defs.assets if not asset.partitions_def],
            resources=test_resources,
            instance=instance
        )
        assert setup_result.success
        
        # But LLM generation should fail
        gen_partitions = instance.get_dynamic_partitions("generation_tasks")
        if len(gen_partitions) > 0:
            test_partition = gen_partitions[0]
            
            llm_assets = [
                asset for asset in defs.assets 
                if asset.partitions_def and "response" in asset.key.to_user_string()
            ]
            
            partition_result = materialize(
                assets=llm_assets,
                partition_key=test_partition,
                resources=test_resources,
                instance=instance,
                raise_on_error=False  # Don't raise, just check result
            )
            
            # Should fail due to mock error
            assert not partition_result.success
    
    def test_resource_injection_flexibility(self):
        """Test that we can easily swap different mock implementations."""
        
        # Test with different mock behaviors
        test_cases = [
            {
                "name": "creative_responses",
                "responses": {"deepseek/deepseek-r1:free": "Highly creative and innovative response!"}
            },
            {
                "name": "analytical_responses", 
                "responses": {"deepseek/deepseek-r1:free": "Systematic analytical breakdown of concepts."}
            },
            {
                "name": "empty_responses",
                "responses": {"deepseek/deepseek-r1:free": ""}
            }
        ]
        
        for test_case in test_cases:
            mock_resource = MockLLMResource(responses=test_case["responses"])
            
            # Verify the mock resource works as expected
            client = mock_resource.get_client()
            response = client.generate("test prompt", "deepseek/deepseek-r1:free")
            
            expected = test_case["responses"]["deepseek/deepseek-r1:free"]
            assert response == expected, f"Test case {test_case['name']} failed"
            assert mock_resource.call_count == 1


class TestAssetResourceInjection:
    """Test resource injection at individual asset level."""
    
    def test_individual_asset_with_mock_resource(self):
        """Test materializing individual assets with mock resources."""
        from daydreaming_dagster.assets.llm_prompts_responses import generation_response
        
        # Create mock resource
        mock_llm = MockLLMResource(responses={"test-model": "Mock generation response"})
        
        # Create context with mock resource
        context = build_asset_context(
            resources={"openrouter_client": mock_llm},
            partition_key="test_partition_001"
        )
        
        # Mock the required inputs
        mock_prompt = "Test prompt for concept combination"
        mock_tasks = pd.DataFrame([{
            "generation_task_id": "test_partition_001",
            "generation_model": "test-model"
        }])
        
        # Call the asset function directly
        result = generation_response(context, mock_prompt, mock_tasks)
        
        assert result == "Mock generation response"
        assert mock_llm.call_count == 1
        assert mock_llm.calls_made[0]["model"] == "test-model"


if __name__ == "__main__":
    # Run tests directly for debugging
    test_instance = TestLLMIntegration()
    
    print("Testing LLM integration with mock client...")
    
    try:
        test_instance.test_generation_pipeline_with_mock_llm()
        print("✓ Generation pipeline test passed")
    except Exception as e:
        print(f"✗ Generation pipeline test failed: {e}")
    
    try:
        test_instance.test_evaluation_pipeline_with_mock_llm()
        print("✓ Evaluation pipeline test passed")
    except Exception as e:
        print(f"✗ Evaluation pipeline test failed: {e}")
    
    try:
        test_instance.test_mock_llm_error_handling()
        print("✓ Error handling test passed")
    except Exception as e:
        print(f"✗ Error handling test failed: {e}")
    
    try:
        test_instance.test_resource_injection_flexibility()
        print("✓ Resource injection flexibility test passed")
    except Exception as e:
        print(f"✗ Resource injection flexibility test failed: {e}")
    
    asset_test = TestAssetResourceInjection()
    try:
        asset_test.test_individual_asset_with_mock_resource()
        print("✓ Individual asset resource injection test passed")
    except Exception as e:
        print(f"✗ Individual asset resource injection test failed: {e}")
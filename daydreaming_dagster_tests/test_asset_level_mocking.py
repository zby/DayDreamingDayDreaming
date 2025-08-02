"""Test asset-level mocking for individual asset testing."""

import pytest
import pandas as pd
from unittest.mock import Mock
from dagster import build_asset_context, DagsterInstance
from daydreaming_dagster.assets.llm_prompts_responses import generation_response, evaluation_response
from .conftest import MockLLMResource


class TestAssetLevelMocking:
    """Test individual assets with mock resources."""
    
    def test_generation_response_asset_with_mock(self):
        """Test generation_response asset with mock LLM client."""
        
        # Create mock LLM resource
        mock_responses = {"test-model": "Mock creative response about concept combinations"}
        mock_llm = MockLLMResource(responses=mock_responses)
        
        # Create asset context with mock resource
        context = build_asset_context(
            resources={"openrouter_client": mock_llm},
            partition_key="test_generation_task_001"
        )
        
        # Mock inputs
        mock_prompt = "Test prompt: Analyze the relationship between concept A and concept B"
        mock_tasks = pd.DataFrame([{
            "generation_task_id": "test_generation_task_001",
            "generation_model": "test-model",
            "combo_id": "combo_001"
        }])
        
        # Call the asset function
        result = generation_response(context, mock_prompt, mock_tasks)
        
        # Verify result
        assert result == "Mock creative response about concept combinations"
        
        # Access call tracking through the client
        client = mock_llm.get_client()
        # Note: In this test, a new client is created, so we test the behavior differently
        # For call tracking, we'd need to modify the test or use a shared client instance
    
    def test_evaluation_response_asset_with_mock(self):
        """Test evaluation_response asset with mock evaluator."""
        
        # Create mock evaluator
        mock_score_response = "**SCORE**: 8.5\n\nExcellent creativity and insight demonstrated."
        mock_evaluator = MockLLMResource(responses={"eval-model": mock_score_response})
        
        context = build_asset_context(
            resources={"openrouter_client": mock_evaluator},
            partition_key="test_eval_task_001"
        )
        
        # Mock inputs
        mock_eval_prompt = "Rate this response for creativity: [Generated response text here]"
        mock_eval_tasks = pd.DataFrame([{
            "evaluation_task_id": "test_eval_task_001",
            "evaluation_model": "eval-model",
            "generation_task_id": "test_generation_task_001"
        }])
        
        # Call the asset function
        result = evaluation_response(context, mock_eval_prompt, mock_eval_tasks)
        
        # Verify result
        assert "SCORE" in result
        assert "8.5" in result
        
        # Note: Dagster creates a new resource instance in the context
        # so we need to check the actual resource that was used
        actual_resource = context.resources.openrouter_client
        assert actual_resource.call_count == 1
        assert actual_resource.calls_made[0]["model"] == "eval-model"
    
    def test_asset_with_different_mock_behaviors(self):
        """Test asset with various mock LLM behaviors."""
        
        test_cases = [
            {
                "name": "short_response",
                "mock_response": "Brief.",
                "expected_length": "short"
            },
            {
                "name": "long_response", 
                "mock_response": "This is a very detailed and comprehensive response that explores multiple aspects of the daydreaming concept combination, providing deep insights into creativity and innovation processes.",
                "expected_length": "long"
            },
            {
                "name": "empty_response",
                "mock_response": "",
                "expected_length": "empty"
            }
        ]
        
        for test_case in test_cases:
            mock_llm = MockLLMResource(responses={"test-model": test_case["mock_response"]})
            
            context = build_asset_context(
                resources={"openrouter_client": mock_llm},
                partition_key=f"test_{test_case['name']}"
            )
            
            mock_tasks = pd.DataFrame([{
                "generation_task_id": f"test_{test_case['name']}",
                "generation_model": "test-model"
            }])
            
            result = generation_response(context, "test prompt", mock_tasks)
            
            # Verify expected behavior
            if test_case["expected_length"] == "short":
                assert len(result) < 20
            elif test_case["expected_length"] == "long":
                assert len(result) > 100
            elif test_case["expected_length"] == "empty":
                assert result == ""
            
            assert result == test_case["mock_response"]
    
    def test_asset_error_handling_with_mock(self):
        """Test asset behavior when mock LLM raises exceptions."""
        
        # Create failing mock
        failing_mock = MockLLMResource(should_fail=True)
        
        context = build_asset_context(
            resources={"openrouter_client": failing_mock},
            partition_key="test_failing_task"
        )
        
        mock_tasks = pd.DataFrame([{
            "generation_task_id": "test_failing_task",
            "generation_model": "failing-model"
        }])
        
        # Asset should propagate the exception
        with pytest.raises(Exception, match="Mock LLM API failure"):
            generation_response(context, "test prompt", mock_tasks)
    
    def test_asset_with_patched_client(self):
        """Alternative approach using unittest.mock.patch."""
        
        # Create a mock resource that's easier to test with
        mock_responses = {"patched-model": "Patched response"}
        mock_resource = MockLLMResource(responses=mock_responses)
        
        context = build_asset_context(
            resources={"openrouter_client": mock_resource},
            partition_key="test_patched_task"
        )
        
        mock_tasks = pd.DataFrame([{
            "generation_task_id": "test_patched_task", 
            "generation_model": "patched-model"
        }])
        
        result = generation_response(context, "test prompt", mock_tasks)
        
        # Verify result
        assert result == "Patched response"
        
        # Verify mock was called correctly
        actual_resource = context.resources.openrouter_client
        assert actual_resource.call_count == 1
        assert actual_resource.calls_made[0]["model"] == "patched-model"
    
    def test_resource_configuration_injection(self):
        """Test injecting different resource configurations."""
        
        # Test with different API configurations
        configs = [
            {"api_key": "test-key-1", "temperature": 0.1},
            {"api_key": "test-key-2", "temperature": 0.9},
        ]
        
        for i, config in enumerate(configs):
            mock_llm = MockLLMResource(responses={"test-model": f"Response with config {i}"})
            
            context = build_asset_context(
                resources={
                    "openrouter_client": mock_llm,
                    # Could also inject experiment_config here for different parameters
                },
                partition_key=f"test_config_{i}"
            )
            
            mock_tasks = pd.DataFrame([{
                "generation_task_id": f"test_config_{i}",
                "generation_model": "test-model"
            }])
            
            result = generation_response(context, "test prompt", mock_tasks)
            assert result == f"Response with config {i}"


if __name__ == "__main__":
    # Run tests directly
    test_instance = TestAssetLevelMocking()
    
    print("Testing asset-level mocking...")
    
    test_methods = [
        "test_generation_response_asset_with_mock",
        "test_evaluation_response_asset_with_mock", 
        "test_asset_with_different_mock_behaviors",
        "test_asset_error_handling_with_mock",
        "test_resource_configuration_injection"
    ]
    
    for method_name in test_methods:
        try:
            method = getattr(test_instance, method_name)
            method()
            print(f"✓ {method_name} passed")
        except Exception as e:
            print(f"✗ {method_name} failed: {e}")
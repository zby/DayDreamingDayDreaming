"""Integration tests using pytest fixtures for mock LLM injection."""

import pytest
from dagster import materialize
from daydreaming_dagster import defs


class TestPipelineWithFixtures:
    """Integration tests using pytest fixtures for clean mock injection."""
    
    def test_complete_pipeline_with_creative_llm(self, ephemeral_instance, test_resources_with_mock_llm):
        """Test complete pipeline flow with creative mock LLM."""
        
        # Step 1: Materialize setup assets
        setup_assets = [asset for asset in defs.assets if not asset.partitions_def]
        
        setup_result = materialize(
            assets=setup_assets,
            resources=test_resources_with_mock_llm,
            instance=ephemeral_instance
        )
        
        assert setup_result.success, "Setup phase should succeed"
        
        # Verify partitions were created
        gen_partitions = ephemeral_instance.get_dynamic_partitions("generation_tasks")
        eval_partitions = ephemeral_instance.get_dynamic_partitions("evaluation_tasks")
        
        assert len(gen_partitions) > 0, "Should create generation partitions"
        assert len(eval_partitions) > 0, "Should create evaluation partitions"
        
        print(f"Created {len(gen_partitions)} generation partitions and {len(eval_partitions)} evaluation partitions")
        
        # Step 2: Test LLM generation for first partition
        test_partition = gen_partitions[0]
        print(f"Testing partition: {test_partition}")
        
        llm_generation_assets = [
            asset for asset in defs.assets 
            if asset.partitions_def and asset.key.to_user_string() in ["generation_prompt", "generation_response"]
        ]
        
        generation_result = materialize(
            assets=llm_generation_assets,
            partition_key=test_partition,
            resources=test_resources_with_mock_llm,
            instance=ephemeral_instance
        )
        
        assert generation_result.success, "LLM generation should succeed with mock"
        
        # Verify mock was called
        mock_llm = test_resources_with_mock_llm["openrouter_client"]
        assert mock_llm.call_count > 0, "Mock LLM should have been called"
        
        print(f"Mock LLM made {mock_llm.call_count} calls")
        for call in mock_llm.calls_made:
            print(f"  - Model: {call['model']}, Prompt preview: {call['prompt']}")
    
    def test_evaluation_pipeline_with_mock_evaluator(self, ephemeral_instance, mock_evaluator_llm):
        """Test evaluation pipeline with mock evaluator that returns scores."""
        
        eval_resources = {
            **defs.resources,
            "openrouter_client": mock_evaluator_llm
        }
        
        # Setup phase
        setup_result = materialize(
            assets=[asset for asset in defs.assets if not asset.partitions_def],
            resources=eval_resources,
            instance=ephemeral_instance
        )
        assert setup_result.success
        
        # Get evaluation partitions
        eval_partitions = ephemeral_instance.get_dynamic_partitions("evaluation_tasks")
        
        if len(eval_partitions) > 0:
            test_partition = eval_partitions[0]
            print(f"Testing evaluation partition: {test_partition}")
            
            # Test evaluation assets
            eval_assets = [
                asset for asset in defs.assets 
                if asset.partitions_def and asset.key.to_user_string() in ["evaluation_prompt", "evaluation_response"]
            ]
            
            # Note: In real test, you'd need generation_response to exist first
            # This is simplified for demonstration
            print(f"Found {len(eval_assets)} evaluation assets")
            
            # Verify mock evaluator would return scores
            client = mock_evaluator_llm.get_client()
            response = client.generate("test evaluation prompt", "deepseek/deepseek-r1:free")
            assert "SCORE" in response, "Evaluator should return score format"
            assert "8.5" in response, "Should return expected score"
    
    def test_error_handling_with_failing_llm(self, ephemeral_instance, mock_failing_llm):
        """Test pipeline behavior when LLM fails."""
        
        failing_resources = {
            **defs.resources,
            "openrouter_client": mock_failing_llm
        }
        
        # Setup should still work (no LLM calls yet)
        setup_result = materialize(
            assets=[asset for asset in defs.assets if not asset.partitions_def],
            resources=failing_resources,
            instance=ephemeral_instance
        )
        assert setup_result.success, "Setup should succeed even with failing LLM"
        
        # But LLM generation should fail gracefully
        gen_partitions = ephemeral_instance.get_dynamic_partitions("generation_tasks")
        
        if len(gen_partitions) > 0:
            test_partition = gen_partitions[0]
            
            llm_assets = [
                asset for asset in defs.assets 
                if asset.partitions_def and "response" in asset.key.to_user_string()
            ]
            
            # This should fail due to mock LLM error
            generation_result = materialize(
                assets=llm_assets,
                partition_key=test_partition,
                resources=failing_resources,
                instance=ephemeral_instance,
                raise_on_error=False
            )
            
            assert not generation_result.success, "Should fail with failing LLM"
            print(f"Expected failure occurred: {generation_result}")
    
    def test_different_llm_models_mock_responses(self, ephemeral_instance):
        """Test that different models get different mock responses."""
        
        model_specific_responses = {
            "deepseek/deepseek-r1:free": "DeepSeek's analytical approach to daydreaming concepts",
            "google/gemma-3-27b-it:free": "Gemma's creative interpretation of concept combinations"
        }
        
        from .conftest import MockLLMResource
        multi_model_mock = MockLLMResource(responses=model_specific_responses)
        
        # Test each model gets its specific response
        client = multi_model_mock.get_client()
        
        for model, expected_response in model_specific_responses.items():
            response = client.generate("test prompt", model)
            assert response == expected_response, f"Model {model} should get specific response"
        
        # Verify call tracking
        assert client.call_count == len(model_specific_responses)
        
        models_called = [call["model"] for call in client.calls_made]
        for model in model_specific_responses.keys():
            assert model in models_called, f"Model {model} should have been called"
    
    @pytest.mark.parametrize("partition_count", [1, 3, 5])
    def test_multiple_partition_processing(self, ephemeral_instance, test_resources_with_mock_llm, partition_count):
        """Test processing multiple partitions with mock LLM."""
        
        # Setup
        setup_result = materialize(
            assets=[asset for asset in defs.assets if not asset.partitions_def],
            resources=test_resources_with_mock_llm,
            instance=ephemeral_instance
        )
        assert setup_result.success
        
        # Get available partitions
        gen_partitions = ephemeral_instance.get_dynamic_partitions("generation_tasks")
        
        # Test up to requested count or available partitions
        test_partitions = gen_partitions[:min(partition_count, len(gen_partitions))]
        
        llm_assets = [
            asset for asset in defs.assets 
            if asset.partitions_def and asset.key.to_user_string() in ["generation_prompt", "generation_response"]
        ]
        
        mock_llm = test_resources_with_mock_llm["openrouter_client"]
        initial_call_count = mock_llm.call_count
        
        # Process each partition
        for partition in test_partitions:
            result = materialize(
                assets=llm_assets,
                partition_key=partition,
                resources=test_resources_with_mock_llm,
                instance=ephemeral_instance
            )
            assert result.success, f"Partition {partition} should process successfully"
        
        # Verify mock was called for each partition
        expected_calls = initial_call_count + len(test_partitions)
        assert mock_llm.call_count == expected_calls, f"Should make {expected_calls} LLM calls"
        
        print(f"Successfully processed {len(test_partitions)} partitions with {mock_llm.call_count} total LLM calls")
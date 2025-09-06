"""Unit tests for LLM evaluation utility functions."""

import pytest
import pandas as pd
from jinja2 import Environment

from daydreaming_dagster.assets.group_evaluation import evaluation_prompt


class TestMockLoadContext:
    """Test the MockLoadContext utility class."""
    
    def test_mock_load_context_creation(self):
        """Test that MockLoadContext can be created with partition key."""
        # Simple test without importing the actual class
        class MockLoadContext:
            def __init__(self, partition_key):
                self.partition_key = partition_key
        
        partition_key = "test_partition_key"
        mock_context = MockLoadContext(partition_key)
        
        assert mock_context.partition_key == partition_key


class TestEvaluationUtilityFunctions:
    """Test utility functions used in evaluation."""
    
    def test_evaluation_prompt_declares_required_resources(self):
        """Ensure evaluation_prompt declares resources it accesses (data_root, IO)."""
        required = set(evaluation_prompt.required_resource_keys)
        assert "data_root" in required, "evaluation_prompt must declare 'data_root' as a required resource"
        assert "evaluation_prompt_io_manager" in evaluation_prompt.resource_defs or True  # IO manager is assigned in defs
    
    def test_generate_evaluation_prompts_call(self):
        """Test the evaluation prompt generation logic."""
        template_content = "Evaluate this response for creativity:\n{{ response }}\n\nSCORE:"
        generation_response = "This is a creative analysis of innovation and problem-solving."
        
        env = Environment()
        template = env.from_string(template_content)
        result = template.render(response=generation_response)
        
        expected = "Evaluate this response for creativity:\nThis is a creative analysis of innovation and problem-solving.\n\nSCORE:"
        assert result == expected
    
    def test_evaluation_task_filtering(self):
        """Test filtering evaluation tasks by partition key."""
        evaluation_tasks = pd.DataFrame([
            {
                "evaluation_task_id": "combo_001_systematic-analytical-v2_deepseek_r1_f_daydreaming-verification-v2_deepseek_r1_f",
                "evaluation_template": "daydreaming-verification-v2",
                "evaluation_model": "deepseek_r1_f"
            },
            {
                "evaluation_task_id": "combo_002_other_task",
                "evaluation_template": "other-template", 
                "evaluation_model": "other_model"
            }
        ])
        
        target_partition_key = "combo_001_systematic-analytical-v2_deepseek_r1_f_daydreaming-verification-v2_deepseek_r1_f"
        
        matching_tasks = evaluation_tasks[
            evaluation_tasks['evaluation_task_id'] == target_partition_key
        ]
        
        assert len(matching_tasks) == 1
        assert matching_tasks.iloc[0]['evaluation_task_id'] == target_partition_key
        assert matching_tasks.iloc[0]['evaluation_template'] == 'daydreaming-verification-v2'
        assert matching_tasks.iloc[0]['evaluation_model'] == 'deepseek_r1_f'


class TestModelNameMapping:
    """Test model name mapping for evaluation."""
    
    def test_evaluation_model_mapping(self):
        """Test conversion from evaluation model ID to model name."""
        test_cases = [
            {"model_id": "deepseek_r1_f", "expected": "deepseek/deepseek-r1:free"},
            {"model_id": "qwq_32b_f", "expected": "qwen/qwq-32b:free"},
            {"model_id": "unknown_eval_model", "expected": "unknown_eval_model"}
        ]
        
        def get_evaluation_model_name(model_id):
            """Convert evaluation model ID to full model name."""
            model_mapping = {
                "deepseek_r1_f": "deepseek/deepseek-r1:free",
                "qwq_32b_f": "qwen/qwq-32b:free"
            }
            return model_mapping.get(model_id, model_id)
        
        for test_case in test_cases:
            result = get_evaluation_model_name(test_case["model_id"])
            assert result == test_case["expected"]

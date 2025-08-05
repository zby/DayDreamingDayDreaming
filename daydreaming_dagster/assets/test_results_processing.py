"""Unit tests for results processing utility functions."""

import pytest
import pandas as pd
import numpy as np


class TestDataFrameProcessing:
    """Test DataFrame processing utilities."""
    
    def test_metadata_calculation(self):
        """Test metadata calculation logic."""
        # Sample data similar to what the function processes
        test_df = pd.DataFrame([
            {"combo_id": "combo_001", "score": 8.5, "error": None},
            {"combo_id": "combo_002", "score": 7.2, "error": None},
            {"combo_id": "combo_003", "score": np.nan, "error": "Parse failed"},
            {"combo_id": "combo_004", "score": 9.0, "error": None}
        ])
        
        # Calculate metadata like the actual function
        total_responses = len(test_df)
        successful_parses = len(test_df[test_df['error'].isna()])
        failed_parses = total_responses - successful_parses
        success_rate = (successful_parses / total_responses * 100) if total_responses > 0 else 0.0
        
        assert total_responses == 4
        assert successful_parses == 3
        assert failed_parses == 1
        assert success_rate == 75.0


class TestModelProviderMapping:
    """Test model provider extraction logic."""
    
    def test_get_model_provider(self):
        """Test model provider extraction from model ID."""
        def get_model_provider(model_id):
            """Extract provider from model ID like 'deepseek_r1_f' or 'qwq_32b_f'."""
            if pd.isna(model_id):
                return 'unknown'
            
            model_str = str(model_id).lower()
            if 'deepseek' in model_str:
                return 'deepseek'
            elif 'qwq' in model_str or 'qwen' in model_str:
                return 'qwen'
            elif 'gemma' in model_str or 'google' in model_str:
                return 'google'
            else:
                return 'unknown'
        
        test_cases = [
            {"model_id": "deepseek_r1_f", "expected": "deepseek"},
            {"model_id": "qwq_32b_f", "expected": "qwen"},
            {"model_id": "gemma_3_27b_f", "expected": "google"},
            {"model_id": "google_gemma", "expected": "google"},
            {"model_id": "unknown_model", "expected": "unknown"},
            {"model_id": None, "expected": "unknown"}
        ]
        
        for test_case in test_cases:
            result = get_model_provider(test_case["model_id"])
            assert result == test_case["expected"]


class TestColumnReordering:
    """Test column reordering logic."""
    
    def test_column_order_filtering(self):
        """Test filtering columns that exist in DataFrame."""
        test_df = pd.DataFrame({
            "combo_id": ["combo_001"],
            "generation_template": ["template1"],
            "score": [8.5],
            "error": [None],
            "extra_column": ["extra"]
        })
        
        # Desired column order like in the function
        column_order = [
            'combo_id',
            'generation_template', 
            'generation_model_provider',  # This doesn't exist
            'evaluation_template',
            'evaluation_model_provider',
            'score',
            'error'
        ]
        
        # Filter to only existing columns
        existing_columns = [col for col in column_order if col in test_df.columns]
        
        expected_columns = ['combo_id', 'generation_template', 'score', 'error']
        assert existing_columns == expected_columns


class TestPathParsing:
    """Test file path parsing logic used in results processing."""
    
    def test_path_structure_parsing(self):
        """Test parsing of nested evaluation response paths."""
        # Simulate the path parsing logic from the function
        test_path = "combo_001_02_problem_solving_deepseek/deepseek-r1:free_daydreaming_verification_qwen/qwq-32b:free.txt"
        path_parts = test_path.split('/')
        
        if len(path_parts) >= 2:
            generation_task_part = path_parts[0]  # combo_001_02_problem_solving_deepseek
            eval_part = path_parts[1]  # deepseek-r1:free_daydreaming_verification_qwen
            model_file = path_parts[2] if len(path_parts) > 2 else "qwq-32b:free.txt"
            
            # Test parsing generation task part
            gen_parts = generation_task_part.split('_')
            assert len(gen_parts) >= 4
            assert gen_parts[0] == "combo"
            assert gen_parts[1] == "001"
            
            # Test model file parsing
            model_name = model_file.replace('.txt', '') if model_file.endswith('.txt') else model_file
            assert model_name == "qwq-32b:free"


class TestDataFrameJoins:
    """Test DataFrame join operations used in new clean approach."""
    
    def test_evaluation_tasks_join(self):
        """Test joining parsed scores with evaluation tasks."""
        # Sample parsed scores (basic data from file parsing)
        parsed_df = pd.DataFrame([
            {"evaluation_task_id": "combo_001_eval_task", "score": 8.5, "error": None},
            {"evaluation_task_id": "combo_002_eval_task", "score": 7.2, "error": None},
            {"evaluation_task_id": "combo_003_missing", "score": 6.0, "error": None}
        ])
        
        # Sample evaluation tasks (metadata from CSV)
        evaluation_tasks = pd.DataFrame([
            {
                "evaluation_task_id": "combo_001_eval_task",
                "generation_task_id": "combo_001_gen",
                "evaluation_template": "daydreaming-verification-v2",
                "evaluation_model": "deepseek_r1_f"
            },
            {
                "evaluation_task_id": "combo_002_eval_task", 
                "generation_task_id": "combo_002_gen",
                "evaluation_template": "creativity-metrics",
                "evaluation_model": "qwq_32b_f"
            }
        ])
        
        # Test the join operation (like in new clean approach)
        enriched_df = parsed_df.merge(
            evaluation_tasks[['evaluation_task_id', 'generation_task_id', 'evaluation_template', 'evaluation_model']],
            on='evaluation_task_id',
            how='left'
        )
        
        # Verify the join worked
        assert len(enriched_df) == 3  # All original rows preserved
        assert 'generation_task_id' in enriched_df.columns
        assert 'evaluation_template' in enriched_df.columns
        
        # Check specific values
        combo_001_row = enriched_df[enriched_df['evaluation_task_id'] == 'combo_001_eval_task'].iloc[0]
        assert combo_001_row['generation_task_id'] == 'combo_001_gen'
        assert combo_001_row['evaluation_template'] == 'daydreaming-verification-v2'
        
        # Check missing data handled correctly
        combo_003_row = enriched_df[enriched_df['evaluation_task_id'] == 'combo_003_missing'].iloc[0]
        assert pd.isna(combo_003_row['generation_task_id'])  # Should be NaN for missing join
    
    def test_generation_tasks_join(self):
        """Test joining with generation tasks for full metadata."""
        # Sample data after evaluation tasks join
        enriched_df = pd.DataFrame([
            {
                "evaluation_task_id": "combo_001_eval",
                "score": 8.5,
                "generation_task_id": "combo_001_gen",
                "evaluation_template": "daydreaming-verification-v2"
            },
            {
                "evaluation_task_id": "combo_002_eval",
                "score": 7.2, 
                "generation_task_id": "combo_002_gen",
                "evaluation_template": "creativity-metrics"
            }
        ])
        
        # Sample generation tasks
        generation_tasks = pd.DataFrame([
            {
                "generation_task_id": "combo_001_gen",
                "combo_id": "combo_001", 
                "generation_template": "systematic-analytical-v2",
                "generation_model": "deepseek_r1_f"
            },
            {
                "generation_task_id": "combo_002_gen",
                "combo_id": "combo_002",
                "generation_template": "creative-synthesis-v2", 
                "generation_model": "gemma_3_27b_f"
            }
        ])
        
        # Test the second join operation
        final_df = enriched_df.merge(
            generation_tasks[['generation_task_id', 'combo_id', 'generation_template', 'generation_model']],
            on='generation_task_id',
            how='left'
        )
        
        # Verify the final result
        assert len(final_df) == 2
        assert 'combo_id' in final_df.columns
        assert 'generation_template' in final_df.columns
        assert 'generation_model' in final_df.columns
        
        # Check specific values
        combo_001_row = final_df[final_df['evaluation_task_id'] == 'combo_001_eval'].iloc[0]
        assert combo_001_row['combo_id'] == 'combo_001'
        assert combo_001_row['generation_template'] == 'systematic-analytical-v2'
        assert combo_001_row['generation_model'] == 'deepseek_r1_f'
"""Unit tests for results analysis utility functions."""

import pytest
import pandas as pd
import numpy as np


class TestAgreementClassification:
    """Test agreement classification logic."""
    
    def test_classify_agreement(self):
        """Test agreement classification based on score range and evaluator count."""
        def classify_agreement(row):
            """Classify evaluator agreement based on score range and CV."""
            if pd.isna(row['std_dev']) or row['evaluator_count'] < 2:
                return 'insufficient_data'
            elif row['score_range'] <= 1.0:
                return 'high_agreement'
            elif row['score_range'] <= 2.0:
                return 'moderate_agreement'
            elif row['score_range'] <= 3.0:
                return 'low_agreement'
            else:
                return 'poor_agreement'
        
        test_cases = [
            {"score_range": 0.5, "std_dev": 0.2, "evaluator_count": 3, "expected": "high_agreement"},
            {"score_range": 1.5, "std_dev": 0.7, "evaluator_count": 2, "expected": "moderate_agreement"},
            {"score_range": 2.5, "std_dev": 1.2, "evaluator_count": 4, "expected": "low_agreement"},
            {"score_range": 4.0, "std_dev": 1.8, "evaluator_count": 3, "expected": "poor_agreement"},
            {"score_range": 1.0, "std_dev": np.nan, "evaluator_count": 2, "expected": "insufficient_data"},
            {"score_range": 1.0, "std_dev": 0.5, "evaluator_count": 1, "expected": "insufficient_data"}
        ]
        
        for test_case in test_cases:
            row = pd.Series(test_case)
            result = classify_agreement(row)
            assert result == test_case["expected"]


class TestGenerationTaskIdExtraction:
    """Test generation task ID extraction logic."""
    
    def test_extract_generation_task_id(self):
        """Test extraction of generation_task_id from evaluation_task_id."""
        def extract_generation_task_id(eval_task_id):
            """Extract generation_task_id from evaluation_task_id."""
            if pd.isna(eval_task_id):
                return None
            
            # Look for the daydreaming-verification part to know where generation task ends
            if 'daydreaming-verification-v2' in eval_task_id:
                verification_index = eval_task_id.find('_daydreaming-verification-v2')
                if verification_index > 0:
                    return eval_task_id[:verification_index]
            
            # Fallback: assume first 4 parts (combo_001_template_model)
            parts = eval_task_id.split('_')
            if len(parts) >= 4:
                return '_'.join(parts[:4])
            
            return eval_task_id
        
        test_cases = [
            {
                "eval_task_id": "combo_001_systematic-analytical-v2_deepseek_r1_f_daydreaming-verification-v2_qwq_32b_f",
                "expected_gen_id": "combo_001_systematic-analytical-v2_deepseek_r1_f"
            },
            {
                "eval_task_id": "combo_002_creative_synthesis_google_creativity-metrics_deepseek",
                "expected_gen_id": "combo_002_creative_synthesis"  # First 4 parts: combo, 002, creative, synthesis
            },
            {
                "eval_task_id": "simple_task_id",
                "expected_gen_id": "simple_task_id"  # Fallback case
            }
        ]
        
        for test_case in test_cases:
            result = extract_generation_task_id(test_case["eval_task_id"])
            assert result == test_case["expected_gen_id"]


class TestVarianceStabilityClassification:
    """Test stability classification for variance analysis."""
    
    def test_classify_variance_stability(self):
        """Test stability classification logic."""
        def classify_variance_stability(row):
            """Classify variance stability based on score range."""
            total_evals = row.get('total_evaluations', row.get('template_evaluations', row.get('model_evaluations', 1)))
            if pd.isna(row['coefficient_of_variation']) or total_evals < 2:
                return 'insufficient_data'
            elif row['score_range'] <= 1.0:
                return 'high_agreement'
            elif row['score_range'] <= 2.0:
                return 'moderate_agreement'
            elif row['score_range'] <= 3.0:
                return 'low_agreement'
            else:
                return 'poor_agreement'
        
        test_cases = [
            {"score_range": 0.8, "coefficient_of_variation": 0.1, "total_evaluations": 3, "expected": "high_agreement"},
            {"score_range": 1.5, "coefficient_of_variation": 0.2, "model_evaluations": 2, "expected": "moderate_agreement"},
            {"score_range": 2.8, "coefficient_of_variation": 0.3, "template_evaluations": 4, "expected": "low_agreement"},
            {"score_range": 3.5, "coefficient_of_variation": 0.4, "total_evaluations": 3, "expected": "poor_agreement"},
            {"score_range": 1.0, "coefficient_of_variation": np.nan, "total_evaluations": 2, "expected": "insufficient_data"}
        ]
        
        for test_case in test_cases:
            row = pd.Series(test_case)
            result = classify_variance_stability(row)
            assert result == test_case["expected"]


class TestEvaluationInfoParsing:
    """Test parsing of evaluation template and model from task ID."""
    
    def test_parse_evaluation_info(self):
        """Test parsing of evaluation template and model from task ID."""
        def parse_evaluation_info(eval_task_id):
            """Extract evaluation template and model from task ID."""
            if pd.isna(eval_task_id):
                return None, None
                
            eval_templates = ['daydreaming-verification-v2', 'creativity-metrics', 'scientific-rigor', 'iterative-loops']
            
            eval_template = 'unknown'
            eval_model = 'unknown'
            
            for template in eval_templates:
                if template in eval_task_id:
                    eval_template = template
                    template_index = eval_task_id.find(f'_{template}_')
                    if template_index >= 0:
                        remaining = eval_task_id[template_index + len(template) + 2:]
                        eval_model = remaining if remaining else 'unknown'
                    break
            
            return eval_template, eval_model
        
        test_cases = [
            {
                "task_id": "combo_001_gen_template_model_daydreaming-verification-v2_evaluator_model",
                "expected": ("daydreaming-verification-v2", "evaluator_model") 
            },
            {
                "task_id": "combo_002_gen_creativity-metrics_eval_model",
                "expected": ("creativity-metrics", "eval_model")
            },
            {
                "task_id": "unknown_template_task",
                "expected": ("unknown", "unknown")
            },
            {
                "task_id": None,
                "expected": (None, None)
            }
        ]
        
        for test_case in test_cases:
            result = parse_evaluation_info(test_case["task_id"])
            assert result == test_case["expected"]


class TestVarianceUtilityFunctions:
    """Test utility functions used in variance analysis."""
    
    def test_coefficient_of_variation_calculation(self):
        """Test coefficient of variation calculation."""
        test_cases = [
            {"mean": 8.0, "std": 1.0, "expected": 0.125},
            {"mean": 5.0, "std": 2.0, "expected": 0.4},
            {"mean": 0.0, "std": 1.0, "expected": float('inf')},  # Division by zero
            {"mean": 10.0, "std": 0.0, "expected": 0.0}
        ]
        
        for test_case in test_cases:
            if test_case["mean"] == 0.0:
                # Handle division by zero case
                cv = float('inf') if test_case["std"] != 0 else 0.0
            else:
                cv = test_case["std"] / test_case["mean"]
            
            if test_case["expected"] == float('inf'):
                assert cv == float('inf')
            else:
                assert abs(cv - test_case["expected"]) < 0.001
    
    def test_score_range_calculation(self):
        """Test score range calculation."""
        test_scores = [7.5, 8.0, 6.5, 9.0, 8.5]
        
        score_range = max(test_scores) - min(test_scores)
        assert score_range == 2.5  # 9.0 - 6.5
        
        single_score = [8.0]
        single_range = max(single_score) - min(single_score)
        assert single_range == 0.0
    
    def test_aggregation_functions(self):
        """Test aggregation functions used in analysis."""
        test_data = pd.DataFrame({
            "generation_task_id": ["gen_001", "gen_001", "gen_002"],
            "score": [8.0, 7.5, 9.0],
            "eval_template": ["template1", "template1", "template2"],
            "eval_model": ["model1", "model2", "model1"]
        })
        
        # Test groupby aggregation
        grouped = test_data.groupby('generation_task_id')['score'].agg([
            ('count', 'count'),
            ('mean', 'mean'),
            ('std', 'std'),
            ('min', 'min'),
            ('max', 'max')
        ])
        
        # Verify gen_001 has 2 evaluations
        gen_001_stats = grouped.loc['gen_001']
        assert gen_001_stats['count'] == 2
        assert gen_001_stats['mean'] == 7.75  # (8.0 + 7.5) / 2
        assert gen_001_stats['min'] == 7.5
        assert gen_001_stats['max'] == 8.0
        
        # Verify gen_002 has 1 evaluation 
        gen_002_stats = grouped.loc['gen_002']
        assert gen_002_stats['count'] == 1
        assert gen_002_stats['mean'] == 9.0
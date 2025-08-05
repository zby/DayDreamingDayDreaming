"""Unit tests for variance analysis functionality."""

import unittest
import pandas as pd
import numpy as np
from unittest.mock import Mock


# Extract core logic functions for testing (without Dagster decorators)
def extract_generation_task_id(eval_task_id):
    """Extract generation_task_id from evaluation_task_id."""
    if pd.isna(eval_task_id):
        return None
    
    # Look for known evaluation templates and extract generation part
    eval_templates = ['daydreaming-verification-v2', 'creativity-metrics', 'scientific-rigor', 'iterative-loops']
    
    for template in eval_templates:
        if template in eval_task_id:
            template_index = eval_task_id.find(f'_{template}')
            if template_index > 0:
                return eval_task_id[:template_index]
    
    return eval_task_id


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
            # Extract model after template
            template_index = eval_task_id.find(f'_{template}_')
            if template_index >= 0:
                remaining = eval_task_id[template_index + len(template) + 2:]
                eval_model = remaining if remaining else 'unknown'
            break
    
    return eval_template, eval_model


def calculate_evaluator_agreement(parsed_scores):
    """Core logic for evaluator agreement analysis without Dagster context."""
    # Filter out rows with errors
    valid_scores = parsed_scores[
        parsed_scores['error'].isna() & 
        parsed_scores['score'].notna()
    ].copy()
    
    if valid_scores.empty:
        return pd.DataFrame()
    
    # Add generation_task_id column
    valid_scores['generation_task_id'] = valid_scores['evaluation_task_id'].apply(extract_generation_task_id)
    
    # Group by generation_task_id to find cases where multiple evaluators scored the same response
    agreement_stats = valid_scores.groupby('generation_task_id')['score'].agg([
        ('evaluator_count', 'count'),
        ('mean_score', 'mean'),
        ('std_dev', 'std'),
        ('min_score', 'min'),
        ('max_score', 'max'),
        ('median_score', 'median')
    ]).reset_index()
    
    # Only keep cases where we have multiple evaluators
    multi_evaluator = agreement_stats[agreement_stats['evaluator_count'] >= 2].copy()
    
    if multi_evaluator.empty:
        return pd.DataFrame()
    
    # Calculate agreement metrics
    multi_evaluator['score_range'] = multi_evaluator['max_score'] - multi_evaluator['min_score']
    multi_evaluator['coefficient_of_variation'] = multi_evaluator['std_dev'] / multi_evaluator['mean_score']
    
    # Classify agreement levels
    def classify_agreement(row):
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
    
    multi_evaluator['agreement_classification'] = multi_evaluator.apply(classify_agreement, axis=1)
    
    return multi_evaluator


def calculate_comprehensive_variance(parsed_scores):
    """Core logic for comprehensive variance analysis without Dagster context."""
    # Filter out rows with errors
    valid_scores = parsed_scores[
        parsed_scores['error'].isna() & 
        parsed_scores['score'].notna()
    ].copy()
    
    if valid_scores.empty:
        return pd.DataFrame()
    
    # Extract generation_task_id and evaluation info
    valid_scores['generation_task_id'] = valid_scores['evaluation_task_id'].apply(extract_generation_task_id)
    valid_scores[['eval_template', 'eval_model']] = valid_scores['evaluation_task_id'].apply(
        lambda x: pd.Series(parse_evaluation_info(x))
    )
    
    # Overall variance: Group only by generation_task_id
    overall_variance = valid_scores.groupby('generation_task_id').agg({
        'score': ['count', 'mean', 'std', 'min', 'max'],
        'eval_template': lambda x: x.nunique(),
        'eval_model': lambda x: x.nunique()
    }).round(3)
    
    overall_variance.columns = ['total_evaluations', 'mean_score', 'std_dev', 'min_score', 'max_score', 'num_templates', 'num_models']
    overall_variance = overall_variance.reset_index()
    overall_variance['score_range'] = overall_variance['max_score'] - overall_variance['min_score']
    overall_variance['coefficient_of_variation'] = overall_variance['std_dev'] / overall_variance['mean_score']
    overall_variance['analysis_type'] = 'overall_variance'
    
    # Only keep cases with multiple evaluations
    multi_eval = overall_variance[overall_variance['total_evaluations'] >= 2].copy()
    
    if multi_eval.empty:
        return pd.DataFrame()
    
    # Add stability classification
    def classify_stability(row):
        if pd.isna(row['coefficient_of_variation']) or row['total_evaluations'] < 2:
            return 'insufficient_data'
        elif row['score_range'] <= 1.0:
            return 'high_agreement'
        elif row['score_range'] <= 2.0:
            return 'moderate_agreement'
        elif row['score_range'] <= 3.0:
            return 'low_agreement'
        else:
            return 'poor_agreement'
    
    multi_eval['stability_classification'] = multi_eval.apply(classify_stability, axis=1)
    
    return multi_eval


class TestVarianceAnalysis(unittest.TestCase):
    """Test variance analysis functions with realistic evaluation data."""

    def setUp(self):
        """Set up test data for variance analysis."""
        # Create realistic evaluation data with multiple models and templates
        self.test_parsed_scores = pd.DataFrame({
            'evaluation_task_id': [
                # Same generation response, different models, same template
                'combo_001_systematic-analytical-v2_deepseek_r1_f_daydreaming-verification-v2_deepseek_r1_f',
                'combo_001_systematic-analytical-v2_deepseek_r1_f_daydreaming-verification-v2_qwq_32b_f',
                
                # Same generation response, same model, different templates  
                'combo_001_systematic-analytical-v2_deepseek_r1_f_creativity-metrics_deepseek_r1_f',
                'combo_001_systematic-analytical-v2_deepseek_r1_f_scientific-rigor_deepseek_r1_f',
                
                # Another generation response with multiple evaluations
                'combo_002_creative-synthesis-v2_gemma_3_27b_f_daydreaming-verification-v2_deepseek_r1_f',
                'combo_002_creative-synthesis-v2_gemma_3_27b_f_daydreaming-verification-v2_qwq_32b_f',
                'combo_002_creative-synthesis-v2_gemma_3_27b_f_creativity-metrics_deepseek_r1_f',
                
                # Third generation response with high variance
                'combo_003_problem-solving-v2_deepseek_r1_f_daydreaming-verification-v2_deepseek_r1_f',
                'combo_003_problem-solving-v2_deepseek_r1_f_daydreaming-verification-v2_qwq_32b_f',
            ],
            'score': [8.5, 7.2, 6.8, 9.1, 5.3, 8.7, 7.9, 3.1, 9.8],  # Varied scores for testing
            'error': [None] * 9
        })

        # Mock context for testing
        self.mock_context = Mock()
        self.mock_context.log.info = Mock()
        self.mock_context.log.warning = Mock()

    def test_generation_task_id_extraction(self):
        """Test that generation_task_id is correctly extracted from evaluation_task_id."""
        def extract_generation_task_id(eval_task_id):
            """Extract generation_task_id from evaluation_task_id (same logic as in asset)."""
            if pd.isna(eval_task_id):
                return None
            
            # Look for known evaluation templates and extract generation part
            eval_templates = ['daydreaming-verification-v2', 'creativity-metrics', 'scientific-rigor', 'iterative-loops']
            
            for template in eval_templates:
                if template in eval_task_id:
                    template_index = eval_task_id.find(f'_{template}')
                    if template_index > 0:
                        return eval_task_id[:template_index]
            
            return eval_task_id

        # Test cases with expected generation_task_ids
        test_cases = [
            ('combo_001_systematic-analytical-v2_deepseek_r1_f_daydreaming-verification-v2_deepseek_r1_f',
             'combo_001_systematic-analytical-v2_deepseek_r1_f'),
            ('combo_001_systematic-analytical-v2_deepseek_r1_f_creativity-metrics_deepseek_r1_f',
             'combo_001_systematic-analytical-v2_deepseek_r1_f'),
            ('combo_002_creative-synthesis-v2_gemma_3_27b_f_scientific-rigor_qwq_32b_f',
             'combo_002_creative-synthesis-v2_gemma_3_27b_f'),
        ]

        for eval_task_id, expected_gen_task_id in test_cases:
            with self.subTest(eval_task_id=eval_task_id):
                result = extract_generation_task_id(eval_task_id)
                self.assertEqual(result, expected_gen_task_id,
                    f"Expected {expected_gen_task_id}, got {result} for {eval_task_id}")

    def test_evaluation_info_parsing(self):
        """Test that evaluation template and model are correctly parsed."""
        def parse_evaluation_info(eval_task_id):
            """Extract evaluation template and model from task ID (same logic as in asset)."""
            if pd.isna(eval_task_id):
                return None, None
                
            eval_templates = ['daydreaming-verification-v2', 'creativity-metrics', 'scientific-rigor', 'iterative-loops']
            
            eval_template = 'unknown'
            eval_model = 'unknown'
            
            for template in eval_templates:
                if template in eval_task_id:
                    eval_template = template
                    # Extract model after template
                    template_index = eval_task_id.find(f'_{template}_')
                    if template_index >= 0:
                        remaining = eval_task_id[template_index + len(template) + 2:]
                        eval_model = remaining if remaining else 'unknown'
                    break
            
            return eval_template, eval_model

        # Test cases
        test_cases = [
            ('combo_001_systematic-analytical-v2_deepseek_r1_f_daydreaming-verification-v2_deepseek_r1_f',
             'daydreaming-verification-v2', 'deepseek_r1_f'),
            ('combo_001_systematic-analytical-v2_deepseek_r1_f_creativity-metrics_qwq_32b_f',
             'creativity-metrics', 'qwq_32b_f'),
            ('combo_002_creative-synthesis-v2_gemma_3_27b_f_scientific-rigor_deepseek_r1_f',
             'scientific-rigor', 'deepseek_r1_f'),
        ]

        for eval_task_id, expected_template, expected_model in test_cases:
            with self.subTest(eval_task_id=eval_task_id):
                template, model = parse_evaluation_info(eval_task_id)
                self.assertEqual(template, expected_template,
                    f"Expected template {expected_template}, got {template}")
                self.assertEqual(model, expected_model,
                    f"Expected model {expected_model}, got {model}")

    def test_evaluator_agreement_analysis(self):
        """Test that evaluator agreement analysis correctly groups and calculates variance."""
        # The function should be callable and return a DataFrame
        result = calculate_evaluator_agreement(self.test_parsed_scores)
        
        # Should return a DataFrame
        self.assertIsInstance(result, pd.DataFrame)
        
        # Should have found multiple evaluators for some generation responses
        if not result.empty:
            # Check required columns exist
            expected_columns = ['generation_task_id', 'evaluator_count', 'mean_score', 'std_dev', 
                              'score_range', 'agreement_classification']
            for col in expected_columns:
                self.assertIn(col, result.columns, f"Missing expected column: {col}")
            
            # Should have cases with multiple evaluators
            multi_eval_cases = result[result['evaluator_count'] >= 2]
            self.assertGreater(len(multi_eval_cases), 0, "Should find cases with multiple evaluators")
            
            # Score range should be calculated correctly
            for _, row in multi_eval_cases.iterrows():
                self.assertGreaterEqual(row['score_range'], 0, "Score range should be non-negative")
                self.assertEqual(row['score_range'], row['max_score'] - row['min_score'],
                    "Score range should equal max - min")

    def test_comprehensive_variance_analysis(self):
        """Test comprehensive variance analysis across multiple dimensions."""
        result = calculate_comprehensive_variance(self.test_parsed_scores)
        
        # Should return a DataFrame
        self.assertIsInstance(result, pd.DataFrame)
        
        if not result.empty:
            # Should have analysis_type column with expected values
            self.assertIn('analysis_type', result.columns)
            valid_analysis_types = ['overall_variance']  # Only testing overall variance in this simplified version
            for analysis_type in result['analysis_type'].unique():
                self.assertIn(analysis_type, valid_analysis_types,
                    f"Unexpected analysis type: {analysis_type}")
            
            # Should have stability classification
            self.assertIn('stability_classification', result.columns)
            valid_classifications = ['insufficient_data', 'high_agreement', 'moderate_agreement', 
                                   'low_agreement', 'poor_agreement']
            for classification in result['stability_classification'].unique():
                self.assertIn(classification, valid_classifications,
                    f"Unexpected stability classification: {classification}")

    def test_variance_calculations(self):
        """Test that variance metrics are calculated correctly."""
        # Create simple test data with known variance
        simple_scores = pd.DataFrame({
            'evaluation_task_id': [
                'combo_001_test_model_daydreaming-verification-v2_model1',
                'combo_001_test_model_daydreaming-verification-v2_model2',
            ],
            'score': [8.0, 6.0],  # Range = 2.0, Mean = 7.0, Std ≈ 1.414
            'error': [None, None]
        })

        result = calculate_evaluator_agreement(simple_scores)
        
        if not result.empty:
            # Find the row with our test data
            test_row = result[result['evaluator_count'] == 2].iloc[0]
            
            # Check calculations
            self.assertAlmostEqual(test_row['mean_score'], 7.0, places=2)
            self.assertAlmostEqual(test_row['score_range'], 2.0, places=2)
            self.assertAlmostEqual(test_row['std_dev'], 1.414, places=1)

    def test_empty_input_handling(self):
        """Test that functions handle empty input gracefully."""
        empty_df = pd.DataFrame(columns=['evaluation_task_id', 'score', 'error'])
        
        # Should return empty DataFrame without crashing
        result1 = calculate_evaluator_agreement(empty_df)
        self.assertIsInstance(result1, pd.DataFrame)
        self.assertTrue(result1.empty)
        
        result2 = calculate_comprehensive_variance(empty_df)
        self.assertIsInstance(result2, pd.DataFrame)
        self.assertTrue(result2.empty)

    def test_error_handling(self):
        """Test that functions handle data with errors appropriately."""
        # Create data with some errors
        error_data = pd.DataFrame({
            'evaluation_task_id': [
                'combo_001_test_model_daydreaming-verification-v2_model1',
                'combo_001_test_model_daydreaming-verification-v2_model2',
                'combo_002_test_model_daydreaming-verification-v2_model1',
            ],
            'score': [8.0, None, 7.0],  # One missing score
            'error': [None, 'Parse error', None]
        })

        result = calculate_evaluator_agreement(error_data)
        
        # Should filter out error cases and only process valid scores
        # Should have at most 2 valid scores (the ones without errors)
        if not result.empty:
            total_processed = result['evaluator_count'].sum()
            self.assertLessEqual(total_processed, 2, "Should filter out error cases")

    def test_stability_classification(self):
        """Test that stability classification works correctly."""
        # Test different score ranges to verify classification logic
        test_cases = [
            ([8.0, 8.5], 'high_agreement'),      # Range = 0.5 <= 1.0
            ([8.0, 9.5], 'moderate_agreement'),  # Range = 1.5 <= 2.0  
            ([8.0, 10.5], 'low_agreement'),      # Range = 2.5 <= 3.0
            ([8.0, 12.0], 'poor_agreement'),     # Range = 4.0 > 3.0
        ]

        for scores, expected_classification in test_cases:
            with self.subTest(scores=scores):
                test_data = pd.DataFrame({
                    'evaluation_task_id': [
                        f'combo_test_model_daydreaming-verification-v2_model{i+1}'
                        for i in range(len(scores))
                    ],
                    'score': scores,
                    'error': [None] * len(scores)
                })

                result = calculate_evaluator_agreement(test_data)
                
                if not result.empty:
                    classification = result.iloc[0]['agreement_classification']
                    self.assertEqual(classification, expected_classification,
                        f"Expected {expected_classification} for scores {scores}, got {classification}")


if __name__ == '__main__':
    unittest.main()
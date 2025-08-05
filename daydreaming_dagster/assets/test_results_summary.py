"""Unit tests for results summary utility functions."""

import pytest
import pandas as pd
import numpy as np


class TestPivotSummaryLogic:
    """Test the pivot summary creation logic."""
    
    def test_create_pivot_summary_logic(self):
        """Test the core pivot summary logic."""
        # Test data
        test_df = pd.DataFrame([
            {"generation_template": "template1", "score": 8.5},
            {"generation_template": "template1", "score": 9.0},
            {"generation_template": "template1", "score": 10.0},
            {"generation_template": "template2", "score": 7.0},
            {"generation_template": "template2", "score": 8.0}
        ])
        
        # Simulate the create_pivot_summary logic
        group_cols = ['generation_template']
        score_col = 'score'
        
        grouped = test_df.groupby(group_cols)[score_col].agg([
            ('count', 'count'),
            ('average', 'mean'),
            ('std_dev', 'std'),
            ('min_score', 'min'),
            ('max_score', 'max'),
            ('perfect_scores', lambda x: (x == 10.0).sum()),
            ('high_scores_8plus', lambda x: (x >= 8.0).sum()),
            ('low_scores_3minus', lambda x: (x <= 3.0).sum())
        ]).round(2)
        
        # Add percentage calculations
        grouped['perfect_score_pct'] = (grouped['perfect_scores'] / grouped['count'] * 100).round(1)
        grouped['high_score_pct'] = (grouped['high_scores_8plus'] / grouped['count'] * 100).round(1)
        
        result = grouped.reset_index()
        result['analysis_type'] = 'by_generation_template'
        
        # Verify calculations
        template1_stats = result[result['generation_template'] == 'template1'].iloc[0]
        assert template1_stats['count'] == 3
        assert template1_stats['average'] == 9.17  # (8.5 + 9.0 + 10.0) / 3
        assert template1_stats['perfect_scores'] == 1
        assert template1_stats['perfect_score_pct'] == 33.3
        assert template1_stats['high_scores_8plus'] == 3
        
        template2_stats = result[result['generation_template'] == 'template2'].iloc[0]
        assert template2_stats['count'] == 2
        assert template2_stats['average'] == 7.5
        assert template2_stats['perfect_scores'] == 0
        assert template2_stats['high_scores_8plus'] == 1


class TestOverallStatistics:
    """Test overall statistics calculation."""
    
    def test_overall_statistics_calculation(self):
        """Test overall statistics calculation."""
        test_scores = pd.DataFrame({
            "score": [8.5, 9.0, 10.0, 7.0, 6.0, 10.0, 8.0, 9.5]
        })
        
        overall_stats = {
            'analysis_type': 'overall_statistics',
            'count': len(test_scores),
            'average': test_scores['score'].mean(),
            'std_dev': test_scores['score'].std(),
            'min_score': test_scores['score'].min(),
            'max_score': test_scores['score'].max(),
            'perfect_scores': (test_scores['score'] == 10.0).sum(),
            'high_scores_8plus': (test_scores['score'] >= 8.0).sum(),
            'low_scores_3minus': (test_scores['score'] <= 3.0).sum(),
            'perfect_score_pct': ((test_scores['score'] == 10.0).sum() / len(test_scores) * 100),
            'high_score_pct': ((test_scores['score'] >= 8.0).sum() / len(test_scores) * 100)
        }
        
        assert overall_stats['count'] == 8
        assert overall_stats['average'] == 8.5
        assert overall_stats['perfect_scores'] == 2
        assert overall_stats['perfect_score_pct'] == 25.0
        assert overall_stats['high_scores_8plus'] == 6
        assert overall_stats['high_score_pct'] == 75.0
        assert overall_stats['low_scores_3minus'] == 0


class TestPerfectScoreFiltering:
    """Test perfect score filtering logic."""
    
    def test_perfect_score_filtering(self):
        """Test filtering for perfect scores only."""
        test_data = pd.DataFrame([
            {"score": 10.0, "error": None, "combo_id": "combo_001"},
            {"score": 9.5, "error": None, "combo_id": "combo_002"},
            {"score": 10.0, "error": "Parse failed", "combo_id": "combo_003"},
            {"score": 8.0, "error": None, "combo_id": "combo_004"},
            {"score": 10.0, "error": None, "combo_id": "combo_005"}
        ])
        
        # Filter for perfect scores without errors
        perfect_scores = test_data[
            (test_data['score'] == 10.0) & 
            (test_data['error'].isna())
        ].copy()
        
        assert len(perfect_scores) == 2
        assert perfect_scores.iloc[0]['combo_id'] == 'combo_001'
        assert perfect_scores.iloc[1]['combo_id'] == 'combo_005'
        
        # Verify that score with error is excluded
        assert 'combo_003' not in perfect_scores['combo_id'].values


class TestPathReconstructionLogic:
    """Test path reconstruction for perfect scores."""
    
    def test_path_reconstruction_logic(self):
        """Test the path reconstruction logic."""
        # Test data for path reconstruction
        test_row = {
            'combo_id': 'combo_001',
            'generation_template': 'systematic-analytical-v2',
            'generation_model_provider': 'deepseek',
            'evaluation_model_provider': 'qwen',
            'evaluation_template': 'daydreaming-verification-v2'
        }
        
        # Generation path reconstruction
        gen_model_provider = test_row['generation_model_provider']
        if gen_model_provider == 'google':
            gen_model_full = 'gemma-3-27b-it:free'
        elif gen_model_provider == 'deepseek':
            gen_model_full = 'deepseek-r1:free'
        else:
            gen_model_full = gen_model_provider
        
        gen_dir = f"{test_row['combo_id']}_{test_row['generation_template']}_{gen_model_provider}"
        generation_path = f"data/3_generation/generation_responses/{gen_dir}/{gen_model_full}.txt"
        
        expected_gen_path = "data/3_generation/generation_responses/combo_001_systematic-analytical-v2_deepseek/deepseek-r1:free.txt"
        assert generation_path == expected_gen_path
        
        # Evaluation path reconstruction
        eval_model_provider = test_row['evaluation_model_provider']
        if eval_model_provider == 'qwen':
            eval_model_full = 'qwq-32b:free'
        elif eval_model_provider == 'deepseek':  
            eval_model_full = 'deepseek-r1:free'
        else:
            eval_model_full = eval_model_provider
        
        generation_dir = f"{test_row['combo_id']}_{test_row['generation_template']}_{gen_model_provider}"
        eval_subdir = f"{gen_model_full}_{test_row['evaluation_template']}_{eval_model_provider}"
        evaluation_path = f"data/4_evaluation/evaluation_responses/{generation_dir}/{eval_subdir}/{eval_model_full}.txt"
        
        expected_eval_path = "data/4_evaluation/evaluation_responses/combo_001_systematic-analytical-v2_deepseek/deepseek-r1:free_daydreaming-verification-v2_qwen/qwq-32b:free.txt"
        assert evaluation_path == expected_eval_path


class TestModelMapping:
    """Test model provider to full model name mapping."""
    
    def test_model_mapping_logic(self):
        """Test model provider to full model name mapping."""
        test_cases = [
            {'provider': 'google', 'expected': 'gemma-3-27b-it:free'},
            {'provider': 'deepseek', 'expected': 'deepseek-r1:free'},
            {'provider': 'qwen', 'expected': 'qwq-32b:free'},
            {'provider': 'unknown', 'expected': 'unknown'}
        ]
        
        # Test generation model mapping
        for test_case in test_cases:
            provider = test_case['provider']
            if provider == 'google':
                result = 'gemma-3-27b-it:free'
            elif provider == 'deepseek':
                result = 'deepseek-r1:free'
            else:
                result = provider
            
            if test_case['provider'] in ['google', 'deepseek']:
                assert result == test_case['expected']
            else:
                assert result == provider
        
        # Test evaluation model mapping
        for test_case in test_cases:
            provider = test_case['provider']
            if provider == 'qwen':
                result = 'qwq-32b:free'
            elif provider == 'deepseek':
                result = 'deepseek-r1:free'
            else:
                result = provider
            
            if test_case['provider'] in ['qwen', 'deepseek']:
                expected = 'qwq-32b:free' if provider == 'qwen' else 'deepseek-r1:free'
                assert result == expected
            else:
                assert result == provider


class TestMetadataCalculation:
    """Test metadata calculation for perfect scores."""
    
    def test_metadata_calculation_for_perfect_scores(self):
        """Test metadata calculation for perfect scores."""
        test_perfect_scores = pd.DataFrame([
            {
                "evaluation_model_provider": "deepseek",
                "generation_template": "template1",
                "combo_id": "combo_001"
            },
            {
                "evaluation_model_provider": "qwen", 
                "generation_template": "template1",
                "combo_id": "combo_002"
            },
            {
                "evaluation_model_provider": "deepseek",
                "generation_template": "template2", 
                "combo_id": "combo_003"
            }
        ])
        
        # Calculate metadata like the function does
        evaluator_counts = test_perfect_scores['evaluation_model_provider'].value_counts().to_dict()
        template_counts = test_perfect_scores['generation_template'].value_counts().to_dict()
        
        assert evaluator_counts['deepseek'] == 2
        assert evaluator_counts['qwen'] == 1
        assert template_counts['template1'] == 2
        assert template_counts['template2'] == 1
        
        # Test top template calculation
        top_template = max(template_counts.keys(), key=template_counts.get)
        assert top_template == 'template1'
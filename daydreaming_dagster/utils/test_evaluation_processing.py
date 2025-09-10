"""Tests for evaluation processing utilities."""

import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import Mock

from daydreaming_dagster.utils.evaluation_processing import (
    enrich_evaluation_data,
    calculate_evaluation_metadata,
)


"""Legacy parsing helpers were removed. Remaining helpers operate on DataFrames.

This test module validates the enrichment utilities that are still used by
gens-store based assets.
"""


def test_enrich_evaluation_data_basic():
    """Test basic evaluation metadata enrichment."""
    parsed_df = pd.DataFrame({
        'evaluation_task_id': ['task1', 'task2'],
        'score': [8.0, 9.0],
        'error': [None, None]
    })
    
    evaluation_tasks = pd.DataFrame({
        'evaluation_task_id': ['task1', 'task2'],
        'essay_task_id': ['essay1', 'essay2'],
        'evaluation_template': ['template1', 'template2'],
        'evaluation_model': ['model1', 'model2']
    })
    
    essay_generation_tasks = pd.DataFrame({
        'essay_task_id': ['essay1', 'essay2'],
        'combo_id': ['combo1', 'combo2'],
        'essay_template': ['essay_template1', 'essay_template2'],
        'generation_model': ['gen_model1', 'gen_model2']
    })
    
    result = enrich_evaluation_data(parsed_df, evaluation_tasks, essay_generation_tasks)
    
    assert len(result) == 2
    assert 'combo_id' in result.columns
    assert 'essay_template' in result.columns
    assert result.iloc[0]['combo_id'] == 'combo1'


def test_calculate_evaluation_metadata():
    """Test evaluation metadata calculation."""
    df = pd.DataFrame({
        'score': [8.0, 9.0, 7.0],
        'error': [None, None, None]
    })
    
    metadata = calculate_evaluation_metadata(df)
    
    assert metadata['total_responses'].value == 3
    assert metadata['successful_parses'].value == 3
    assert metadata['success_rate'].value == 100.0
    assert metadata['avg_score'].value == 8.0


    # Parsing functions removed; file path helper removed.

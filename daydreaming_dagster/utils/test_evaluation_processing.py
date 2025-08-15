"""Tests for evaluation processing utilities."""

import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import Mock

from daydreaming_dagster.utils.evaluation_processing import parse_evaluation_files, enrich_evaluation_data, calculate_evaluation_metadata, add_evaluation_file_paths


def test_parse_evaluation_files_empty_tasks():
    """Test parsing with empty evaluation tasks."""
    empty_tasks = pd.DataFrame()
    result = parse_evaluation_files(empty_tasks, Path("/tmp"), lambda x, y: {"score": 5, "error": None})
    
    assert len(result) == 0
    assert list(result.columns) == ['evaluation_task_id', 'score', 'error']


def test_enrich_evaluation_data_basic():
    """Test basic evaluation metadata enrichment."""
    parsed_df = pd.DataFrame({
        'evaluation_task_id': ['task1', 'task2'],
        'score': [8.0, 9.0],
        'error': [None, None]
    })
    
    evaluation_tasks = pd.DataFrame({
        'evaluation_task_id': ['task1', 'task2'],
        'generation_task_id': ['gen1', 'gen2'],
        'evaluation_template': ['template1', 'template2'],
        'evaluation_model': ['model1', 'model2']
    })
    
    generation_tasks = pd.DataFrame({
        'generation_task_id': ['gen1', 'gen2'],
        'combo_id': ['combo1', 'combo2'],
        'generation_template': ['gen_template1', 'gen_template2'],
        'generation_model': ['gen_model1', 'gen_model2']
    })
    
    result = enrich_evaluation_data(parsed_df, evaluation_tasks, generation_tasks)
    
    assert len(result) == 2
    assert 'combo_id' in result.columns
    assert 'generation_template' in result.columns
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


def test_add_evaluation_file_paths():
    """Test adding evaluation file paths."""
    df = pd.DataFrame({
        'combo_id': ['combo1', 'combo2'],
        'template': ['template1', 'template2']
    })
    
    result = add_evaluation_file_paths(df, "data/path", "{combo_id}_{template}.txt")
    
    assert 'file_path' in result.columns
    assert result.iloc[0]['file_path'] == "data/path/combo1_template1.txt"

"""Tests for evaluation processing utilities."""

import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import Mock

from daydreaming_dagster.utils.evaluation_processing import parse_evaluation_files, enrich_evaluation_data, calculate_evaluation_metadata, add_evaluation_file_paths


def test_parse_evaluation_files_empty_tasks():
    """Test parsing with empty evaluation tasks."""
    empty_tasks = pd.DataFrame()
    eval_templates = pd.DataFrame({"template_id": [], "parser": []})
    result = parse_evaluation_files(empty_tasks, Path("/tmp"), lambda x, y: {"score": 5, "error": None}, evaluation_templates=eval_templates)
    
    assert len(result) == 0
    assert list(result.columns) == ['evaluation_task_id', 'score', 'error']


def test_parse_evaluation_files_respects_parser_complex(tmp_path: Path):
    """Default parser selection should honor explicit parser='complex'."""
    # Prepare task row and file
    task_id = "doc__templateX__modelY"
    (tmp_path / f"{task_id}.txt").write_text("REASONING: ok\nTotal Score: 7/10", encoding="utf-8")
    tasks = pd.DataFrame([
        {"evaluation_task_id": task_id, "evaluation_template": "templateX"}
    ])
    eval_templates = pd.DataFrame([
        {"template_id": "templateX", "parser": "complex"}
    ])
    out = parse_evaluation_files(tasks, tmp_path, evaluation_templates=eval_templates)
    assert len(out) == 1
    assert out.iloc[0]["score"] == 7.0
    assert out.iloc[0]["used_parser"] == "complex"


def test_parse_evaluation_files_respects_parser_in_last_line(tmp_path: Path):
    """Default parser selection should honor explicit parser='in_last_line'."""
    task_id = "doc__templateY__modelZ"
    (tmp_path / f"{task_id}.txt").write_text("Some text\nSCORE: 8", encoding="utf-8")
    tasks = pd.DataFrame([
        {"evaluation_task_id": task_id, "evaluation_template": "templateY"}
    ])
    eval_templates = pd.DataFrame([
        {"template_id": "templateY", "parser": "in_last_line"}
    ])
    out = parse_evaluation_files(tasks, tmp_path, evaluation_templates=eval_templates)
    assert len(out) == 1
    assert out.iloc[0]["score"] == 8.0
    assert out.iloc[0]["used_parser"] == "in_last_line"


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


def test_add_evaluation_file_paths():
    """Test adding evaluation file paths."""
    df = pd.DataFrame({
        'combo_id': ['combo1', 'combo2'],
        'template': ['template1', 'template2']
    })
    
    result = add_evaluation_file_paths(df, "data/path", "{combo_id}_{template}.txt")
    
    assert 'file_path' in result.columns
    assert result.iloc[0]['file_path'] == "data/path/combo1_template1.txt"

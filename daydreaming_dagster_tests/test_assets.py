import pytest
import pandas as pd
from unittest.mock import MagicMock
from dagster import build_asset_context
from daydreaming_dagster.assets.core import (
    concepts_and_content,
    concept_contents,
    concept_combinations,
    generation_tasks,
    evaluation_tasks
)
from daydreaming_dagster.resources.api_client import ExperimentConfig

@pytest.fixture
def mock_experiment_config():
    """Mock experiment configuration resource."""
    config = MagicMock()
    config.to_dict.return_value = {
        "k_max": 3,
        "description_level": "paragraph",
        "current_gen_template": "00_systematic_analytical",
        "current_eval_template": "creativity_metrics"
    }
    return config

@pytest.fixture
def mock_concepts_metadata():
    """Mock concepts metadata DataFrame."""
    return pd.DataFrame([
        {"concept_id": "test-concept-1", "name": "Test Concept 1"},
        {"concept_id": "test-concept-2", "name": "Test Concept 2"},
    ])

@pytest.fixture
def mock_concept_descriptions():
    """Mock concept descriptions dictionaries."""
    return {
        "test-concept-1": "Test description 1",
        "test-concept-2": "Test description 2",
    }

@pytest.fixture
def mock_generation_templates():
    """Mock generation templates dictionary."""
    return {
        "00_systematic_analytical": "Test systematic template content",
        "01_creative_synthesis": "Test creative template content",
    }

@pytest.fixture
def mock_evaluation_templates():
    """Mock evaluation templates dictionary."""
    return {
        "creativity_metrics": "Test creativity evaluation template",
        "scientific_rigor": "Test rigor evaluation template",
    }

def test_concepts_and_content(mock_experiment_config, mock_concepts_metadata, mock_concept_descriptions):
    """Test concepts_and_content asset."""
    context = build_asset_context(resources={"experiment_config": mock_experiment_config})
    
    result = concepts_and_content(
        context,
        mock_concepts_metadata,
        mock_concept_descriptions,
        mock_concept_descriptions,
        mock_concept_descriptions
    )
    
    assert isinstance(result, tuple)
    assert len(result) == 2
    concepts_list, concept_contents_dict = result
    assert isinstance(concepts_list, list)
    assert isinstance(concept_contents_dict, dict)

def test_concept_contents():
    """Test concept_contents asset."""
    mock_concepts_and_content = (
        [{"concept_id": "test-1", "name": "Test 1"}],
        {"test-1": "Test content 1", "test-2": "Test content 2"}
    )
    
    result = concept_contents(mock_concepts_and_content)
    
    assert isinstance(result, dict)
    assert "test-1" in result
    assert "test-2" in result

def test_concept_combinations(mock_experiment_config):
    """Test concept_combinations asset."""
    context = build_asset_context(resources={"experiment_config": mock_experiment_config})
    mock_concepts_and_content = (
        [{"concept_id": "test-1", "name": "Test 1"}, {"concept_id": "test-2", "name": "Test 2"}],
        {"test-1": "content1", "test-2": "content2"}
    )
    
    result = concept_combinations(context, mock_concepts_and_content)
    
    assert isinstance(result, tuple)
    assert len(result) == 2
    combo_df, combo_relationships = result
    assert isinstance(combo_df, pd.DataFrame)
    assert isinstance(combo_relationships, pd.DataFrame)

def test_generation_tasks(mock_experiment_config, mock_generation_templates, mock_evaluation_templates):
    """Test generation_tasks asset."""
    context = build_asset_context(resources={"experiment_config": mock_experiment_config})
    
    # Mock concept combinations
    mock_combo_df = pd.DataFrame([{"combo_id": "combo_001", "description": "test combo", "num_concepts": 2}])
    mock_combo_relationships = pd.DataFrame([{"combo_id": "combo_001", "concept_id": "test-1", "position": 0}])
    mock_concept_combinations = (mock_combo_df, mock_combo_relationships)
    
    # Mock models
    mock_generation_models = pd.DataFrame([{"model_name": "test_model", "model_short": "test", "active": True}])
    mock_evaluation_models = pd.DataFrame([{"model_name": "eval_model", "model_short": "eval", "active": True}])
    
    result = generation_tasks(
        context,
        mock_concept_combinations,
        mock_generation_models,
        mock_evaluation_models,
        mock_generation_templates,
        mock_evaluation_templates
    )
    
    assert isinstance(result, pd.DataFrame)

def test_evaluation_tasks(mock_experiment_config, mock_generation_templates, mock_evaluation_templates):
    """Test evaluation_tasks asset."""
    context = build_asset_context(resources={"experiment_config": mock_experiment_config})
    
    # Mock concept combinations
    mock_combo_df = pd.DataFrame([{"combo_id": "combo_001", "description": "test combo", "num_concepts": 2}])
    mock_combo_relationships = pd.DataFrame([{"combo_id": "combo_001", "concept_id": "test-1", "position": 0}])
    mock_concept_combinations = (mock_combo_df, mock_combo_relationships)
    
    # Mock models
    mock_generation_models = pd.DataFrame([{"model_name": "test_model", "model_short": "test", "active": True}])
    mock_evaluation_models = pd.DataFrame([{"model_name": "eval_model", "model_short": "eval", "active": True}])
    
    result = evaluation_tasks(
        context,
        mock_concept_combinations,
        mock_generation_models,
        mock_evaluation_models,
        mock_generation_templates,
        mock_evaluation_templates
    )
    
    assert isinstance(result, pd.DataFrame)
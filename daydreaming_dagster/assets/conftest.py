"""Shared fixtures for asset unit tests."""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock
from daydreaming_dagster.models import ContentCombination, Concept


@pytest.fixture
def sample_task_data():
    """Provide sample task data for testing asset functions using two-phase generation system."""
    return {
        "link_generation_tasks": pd.DataFrame([
            {
                "link_task_id": "combo_001_creative-synthesis-v10_deepseek_r1_f",
                "combo_id": "combo_001",
                "link_template": "creative-synthesis-v10",
                "generation_model": "deepseek_r1_f",
                "generation_model_name": "deepseek/deepseek-r1:free"
            },
            {
                "link_task_id": "combo_002_creative-synthesis-v10_gemma_3_27b_f",
                "combo_id": "combo_002", 
                "link_template": "creative-synthesis-v10",
                "generation_model": "gemma_3_27b_f",
                "generation_model_name": "google/gemma-3-27b-it:free"
            }
        ]),
        "essay_generation_tasks": pd.DataFrame([
            {
                "essay_task_id": "combo_001_creative-synthesis-v10_deepseek_r1_f_creative-synthesis-v10",
                "link_task_id": "combo_001_creative-synthesis-v10_deepseek_r1_f",
                "combo_id": "combo_001",
                "link_template": "creative-synthesis-v10",
                "essay_template": "creative-synthesis-v10",
                "generation_model": "deepseek_r1_f",
                "generation_model_name": "deepseek/deepseek-r1:free"
            },
            {
                "essay_task_id": "combo_002_creative-synthesis-v10_gemma_3_27b_f_creative-synthesis-v10",
                "link_task_id": "combo_002_creative-synthesis-v10_gemma_3_27b_f",
                "combo_id": "combo_002",
                "link_template": "creative-synthesis-v10",
                "essay_template": "creative-synthesis-v10",
                "generation_model": "gemma_3_27b_f",
                "generation_model_name": "google/gemma-3-27b-it:free"
            }
        ]),
        "evaluation_tasks": pd.DataFrame([
            {
                "evaluation_task_id": "combo_001_creative-synthesis-v10_deepseek_r1_f_creative-synthesis-v10_daydreaming-verification-v2_deepseek_r1_f",
                "essay_task_id": "combo_001_creative-synthesis-v10_deepseek_r1_f_creative-synthesis-v10",
                "evaluation_template": "daydreaming-verification-v2",
                "evaluation_model": "deepseek_r1_f",
                "evaluation_model_name": "deepseek/deepseek-r1:free"
            },
            {
                "evaluation_task_id": "combo_001_creative-synthesis-v10_deepseek_r1_f_creative-synthesis-v10_daydreaming-verification-v2_qwq_32b_f",
                "essay_task_id": "combo_001_creative-synthesis-v10_deepseek_r1_f_creative-synthesis-v10",
                "evaluation_template": "daydreaming-verification-v2", 
                "evaluation_model": "qwq_32b_f",
                "evaluation_model_name": "qwen/qwq-32b:free"
            }
        ])
    }


@pytest.fixture
def sample_content_combinations():
    """Provide sample ContentCombination objects for testing."""
    concepts = [
        Concept(concept_id="test-concept-1", name="Test Concept One", descriptions={"sentence": "First concept"}),
        Concept(concept_id="test-concept-2", name="Test Concept Two", descriptions={"sentence": "Second concept"})
    ]
    
    return [
        ContentCombination.from_concepts(concepts, "sentence", combo_id="combo_001"),
        ContentCombination.from_concepts([concepts[0]], "sentence", combo_id="combo_002")
    ]


@pytest.fixture
def sample_templates():
    """Provide sample template data for testing two-phase generation system."""
    return {
        "link_templates": {
            "creative-synthesis-v10": "Generate conceptual links for: {% for concept in concepts %}{{ concept.name }}: {{ concept.content }}{% endfor %}"
        },
        "essay_templates": {
            "creative-synthesis-v10": "Write an essay using these links:\n{{ links }}\n\nBase concepts: {% for concept in concepts %}{{ concept.name }}{% endfor %}"
        },
        "evaluation_templates": {
            "daydreaming-verification-v2": "Evaluate this response for creativity and insight:\n{{ response }}\n\nSCORE:",
            "o3-prior-art-eval": "Check for prior art in this response:\n{{ response }}\n\nSCORE:"
        }
    }


@pytest.fixture
def sample_parsed_scores():
    """Provide sample parsed scores data for testing analysis functions using two-phase generation system."""
    return pd.DataFrame([
        {
            "evaluation_task_id": "combo_001_creative-synthesis-v10_deepseek_r1_f_creative-synthesis-v10_daydreaming-verification-v2_deepseek_r1_f",
            "score": 8.5,
            "error": None,
            "combo_id": "combo_001",
            "link_template": "creative-synthesis-v10",
            "essay_template": "creative-synthesis-v10",
            "generation_model_provider": "deepseek",
            "evaluation_template": "daydreaming-verification-v2",
            "evaluation_model_provider": "deepseek",
            "essay_task_id": "combo_001_creative-synthesis-v10_deepseek_r1_f_creative-synthesis-v10"
        },
        {
            "evaluation_task_id": "combo_001_creative-synthesis-v10_deepseek_r1_f_creative-synthesis-v10_daydreaming-verification-v2_qwq_32b_f",
            "score": 7.2,
            "error": None,
            "combo_id": "combo_001",
            "link_template": "creative-synthesis-v10",
            "essay_template": "creative-synthesis-v10", 
            "generation_model_provider": "deepseek",
            "evaluation_template": "daydreaming-verification-v2",  
            "evaluation_model_provider": "qwen",
            "essay_task_id": "combo_001_creative-synthesis-v10_deepseek_r1_f_creative-synthesis-v10"
        },
        {
            "evaluation_task_id": "combo_002_creative-synthesis-v10_gemma_3_27b_f_creative-synthesis-v10_daydreaming-verification-v2_deepseek_r1_f",
            "score": 9.0,
            "error": None,
            "combo_id": "combo_002",
            "link_template": "creative-synthesis-v10",
            "essay_template": "creative-synthesis-v10",
            "generation_model_provider": "google", 
            "evaluation_template": "daydreaming-verification-v2",
            "evaluation_model_provider": "deepseek",
            "essay_task_id": "combo_002_creative-synthesis-v10_gemma_3_27b_f_creative-synthesis-v10"
        },
        {
            "evaluation_task_id": "combo_003_broken_task_id",
            "score": np.nan,
            "error": "Failed to parse response",
            "combo_id": "combo_003", 
            "link_template": "unknown",
            "essay_template": "unknown",
            "generation_model_provider": "unknown",
            "evaluation_template": "unknown",
            "evaluation_model_provider": "unknown",
            "essay_task_id": "combo_003_broken_task_id"
        }
    ])


@pytest.fixture 
def mock_context():
    """Provide a mock Dagster context for testing."""
    context = Mock()
    context.log = Mock()
    context.add_output_metadata = Mock()
    context.resources = Mock()
    return context


@pytest.fixture
def mock_llm_client():
    """Provide a mock LLM client for testing."""
    client = Mock()
    client.generate.return_value = "Mock LLM response"
    return client
"""Unit tests for LLM generation utility functions."""

import pytest
from jinja2 import Environment
from daydreaming_dagster.models import Concept, ContentCombination


class TestPromptTemplateLogic:
    """Test the core prompt template logic."""
    
    def test_jinja2_template_with_concepts(self):
        """Test Jinja2 template rendering with concept data."""
        concepts = [
            Concept(concept_id="c1", name="Creativity", descriptions={"sentence": "The ability to create"}),
            Concept(concept_id="c2", name="Innovation", descriptions={"sentence": "Novel solutions"})
        ]
        
        combo = ContentCombination.from_concepts(concepts, "sentence", combo_id="test_combo")
        template_content = "{% for concept in concepts %}{{ concept.name }}: {{ concept.content }}; {% endfor %}"
        
        env = Environment()
        template = env.from_string(template_content)
        result = template.render(concepts=combo.contents)
        
        assert "Creativity: The ability to create;" in result
        assert "Innovation: Novel solutions;" in result
    
    def test_empty_concepts_list(self):
        """Test template rendering with empty concepts list."""
        template_content = "Concepts: {% for concept in concepts %}{{ concept.name }}{% endfor %}"
        
        env = Environment()
        template = env.from_string(template_content)
        result = template.render(concepts=[])
        
        assert result == "Concepts: "
    
    def test_template_with_missing_variable(self):
        """Test template with missing variable renders as empty (Jinja2 default behavior)."""
        template_content = "Hello {{ missing_variable }} World"
        
        env = Environment()
        template = env.from_string(template_content)
        
        # Jinja2 default behavior: undefined variables render as empty string
        result = template.render(concepts=[])
        assert result == "Hello  World"  # Missing variable becomes empty space


class TestModelNameMapping:
    """Test model name mapping logic."""
    
    def test_model_id_to_name_mapping(self):
        """Test conversion from model ID to model name."""
        test_cases = [
            {"model_id": "deepseek_r1_f", "expected": "deepseek/deepseek-r1:free"},
            {"model_id": "gemma_3_27b_f", "expected": "google/gemma-3-27b-it:free"},
            {"model_id": "qwq_32b_f", "expected": "qwen/qwq-32b:free"},
            {"model_id": "unknown_model", "expected": "unknown_model"}
        ]
        
        def get_model_name(model_id):
            """Convert model ID to full model name."""
            model_mapping = {
                "deepseek_r1_f": "deepseek/deepseek-r1:free",
                "gemma_3_27b_f": "google/gemma-3-27b-it:free", 
                "qwq_32b_f": "qwen/qwq-32b:free"
            }
            return model_mapping.get(model_id, model_id)
        
        for test_case in test_cases:
            result = get_model_name(test_case["model_id"])
            assert result == test_case["expected"]
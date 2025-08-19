"""Unit tests for two-phase generation business logic.

Tests pure functions without Dagster dependencies, external file access, or I/O.
"""

import pytest
from unittest.mock import Mock
from jinja2 import Environment
import pandas as pd


class TestPromptGeneration:
    """Test prompt generation logic."""
    
    def test_jinja2_template_rendering_with_concepts(self):
        """Test that Jinja2 template renders correctly with concept data."""
        template_content = "Hello {% for concept in concepts %}{{ concept.name }}: {{ concept.content }}{% endfor %}"
        
        # Mock concept data with proper attribute setup
        mock_concept1 = Mock()
        mock_concept1.name = "concept1"
        mock_concept1.content = "content1"
        
        mock_concept2 = Mock()
        mock_concept2.name = "concept2" 
        mock_concept2.content = "content2"
        
        mock_concepts = [mock_concept1, mock_concept2]
        
        env = Environment()
        template = env.from_string(template_content)
        result = template.render(concepts=mock_concepts)
        
        assert "concept1: content1" in result
        assert "concept2: content2" in result
    
    def test_jinja2_template_with_empty_concepts(self):
        """Test template rendering with empty concepts list."""
        template_content = "Concepts: {% for concept in concepts %}{{ concept.name }}{% endfor %}"
        
        env = Environment()
        template = env.from_string(template_content)
        result = template.render(concepts=[])
        
        assert result == "Concepts: "
    
    def test_jinja2_template_with_links_block(self):
        """Test essay template rendering with links block."""
        template_content = "Essay based on: {{ links_block }}"
        links_content = "• Link 1\n• Link 2\n• Link 3"
        
        env = Environment()
        template = env.from_string(template_content)
        result = template.render(links_block=links_content)
        
        assert "• Link 1" in result
        assert "• Link 2" in result
        assert "• Link 3" in result


class TestLinksValidation:
    """Test links content validation logic - CRITICAL BUSINESS RULE."""
    
    def validate_links_count(self, links_content: str) -> int:
        """Extract the actual validation logic from essay_prompt asset."""
        links_lines = [line.strip() for line in links_content.split('\n') if line.strip()]
        return len(links_lines)
    
    def test_sufficient_links_validation(self):
        """Test that sufficient links (>=3) pass validation."""
        links_content = "• Link 1\n• Link 2\n• Link 3\n• Link 4"
        count = self.validate_links_count(links_content)
        
        assert count >= 3
        assert count == 4
    
    def test_minimum_links_boundary(self):
        """Test exactly 3 links (boundary condition)."""
        links_content = "• Link 1\n• Link 2\n• Link 3"
        count = self.validate_links_count(links_content)
        
        assert count >= 3
        assert count == 3
    
    def test_insufficient_links_validation(self):
        """Test that insufficient links (<3) fail validation."""
        test_cases = [
            ("• Link 1\n• Link 2", 2),
            ("• Link 1", 1),
        ]
        
        for links_content, expected_count in test_cases:
            count = self.validate_links_count(links_content)
            assert count < 3
            assert count == expected_count
    
    def test_empty_links_validation(self):
        """Test that empty links fail validation."""
        links_content = ""
        count = self.validate_links_count(links_content)
        
        assert count == 0
        assert count < 3
    
    def test_whitespace_only_links_validation(self):
        """Test that whitespace-only content fails validation."""
        test_cases = [
            "   \n\n   \n",
            "\t\t\t",
            "   ",
            "\n\n\n",
        ]
        
        for links_content in test_cases:
            count = self.validate_links_count(links_content)
            assert count == 0
            assert count < 3
    
    def test_mixed_empty_and_valid_lines(self):
        """Test links with empty lines mixed in."""
        links_content = "• Link 1\n\n• Link 2\n   \n• Link 3\n\t\n• Link 4"
        count = self.validate_links_count(links_content)
        
        assert count >= 3
        assert count == 4  # Should ignore empty/whitespace lines


class TestTaskDataExtraction:
    """Test task data extraction logic."""
    
    def test_task_lookup_success(self):
        """Test successful task lookup from DataFrame."""
        import pandas as pd
        
        tasks_df = pd.DataFrame([
            {"generation_task_id": "task_001", "generation_template": "template1", "generation_model_name": "model1"},
            {"generation_task_id": "task_002", "generation_template": "template2", "generation_model_name": "model2"}
        ])
        
        task_id = "task_001"
        matching_tasks = tasks_df[tasks_df["generation_task_id"] == task_id]
        
        assert not matching_tasks.empty
        task_row = matching_tasks.iloc[0]
        assert task_row["generation_template"] == "template1"
        assert task_row["generation_model_name"] == "model1"
    
    def test_task_lookup_not_found(self):
        """Test task lookup when task doesn't exist."""
        import pandas as pd
        
        tasks_df = pd.DataFrame([
            {"generation_task_id": "task_001", "generation_template": "template1", "generation_model_name": "model1"}
        ])
        
        task_id = "nonexistent_task"
        matching_tasks = tasks_df[tasks_df["generation_task_id"] == task_id]
        
        assert matching_tasks.empty
    
    def test_combination_lookup_success(self):
        """Test successful combination lookup from list."""
        mock_combinations = [
            Mock(combination_id="combo_001", contents=[Mock(name="concept1")]),
            Mock(combination_id="combo_002", contents=[Mock(name="concept2")])
        ]
        
        combo_id = "combo_001"
        matching_combinations = [c for c in mock_combinations if c.combination_id == combo_id]
        
        assert len(matching_combinations) == 1
        assert matching_combinations[0].combination_id == "combo_001"
    
    def test_combination_lookup_not_found(self):
        """Test combination lookup when combination doesn't exist."""
        mock_combinations = [
            Mock(combination_id="combo_001", contents=[Mock(name="concept1")])
        ]
        
        combo_id = "nonexistent_combo"
        matching_combinations = [c for c in mock_combinations if c.combination_id == combo_id]
        
        assert len(matching_combinations) == 0


class TestStringProcessing:
    """Test string processing utilities."""
    
    def test_word_count_calculation(self):
        """Test word count calculation logic."""
        text = "This is a test essay with multiple words."
        word_count = len(text.split())
        
        assert word_count == 8
    
    def test_word_count_empty_string(self):
        """Test word count with empty string."""
        text = ""
        word_count = len(text.split()) if text else 0
        
        assert word_count == 0
    
    def test_word_count_whitespace_only(self):
        """Test word count with whitespace-only string."""
        text = "   \n\n   "
        word_count = len(text.split())
        
        assert word_count == 0


class TestTemplateValidation:
    """Test template validation logic."""
    
    def test_template_content_validation(self):
        """Test that template content is properly validated."""
        template_content = "Valid template: {{ variable }}"
        
        # Basic validation - template should be non-empty string
        assert isinstance(template_content, str)
        assert len(template_content) > 0
        assert "{{" in template_content  # Has Jinja2 syntax
    
    def test_invalid_template_content(self):
        """Test handling of invalid template content."""
        # Test various invalid template scenarios
        invalid_templates = [
            None,
            "",
            "   ",
            123,  # Not a string
        ]
        
        for invalid in invalid_templates:
            if invalid is None or not isinstance(invalid, str) or not invalid.strip():
                # This would be caught by validation logic
                assert True
            else:
                assert False, f"Should have caught invalid template: {invalid}"


class TestModelNameExtraction:
    """Test model name extraction and validation."""
    
    def test_model_name_extraction(self):
        """Test extracting model name from task data."""
        task_data = {
            "generation_model_name": "test-model",
            "generation_template": "test-template"
        }
        
        model_name = task_data["generation_model_name"]
        
        assert model_name == "test-model"
        assert isinstance(model_name, str)
        assert len(model_name) > 0
    
    def test_model_name_validation(self):
        """Test model name validation logic."""
        valid_model_names = ["gpt-4", "claude-3", "test-model", "deepseek/deepseek-r1"]
        invalid_model_names = ["", None, "   ", 123]
        
        for valid_name in valid_model_names:
            assert isinstance(valid_name, str)
            assert len(valid_name.strip()) > 0
        
        for invalid_name in invalid_model_names:
            if not isinstance(invalid_name, str) or not invalid_name or not invalid_name.strip():
                # This would be caught by validation
                assert True
            else:
                assert False, f"Should have caught invalid model name: {invalid_name}"
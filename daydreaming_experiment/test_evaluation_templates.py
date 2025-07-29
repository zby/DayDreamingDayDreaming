import pytest
from unittest.mock import Mock, patch, mock_open, MagicMock
import tempfile
from pathlib import Path

from daydreaming_experiment.evaluation_templates import EvaluationTemplateLoader


class TestEvaluationTemplateLoader:

    def test_init_with_valid_directory(self):
        """Test initialization with valid templates directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            templates_dir = Path(temp_dir)
            
            # Create a test template file
            template_file = templates_dir / "test_template.txt"
            template_file.write_text("Test template content: {{ response }}")
            
            loader = EvaluationTemplateLoader(str(templates_dir))
            assert loader.templates_dir == templates_dir

    def test_init_with_invalid_directory(self):
        """Test initialization with non-existent directory."""
        with pytest.raises(FileNotFoundError, match="Templates directory not found"):
            EvaluationTemplateLoader("/nonexistent/directory")

    def test_list_templates(self):
        """Test listing available templates."""
        with tempfile.TemporaryDirectory() as temp_dir:
            templates_dir = Path(temp_dir)
            
            # Create multiple template files
            (templates_dir / "template_a.txt").write_text("Template A")
            (templates_dir / "template_b.txt").write_text("Template B")
            (templates_dir / "template_c.txt").write_text("Template C")
            # Create non-template file (should be ignored)
            (templates_dir / "readme.md").write_text("Not a template")
            
            loader = EvaluationTemplateLoader(str(templates_dir))
            templates = loader.list_templates()
            
            assert templates == ["template_a", "template_b", "template_c"]

    def test_list_templates_empty_directory(self):
        """Test listing templates in empty directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            loader = EvaluationTemplateLoader(temp_dir)
            templates = loader.list_templates()
            assert templates == []

    def test_load_template_success(self):
        """Test loading a valid template."""
        with tempfile.TemporaryDirectory() as temp_dir:
            templates_dir = Path(temp_dir)
            template_content = "Evaluate this response: {{ response }}\nAnswer: YES/NO"
            
            template_file = templates_dir / "test_template.txt"
            template_file.write_text(template_content)
            
            loader = EvaluationTemplateLoader(str(templates_dir))
            template = loader.load_template("test_template")
            
            # Template should be a Jinja2 Template object
            assert hasattr(template, 'render')
            
            # Test rendering
            rendered = template.render(response="Test response")
            assert "Evaluate this response: Test response" in rendered

    def test_load_template_not_found(self):
        """Test loading non-existent template."""
        with tempfile.TemporaryDirectory() as temp_dir:
            templates_dir = Path(temp_dir)
            (templates_dir / "existing.txt").write_text("Exists")
            
            loader = EvaluationTemplateLoader(str(templates_dir))
            
            with pytest.raises(ValueError, match="Template 'nonexistent' not found"):
                loader.load_template("nonexistent")

    def test_render_evaluation_prompt(self):
        """Test rendering evaluation prompt with template."""
        with tempfile.TemporaryDirectory() as temp_dir:
            templates_dir = Path(temp_dir)
            template_content = """Evaluate the response for creativity:
{{ response }}

Rate: HIGH/MEDIUM/LOW"""
            
            template_file = templates_dir / "creativity.txt"
            template_file.write_text(template_content)
            
            loader = EvaluationTemplateLoader(str(templates_dir))
            prompt = loader.render_evaluation_prompt("creativity", "This is a creative response.")
            
            expected = """Evaluate the response for creativity:
This is a creative response.

Rate: HIGH/MEDIUM/LOW"""
            assert prompt == expected

    def test_get_default_template_iterative_loops_available(self):
        """Test getting default template when iterative_loops is available."""
        with tempfile.TemporaryDirectory() as temp_dir:
            templates_dir = Path(temp_dir)
            
            # Create multiple templates including iterative_loops
            (templates_dir / "iterative_loops.txt").write_text("Iterative loops template")
            (templates_dir / "other_template.txt").write_text("Other template")
            
            loader = EvaluationTemplateLoader(str(templates_dir))
            default = loader.get_default_template()
            
            assert default == "iterative_loops"

    def test_get_default_template_no_iterative_loops(self):
        """Test getting default template when iterative_loops is not available."""
        with tempfile.TemporaryDirectory() as temp_dir:
            templates_dir = Path(temp_dir)
            
            # Create templates but not iterative_loops
            (templates_dir / "template_a.txt").write_text("Template A")
            (templates_dir / "template_b.txt").write_text("Template B")
            
            loader = EvaluationTemplateLoader(str(templates_dir))
            default = loader.get_default_template()
            
            # Should return first available template alphabetically
            assert default == "template_a"

    def test_get_default_template_no_templates(self):
        """Test getting default template when no templates exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            loader = EvaluationTemplateLoader(temp_dir)
            
            with pytest.raises(ValueError, match="No evaluation templates found"):
                loader.get_default_template()

    def test_template_with_jinja2_features(self):
        """Test template with advanced Jinja2 features."""
        with tempfile.TemporaryDirectory() as temp_dir:
            templates_dir = Path(temp_dir)
            template_content = """
{%- if response|length > 100 -%}
Long response evaluation: {{ response[:50] }}...
{%- else -%}
Short response evaluation: {{ response }}
{%- endif -%}

Answer: YES/NO"""
            
            template_file = templates_dir / "conditional.txt"
            template_file.write_text(template_content)
            
            loader = EvaluationTemplateLoader(str(templates_dir))
            
            # Test with short response
            short_prompt = loader.render_evaluation_prompt("conditional", "Short")
            assert "Short response evaluation: Short" in short_prompt
            
            # Test with long response
            long_response = "A" * 150
            long_prompt = loader.render_evaluation_prompt("conditional", long_response)
            assert "Long response evaluation:" in long_prompt
            assert "..." in long_prompt


# Built-in evaluation prompt functionality removed - now using template system only


# Integration tests with real template files moved to tests/test_integration_data_dependent.py
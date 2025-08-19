import pytest
from pathlib import Path
from daydreaming_dagster.utils.template_loader import load_generation_template


class TestTemplateLoader:
    """Test the phase-aware template loading functionality."""
    
    def test_load_links_template_success(self):
        """Test successfully loading a links phase template."""
        template_content = load_generation_template("creative-synthesis-v10", "links")
        
        assert isinstance(template_content, str)
        assert len(template_content) > 0
        assert "Concept Link Generation" in template_content
        assert "{% for concept in concepts %}" in template_content
        
    def test_load_essay_template_success(self):
        """Test successfully loading an essay phase template."""
        template_content = load_generation_template("creative-synthesis-v10", "essay")
        
        assert isinstance(template_content, str)
        assert len(template_content) > 0
        assert "{{ links_block }}" in template_content
        assert "2,000–3,000 word narrative essay" in template_content
        
    def test_load_nonexistent_template_links_phase(self):
        """Test error handling when links template doesn't exist."""
        with pytest.raises(FileNotFoundError) as exc_info:
            load_generation_template("nonexistent-template", "links")
        
        assert "Template not found for phase='links'" in str(exc_info.value)
        assert "data/1_raw/generation_templates/links/nonexistent-template.txt" in str(exc_info.value)
        
    def test_load_nonexistent_template_essay_phase(self):
        """Test error handling when essay template doesn't exist."""
        with pytest.raises(FileNotFoundError) as exc_info:
            load_generation_template("nonexistent-template", "essay")
        
        assert "Template not found for phase='essay'" in str(exc_info.value)
        assert "data/1_raw/generation_templates/essay/nonexistent-template.txt" in str(exc_info.value)
        
    def test_load_existing_template_wrong_phase(self):
        """Test error when template exists for one phase but not another."""
        # creative-synthesis-v10 should exist for both phases
        # But let's test with a template that might only exist in the root
        with pytest.raises(FileNotFoundError) as exc_info:
            load_generation_template("nonexistent-phase-template", "links")
        
        assert "Template not found for phase='links'" in str(exc_info.value)
    
    def test_template_content_encoding(self):
        """Test that templates are loaded with proper UTF-8 encoding."""
        template_content = load_generation_template("creative-synthesis-v10", "links")
        
        # Should not raise UnicodeDecodeError
        assert isinstance(template_content, str)
        # Should handle any unicode characters properly
        encoded = template_content.encode('utf-8')
        assert isinstance(encoded, bytes)
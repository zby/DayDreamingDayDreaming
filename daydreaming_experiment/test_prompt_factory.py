import pytest
from daydreaming_experiment.concept import Concept
from daydreaming_experiment.prompt_factory import PromptFactory, PromptIterator, DEFAULT_TEMPLATES


class TestPromptFactory:
    def test_default_template_creation(self):
        """Test creating PromptFactory with default templates."""
        factory = PromptFactory()
        assert factory.get_template_count() == 5
        assert factory.templates == DEFAULT_TEMPLATES
    
    def test_custom_template_creation(self):
        """Test creating PromptFactory with custom templates."""
        custom_templates = ("Template 1: {concepts}", "Template 2: {concepts}")
        factory = PromptFactory(custom_templates)
        assert factory.get_template_count() == 2
        assert factory.templates == custom_templates
    
    def test_generate_prompt_single_concept(self):
        """Test generating prompt with single concept."""
        concept = Concept(
            name="neural_networks",
            sentence="Networks mimic brains.",
            paragraph="Neural networks are computational models inspired by biological systems."
        )
        factory = PromptFactory()
        
        prompt = factory.generate_prompt([concept], "sentence", 0)
        assert "- Networks mimic brains." in prompt
        assert "Below are several concepts to work with:" in prompt
    
    def test_generate_prompt_multiple_concepts(self):
        """Test generating prompt with multiple concepts."""
        concepts = [
            Concept(name="concept1", paragraph="First concept description."),
            Concept(name="concept2", paragraph="Second concept description.")
        ]
        factory = PromptFactory()
        
        prompt = factory.generate_prompt(concepts, "paragraph", 0)
        assert "- First concept description." in prompt
        assert "- Second concept description." in prompt
    
    def test_generate_prompt_template_selection(self):
        """Test generating prompt with different template indices."""
        concept = Concept(name="test", sentence="Test sentence.")
        factory = PromptFactory()
        
        prompt0 = factory.generate_prompt([concept], "sentence", 0)
        prompt1 = factory.generate_prompt([concept], "sentence", 1)
        
        assert prompt0 != prompt1
        assert "Below are several concepts to work with:" in prompt0
        assert "Here are some concepts for creative exploration:" in prompt1
    
    def test_generate_prompt_invalid_template_index(self):
        """Test generating prompt with invalid template index."""
        concept = Concept(name="test", sentence="Test sentence.")
        factory = PromptFactory()
        
        with pytest.raises(IndexError, match="Template index 5 out of range"):
            factory.generate_prompt([concept], "sentence", 5)
    
    def test_generate_prompt_missing_level_content(self):
        """Test generating prompt when concept lacks content at specified level."""
        concept = Concept(name="test", sentence="Has sentence.")  # No paragraph
        factory = PromptFactory()
        
        with pytest.raises(ValueError, match="Concept 'test' has no content at level 'paragraph'"):
            factory.generate_prompt([concept], "paragraph", 0)
    
    def test_generate_prompt_different_levels(self):
        """Test generating prompts at different granularity levels."""
        concept = Concept(
            name="test",
            sentence="Short description.",
            paragraph="Longer paragraph description.",
            article="Full article content."
        )
        factory = PromptFactory()
        
        sentence_prompt = factory.generate_prompt([concept], "sentence", 0)
        paragraph_prompt = factory.generate_prompt([concept], "paragraph", 0)
        article_prompt = factory.generate_prompt([concept], "article", 0)
        
        assert "- Short description." in sentence_prompt
        assert "- Longer paragraph description." in paragraph_prompt
        assert "- Full article content." in article_prompt
    
    def test_prompt_formatting_structure(self):
        """Test that prompts are formatted correctly with bullet points."""
        concepts = [
            Concept(name="c1", sentence="First concept."),
            Concept(name="c2", sentence="Second concept.")
        ]
        factory = PromptFactory()
        
        prompt = factory.generate_prompt(concepts, "sentence", 0)
        
        # Should contain both concepts as bullet points
        assert "- First concept." in prompt
        assert "- Second concept." in prompt
        
        # Should have the expected template structure
        assert "Below are several concepts to work with:" in prompt


class TestPromptIterator:
    def test_iterator_creation(self):
        """Test creating PromptIterator."""
        concepts = [
            [Concept(name="c1", sentence="Sentence 1.")],
            [Concept(name="c2", sentence="Sentence 2.")]
        ]
        factory = PromptFactory()
        iterator = PromptIterator(factory, concepts, "sentence")
        
        assert iterator.concept_combinations == concepts
        assert iterator.level == "sentence"
        assert iterator.get_total_count() == 10  # 2 combinations * 5 templates
    
    def test_iterator_generate_all(self):
        """Test generating all prompts from iterator."""
        concept1 = Concept(name="c1", sentence="First concept.")
        concept2 = Concept(name="c2", sentence="Second concept.")
        combinations = [[concept1], [concept2]]
        
        factory = PromptFactory()
        iterator = PromptIterator(factory, combinations, "sentence")
        
        all_prompts = iterator.generate_all()
        assert len(all_prompts) == 10  # 2 combinations * 5 templates
        
        # Check structure of results
        concepts, template_idx, prompt = all_prompts[0]
        assert len(concepts) == 1
        assert concepts[0].name == "c1"
        assert isinstance(template_idx, int)
        assert isinstance(prompt, str)
        assert "- First concept." in prompt
    
    def test_iterator_template_variation(self):
        """Test that iterator generates different templates for same concepts."""
        concept = Concept(name="test", sentence="Test sentence.")
        combinations = [[concept]]
        
        factory = PromptFactory()
        iterator = PromptIterator(factory, combinations, "sentence")
        
        all_prompts = iterator.generate_all()
        assert len(all_prompts) == 5  # 1 combination * 5 templates
        
        _, template_idx_0, prompt_0 = all_prompts[0]
        _, template_idx_1, prompt_1 = all_prompts[1]
        
        assert template_idx_0 == 0
        assert template_idx_1 == 1
        assert prompt_0 != prompt_1
    
    def test_iterator_multiple_concept_combinations(self):
        """Test iterator with multiple concept combinations."""
        concept1 = Concept(name="c1", paragraph="Para 1.")
        concept2 = Concept(name="c2", paragraph="Para 2.")
        concept3 = Concept(name="c3", paragraph="Para 3.")
        
        combinations = [
            [concept1],
            [concept2],
            [concept1, concept2],
            [concept1, concept3]
        ]
        
        factory = PromptFactory()
        iterator = PromptIterator(factory, combinations, "paragraph")
        
        all_prompts = iterator.generate_all()
        assert len(all_prompts) == 20  # 4 combinations * 5 templates
        
        # Check that we get different concept combinations
        concept_sets = set()
        for concepts, _, _ in all_prompts:
            concept_names = tuple(c.name for c in concepts)
            concept_sets.add(concept_names)
        
        expected_sets = {("c1",), ("c2",), ("c1", "c2"), ("c1", "c3")}
        assert concept_sets == expected_sets
    
    def test_iterator_with_custom_templates(self):
        """Test iterator with custom template factory."""
        custom_templates = ("Custom template 1: {concepts}",)
        factory = PromptFactory(custom_templates)
        
        concept = Concept(name="test", sentence="Test.")
        combinations = [[concept]]
        
        iterator = PromptIterator(factory, combinations, "sentence")
        assert iterator.get_total_count() == 1  # 1 combination * 1 template
        
        all_prompts = iterator.generate_all()
        assert len(all_prompts) == 1
        
        _, template_idx, prompt = all_prompts[0]
        assert template_idx == 0
        assert "Custom template 1:" in prompt
    
    def test_iterator_empty_combinations(self):
        """Test iterator with empty concept combinations list."""
        factory = PromptFactory()
        iterator = PromptIterator(factory, [], "sentence")
        
        assert iterator.get_total_count() == 0
        assert iterator.generate_all() == []
    
    def test_iterator_preserves_concept_order(self):
        """Test that iterator preserves the order of concepts in combinations."""
        concept1 = Concept(name="first", sentence="First concept.")
        concept2 = Concept(name="second", sentence="Second concept.")
        
        combinations = [[concept1, concept2]]
        factory = PromptFactory()
        iterator = PromptIterator(factory, combinations, "sentence")
        
        all_prompts = iterator.generate_all()
        
        for concepts, _, prompt in all_prompts:
            assert concepts[0].name == "first"
            assert concepts[1].name == "second"
            # First concept should appear before second in prompt
            first_pos = prompt.find("First concept.")
            second_pos = prompt.find("Second concept.")
            assert first_pos < second_pos


class TestDefaultTemplates:
    def test_default_templates_structure(self):
        """Test the structure and content of default templates."""
        assert len(DEFAULT_TEMPLATES) == 5
        
        # All templates should have {concepts} placeholder
        for template in DEFAULT_TEMPLATES:
            assert isinstance(template, str)
            assert "{concepts}" in template
            assert len(template) > 100  # Reasonable length for improved templates
    
    def test_default_templates_are_different(self):
        """Test that default templates are meaningfully different."""
        # Check that all templates are unique
        assert len(set(DEFAULT_TEMPLATES)) == len(DEFAULT_TEMPLATES)
        
        # They should have different opening text
        opening_lines = [template.split('\n')[0] for template in DEFAULT_TEMPLATES]
        assert len(set(opening_lines)) == len(opening_lines)
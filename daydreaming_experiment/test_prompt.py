"""
Tests for the prompt module.
"""
import pytest
from .concept_dag import ConceptDAG, ConceptLevel
from .prompt import PromptGenerator, PromptTemplate, PromptTemplateLibrary


class TestPromptGenerator:
    """Test cases for PromptGenerator class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.dag = ConceptDAG()
        self.dag.add_node("root", "This is a comprehensive article about machine learning. Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models. It enables computer systems to improve their performance on a specific task through experience without being explicitly programmed. The field has grown rapidly in recent decades. Applications include computer vision, natural language processing, and robotics.", ConceptLevel.ARTICLE)
        self.dag.add_node("child1", "Neural networks are a key component of deep learning. They consist of interconnected nodes that process information.", ConceptLevel.PARAGRAPH, ["root"])
        self.dag.add_node("child2", "Supervised learning uses labeled training data. It includes classification and regression tasks.", ConceptLevel.PARAGRAPH, ["root"])
        self.dag.add_node("grandchild", "Convolutional neural networks excel at image recognition tasks.", ConceptLevel.SENTENCE, ["child1"])
        
        self.generator = PromptGenerator(self.dag)
        
    def test_generate_prompt_basic(self):
        """Test basic prompt generation."""
        template = PromptTemplate(
            template="Discuss $root and its relationship to $child1",
            required_concepts=["root", "child1"],
            concept_level=ConceptLevel.SENTENCE
        )
        
        prompt = self.generator.generate_prompt(template, "grandchild")
        
        assert "machine learning" in prompt.lower()
        assert "neural networks" in prompt.lower()
        
    def test_generate_prompt_missing_concept(self):
        """Test prompt generation with missing required concept."""
        template = PromptTemplate(
            template="Discuss $nonexistent concept",
            required_concepts=["nonexistent"],
            concept_level=ConceptLevel.PARAGRAPH
        )
        
        with pytest.raises(ValueError, match="Missing required concepts"):
            self.generator.generate_prompt(template, "grandchild")
            
    def test_generate_prompt_with_additional_context(self):
        """Test prompt generation with additional context."""
        template = PromptTemplate(
            template="In the context of $domain, discuss $root",
            required_concepts=["root", "domain"],
            concept_level=ConceptLevel.SENTENCE
        )
        
        additional_context = {"domain": "artificial intelligence"}
        prompt = self.generator.generate_prompt(template, "grandchild", additional_context)
        
        assert "artificial intelligence" in prompt
        assert "machine learning" in prompt.lower()
        
    def test_generate_prompts_for_leaf_nodes(self):
        """Test generating prompts for all leaf nodes."""
        template = PromptTemplate(
            template="Analyze: $grandchild",
            required_concepts=["grandchild"],
            concept_level=ConceptLevel.PARAGRAPH
        )
        
        prompts = self.generator.generate_prompts_for_leaf_nodes(template)
        
        # Only grandchild should be a leaf node in our test setup
        assert "grandchild" in prompts
        assert "convolutional neural networks" in prompts["grandchild"].lower()
        
    def test_generate_multi_level_prompts(self):
        """Test generating prompts at multiple levels."""
        base_template = "Explain $root in detail"
        
        results = self.generator.generate_multi_level_prompts(base_template, "child1")
        
        assert ConceptLevel.ARTICLE in results
        assert ConceptLevel.PARAGRAPH in results
        assert ConceptLevel.SENTENCE in results
        
        # Article should be longest, sentence shortest
        article_len = len(results[ConceptLevel.ARTICLE])
        paragraph_len = len(results[ConceptLevel.PARAGRAPH])
        sentence_len = len(results[ConceptLevel.SENTENCE])
        
        assert article_len >= paragraph_len >= sentence_len
        


class TestPromptTemplateLibrary:
    """Test cases for PromptTemplateLibrary."""
    
    def test_concept_exploration_template(self):
        """Test concept exploration template."""
        template = PromptTemplateLibrary.concept_exploration_template(ConceptLevel.SENTENCE)
        
        assert "$concept" in template.template
        assert "concept" in template.required_concepts
        assert template.concept_level == ConceptLevel.SENTENCE
        
    def test_hierarchical_concept_template(self):
        """Test hierarchical concept template."""
        template = PromptTemplateLibrary.hierarchical_concept_template()
        
        assert "$foundation" in template.template
        assert "$development" in template.template
        assert "$exploration" in template.template
        assert len(template.required_concepts) == 3
        
    def test_comparative_analysis_template(self):
        """Test comparative analysis template."""
        template = PromptTemplateLibrary.comparative_analysis_template()
        
        assert "$concept1" in template.template
        assert "$concept2" in template.template
        assert template.concept_level == ConceptLevel.SENTENCE
        
    def test_synthesis_template(self):
        """Test synthesis template."""
        template = PromptTemplateLibrary.synthesis_template(ConceptLevel.ARTICLE)
        
        assert "$idea1" in template.template
        assert "$idea2" in template.template
        assert template.concept_level == ConceptLevel.ARTICLE



import pytest
from daydreaming_experiment.concept import Concept


class TestConcept:
    def test_concept_creation_minimal(self):
        """Test creating concept with just a name."""
        concept = Concept(name="test_concept")
        assert concept.name == "test_concept"
        assert concept.sentence is None
        assert concept.paragraph is None
        assert concept.article is None
        assert concept.article_path is None
    
    def test_concept_creation_full(self):
        """Test creating concept with all fields."""
        concept = Concept(
            name="neural_networks",
            sentence="Networks that mimic brain structure.",
            paragraph="Neural networks are computational models inspired by biological systems.",
            article="Full article content here...",
            article_path="articles/neural_networks.txt"
        )
        assert concept.name == "neural_networks"
        assert concept.sentence == "Networks that mimic brain structure."
        assert concept.paragraph == "Neural networks are computational models inspired by biological systems."
        assert concept.article == "Full article content here..."
        assert concept.article_path == "articles/neural_networks.txt"
    
    def test_concept_equality(self):
        """Test concept equality comparison."""
        concept1 = Concept(name="test", sentence="Same sentence")
        concept2 = Concept(name="test", sentence="Same sentence")
        concept3 = Concept(name="different", sentence="Same sentence")
        
        assert concept1 == concept2
        assert concept1 != concept3
    
    def test_concept_with_partial_fields(self):
        """Test creating concept with some fields."""
        concept = Concept(
            name="partial_concept",
            sentence="Just has sentence level.",
            paragraph="And paragraph level too."
        )
        assert concept.name == "partial_concept"
        assert concept.sentence == "Just has sentence level."
        assert concept.paragraph == "And paragraph level too."
        assert concept.article is None
        assert concept.article_path is None
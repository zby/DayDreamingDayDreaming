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
        descriptions = {
            "sentence": "Networks that mimic brain structure.",
            "paragraph": "Neural networks are computational models inspired by biological systems.",
            "article": "Full article content here...",
        }
        concept = Concept(
            name="neural_networks",
            descriptions=descriptions,
            article_path="articles/neural_networks.txt",
        )
        assert concept.name == "neural_networks"
        assert concept.sentence == "Networks that mimic brain structure."
        assert (
            concept.paragraph
            == "Neural networks are computational models inspired by biological systems."
        )
        assert concept.article == "Full article content here..."
        assert concept.article_path == "articles/neural_networks.txt"

    def test_concept_equality(self):
        """Test concept equality comparison."""
        concept1 = Concept(name="test", descriptions={"sentence": "Same sentence"})
        concept2 = Concept(name="test", descriptions={"sentence": "Same sentence"})
        concept3 = Concept(name="different", descriptions={"sentence": "Same sentence"})

        assert concept1 == concept2
        assert concept1 != concept3

    def test_concept_with_partial_fields(self):
        """Test creating concept with some fields."""
        descriptions = {
            "sentence": "Just has sentence level.",
            "paragraph": "And paragraph level too.",
        }
        concept = Concept(name="partial_concept", descriptions=descriptions)
        assert concept.name == "partial_concept"
        assert concept.sentence == "Just has sentence level."
        assert concept.paragraph == "And paragraph level too."
        assert concept.article is None
        assert concept.article_path is None

    def test_get_description_strict_mode(self):
        """Test get_description in strict mode."""
        descriptions = {
            "sentence": "Short description.",
            "paragraph": "Longer description.",
        }
        concept = Concept(name="test", descriptions=descriptions)

        # Should work for available levels
        assert concept.get_description("sentence") == "Short description."
        assert concept.get_description("paragraph") == "Longer description."

        # Should fail for unavailable levels in strict mode
        with pytest.raises(ValueError, match="has no content at level 'article'"):
            concept.get_description("article", strict=True)

    def test_get_description_non_strict_mode(self):
        """Test get_description with fallback in non-strict mode."""
        descriptions = {
            "sentence": "Short description.",
            "paragraph": "Longer description.",
        }
        concept = Concept(name="test", descriptions=descriptions)

        # Should fallback to paragraph when article requested
        assert concept.get_description("article", strict=False) == "Longer description."

        # Should fallback to sentence when paragraph requested but only sentence available
        concept_minimal = Concept(
            name="minimal", descriptions={"sentence": "Only sentence."}
        )
        assert (
            concept_minimal.get_description("paragraph", strict=False)
            == "Only sentence."
        )

        # Should fallback to name when no content available
        concept_empty = Concept(name="empty_concept", descriptions={})
        assert (
            concept_empty.get_description("paragraph", strict=False) == "empty_concept"
        )

    def test_get_description_invalid_level(self):
        """Test get_description with invalid level."""
        concept = Concept(name="test", descriptions={"sentence": "Test."})

        with pytest.raises(ValueError, match="Unknown description level 'invalid'"):
            concept.get_description("invalid", strict=False)

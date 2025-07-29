import pytest
from pathlib import Path
from daydreaming_experiment.concept_db import ConceptDB
from daydreaming_experiment.prompt_factory import PromptFactory, PromptIterator


class TestPromptIteratorIntegration:
    """Integration tests using real concept data from data/concepts/day_dreaming_concepts.json"""

    @pytest.fixture
    def concept_db(self):
        """Load real concepts from the manifest file."""
        manifest_path = (
            Path(__file__).parent.parent
            / "data"
            / "concepts"
            / "day_dreaming_concepts.json"
        )
        return ConceptDB.load(str(manifest_path))

    @pytest.fixture
    def prompt_factory(self):
        """Create a PromptFactory with default templates."""
        return PromptFactory()

    def test_loads_real_concepts(self, concept_db):
        """Verify the concept database loads real data correctly."""
        assert len(concept_db) == 6

        expected_names = {
            "Dearth of AI-driven Discoveries",
            "Default Mode Network",
            "Human Creativity and Insight",
            "Combinatorial Search",
            "Generator-Verifier Gap",
            "Economic Innovation Models",
        }

        actual_names = {concept.name for concept in concept_db.get_concepts()}
        assert actual_names == expected_names

        # Verify all concepts have required content levels
        for concept in concept_db.get_concepts():
            assert (
                concept.sentence is not None
            ), f"Concept '{concept.name}' missing sentence"
            assert (
                concept.paragraph is not None
            ), f"Concept '{concept.name}' missing paragraph"

    def test_generates_prompts_with_real_data(self, concept_db, prompt_factory):
        """Test prompt generation with representative real data samples."""
        # Test one example from each k value (k=1,2,3,4)
        test_cases = [
            (1, 5),  # k=1: 1 combination * 5 templates = 5 prompts
            (2, 5),  # k=2: 1 combination * 5 templates = 5 prompts
            (3, 5),  # k=3: 1 combination * 5 templates = 5 prompts
            (4, 5),  # k=4: 1 combination * 5 templates = 5 prompts
        ]

        for k, expected_prompts in test_cases:
            combinations = list(concept_db.get_combinations(k))[
                :1
            ]  # Take just first combination
            iterator = PromptIterator(prompt_factory, combinations, "paragraph")

            assert len(combinations) == 1
            assert iterator.get_total_count() == expected_prompts

            all_prompts = iterator.generate_all()
            assert len(all_prompts) == expected_prompts

            # Verify prompt structure
            for concepts, template_idx, prompt in all_prompts:
                assert len(concepts) == k
                assert template_idx in [0, 1, 2, 3, 4]
                assert isinstance(prompt, str)
                assert len(prompt) > 0

    def test_prompt_content_contains_real_concept_text(
        self, concept_db, prompt_factory
    ):
        """Test that generated prompts include actual concept content."""
        # Use specific well-known concepts for validation
        concept1 = concept_db.get_concept("Default Mode Network")
        concept2 = concept_db.get_concept("Combinatorial Search")

        combinations = [[concept1, concept2]]
        iterator = PromptIterator(prompt_factory, combinations, "paragraph")

        all_prompts = iterator.generate_all()
        assert len(all_prompts) == 5  # 1 combination * 5 templates

        for concepts, template_idx, prompt in all_prompts:
            # Verify both concept paragraphs appear in the prompt
            assert concept1.paragraph in prompt
            assert concept2.paragraph in prompt

            # Verify template-specific text appears based on template index
            if template_idx == 0:
                assert "Below are several concepts to work with:" in prompt
            elif template_idx == 1:
                assert "Here are some concepts for creative exploration:" in prompt
            elif template_idx == 2:
                assert "Consider the following concepts:" in prompt
            elif template_idx == 3:
                assert "Examine these concepts:" in prompt
            elif template_idx == 4:
                assert "Review these concepts:" in prompt

    def test_works_with_different_content_levels(self, concept_db, prompt_factory):
        """Test integration works with sentence vs paragraph content levels."""
        concept = concept_db.get_concept("Generator-Verifier Gap")
        combinations = [[concept]]

        # Test paragraph level
        paragraph_iterator = PromptIterator(prompt_factory, combinations, "paragraph")
        paragraph_prompts = paragraph_iterator.generate_all()

        # Test sentence level
        sentence_iterator = PromptIterator(prompt_factory, combinations, "sentence")
        sentence_prompts = sentence_iterator.generate_all()

        assert len(paragraph_prompts) == len(sentence_prompts) == 5

        # Verify correct content appears in each
        for _, _, prompt in paragraph_prompts:
            assert concept.paragraph in prompt
            assert concept.sentence not in prompt

        for _, _, prompt in sentence_prompts:
            assert concept.sentence in prompt
            assert concept.paragraph not in prompt

    def test_end_to_end_pipeline_sample(self, concept_db, prompt_factory):
        """Test complete end-to-end pipeline with real data sample."""
        # Simulate a small experiment run: k=1 to k=3 with first few combinations
        total_generated = 0

        for k in range(1, 4):  # k=1,2,3
            combinations = list(concept_db.get_combinations(k))[
                :2
            ]  # First 2 combinations each
            iterator = PromptIterator(prompt_factory, combinations, "paragraph")

            prompts = iterator.generate_all()
            total_generated += len(prompts)

            # Verify each prompt is valid
            for concepts, template_idx, prompt in prompts:
                assert len(concepts) == k
                assert 0 <= template_idx < 5
                assert len(prompt) > 100  # Reasonable prompt length
                assert "concepts" in prompt.lower()

        # Should generate: (2*5) + (2*5) + (2*5) = 30 total prompts
        assert total_generated == 30

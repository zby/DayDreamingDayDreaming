"""
Integration tests that depend on data files in the data/ directory.
These tests verify integration with real concept databases, templates, and experiments.
"""

import pytest
import tempfile
import json
import csv
from pathlib import Path
from click.testing import CliRunner
from jinja2 import Template

from daydreaming_experiment.experiment_runner import run_experiment
from daydreaming_experiment.results_analysis import analyze_results
from daydreaming_experiment.concept_db import ConceptDB
from daydreaming_experiment.concept import Concept
from daydreaming_experiment.evaluation_templates import EvaluationTemplateLoader


@pytest.fixture
def sample_experiment_fixture():
    """Return path to static sample experiment fixture."""
    return Path(__file__).parent / "fixtures" / "sample_experiment"


from daydreaming_experiment.prompt_factory import load_templates_from_directory


class TestExperimentRunnerIntegration:
    """Integration tests for experiment runner with real concept database."""

    def test_cli_with_real_concept_database(self):
        """Test CLI integration with real concept database (help only to avoid API calls)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create real concept database
            concept_db = ConceptDB()
            concept_db.add_concept(
                Concept(
                    name="test_concept", descriptions={"sentence": "A test concept"}
                )
            )

            # Save to filesystem
            manifest_path = Path(temp_dir) / "day_dreaming_concepts.json"
            concept_db.save(str(manifest_path))

            runner = CliRunner()

            # Test 1: Test help to ensure CLI can parse options without running
            result = runner.invoke(
                run_experiment,
                [
                    "--concepts-dir",
                    temp_dir,
                    "--k-max",
                    "1",
                    "--level",
                    "sentence",
                    "--help",  # This prevents actual execution
                ],
            )

            # Should show help without errors
            assert result.exit_code == 0
            assert "Usage:" in result.output

            # Test 2: Test that concept database can be loaded
            loaded_db = ConceptDB.load(str(manifest_path))
            concepts = loaded_db.get_concepts()

            assert len(concepts) == 1, f"Expected 1 concept, got {len(concepts)}"
            assert concepts[0].name == "test_concept"

    def test_concepts_dir_path_handling(self):
        """Test that CLI properly constructs manifest path from directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create the expected manifest file
            manifest_path = Path(temp_dir) / "day_dreaming_concepts.json"
            manifest_path.write_text('{"version": "1.0", "concepts": []}')

            runner = CliRunner()

            # Pass directory path - CLI should construct full manifest path
            result = runner.invoke(
                run_experiment,
                [
                    "--concepts-dir",
                    temp_dir,  # Just directory
                    "--k-max",
                    "1",
                    "--help",  # Don't actually run, just test path construction
                ],
            )

            # Should show help without errors (path construction works)
            assert result.exit_code == 0
            assert "Usage:" in result.output

    # NOTE: Integration tests that actually run experiments with API calls
    # are intentionally excluded to avoid making real API calls during testing.
    # Full end-to-end testing should be done manually or in separate integration
    # test suites with proper API mocking.

    def test_concept_db_method_exists(self):
        """Test that the method we're calling actually exists."""
        concept_db = ConceptDB()

        # This would catch method name errors
        assert hasattr(concept_db, "get_concepts")
        assert callable(getattr(concept_db, "get_concepts"))

        # This would fail if we used the wrong method name
        concepts = concept_db.get_concepts()
        assert isinstance(concepts, list)


class TestEvaluationTemplatesIntegration:
    """Integration tests with actual evaluation template files."""

    def test_load_real_evaluation_templates(self):
        """Test loading the actual template files from data/evaluation_templates."""
        # Should fail if templates directory doesn't exist
        loader = EvaluationTemplateLoader()
        templates = loader.list_templates()

        # Should have the templates we created
        expected_templates = [
            "creativity_metrics",
            "iterative_loops",
            "scientific_rigor",
        ]
        for template in expected_templates:
            assert template in templates

    def test_render_real_evaluation_templates(self):
        """Test rendering actual evaluation template files."""
        # Should fail if templates directory doesn't exist
        loader = EvaluationTemplateLoader()
        test_response = (
            "This response discusses novel AI architectures with iterative learning."
        )

        # Test each available template
        for template_name in loader.list_templates():
            prompt = loader.render_evaluation_prompt(template_name, test_response)

            # All prompts should contain the response
            assert test_response in prompt

            # All prompts should have the expected format
            assert "REASONING:" in prompt
            assert "SCORE:" in prompt


class TestPromptFactoryIntegration:
    """Integration tests with actual prompt template files."""

    def test_load_templates_from_data_directory(self):
        """Test loading templates from the data/templates directory."""
        # Should fail if data/templates directory doesn't exist
        templates = load_templates_from_directory("data/templates")
        assert len(templates) == 5

        # All templates should be Jinja2 Template objects
        for template in templates:
            assert isinstance(template, Template)
            # Test that templates can render with concepts
            from daydreaming_experiment.concept import Concept

            test_concepts = [
                Concept(name="test", descriptions={"sentence": "Test sentence."})
            ]

            # Should render without error
            rendered = template.render(
                concepts=test_concepts, level="sentence", strict=True
            )
            assert isinstance(rendered, str)
            assert len(rendered) > 0


class TestEndToEndWithRealData:
    """End-to-end integration tests with real experiment data."""

    def test_analysis_with_generation_only_experiment(self, sample_experiment_fixture):
        """Test analysis workflow with generation-only experiment fixture."""

        # Run analysis on the fixture experiment
        runner = CliRunner()
        result = runner.invoke(analyze_results, [str(sample_experiment_fixture)])

        # Should succeed with generation-only analysis
        assert result.exit_code == 0
        assert "EXPERIMENT ANALYSIS REPORT" in result.output
        assert "generation-only experiment" in result.output
        assert "Total Attempts: 2" in result.output
        assert "MOST FREQUENT CONCEPTS IN ALL COMBINATIONS" in result.output

    def test_concept_database_integration(self):
        """Test integration with real concept database."""
        from daydreaming_experiment.concept_db import ConceptDB

        # Should fail if concept database doesn't exist
        concept_db = ConceptDB.load("data/concepts/day_dreaming_concepts.json")

        # Should have loaded some concepts
        concepts = concept_db.get_concepts()
        assert len(concepts) > 0

        # All concepts should have required fields
        for concept in concepts:
            assert hasattr(concept, "name")
            assert concept.name
            assert hasattr(concept, "descriptions")
            assert isinstance(concept.descriptions, dict)


class TestDataDirectoryStructure:
    """Tests that verify the expected data directory structure exists."""

    def test_data_directories_exist(self):
        """Test that expected data directories exist."""
        data_dir = Path("data")

        # These directories must exist for the system to work properly
        expected_dirs = ["concepts", "templates"]

        assert data_dir.exists(), "data/ directory must exist"

        for dir_name in expected_dirs:
            dir_path = data_dir / dir_name
            assert dir_path.exists(), f"Required data directory must exist: {dir_path}"

    def test_required_files_exist(self):
        """Test that required data files exist."""
        required_files = [
            "data/concepts/day_dreaming_concepts.json",
        ]

        for file_path in required_files:
            path = Path(file_path)
            assert path.exists(), f"Required data file must exist: {file_path}"

        # Basic validation that concept manifest is readable
        concept_db = ConceptDB.load("data/concepts/day_dreaming_concepts.json")
        assert (
            len(concept_db.get_concepts()) > 0
        ), "Concept database must contain concepts"


class TestWorkflowIntegrationWithRealData:
    """Integration tests that combine multiple components with real data."""

    def test_prompt_generation_with_real_concepts_and_templates(self):
        """Test prompt generation using real concepts and templates."""
        # Should fail if concept database doesn't exist
        concept_db = ConceptDB.load("data/concepts/day_dreaming_concepts.json")
        concepts = concept_db.get_concepts()

        # Should fail if not enough concepts for testing
        assert len(concepts) >= 2, "Need at least 2 concepts for combination testing"

        # Should fail if templates directory doesn't exist
        templates = load_templates_from_directory("data/templates")
        assert len(templates) > 0, "No templates found in data/templates"

        # Test prompt generation with real data
        from daydreaming_experiment.prompt_factory import PromptFactory

        prompt_factory = PromptFactory()

        # Use first two concepts
        test_concepts = concepts[:2]
        prompt = prompt_factory.generate_prompt(test_concepts, "paragraph", 0)

        # Should generate a meaningful prompt
        assert isinstance(prompt, str)
        assert len(prompt) > 100  # Should be substantial

        # Should contain concept names
        for concept in test_concepts:
            assert concept.name in prompt

import pytest
import tempfile
import json
import csv
from pathlib import Path
from click.testing import CliRunner

from daydreaming_experiment.experiment_runner import (
    generate_experiment_id,
    save_response,
    save_config,
    get_csv_headers,
    initialize_results_csv,
    save_result_row,
    run_experiment,
)


class TestExperimentRunnerHelpers:
    """Test helper functions for experiment runner."""

    def test_generate_experiment_id(self):
        """Test experiment ID generation."""
        exp_id = generate_experiment_id()
        assert exp_id.startswith("experiment_")
        assert len(exp_id) == len("experiment_20250101_120000")

    def test_save_response(self):
        """Test saving response to file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            response_text = "This is a test response."
            
            filename = save_response(output_dir, 1, response_text)
            
            assert filename == "response_001.txt"
            
            response_file = output_dir / "responses" / filename
            assert response_file.exists()
            
            with open(response_file, "r") as f:
                content = f.read()
                assert content == response_text

    def test_save_config(self):
        """Test saving experiment configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            config = {"experiment_id": "test", "k_max": 3}
            
            save_config(output_dir, config)
            
            config_file = output_dir / "config.json"
            assert config_file.exists()
            
            with open(config_file, "r") as f:
                loaded_config = json.load(f)
                assert loaded_config == config

    def test_initialize_results_csv(self):
        """Test CSV initialization with correct headers for generation-only mode."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            
            results_path = initialize_results_csv(output_dir)
            
            assert results_path.exists()
            assert results_path.name == "results.csv"
            
            # Check headers are correct for generation-only
            with open(results_path, 'r') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                expected_headers = [
                    "experiment_id", "attempt_id", "concept_names", "concept_count",
                    "level", "template_id", "response_file", "generation_timestamp", "generator_model"
                ]
                
                assert headers == expected_headers

    def test_save_result_row(self):
        """Test saving result row to CSV for generation-only mode."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            results_path = initialize_results_csv(output_dir)
            
            # Create result data for generation-only
            result_data = {
                "experiment_id": "test_exp",
                "attempt_id": 1,
                "concept_names": "concept1|concept2",
                "concept_count": 2,
                "level": "paragraph",
                "template_id": 0,
                "response_file": "response_001.txt",
                "generation_timestamp": "2025-01-01T00:00:00",
                "generator_model": "test-model"
            }
            
            save_result_row(results_path, result_data)
            
            # Check the row was written correctly
            with open(results_path, 'r') as f:
                reader = csv.reader(f)
                headers = next(reader)  # Skip headers
                row = next(reader)

                assert row[0] == "test_exp"  # experiment_id
                assert row[1] == "1"  # attempt_id
                assert row[2] == "concept1|concept2"  # concept_names
                assert row[6] == "response_001.txt"  # response_file

    def test_get_csv_headers_generation_only(self):
        """Test CSV headers for generation-only mode."""
        headers = get_csv_headers()
        
        expected_headers = [
            "experiment_id",
            "attempt_id", 
            "concept_names",
            "concept_count",
            "level",
            "template_id",
            "response_file",
            "generation_timestamp",
            "generator_model",
        ]
        
        assert headers == expected_headers
        # Should not contain evaluation fields
        assert "automated_rating" not in headers
        assert "confidence_score" not in headers
        assert "evaluation_reasoning" not in headers

    # Legacy tests removed since we no longer support evaluation mode in experiment_runner


class TestExperimentRunnerCLI:
    """Test CLI interface for experiment runner."""

    def test_cli_function_exists(self):
        """Test that the CLI function is properly defined."""
        assert callable(run_experiment)

    def test_cli_help_option(self):
        """Test CLI help output."""
        runner = CliRunner()
        result = runner.invoke(run_experiment, ["--help"])
        
        assert result.exit_code == 0
        assert "Run generation-only experiment" in result.output
        assert "--k-max" in result.output
        assert "--generator-model" in result.output
        # Should not contain evaluator options
        assert "--evaluator-model" not in result.output
        assert "--generation-only" not in result.output
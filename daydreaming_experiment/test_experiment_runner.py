import pytest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import json
import csv
from pathlib import Path
from datetime import datetime

from daydreaming_experiment.experiment_runner import (
    generate_experiment_id,
    save_response, 
    save_config,
    initialize_results_csv,
    save_result_row,
    get_csv_headers,
)
from daydreaming_experiment.concept import Concept


class TestExperimentRunnerHelpers:

    def test_generate_experiment_id(self):
        """Test experiment ID generation format."""
        exp_id = generate_experiment_id()
        assert exp_id.startswith("experiment_")
        assert len(exp_id) == len("experiment_YYYYMMDD_HHMMSS")

    def test_save_response(self):
        """Test saving LLM response to file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            response_content = "Test response content"

            filename = save_response(output_dir, 42, response_content)

            assert filename == "response_042.txt"
            response_file = output_dir / "responses" / filename
            assert response_file.exists()

            with open(response_file, "r") as f:
                assert f.read() == response_content

    def test_save_config(self):
        """Test saving experiment configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            config = {"experiment_id": "test_exp", "k_max": 3, "level": "paragraph"}

            save_config(output_dir, config)

            config_file = output_dir / "config.json"
            assert config_file.exists()

            with open(config_file, "r") as f:
                loaded_config = json.load(f)
                assert loaded_config == config

    def test_initialize_results_csv(self):
        """Test CSV initialization with correct headers (legacy full evaluation mode)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            # Test with explicit full evaluation mode for backwards compatibility
            results_path = initialize_results_csv(output_dir, generation_only=False)

            assert results_path.exists()
            assert results_path.name == "results.csv"

            with open(results_path, "r") as f:
                reader = csv.reader(f)
                headers = next(reader)
                expected_headers = [
                    "experiment_id",
                    "attempt_id",
                    "concept_names",
                    "concept_count",
                    "level",
                    "template_id",
                    "response_file",
                    "automated_rating",
                    "confidence_score",
                    "evaluation_reasoning",
                    "evaluation_timestamp",
                    "evaluator_model",
                    "generation_timestamp",
                    "generator_model",
                ]
                assert headers == expected_headers

    def test_save_result_row(self):
        """Test saving result row to CSV (legacy full evaluation mode)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            # Test with explicit full evaluation mode for backwards compatibility
            results_path = initialize_results_csv(output_dir, generation_only=False)

            result_data = {
                "experiment_id": "test_exp",
                "attempt_id": 1,
                "concept_names": "concept1|concept2",
                "concept_count": 2,
                "level": "paragraph",
                "template_id": 0,
                "response_file": "response_001.txt",
                "automated_rating": 1,
                "confidence_score": 0.85,
                "evaluation_reasoning": "Found iterative patterns",
                "evaluation_timestamp": "2025-01-01T12:00:00",
                "generation_timestamp": "2025-01-01T11:59:00",
                "generator_model": "gpt-4",
                "evaluator_model": "gpt-4",
            }

            save_result_row(results_path, result_data, generation_only=False)

            # Read back and verify
            with open(results_path, "r") as f:
                reader = csv.reader(f)
                headers = next(reader)  # Skip headers
                row = next(reader)

                assert row[0] == "test_exp"  # experiment_id
                assert row[1] == "1"  # attempt_id
                assert row[2] == "concept1|concept2"  # concept_names
                assert row[7] == "1"  # automated_rating (same position in full evaluation mode)

    def test_get_csv_headers_generation_only(self):
        """Test CSV headers for generation-only mode."""
        headers = get_csv_headers(generation_only=True)
        
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

    def test_get_csv_headers_with_evaluation(self):
        """Test CSV headers for full evaluation mode."""
        headers = get_csv_headers(generation_only=False)
        
        expected_headers = [
            "experiment_id",
            "attempt_id",
            "concept_names", 
            "concept_count",
            "level",
            "template_id",
            "response_file",
            "automated_rating",
            "confidence_score",
            "evaluation_reasoning",
            "evaluation_timestamp",
            "evaluator_model",
            "generation_timestamp",
            "generator_model",
        ]
        
        assert headers == expected_headers
        # Should contain all evaluation fields
        assert "automated_rating" in headers
        assert "confidence_score" in headers
        assert "evaluation_reasoning" in headers

    def test_initialize_results_csv_generation_only(self):
        """Test CSV initialization for generation-only mode."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            
            results_path = initialize_results_csv(output_dir, generation_only=True)
            
            assert results_path.exists()
            assert results_path.name == "results.csv"
            
            with open(results_path, "r") as f:
                reader = csv.reader(f)
                headers = next(reader)
                expected_headers = get_csv_headers(generation_only=True)
                assert headers == expected_headers

    def test_initialize_results_csv_with_evaluation(self):
        """Test CSV initialization for full evaluation mode."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            
            results_path = initialize_results_csv(output_dir, generation_only=False)
            
            assert results_path.exists()
            assert results_path.name == "results.csv"
            
            with open(results_path, "r") as f:
                reader = csv.reader(f)
                headers = next(reader)
                expected_headers = get_csv_headers(generation_only=False)
                assert headers == expected_headers

    def test_save_result_row_generation_only(self):
        """Test saving result row for generation-only mode."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            results_path = initialize_results_csv(output_dir, generation_only=True)
            
            result_data = {
                "experiment_id": "test_exp",
                "attempt_id": 1,
                "concept_names": "concept1|concept2",
                "concept_count": 2,
                "level": "paragraph",
                "template_id": 0,
                "response_file": "response_001.txt",
                "generation_timestamp": "2025-01-01T12:00:00",
                "generator_model": "qwen/qwen3-235b-a22b-2507",
            }
            
            save_result_row(results_path, result_data, generation_only=True)
            
            # Read back and verify
            with open(results_path, "r") as f:
                reader = csv.reader(f)
                headers = next(reader)  # Skip headers
                row = next(reader)
                
                assert row[0] == "test_exp"  # experiment_id
                assert row[1] == "1"  # attempt_id
                assert row[2] == "concept1|concept2"  # concept_names
                assert row[8] == "qwen/qwen3-235b-a22b-2507"  # generator_model
                # Should have no evaluation data
                assert len(row) == len(get_csv_headers(generation_only=True))

    def test_save_result_row_with_evaluation(self):
        """Test saving result row for full evaluation mode."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            results_path = initialize_results_csv(output_dir, generation_only=False)
            
            result_data = {
                "experiment_id": "test_exp",
                "attempt_id": 1,
                "concept_names": "concept1|concept2", 
                "concept_count": 2,
                "level": "paragraph",
                "template_id": 0,
                "response_file": "response_001.txt",
                "automated_rating": 1,
                "confidence_score": 0.85,
                "evaluation_reasoning": "Found iterative patterns",
                "evaluation_timestamp": "2025-01-01T12:00:30",
                "evaluator_model": "openai/gpt-4",
                "generation_timestamp": "2025-01-01T12:00:00",
                "generator_model": "qwen/qwen3-235b-a22b-2507",
            }
            
            save_result_row(results_path, result_data, generation_only=False)
            
            # Read back and verify
            with open(results_path, "r") as f:
                reader = csv.reader(f)
                headers = next(reader)  # Skip headers
                row = next(reader)
                
                assert row[0] == "test_exp"  # experiment_id
                assert row[7] == "1"  # automated_rating
                assert row[8] == "0.85"  # confidence_score
                assert row[11] == "openai/gpt-4"  # evaluator_model
                # Should have all evaluation data
                assert len(row) == len(get_csv_headers(generation_only=False))


class TestExperimentRunnerCLI:

    def test_cli_function_exists(self):
        """Test that the CLI function can be imported."""
        from daydreaming_experiment.experiment_runner import run_experiment

        assert callable(run_experiment)

    def test_cli_help_option(self):
        """Test that CLI help works."""
        from click.testing import CliRunner
        from daydreaming_experiment.experiment_runner import run_experiment

        runner = CliRunner()
        result = runner.invoke(run_experiment, ["--help"])

        assert result.exit_code == 0
        assert "Run experiment with optional evaluation" in result.output
        assert "--k-max" in result.output
        assert "--level" in result.output
        assert "--generation-only / --with-evaluation" in result.output

    def test_cli_generation_flags(self):
        """Test that CLI correctly parses generation flags."""
        from click.testing import CliRunner
        from daydreaming_experiment.experiment_runner import run_experiment
        
        runner = CliRunner()
        
        # Test --generation-only flag (should be default)
        result = runner.invoke(run_experiment, ["--help"])
        assert "--generation-only / --with-evaluation" in result.output
        assert "default: generation-only" in result.output

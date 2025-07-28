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
        """Test CSV initialization with correct headers."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            results_path = initialize_results_csv(output_dir)

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
                    "generation_timestamp",
                    "generator_model",
                    "evaluator_model",
                ]
                assert headers == expected_headers

    def test_save_result_row(self):
        """Test saving result row to CSV."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            results_path = initialize_results_csv(output_dir)

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

            save_result_row(results_path, result_data)

            # Read back and verify
            with open(results_path, "r") as f:
                reader = csv.reader(f)
                headers = next(reader)  # Skip headers
                row = next(reader)

                assert row[0] == "test_exp"  # experiment_id
                assert row[1] == "1"  # attempt_id
                assert row[2] == "concept1|concept2"  # concept_names
                assert row[7] == "1"  # automated_rating


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
        assert "Run complete experiment" in result.output
        assert "--k-max" in result.output
        assert "--level" in result.output

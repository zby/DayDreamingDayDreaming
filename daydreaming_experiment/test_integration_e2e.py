import pytest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import json
import csv
from pathlib import Path
from click.testing import CliRunner

from daydreaming_experiment.experiment_runner import run_experiment
from daydreaming_experiment.evaluation_runner import evaluate_experiment
from daydreaming_experiment.results_analysis import analyze_results


class TestEndToEndIntegration:
    """End-to-end integration tests for the complete two-step workflow."""

    # Integration test with real experiment data moved to tests/test_integration_data_dependent.py

    @patch("daydreaming_experiment.evaluation_runner.load_dotenv")
    @patch("daydreaming_experiment.evaluation_runner.SimpleModelClient")
    @patch("daydreaming_experiment.evaluation_runner.EvaluationTemplateLoader")
    def test_evaluation_phase_integration(
        self, mock_template_loader_class, mock_client_class, mock_load_dotenv
    ):
        """Test evaluation phase with mocked dependencies."""

        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create generation experiment results
            config = {
                "experiment_id": "test_eval_integration",
                "generation_only": True,
                "k_max": 2,
                "level": "paragraph",
                "generator_model": "test-model",
            }

            config_file = exp_dir / "config.json"
            with open(config_file, "w") as f:
                json.dump(config, f)

            # Create generation results CSV
            results_file = exp_dir / "results.csv"
            with open(results_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
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
                )
                writer.writerow(
                    [
                        "test_eval_integration",
                        "1",
                        "concept1|concept2",
                        "2",
                        "paragraph",
                        "0",
                        "response_001.txt",
                        "2025-07-29T12:00:00",
                        "test-model",
                    ]
                )

            # Create response file
            responses_dir = exp_dir / "responses"
            responses_dir.mkdir()
            response_file = responses_dir / "response_001.txt"
            with open(response_file, "w") as f:
                f.write(
                    "Test response with iterative creative loops and meta-cognitive awareness."
                )

            # Setup evaluation mocks
            mock_eval_client = Mock()
            mock_eval_client.evaluate.return_value = (
                "Contains iterative patterns",
                "Full evaluation response",
                9.2,
            )
            mock_client_class.return_value = mock_eval_client

            mock_template_loader = Mock()
            mock_template_loader.get_default_template.return_value = "iterative_loops"
            mock_template_loader.list_templates.return_value = ["iterative_loops"]
            mock_template_loader.render_evaluation_prompt.return_value = (
                "Mocked evaluation prompt"
            )
            mock_template_loader_class.return_value = mock_template_loader

            # Run evaluation
            runner = CliRunner()
            result = runner.invoke(
                evaluate_experiment,
                [
                    str(exp_dir),
                    "--evaluator-model",
                    "eval-model",
                    "--evaluation-template",
                    "iterative_loops",
                ],
            )

            assert result.exit_code == 0
            assert "Evaluation completed!" in result.output
            assert "Successful evaluations: 1" in result.output

            # Verify evaluation results file was created
            eval_results_file = exp_dir / "evaluation_results.csv"
            assert eval_results_file.exists()

            # Verify evaluation results content
            with open(eval_results_file) as f:
                reader = csv.DictReader(f)
                eval_rows = list(reader)
                assert len(eval_rows) == 1
                assert eval_rows[0]["raw_score"] == "9.2"
                assert eval_rows[0]["evaluator_model"] == "eval-model"
                assert eval_rows[0]["evaluation_template"] == "iterative_loops"

    def test_analysis_phase_integration(self):
        """Test analysis phase with both generation-only and evaluated experiments."""

        with tempfile.TemporaryDirectory() as temp_dir:

            # Test 1: Analysis of generation-only experiment
            gen_exp_dir = Path(temp_dir) / "gen_only_exp"
            gen_exp_dir.mkdir()

            gen_config = {
                "experiment_id": "gen_only_test",
                "timestamp": "2025-07-29T12:00:00",
                "generation_only": True,
                "k_max": 3,
                "level": "paragraph",
                "generator_model": "test-model",
            }

            with open(gen_exp_dir / "config.json", "w") as f:
                json.dump(gen_config, f)

            with open(gen_exp_dir / "results.csv", "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    ["experiment_id", "concept_names", "concept_count", "response_file"]
                )
                writer.writerow(
                    ["gen_only_test", "concept1|concept2|concept3", "3", "resp1.txt"]
                )
                writer.writerow(
                    ["gen_only_test", "concept4|concept5|concept6", "3", "resp2.txt"]
                )

            # Test generation-only analysis
            runner = CliRunner()
            result = runner.invoke(analyze_results, [str(gen_exp_dir)])

            assert result.exit_code == 0
            assert "generation-only experiment" in result.output
            assert "MOST FREQUENT CONCEPTS IN ALL COMBINATIONS" in result.output
            assert "Total Attempts: 2" in result.output

            # Test 2: Analysis of evaluated experiment
            eval_exp_dir = Path(temp_dir) / "eval_exp"
            eval_exp_dir.mkdir()

            eval_config = {
                "experiment_id": "eval_test",
                "timestamp": "2025-07-29T12:00:00",
                "k_max": 2,
                "level": "paragraph",
                "generator_model": "test-model",
                "evaluator_model": "eval-model",
            }

            with open(eval_exp_dir / "config.json", "w") as f:
                json.dump(eval_config, f)

            # Create evaluation results (preferred over results.csv)
            with open(eval_exp_dir / "evaluation_results.csv", "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "experiment_id",
                        "automated_rating",
                        "raw_score",
                        "concept_names",
                        "concept_count",
                        "template_id",
                    ]
                )
                writer.writerow(["eval_test", 1, 8.5, "concept1|concept2", 2, 0])
                writer.writerow(["eval_test", 0, 4.5, "concept3|concept4", 2, 1])
                writer.writerow(["eval_test", 1, 9.2, "concept1|concept3", 2, 0])

            # Test evaluated experiment analysis
            result = runner.invoke(analyze_results, [str(eval_exp_dir)])

            assert result.exit_code == 0
            assert "Overall Success Rate: 66.67%" in result.output
            assert "Successful Attempts: 2" in result.output
            assert "MOST FREQUENT CONCEPTS IN SUCCESSFUL COMBINATIONS" in result.output
            assert "RAW SCORE ANALYSIS" in result.output
            assert "SUCCESSFUL CONCEPT COMBINATIONS" in result.output

            # Test score filtering
            result = runner.invoke(
                analyze_results, [str(eval_exp_dir), "--min-score", "9.0"]
            )

            assert result.exit_code == 0
            assert "Filtered to 1 results with raw score >= 9.0" in result.output

    def test_error_handling_integration(self):
        """Test error handling across the workflow."""

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = CliRunner()

            # Test analysis of non-existent experiment
            result = runner.invoke(analyze_results, ["/nonexistent/directory"])
            assert result.exit_code != 0

            # Test evaluation of experiment without results
            empty_dir = Path(temp_dir) / "empty_exp"
            empty_dir.mkdir()

            with open(empty_dir / "config.json", "w") as f:
                json.dump({"experiment_id": "empty"}, f)

            result = runner.invoke(evaluate_experiment, [str(empty_dir)])
            assert result.exit_code == 0
            assert "Results file not found" in result.output

            # Test analysis of experiment with missing config
            no_config_dir = Path(temp_dir) / "no_config_exp"
            no_config_dir.mkdir()

            result = runner.invoke(analyze_results, [str(no_config_dir)])
            assert result.exit_code == 0
            assert "Error loading experiment results" in result.output

    @patch("daydreaming_experiment.evaluation_runner.load_dotenv")
    @patch("daydreaming_experiment.evaluation_runner.SimpleModelClient")
    def test_evaluation_with_nonexistent_template(
        self, mock_client_class, mock_load_dotenv
    ):
        """Test error handling when requesting a non-existent evaluation template."""

        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create minimal experiment setup
            config = {"experiment_id": "template_test", "generation_only": True}
            with open(exp_dir / "config.json", "w") as f:
                json.dump(config, f)

            with open(exp_dir / "results.csv", "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
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
                )
                writer.writerow(
                    [
                        "template_test",
                        "1",
                        "concept1|concept2",
                        "2",
                        "paragraph",
                        "0",
                        "test_response.txt",
                        "2025-07-29T12:00:00",
                        "test-model",
                    ]
                )

            responses_dir = exp_dir / "responses"
            responses_dir.mkdir()
            with open(responses_dir / "test_response.txt", "w") as f:
                f.write("Test response content")

            # Mock client (shouldn't be reached, but needed to pass initialization)
            mock_client = Mock()
            mock_client_class.return_value = mock_client

            # Test evaluation with non-existent template name
            runner = CliRunner()
            result = runner.invoke(
                evaluate_experiment,
                [str(exp_dir), "--evaluation-template", "nonexistent_template"],
            )

            # Should fail gracefully with error message
            assert (
                result.exit_code == 0
            )  # Click doesn't set non-zero exit code for our errors
            assert "Template 'nonexistent_template' not found" in result.output


class TestWorkflowValidation:
    """Test workflow validation and compatibility."""

    def test_csv_format_compatibility(self):
        """Test that generation and evaluation CSV formats are compatible."""

        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create generation results with expected format
            generation_headers = [
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

            with open(exp_dir / "results.csv", "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(generation_headers)
                writer.writerow(
                    [
                        "test",
                        "1",
                        "c1|c2",
                        "2",
                        "paragraph",
                        "0",
                        "resp.txt",
                        "2025-01-01T00:00:00",
                        "model",
                    ]
                )

            # Load with results analysis
            from daydreaming_experiment.results_analysis import load_experiment_results

            # Create minimal config
            with open(exp_dir / "config.json", "w") as f:
                json.dump({"experiment_id": "test"}, f)

            config, df, has_evaluation = load_experiment_results(str(exp_dir))

            # Verify format compatibility
            assert has_evaluation == False  # No automated_rating
            assert len(df) == 1
            assert "concept_names" in df.columns
            assert "response_file" in df.columns

            # Test that all expected columns are present for evaluation
            required_for_eval = [
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

            for col in required_for_eval:
                assert col in df.columns, f"Missing required column: {col}"

    def test_backward_compatibility(self):
        """Test that new system works with old experiment formats."""

        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create experiment with evaluation already included (new format)
            eval_headers = [
                "experiment_id",
                "raw_score",
                "concept_names",
                "concept_count",
                "template_id",
            ]

            with open(exp_dir / "results.csv", "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(eval_headers)
                writer.writerow(["old_exp", 8.0, "concept1", "1", 0])  # 8.0 >= 5.0 (success)
                writer.writerow(["old_exp", 3.0, "concept2", "1", 1])  # 3.0 < 5.0 (failure)

            with open(exp_dir / "config.json", "w") as f:
                json.dump(
                    {
                        "experiment_id": "old_exp",
                        "timestamp": "2025-07-29T12:00:00",
                        "k_max": 1,
                        "level": "paragraph",
                        "generator_model": "test-model",
                    },
                    f,
                )

            # Test analysis still works
            runner = CliRunner()
            result = runner.invoke(analyze_results, [str(exp_dir)])

            assert result.exit_code == 0
            assert "Overall Success Rate: 50.00%" in result.output
            assert "Successful Attempts: 1" in result.output

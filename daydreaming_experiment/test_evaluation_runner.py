import pytest
from unittest.mock import Mock, patch, mock_open, MagicMock
import tempfile
import json
import csv
from pathlib import Path
from datetime import datetime

from daydreaming_experiment.evaluation_runner import (
    load_experiment_info,
    load_response_content,
    save_evaluation_prompt,
    save_evaluation_response,
    log_evaluation_error,
    create_evaluation_results_csv,
    save_evaluation_result,
    evaluate_experiment,
)


class TestEvaluationRunnerHelpers:

    def test_load_experiment_info_success(self):
        """Test loading experiment configuration and generation results."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create config file
            config = {"experiment_id": "test_exp", "generation_only": True}
            config_path = exp_dir / "config.json"
            with open(config_path, "w") as f:
                json.dump(config, f)

            # Create results file
            results_path = exp_dir / "results.csv"
            with open(results_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["experiment_id", "attempt_id", "response_file"])
                writer.writerow(["test_exp", "1", "response_001.txt"])
                writer.writerow(["test_exp", "2", "response_002.txt"])

            loaded_config, generation_results = load_experiment_info(exp_dir)

            assert loaded_config == config
            assert len(generation_results) == 2
            assert generation_results[0]["experiment_id"] == "test_exp"
            assert generation_results[0]["attempt_id"] == "1"
            assert generation_results[1]["response_file"] == "response_002.txt"

    def test_load_experiment_info_missing_config(self):
        """Test error handling when config file is missing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            with pytest.raises(FileNotFoundError, match="Config file not found"):
                load_experiment_info(exp_dir)

    def test_load_experiment_info_missing_results(self):
        """Test error handling when results file is missing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create config file only
            config_path = exp_dir / "config.json"
            with open(config_path, "w") as f:
                json.dump({"experiment_id": "test"}, f)

            with pytest.raises(FileNotFoundError, match="Results file not found"):
                load_experiment_info(exp_dir)

    def test_load_response_content_success(self):
        """Test loading response content from file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)
            responses_dir = exp_dir / "responses"
            responses_dir.mkdir()

            # Create response file
            response_file = responses_dir / "response_001.txt"
            response_content = "This is a test response with some content."
            with open(response_file, "w", encoding="utf-8") as f:
                f.write(response_content)

            loaded_content = load_response_content(exp_dir, "response_001.txt")

            assert loaded_content == response_content

    def test_load_response_content_missing_file(self):
        """Test error handling when response file is missing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            with pytest.raises(FileNotFoundError, match="Response file not found"):
                load_response_content(exp_dir, "nonexistent.txt")

    def test_create_evaluation_results_csv(self):
        """Test creating evaluation results CSV with correct headers."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            eval_results_path = create_evaluation_results_csv(exp_dir)

            assert eval_results_path.exists()
            assert eval_results_path.name == "evaluation_results.csv"

            with open(eval_results_path, "r") as f:
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
                    "eval_prompt_file",
                    "eval_response_file",
                    "evaluation_status",
                    "raw_score",
                    "evaluation_timestamp",
                    "evaluator_model",
                    "evaluation_template",
                    "generation_timestamp",
                    "generator_model",
                ]
                assert headers == expected_headers

    def test_save_evaluation_result(self):
        """Test saving evaluation result to CSV."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)
            eval_results_path = create_evaluation_results_csv(exp_dir)

            result_data = {
                "experiment_id": "test_exp",
                "attempt_id": "1",
                "concept_names": "concept1|concept2",
                "concept_count": "2",
                "level": "paragraph",
                "template_id": "0",
                "response_file": "response_001.txt",
                "eval_prompt_file": "eval_prompt_001.txt",
                "eval_response_file": "eval_response_001.txt",
                "evaluation_status": "success",
                "raw_score": 8.5,
                "evaluation_timestamp": "2025-01-01T12:00:00",
                "evaluator_model": "openai/gpt-4",
                "evaluation_template": "default",
                "generation_timestamp": "2025-01-01T11:59:00",
                "generator_model": "qwen/qwen3-235b-a22b-2507",
            }

            save_evaluation_result(eval_results_path, result_data)

            # Read back and verify
            with open(eval_results_path, "r") as f:
                reader = csv.reader(f)
                headers = next(reader)  # Skip headers
                row = next(reader)

                assert row[0] == "test_exp"  # experiment_id
                assert row[1] == "1"  # attempt_id
                assert row[7] == "eval_prompt_001.txt"  # eval_prompt_file
                assert row[8] == "eval_response_001.txt"  # eval_response_file
                assert row[9] == "success"  # evaluation_status
                assert row[10] == "8.5"  # raw_score
                assert row[12] == "openai/gpt-4"  # evaluator_model

    def test_save_evaluation_prompt(self):
        """Test saving evaluation prompt to file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)
            prompt_content = "Does this response contain ideas similar to iterative creative loops?"
            
            filename = save_evaluation_prompt(exp_dir, 1, prompt_content)
            
            assert filename == "eval_prompt_001.txt"
            
            # Verify directory was created
            eval_prompts_dir = exp_dir / "eval_prompts"
            assert eval_prompts_dir.exists()
            assert eval_prompts_dir.is_dir()
            
            # Verify file was created with correct content
            prompt_file = eval_prompts_dir / filename
            assert prompt_file.exists()
            
            with open(prompt_file, "r", encoding="utf-8") as f:
                saved_content = f.read()
            
            assert saved_content == prompt_content

    def test_save_evaluation_response(self):
        """Test saving evaluation response to file.""" 
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)
            response_content = "REASONING: Found iterative patterns\nSCORE: 8.5"
            
            filename = save_evaluation_response(exp_dir, 1, response_content)
            
            assert filename == "eval_response_001.txt"
            
            # Verify directory was created
            eval_responses_dir = exp_dir / "eval_responses"
            assert eval_responses_dir.exists()
            assert eval_responses_dir.is_dir()
            
            # Verify file was created with correct content
            response_file = eval_responses_dir / filename
            assert response_file.exists()
            
            with open(response_file, "r", encoding="utf-8") as f:
                saved_content = f.read()
            
            assert saved_content == response_content

    def test_save_evaluation_prompt_multiple_attempts(self):
        """Test saving multiple evaluation prompts with different attempt IDs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)
            
            # Save first prompt
            filename1 = save_evaluation_prompt(exp_dir, 1, "Prompt 1")
            # Save second prompt
            filename2 = save_evaluation_prompt(exp_dir, 42, "Prompt 42")
            
            assert filename1 == "eval_prompt_001.txt"
            assert filename2 == "eval_prompt_042.txt"
            
            eval_prompts_dir = exp_dir / "eval_prompts"
            assert (eval_prompts_dir / filename1).exists()
            assert (eval_prompts_dir / filename2).exists()
            
            # Verify content
            with open(eval_prompts_dir / filename1, "r") as f:
                assert f.read() == "Prompt 1"
            with open(eval_prompts_dir / filename2, "r") as f:
                assert f.read() == "Prompt 42"

    def test_save_evaluation_response_multiple_attempts(self):
        """Test saving multiple evaluation responses with different attempt IDs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)
            
            # Save first response
            filename1 = save_evaluation_response(exp_dir, 1, "Response 1")
            # Save second response
            filename2 = save_evaluation_response(exp_dir, 42, "Response 42")
            
            assert filename1 == "eval_response_001.txt"
            assert filename2 == "eval_response_042.txt"
            
            eval_responses_dir = exp_dir / "eval_responses"
            assert (eval_responses_dir / filename1).exists()
            assert (eval_responses_dir / filename2).exists()
            
            # Verify content
            with open(eval_responses_dir / filename1, "r") as f:
                assert f.read() == "Response 1"
            with open(eval_responses_dir / filename2, "r") as f:
                assert f.read() == "Response 42"

    def test_log_evaluation_error(self):
        """Test logging evaluation error to centralized log file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)
            
            log_evaluation_error(
                exp_dir, 1, "API_ERROR", "Connection failed", 
                "eval_prompt_001.txt", "response_001.txt"
            )
            
            log_path = exp_dir / "evaluation_errors.log"
            assert log_path.exists()
            
            with open(log_path, "r", encoding="utf-8") as f:
                log_content = f.read()
            
            assert "EVALUATION ERROR - Attempt 001" in log_content
            assert "Error Type: API_ERROR" in log_content
            assert "Error Message: Connection failed" in log_content
            assert "Evaluation Prompt File: eval_prompt_001.txt" in log_content
            assert "Response File: response_001.txt" in log_content

    def test_log_evaluation_error_multiple_entries(self):
        """Test logging multiple evaluation errors to same log file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)
            
            # Log first error
            log_evaluation_error(
                exp_dir, 1, "API_ERROR", "First error", 
                "eval_prompt_001.txt", "response_001.txt"
            )
            
            # Log second error
            log_evaluation_error(
                exp_dir, 2, "PARSING_ERROR", "Second error", 
                "eval_prompt_002.txt", "response_002.txt"
            )
            
            log_path = exp_dir / "evaluation_errors.log"
            assert log_path.exists()
            
            with open(log_path, "r", encoding="utf-8") as f:
                log_content = f.read()
            
            # Both errors should be in the log
            assert "EVALUATION ERROR - Attempt 001" in log_content
            assert "EVALUATION ERROR - Attempt 002" in log_content
            assert "Error Type: API_ERROR" in log_content
            assert "Error Type: PARSING_ERROR" in log_content
            assert "First error" in log_content
            assert "Second error" in log_content
            assert "Evaluation Prompt File: eval_prompt_001.txt" in log_content
            assert "Evaluation Prompt File: eval_prompt_002.txt" in log_content
            assert "Response File: response_001.txt" in log_content
            assert "Response File: response_002.txt" in log_content


class TestEvaluationRunnerCLI:

    def test_cli_function_exists(self):
        """Test that the CLI function can be imported."""
        from daydreaming_experiment.evaluation_runner import evaluate_experiment

        assert callable(evaluate_experiment)

    def test_cli_help_option(self):
        """Test that CLI help works."""
        from click.testing import CliRunner
        from daydreaming_experiment.evaluation_runner import evaluate_experiment

        runner = CliRunner()
        result = runner.invoke(evaluate_experiment, ["--help"])

        assert result.exit_code == 0
        assert "Evaluate responses from a generation-only experiment" in result.output
        assert "--evaluator-model" in result.output
        assert "--evaluation-template" in result.output
        assert "EXPERIMENT_DIRECTORY" in result.output

    def test_cli_missing_directory(self):
        """Test CLI error handling for missing experiment directory."""
        from click.testing import CliRunner
        from daydreaming_experiment.evaluation_runner import evaluate_experiment

        runner = CliRunner()
        result = runner.invoke(evaluate_experiment, ["/nonexistent/directory"])

        assert result.exit_code != 0
        assert "does not exist" in result.output

    @patch("daydreaming_experiment.evaluation_runner.load_dotenv")
    @patch("daydreaming_experiment.evaluation_runner.SimpleModelClient")
    def test_cli_missing_config_file(self, mock_client, mock_load_dotenv):
        """Test CLI error handling for missing config file."""
        from click.testing import CliRunner
        from daydreaming_experiment.evaluation_runner import evaluate_experiment

        with tempfile.TemporaryDirectory() as temp_dir:
            runner = CliRunner()
            result = runner.invoke(evaluate_experiment, [temp_dir])

            assert result.exit_code == 0  # Click handles gracefully
            assert "Config file not found" in result.output

    @patch("daydreaming_experiment.evaluation_runner.load_dotenv")
    @patch("daydreaming_experiment.evaluation_runner.SimpleModelClient")
    def test_cli_no_generation_results(self, mock_client, mock_load_dotenv):
        """Test CLI handling when no generation results are found."""
        from click.testing import CliRunner
        from daydreaming_experiment.evaluation_runner import evaluate_experiment

        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create config file
            config_path = exp_dir / "config.json"
            with open(config_path, "w") as f:
                json.dump({"experiment_id": "test", "generation_only": True}, f)

            # Create empty results file
            results_path = exp_dir / "results.csv"
            with open(results_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["experiment_id", "attempt_id", "response_file"])

            runner = CliRunner()
            result = runner.invoke(evaluate_experiment, [temp_dir])

            assert result.exit_code == 0
            assert "No generation results found" in result.output

    @patch("daydreaming_experiment.evaluation_runner.load_dotenv")
    def test_cli_model_client_error(self, mock_load_dotenv):
        """Test CLI error handling when model client initialization fails."""
        from click.testing import CliRunner
        from daydreaming_experiment.evaluation_runner import evaluate_experiment

        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create config and results files
            config_path = exp_dir / "config.json"
            with open(config_path, "w") as f:
                json.dump({"experiment_id": "test", "generation_only": True}, f)

            results_path = exp_dir / "results.csv"
            with open(results_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["experiment_id", "attempt_id", "response_file"])
                writer.writerow(["test", "1", "response_001.txt"])

            # Mock SimpleModelClient to raise an error
            with patch(
                "daydreaming_experiment.evaluation_runner.SimpleModelClient"
            ) as mock_client:
                mock_client.side_effect = ValueError("API key required")

                runner = CliRunner()
                result = runner.invoke(evaluate_experiment, [temp_dir])

                assert result.exit_code == 0
                assert "Error initializing model client" in result.output


class TestEvaluationRunnerMocking:
    """Tests that use mocking to avoid API calls."""

    @patch("daydreaming_experiment.evaluation_runner.load_dotenv")
    @patch("daydreaming_experiment.evaluation_runner.SimpleModelClient")
    @patch("daydreaming_experiment.evaluation_runner.EvaluationTemplateLoader")
    def test_successful_evaluation_run(
        self, mock_template_loader_class, mock_client_class, mock_load_dotenv
    ):
        """Test successful evaluation run with mocked model client."""
        from click.testing import CliRunner
        from daydreaming_experiment.evaluation_runner import evaluate_experiment

        # Setup mock model client
        mock_client = Mock()
        mock_client.evaluate.return_value = "REASONING: Found iterative patterns\nSCORE: 8.5"
        mock_client_class.return_value = mock_client

        # Setup mock template loader
        mock_template_loader = Mock()
        mock_template_loader.get_default_template.return_value = "iterative_loops"
        mock_template_loader.list_templates.return_value = [
            "iterative_loops",
            "creativity_metrics",
        ]
        mock_template_loader.render_evaluation_prompt.return_value = (
            "Mocked evaluation prompt"
        )
        mock_template_loader_class.return_value = mock_template_loader

        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create config file
            config_path = exp_dir / "config.json"
            with open(config_path, "w") as f:
                json.dump({"experiment_id": "test_exp", "generation_only": True}, f)

            # Create results file
            results_path = exp_dir / "results.csv"
            with open(results_path, "w", newline="") as f:
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
                        "test_exp",
                        "1",
                        "concept1|concept2",
                        "2",
                        "paragraph",
                        "0",
                        "response_001.txt",
                        "2025-01-01T11:59:00",
                        "qwen/qwen3-235b-a22b-2507",
                    ]
                )

            # Create response file
            responses_dir = exp_dir / "responses"
            responses_dir.mkdir()
            response_file = responses_dir / "response_001.txt"
            with open(response_file, "w") as f:
                f.write("Test response content with iterative patterns.")

            runner = CliRunner()
            result = runner.invoke(
                evaluate_experiment,
                [
                    str(exp_dir),
                    "--evaluator-model",
                    "openai/gpt-4",
                    "--evaluation-template",
                    "iterative_loops",
                ],
            )

            assert result.exit_code == 0
            assert "Evaluation completed!" in result.output
            assert "Successful evaluations: 1" in result.output
            assert "Failed evaluations: 0" in result.output

            # Verify evaluation results file was created
            eval_results_path = exp_dir / "evaluation_results.csv"
            assert eval_results_path.exists()
            
            # Verify evaluation prompt and response files were created
            eval_prompts_dir = exp_dir / "eval_prompts"
            eval_responses_dir = exp_dir / "eval_responses"
            assert eval_prompts_dir.exists()
            assert eval_responses_dir.exists()
            
            eval_prompt_file = eval_prompts_dir / "eval_prompt_001.txt"
            eval_response_file = eval_responses_dir / "eval_response_001.txt"
            assert eval_prompt_file.exists()
            assert eval_response_file.exists()
            
            # Verify file contents
            with open(eval_prompt_file, "r") as f:
                assert f.read() == "Mocked evaluation prompt"
            with open(eval_response_file, "r") as f:
                assert f.read() == "REASONING: Found iterative patterns\nSCORE: 8.5"

            # Verify evaluation was called
            mock_client.evaluate.assert_called_once()

            # Verify template loader was used
            mock_template_loader.render_evaluation_prompt.assert_called_once_with(
                "iterative_loops", "Test response content with iterative patterns."
            )

    @patch("daydreaming_experiment.evaluation_runner.load_dotenv")
    @patch("daydreaming_experiment.evaluation_runner.SimpleModelClient")
    @patch("daydreaming_experiment.evaluation_runner.EvaluationTemplateLoader")
    def test_empty_response_handling(
        self, mock_template_loader_class, mock_client_class, mock_load_dotenv
    ):
        """Test evaluation runner handles empty LLM responses properly."""
        from click.testing import CliRunner
        from daydreaming_experiment.evaluation_runner import evaluate_experiment

        # Setup mock model client that returns empty response (will trigger parsing error)
        mock_client = Mock()
        mock_client.evaluate.return_value = ""  # Empty response triggers parsing error
        mock_client_class.return_value = mock_client

        # Setup mock template loader
        mock_template_loader = Mock()
        mock_template_loader.get_default_template.return_value = "iterative_loops"
        mock_template_loader.list_templates.return_value = ["iterative_loops"]
        mock_template_loader.render_evaluation_prompt.return_value = "Mocked evaluation prompt"
        mock_template_loader_class.return_value = mock_template_loader

        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create config file
            config_path = exp_dir / "config.json"
            with open(config_path, "w") as f:
                json.dump({"experiment_id": "test_exp", "generation_only": True}, f)

            # Create results file
            results_path = exp_dir / "results.csv"
            with open(results_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "experiment_id", "attempt_id", "concept_names", "concept_count",
                    "level", "template_id", "response_file", "generation_timestamp", "generator_model"
                ])
                writer.writerow([
                    "test_exp", "1", "concept1|concept2", "2", "paragraph", "0",
                    "response_001.txt", "2025-01-01T11:59:00", "test-model"
                ])

            # Create response file
            responses_dir = exp_dir / "responses"
            responses_dir.mkdir()
            response_file = responses_dir / "response_001.txt"
            with open(response_file, "w") as f:
                f.write("Test response content.")

            runner = CliRunner()
            result = runner.invoke(evaluate_experiment, [str(exp_dir)])

            # Should complete successfully
            assert result.exit_code == 0
            assert "Evaluation completed!" in result.output

            # Verify evaluation response file was created with fallback content
            eval_responses_dir = exp_dir / "eval_responses"
            eval_response_file = eval_responses_dir / "eval_response_001.txt"
            assert eval_response_file.exists()

            # Verify file contains the raw response (empty in this case)
            with open(eval_response_file, "r") as f:
                content = f.read()
                assert content == ""  # Empty response was saved as-is

    @patch("daydreaming_experiment.evaluation_runner.load_dotenv")
    @patch("daydreaming_experiment.evaluation_runner.SimpleModelClient")
    @patch("daydreaming_experiment.evaluation_runner.EvaluationTemplateLoader")
    def test_api_error_no_response_file(
        self, mock_template_loader_class, mock_client_class, mock_load_dotenv
    ):
        """Test that API errors don't create response files."""
        from click.testing import CliRunner
        from daydreaming_experiment.evaluation_runner import evaluate_experiment

        # Setup mock model client that raises API error
        mock_client = Mock()
        mock_client.evaluate.side_effect = Exception("API connection failed")
        mock_client_class.return_value = mock_client

        # Setup mock template loader
        mock_template_loader = Mock()
        mock_template_loader.get_default_template.return_value = "iterative_loops"
        mock_template_loader.list_templates.return_value = ["iterative_loops"]
        mock_template_loader.render_evaluation_prompt.return_value = "Mocked evaluation prompt"
        mock_template_loader_class.return_value = mock_template_loader

        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create config file
            config_path = exp_dir / "config.json"
            with open(config_path, "w") as f:
                json.dump({"experiment_id": "test_exp", "generation_only": True}, f)

            # Create results file
            results_path = exp_dir / "results.csv"
            with open(results_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "experiment_id", "attempt_id", "concept_names", "concept_count",
                    "level", "template_id", "response_file", "generation_timestamp", "generator_model"
                ])
                writer.writerow([
                    "test_exp", "1", "concept1|concept2", "2", "paragraph", "0",
                    "response_001.txt", "2025-01-01T11:59:00", "test-model"
                ])

            # Create response file
            responses_dir = exp_dir / "responses"
            responses_dir.mkdir()
            response_file = responses_dir / "response_001.txt"
            with open(response_file, "w") as f:
                f.write("Test response content.")

            runner = CliRunner()
            result = runner.invoke(evaluate_experiment, [str(exp_dir)])

            # Should complete successfully
            assert result.exit_code == 0
            assert "Evaluation completed!" in result.output

            # Verify eval_prompts directory was created (eval prompt should still be saved)
            eval_prompts_dir = exp_dir / "eval_prompts"
            assert eval_prompts_dir.exists()
            eval_prompt_file = eval_prompts_dir / "eval_prompt_001.txt"
            assert eval_prompt_file.exists()

            # Verify eval_responses directory exists but no response file was created for API error
            eval_responses_dir = exp_dir / "eval_responses"
            eval_response_file = eval_responses_dir / "eval_response_001.txt"
            assert not eval_response_file.exists()  # No response file for API errors

            # Verify evaluation results were still recorded with empty eval_response_file
            eval_results_path = exp_dir / "evaluation_results.csv"
            assert eval_results_path.exists()
            
            with open(eval_results_path, "r") as f:
                reader = csv.DictReader(f)
                results = list(reader)
                assert len(results) == 1
                result_row = results[0]
                assert result_row["evaluation_status"] == "api_error"
                assert result_row["eval_response_file"] == ""  # Empty for API errors
                assert result_row["eval_prompt_file"] == "eval_prompt_001.txt"  # Prompt file should still exist

import pytest
from unittest.mock import Mock, patch, mock_open
import tempfile
import json
import csv
from pathlib import Path
import pandas as pd

from daydreaming_experiment.results_analysis import (
    load_experiment_results,
    analyze_success_rates,
    analyze_concept_patterns,
    analyze_raw_scores,
)


class TestResultsAnalysisHelpers:

    def test_load_experiment_results(self):
        """Test loading experiment configuration and results."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create config file
            config = {"experiment_id": "test_exp", "k_max": 3, "level": "paragraph"}
            config_path = exp_dir / "config.json"
            with open(config_path, "w") as f:
                json.dump(config, f)

            # Create results file
            results_path = exp_dir / "results.csv"
            with open(results_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["experiment_id", "automated_rating", "concept_count"])
                writer.writerow(["test_exp", 1, 2])
                writer.writerow(["test_exp", 0, 1])

            loaded_config, results_df, has_evaluation = load_experiment_results(
                str(exp_dir)
            )

            assert loaded_config == config
            assert len(results_df) == 2
            assert results_df.iloc[0]["automated_rating"] == 1
            assert results_df.iloc[1]["automated_rating"] == 0
            assert has_evaluation == True  # Has automated_rating column

    def test_analyze_success_rates(self):
        """Test success rate analysis by different dimensions."""
        # Create test DataFrame
        data = {
            "automated_rating": [1, 0, 1, 0, 1, 1],
            "concept_count": [1, 1, 2, 2, 3, 3],
            "template_id": [0, 1, 0, 1, 0, 1],
        }
        df = pd.DataFrame(data)

        analysis = analyze_success_rates(df, has_evaluation=True)

        # Overall success rate: 4/6 = 0.667
        assert analysis["overall_success_rate"] == pytest.approx(0.667, rel=1e-3)
        assert analysis["total_attempts"] == 6
        assert analysis["successful_attempts"] == 4

        # Success by k-value (multiple k-values in this test data)
        assert analysis["success_by_k"][1]["success_rate"] == 0.5  # 1/2
        assert analysis["success_by_k"][2]["success_rate"] == 0.5  # 1/2
        assert analysis["success_by_k"][3]["success_rate"] == 1.0  # 2/2
        assert analysis["single_k_strategy"] == False  # Multiple k-values

        # Success by template
        assert analysis["success_by_template"][0]["success_rate"] == 1.0  # 3/3
        assert analysis["success_by_template"][1]["success_rate"] == pytest.approx(
            1 / 3, rel=1e-3
        )  # 1/3

    def test_analyze_success_rates_single_k(self):
        """Test success rate analysis with single k-value (new default strategy)."""
        # Create test DataFrame with single k-value (k=4)
        data = {
            "automated_rating": [1, 0, 1, 1, 0, 1],
            "concept_count": [4, 4, 4, 4, 4, 4],  # All same k-value
            "template_id": [0, 1, 2, 0, 1, 2],
        }
        df = pd.DataFrame(data)

        analysis = analyze_success_rates(df, has_evaluation=True)

        # Overall success rate: 4/6 = 0.667
        assert analysis["overall_success_rate"] == pytest.approx(0.667, rel=1e-3)
        assert analysis["total_attempts"] == 6
        assert analysis["successful_attempts"] == 4

        # Success by k-value (single k-value)
        assert len(analysis["success_by_k"]) == 1
        assert analysis["success_by_k"][4]["success_rate"] == pytest.approx(
            0.667, rel=1e-3
        )
        assert analysis["single_k_strategy"] == True  # Single k-value

        # Success by template
        assert analysis["success_by_template"][0]["success_rate"] == 1.0  # 2/2
        assert analysis["success_by_template"][1]["success_rate"] == 0.0  # 0/2
        assert analysis["success_by_template"][2]["success_rate"] == 1.0  # 2/2

    def test_analyze_concept_patterns(self):
        """Test concept pattern analysis for successful combinations."""
        # Create test DataFrame
        data = {
            "automated_rating": [1, 0, 1, 1],
            "concept_names": [
                "concept1|concept2",
                "concept3",
                "concept1",
                "concept2|concept3",
            ],
        }
        df = pd.DataFrame(data)

        patterns = analyze_concept_patterns(df, has_evaluation=True)

        # concept1 appears in 2 successful results, concept2 in 2, concept3 in 1
        expected_frequency = {"concept1": 2, "concept2": 2, "concept3": 1}
        assert patterns["concept_frequency"] == expected_frequency

        # Most common concepts (top 5)
        most_common = patterns["most_common_concepts"]
        assert len(most_common) == 3
        assert most_common[0] == ("concept1", 2)
        assert most_common[1] == ("concept2", 2)
        assert most_common[2] == ("concept3", 1)

        # Successful combinations
        expected_combinations = [
            ["concept1", "concept2"],
            ["concept1"],
            ["concept2", "concept3"],
        ]
        assert patterns["successful_combinations"] == expected_combinations

    def test_analyze_concept_patterns_no_success(self):
        """Test concept pattern analysis when no results are successful."""
        data = {
            "automated_rating": [0, 0, 0],
            "concept_names": ["concept1", "concept2", "concept3"],
        }
        df = pd.DataFrame(data)

        patterns = analyze_concept_patterns(df, has_evaluation=True)

        assert patterns["concept_frequency"] == {}
        assert patterns["most_common_concepts"] == []
        assert patterns["successful_combinations"] == []

    def test_analyze_raw_scores(self):
        """Test raw score pattern analysis."""
        data = {
            "automated_rating": [1, 1, 1, 0],
            "raw_score": [9.0, 7.0, 8.5, 3.0],
        }
        df = pd.DataFrame(data)

        score_analysis = analyze_raw_scores(df, has_evaluation=True)

        # All results: [9.0, 7.0, 8.5, 3.0]
        assert score_analysis["mean_score"] == pytest.approx(6.875, rel=1e-3)
        assert score_analysis["median_score"] == 7.75  # (7.0 + 8.5) / 2
        assert score_analysis["min_score"] == 3.0
        assert score_analysis["max_score"] == 9.0
        assert score_analysis["has_raw_scores"] is True

        # Score distribution
        assert score_analysis["excellent_scores_8_10"] == 2  # 9.0, 8.5
        assert score_analysis["good_scores_6_8"] == 1  # 7.0
        assert score_analysis["poor_scores_0_4"] == 1  # 3.0

    def test_analyze_raw_scores_no_success(self):
        """Test raw score analysis when no results are successful."""
        data = {"automated_rating": [0, 0, 0], "raw_score": [3.0, 2.0, 1.0]}
        df = pd.DataFrame(data)

        score_analysis = analyze_raw_scores(df, has_evaluation=True)

        assert score_analysis["has_raw_scores"] is True
        assert score_analysis["mean_score"] == 2.0


class TestGenerationOnlyExperiments:
    """Test handling of generation-only experiments without evaluation data."""

    def test_load_generation_only_results(self):
        """Test loading generation-only experiment results."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create config file
            config = {"experiment_id": "gen_only", "generation_only": True}
            config_path = exp_dir / "config.json"
            with open(config_path, "w") as f:
                json.dump(config, f)

            # Create results file without evaluation columns
            results_path = exp_dir / "results.csv"
            with open(results_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["experiment_id", "concept_names", "response_file"])
                writer.writerow(["gen_only", "concept1|concept2", "response_001.txt"])
                writer.writerow(["gen_only", "concept3", "response_002.txt"])

            loaded_config, results_df, has_evaluation = load_experiment_results(
                str(exp_dir)
            )

            assert loaded_config == config
            assert len(results_df) == 2
            assert has_evaluation == False  # No automated_rating column
            assert "automated_rating" not in results_df.columns

    def test_load_evaluation_results_preferred(self):
        """Test that evaluation_results.csv is preferred over results.csv."""
        with tempfile.TemporaryDirectory() as temp_dir:
            exp_dir = Path(temp_dir)

            # Create config file
            config = {"experiment_id": "test_exp"}
            config_path = exp_dir / "config.json"
            with open(config_path, "w") as f:
                json.dump(config, f)

            # Create both results files
            results_path = exp_dir / "results.csv"
            with open(results_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["experiment_id", "concept_names"])
                writer.writerow(["test_exp", "concept1"])

            eval_results_path = exp_dir / "evaluation_results.csv"
            with open(eval_results_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    ["experiment_id", "automated_rating", "raw_score"]
                )
                writer.writerow(["test_exp", 1, 0.85])

            loaded_config, results_df, has_evaluation = load_experiment_results(
                str(exp_dir)
            )

            assert has_evaluation == True
            assert "automated_rating" in results_df.columns
            assert len(results_df) == 1  # From evaluation_results.csv

    def test_analyze_success_rates_generation_only(self):
        """Test success rate analysis for generation-only experiments."""
        data = {
            "experiment_id": ["exp1", "exp1", "exp1"],
            "concept_names": ["concept1", "concept2", "concept3"],
            "response_file": ["resp1.txt", "resp2.txt", "resp3.txt"],
        }
        df = pd.DataFrame(data)

        analysis = analyze_success_rates(df, has_evaluation=False)

        assert analysis["has_evaluation"] == False
        assert analysis["total_attempts"] == 3
        assert analysis["overall_success_rate"] is None
        assert analysis["successful_attempts"] is None
        assert "note" in analysis
        assert "generation-only experiment" in analysis["note"]

    def test_analyze_concept_patterns_generation_only(self):
        """Test concept pattern analysis for generation-only experiments."""
        data = {
            "concept_names": [
                "concept1|concept2",
                "concept1|concept3",
                "concept2|concept3",
                "concept1",
            ]
        }
        df = pd.DataFrame(data)

        patterns = analyze_concept_patterns(df, has_evaluation=False)

        # Should analyze all concept patterns, not just successful ones
        expected_frequency = {"concept1": 3, "concept2": 2, "concept3": 2}
        assert patterns["concept_frequency"] == expected_frequency
        assert patterns["total_combinations"] == 4
        assert "note" in patterns
        assert "no evaluation filtering" in patterns["note"]

    def test_analyze_raw_scores_generation_only(self):
        """Test raw score analysis for generation-only experiments."""
        data = {"experiment_id": ["exp1", "exp1"]}
        df = pd.DataFrame(data)

        score_analysis = analyze_raw_scores(df, has_evaluation=False)

        assert score_analysis["has_raw_scores"] is False
        assert "note" in score_analysis
        assert "No raw score data available" in score_analysis["note"]


class TestResultsAnalysisCLI:

    def test_cli_function_exists(self):
        """Test that the CLI function can be imported."""
        from daydreaming_experiment.results_analysis import analyze_results

        assert callable(analyze_results)

    def test_cli_help_option(self):
        """Test that CLI help works."""
        from click.testing import CliRunner
        from daydreaming_experiment.results_analysis import analyze_results

        runner = CliRunner()
        result = runner.invoke(analyze_results, ["--help"])

        assert result.exit_code == 0
        assert "Analyze results from an experiment directory" in result.output
        assert "--min-score" in result.output
        assert "--export-csv" in result.output

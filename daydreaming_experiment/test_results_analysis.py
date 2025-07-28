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
    analyze_confidence_patterns,
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

            loaded_config, results_df = load_experiment_results(str(exp_dir))

            assert loaded_config == config
            assert len(results_df) == 2
            assert results_df.iloc[0]["automated_rating"] == 1
            assert results_df.iloc[1]["automated_rating"] == 0

    def test_analyze_success_rates(self):
        """Test success rate analysis by different dimensions."""
        # Create test DataFrame
        data = {
            "automated_rating": [1, 0, 1, 0, 1, 1],
            "concept_count": [1, 1, 2, 2, 3, 3],
            "template_id": [0, 1, 0, 1, 0, 1],
        }
        df = pd.DataFrame(data)

        analysis = analyze_success_rates(df)

        # Overall success rate: 4/6 = 0.667
        assert analysis["overall_success_rate"] == pytest.approx(0.667, rel=1e-3)
        assert analysis["total_attempts"] == 6
        assert analysis["successful_attempts"] == 4

        # Success by k-value
        assert analysis["success_by_k"][1]["success_rate"] == 0.5  # 1/2
        assert analysis["success_by_k"][2]["success_rate"] == 0.5  # 1/2
        assert analysis["success_by_k"][3]["success_rate"] == 1.0  # 2/2

        # Success by template
        assert analysis["success_by_template"][0]["success_rate"] == 1.0  # 3/3
        assert analysis["success_by_template"][1]["success_rate"] == pytest.approx(
            1 / 3, rel=1e-3
        )  # 1/3

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

        patterns = analyze_concept_patterns(df)

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

        patterns = analyze_concept_patterns(df)

        assert patterns["concept_frequency"] == {}
        assert patterns["most_common_concepts"] == []
        assert patterns["successful_combinations"] == []

    def test_analyze_confidence_patterns(self):
        """Test confidence pattern analysis."""
        data = {
            "automated_rating": [1, 1, 1, 0],
            "confidence_score": [0.9, 0.7, 0.85, 0.3],
        }
        df = pd.DataFrame(data)

        confidence_analysis = analyze_confidence_patterns(df)

        # Only successful results: [0.9, 0.7, 0.85]
        assert confidence_analysis["mean_confidence"] == pytest.approx(0.817, rel=1e-3)
        assert confidence_analysis["median_confidence"] == 0.85
        assert confidence_analysis["min_confidence"] == 0.7
        assert confidence_analysis["max_confidence"] == 0.9

        # High confidence (>0.8): 2 out of 3 successful
        assert confidence_analysis["high_confidence_count"] == 2
        assert confidence_analysis["high_confidence_rate"] == pytest.approx(
            0.667, rel=1e-3
        )

    def test_analyze_confidence_patterns_no_success(self):
        """Test confidence analysis when no results are successful."""
        data = {"automated_rating": [0, 0, 0], "confidence_score": [0.3, 0.2, 0.1]}
        df = pd.DataFrame(data)

        confidence_analysis = analyze_confidence_patterns(df)

        assert confidence_analysis["no_successful_results"] is True


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
        assert "--min-confidence" in result.output
        assert "--export-csv" in result.output

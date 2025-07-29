import json
import csv
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Tuple

import click
import pandas as pd


def load_experiment_results(experiment_dir: str) -> Tuple[dict, pd.DataFrame]:
    """Load experiment configuration and results."""
    exp_path = Path(experiment_dir)

    # Load config
    config_path = exp_path / "config.json"
    with open(config_path, "r") as f:
        config = json.load(f)

    # Load results
    results_path = exp_path / "results.csv"
    results_df = pd.read_csv(results_path)

    return config, results_df


def analyze_success_rates(results_df: pd.DataFrame) -> Dict:
    """Analyze success rates by different dimensions."""
    analysis = {}

    # Overall success rate
    total_attempts = len(results_df)
    successful_attempts = len(results_df[results_df["automated_rating"] == 1])
    analysis["overall_success_rate"] = (
        successful_attempts / total_attempts if total_attempts > 0 else 0
    )
    analysis["total_attempts"] = total_attempts
    analysis["successful_attempts"] = successful_attempts

    # Success rate by concept count (k-value) - may be single value with new strategy
    success_by_k = {}
    unique_k_values = sorted(results_df["concept_count"].unique())
    for k in unique_k_values:
        k_results = results_df[results_df["concept_count"] == k]
        k_success = len(k_results[k_results["automated_rating"] == 1])
        k_total = len(k_results)
        success_by_k[k] = {
            "success_rate": k_success / k_total if k_total > 0 else 0,
            "successful": k_success,
            "total": k_total,
        }
    analysis["success_by_k"] = success_by_k
    analysis["single_k_strategy"] = len(unique_k_values) == 1

    # Success rate by template
    success_by_template = {}
    for template_id in sorted(results_df["template_id"].unique()):
        t_results = results_df[results_df["template_id"] == template_id]
        t_success = len(t_results[t_results["automated_rating"] == 1])
        t_total = len(t_results)
        success_by_template[template_id] = {
            "success_rate": t_success / t_total if t_total > 0 else 0,
            "successful": t_success,
            "total": t_total,
        }
    analysis["success_by_template"] = success_by_template

    return analysis


def analyze_concept_patterns(results_df: pd.DataFrame) -> Dict:
    """Analyze which concepts appear most in successful combinations."""
    successful_results = results_df[results_df["automated_rating"] == 1]

    # Count concept frequency in successful combinations
    concept_frequency = Counter()
    successful_combinations = []

    for _, row in successful_results.iterrows():
        concepts = row["concept_names"].split("|")
        successful_combinations.append(concepts)
        for concept in concepts:
            concept_frequency[concept] += 1

    # Most successful individual concepts
    total_successful = len(successful_results)
    concept_success_rates = {}

    for concept, count in concept_frequency.items():
        concept_success_rates[concept] = {
            "appearances_in_success": count,
            "success_contribution": (
                count / total_successful if total_successful > 0 else 0
            ),
        }

    return {
        "concept_frequency": dict(concept_frequency.most_common()),
        "concept_success_rates": concept_success_rates,
        "successful_combinations": successful_combinations,
        "most_common_concepts": concept_frequency.most_common(5),
    }


def analyze_confidence_patterns(results_df: pd.DataFrame) -> Dict:
    """Analyze confidence score patterns."""
    successful_results = results_df[results_df["automated_rating"] == 1]

    if len(successful_results) == 0:
        return {"no_successful_results": True}

    confidence_stats = {
        "mean_confidence": successful_results["confidence_score"].mean(),
        "median_confidence": successful_results["confidence_score"].median(),
        "min_confidence": successful_results["confidence_score"].min(),
        "max_confidence": successful_results["confidence_score"].max(),
        "std_confidence": successful_results["confidence_score"].std(),
    }

    # High confidence results (> 0.8)
    high_confidence = successful_results[successful_results["confidence_score"] > 0.8]
    confidence_stats["high_confidence_count"] = len(high_confidence)
    confidence_stats["high_confidence_rate"] = len(high_confidence) / len(
        successful_results
    )

    return confidence_stats


def print_analysis_report(
    config: dict, analysis: Dict, concept_patterns: Dict, confidence_patterns: Dict
):
    """Print formatted analysis report."""
    print("=" * 60)
    print(f"EXPERIMENT ANALYSIS REPORT")
    print("=" * 60)
    print(f"Experiment ID: {config['experiment_id']}")
    print(f"Timestamp: {config['timestamp']}")
    print(f"K-max: {config['k_max']}")
    print(f"Level: {config['level']}")
    print(f"Generator Model: {config['generator_model']}")
    print(f"Evaluator Model: {config['evaluator_model']}")
    print()

    print("OVERALL RESULTS:")
    print("-" * 30)
    print(f"Total Attempts: {analysis['total_attempts']}")
    print(f"Successful Attempts: {analysis['successful_attempts']}")
    print(f"Overall Success Rate: {analysis['overall_success_rate']:.2%}")
    print()

    print("SUCCESS BY CONCEPT COUNT (K-VALUE):")
    print("-" * 40)
    if analysis.get("single_k_strategy", False):
        k, stats = next(iter(analysis["success_by_k"].items()))
        print(f"K={k} (single strategy): {stats['successful']}/{stats['total']} ({stats['success_rate']:.2%})")
    else:
        for k, stats in analysis["success_by_k"].items():
            print(
                f"K={k}: {stats['successful']}/{stats['total']} ({stats['success_rate']:.2%})"
            )
    print()

    print("SUCCESS BY TEMPLATE:")
    print("-" * 25)
    for template_id, stats in analysis["success_by_template"].items():
        print(
            f"Template {template_id}: {stats['successful']}/{stats['total']} ({stats['success_rate']:.2%})"
        )
    print()

    if concept_patterns["most_common_concepts"]:
        print("MOST FREQUENT CONCEPTS IN SUCCESSFUL COMBINATIONS:")
        print("-" * 55)
        for concept, count in concept_patterns["most_common_concepts"]:
            print(f"  {concept}: {count} appearances")
        print()

    if not confidence_patterns.get("no_successful_results"):
        print("CONFIDENCE ANALYSIS:")
        print("-" * 25)
        print(f"Mean Confidence: {confidence_patterns['mean_confidence']:.3f}")
        print(f"Median Confidence: {confidence_patterns['median_confidence']:.3f}")
        print(
            f"Confidence Range: {confidence_patterns['min_confidence']:.3f} - {confidence_patterns['max_confidence']:.3f}"
        )
        print(
            f"High Confidence (>0.8): {confidence_patterns['high_confidence_count']} ({confidence_patterns['high_confidence_rate']:.2%})"
        )
        print()

    if concept_patterns["successful_combinations"]:
        print("SUCCESSFUL CONCEPT COMBINATIONS:")
        print("-" * 35)
        for i, combination in enumerate(
            concept_patterns["successful_combinations"][:10], 1
        ):
            print(f"  {i}. {' + '.join(combination)}")
        if len(concept_patterns["successful_combinations"]) > 10:
            print(
                f"  ... and {len(concept_patterns['successful_combinations']) - 10} more combinations"
            )


@click.command()
@click.argument("experiment_dir", type=click.Path(exists=True))
@click.option(
    "--min-confidence",
    type=float,
    default=0.0,
    help="Minimum confidence threshold for analysis",
)
@click.option("--export-csv", type=click.Path(), help="Export filtered results to CSV")
def analyze_results(experiment_dir: str, min_confidence: float, export_csv: str):
    """Analyze results from an experiment directory."""

    try:
        config, results_df = load_experiment_results(experiment_dir)
    except Exception as e:
        click.echo(f"Error loading experiment results: {e}")
        return

    # Filter by confidence if specified
    if min_confidence > 0:
        original_count = len(results_df)
        results_df = results_df[results_df["confidence_score"] >= min_confidence]
        click.echo(
            f"Filtered to {len(results_df)} results with confidence >= {min_confidence} (from {original_count})"
        )

    # Run analyses
    analysis = analyze_success_rates(results_df)
    concept_patterns = analyze_concept_patterns(results_df)
    confidence_patterns = analyze_confidence_patterns(results_df)

    # Print report
    print_analysis_report(config, analysis, concept_patterns, confidence_patterns)

    # Export if requested
    if export_csv:
        filtered_results = results_df[results_df["automated_rating"] == 1]
        filtered_results.to_csv(export_csv, index=False)
        click.echo(
            f"Exported {len(filtered_results)} successful results to {export_csv}"
        )


if __name__ == "__main__":
    analyze_results()

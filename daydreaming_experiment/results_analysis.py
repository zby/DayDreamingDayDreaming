import json
import csv
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Tuple

import click
import pandas as pd


def load_experiment_results(experiment_dir: str) -> Tuple[dict, pd.DataFrame, bool]:
    """Load experiment configuration and results.
    
    Returns:
        config: Experiment configuration
        results_df: Results dataframe
        has_evaluation: Whether evaluation results are available
    """
    exp_path = Path(experiment_dir)

    # Load config
    config_path = exp_path / "config.json"
    with open(config_path, "r") as f:
        config = json.load(f)

    # Check for evaluation results first (preferred for analysis)
    eval_results_path = exp_path / "evaluation_results.csv"
    results_path = exp_path / "results.csv"
    
    if eval_results_path.exists():
        # Use evaluation results if available
        results_df = pd.read_csv(eval_results_path)
        has_evaluation = True
    elif results_path.exists():
        # Fall back to generation results
        results_df = pd.read_csv(results_path)
        # Check if results have evaluation columns
        has_evaluation = "automated_rating" in results_df.columns
    else:
        raise FileNotFoundError(f"No results file found in {experiment_dir}")

    return config, results_df, has_evaluation


def analyze_success_rates(results_df: pd.DataFrame, has_evaluation: bool) -> Dict:
    """Analyze success rates by different dimensions."""
    analysis = {}
    analysis["has_evaluation"] = has_evaluation

    if not has_evaluation:
        # For generation-only experiments, we can't analyze success rates
        analysis["total_attempts"] = len(results_df)
        analysis["overall_success_rate"] = None
        analysis["successful_attempts"] = None
        analysis["note"] = "No evaluation data available - this is a generation-only experiment"
        return analysis

    # Overall success rate (only for evaluated experiments)
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


def _analyze_all_concept_frequency(results_df: pd.DataFrame) -> Dict:
    """Analyze concept frequency across all combinations (for generation-only experiments)."""
    concept_frequency = Counter()
    
    for _, row in results_df.iterrows():
        if pd.notna(row["concept_names"]):
            concepts = row["concept_names"].split("|")
            for concept in concepts:
                concept_frequency[concept] += 1
    
    return dict(concept_frequency.most_common())


def analyze_concept_patterns(results_df: pd.DataFrame, has_evaluation: bool) -> Dict:
    """Analyze which concepts appear most in successful combinations."""
    if not has_evaluation:
        # For generation-only experiments, analyze all concept patterns
        return {
            "concept_frequency": _analyze_all_concept_frequency(results_df),
            "note": "Analysis based on all generated responses (no evaluation filtering)",
            "total_combinations": len(results_df),
        }
    
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


def analyze_confidence_patterns(results_df: pd.DataFrame, has_evaluation: bool) -> Dict:
    """Analyze confidence score patterns."""
    if not has_evaluation:
        return {
            "note": "No confidence data available - this is a generation-only experiment",
            "has_evaluation": False
        }
    
    successful_results = results_df[results_df["automated_rating"] == 1]

    if len(successful_results) == 0:
        return {"no_successful_results": True, "has_evaluation": True}

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
    config: dict, analysis: Dict, concept_patterns: Dict, confidence_patterns: Dict, has_evaluation: bool
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
    if has_evaluation:
        print(f"Evaluator Model: {config.get('evaluator_model', 'N/A')}")
    else:
        print("Evaluation: None (generation-only experiment)")
    print()

    print("OVERALL RESULTS:")
    print("-" * 30)
    print(f"Total Attempts: {analysis['total_attempts']}")
    
    if has_evaluation:
        print(f"Successful Attempts: {analysis['successful_attempts']}")
        print(f"Overall Success Rate: {analysis['overall_success_rate']:.2%}")
    else:
        print("Note: No evaluation data available (generation-only experiment)")
    print()

    if has_evaluation:
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

    if has_evaluation:
        print("SUCCESS BY TEMPLATE:")
        print("-" * 25)
        for template_id, stats in analysis["success_by_template"].items():
            print(
                f"Template {template_id}: {stats['successful']}/{stats['total']} ({stats['success_rate']:.2%})"
            )
        print()

    # Handle concept patterns for both evaluated and generation-only experiments
    if has_evaluation and concept_patterns.get("most_common_concepts"):
        print("MOST FREQUENT CONCEPTS IN SUCCESSFUL COMBINATIONS:")
        print("-" * 55)
        for concept, count in concept_patterns["most_common_concepts"]:
            print(f"  {concept}: {count} appearances")
        print()
    elif not has_evaluation and concept_patterns.get("concept_frequency"):
        print("MOST FREQUENT CONCEPTS IN ALL COMBINATIONS:")
        print("-" * 45)
        for concept, count in list(concept_patterns["concept_frequency"].items())[:5]:
            print(f"  {concept}: {count} appearances")
        print()

    if has_evaluation and not confidence_patterns.get("no_successful_results"):
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

    if has_evaluation and concept_patterns.get("successful_combinations"):
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
        config, results_df, has_evaluation = load_experiment_results(experiment_dir)
    except Exception as e:
        click.echo(f"Error loading experiment results: {e}")
        return

    # Filter by confidence if specified (only for evaluated experiments)
    if min_confidence > 0:
        if not has_evaluation:
            click.echo("Warning: Cannot filter by confidence - no evaluation data available.")
        elif "confidence_score" in results_df.columns:
            original_count = len(results_df)
            results_df = results_df[results_df["confidence_score"] >= min_confidence]
            click.echo(
                f"Filtered to {len(results_df)} results with confidence >= {min_confidence} (from {original_count})"
            )
        else:
            click.echo("Warning: Confidence scores not found in results.")

    # Run analyses
    analysis = analyze_success_rates(results_df, has_evaluation)
    concept_patterns = analyze_concept_patterns(results_df, has_evaluation)
    confidence_patterns = analyze_confidence_patterns(results_df, has_evaluation)

    # Print report
    print_analysis_report(config, analysis, concept_patterns, confidence_patterns, has_evaluation)

    # Export if requested
    if export_csv:
        if has_evaluation:
            filtered_results = results_df[results_df["automated_rating"] == 1]
            click.echo(
                f"Exported {len(filtered_results)} successful results to {export_csv}"
            )
        else:
            filtered_results = results_df
            click.echo(
                f"Exported {len(filtered_results)} generation results to {export_csv}"
            )
        filtered_results.to_csv(export_csv, index=False)


if __name__ == "__main__":
    analyze_results()

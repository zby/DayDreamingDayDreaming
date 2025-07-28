import os
import json
import csv
import time
from datetime import datetime
from itertools import combinations
from pathlib import Path
from typing import List

import click
from dotenv import load_dotenv

from daydreaming_experiment.concept_db import ConceptDB
from daydreaming_experiment.prompt_factory import PromptFactory
from daydreaming_experiment.model_client import SimpleModelClient


def generate_experiment_id() -> str:
    """Generate timestamp-based experiment ID."""
    return f"experiment_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def save_response(output_dir: Path, attempt_id: int, response: str) -> str:
    """Save LLM response to file and return filename."""
    responses_dir = output_dir / "responses"
    responses_dir.mkdir(exist_ok=True)

    filename = f"response_{attempt_id:03d}.txt"
    filepath = responses_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(response)

    return filename


def save_config(output_dir: Path, config: dict):
    """Save experiment configuration."""
    config_path = output_dir / "config.json"
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)


def initialize_results_csv(output_dir: Path) -> Path:
    """Initialize CSV file with headers."""
    results_path = output_dir / "results.csv"

    headers = [
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

    with open(results_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

    return results_path


def save_result_row(results_path: Path, result_data: dict):
    """Append result row to CSV."""
    with open(results_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                result_data["experiment_id"],
                result_data["attempt_id"],
                result_data["concept_names"],
                result_data["concept_count"],
                result_data["level"],
                result_data["template_id"],
                result_data["response_file"],
                result_data["automated_rating"],
                result_data["confidence_score"],
                result_data["evaluation_reasoning"],
                result_data["evaluation_timestamp"],
                result_data["generation_timestamp"],
                result_data["generator_model"],
                result_data["evaluator_model"],
            ]
        )


@click.command()
@click.option(
    "--k-max", type=int, default=4, help="Maximum number of concepts to combine"
)
@click.option(
    "--level",
    type=click.Choice(["sentence", "paragraph", "article"]),
    default="paragraph",
    help="Concept description level",
)
@click.option(
    "--generator-model", default="openai/gpt-4", help="Model for content generation"
)
@click.option("--evaluator-model", default="openai/gpt-4", help="Model for evaluation")
@click.option(
    "--output", type=click.Path(), help="Output directory (default: auto-generated)"
)
@click.option(
    "--concepts-dir",
    type=click.Path(exists=True),
    default="data/concepts",
    help="Concepts database directory",
)
def run_experiment(
    k_max: int,
    level: str,
    generator_model: str,
    evaluator_model: str,
    output: str,
    concepts_dir: str,
):
    """Run complete experiment with automated evaluation."""

    # Load environment variables from .env file
    load_dotenv()

    # Load concept database
    manifest_path = Path(concepts_dir) / "day_dreaming_concepts.json"
    concept_db = ConceptDB.load(str(manifest_path))
    concepts = concept_db.get_concepts()

    if not concepts:
        click.echo("No concepts found in database!")
        return

    click.echo(f"Loaded {len(concepts)} concepts")

    # Initialize prompt factory
    prompt_factory = PromptFactory()

    # Initialize model client
    model_client = SimpleModelClient()

    # Setup output directory
    if not output:
        output = f"data/experiments/{generate_experiment_id()}"

    output_dir = Path(output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate experiment ID
    experiment_id = output_dir.name

    # Count total combinations
    total_combinations = sum(
        len(list(combinations(concepts, k))) * len(prompt_factory.templates)
        for k in range(1, k_max + 1)
    )

    # Save configuration
    config = {
        "experiment_id": experiment_id,
        "timestamp": datetime.now().isoformat(),
        "k_max": k_max,
        "level": level,
        "generator_model": generator_model,
        "evaluator_model": evaluator_model,
        "concept_count": len(concepts),
        "total_combinations": total_combinations,
        "templates_count": len(prompt_factory.templates),
    }
    save_config(output_dir, config)

    # Initialize results CSV
    results_path = initialize_results_csv(output_dir)

    click.echo(f"Running experiment: {experiment_id}")
    click.echo(f"Total combinations to test: {total_combinations}")
    click.echo(f"Output directory: {output_dir}")

    # Run experiment
    attempt_id = 0

    with click.progressbar(
        length=total_combinations, label="Processing combinations"
    ) as bar:

        for k in range(1, k_max + 1):
            for concept_combination in combinations(concepts, k):
                for template_idx in range(len(prompt_factory.templates)):
                    attempt_id += 1

                    # Generate prompt
                    prompt = prompt_factory.generate_prompt(
                        list(concept_combination), level, template_idx
                    )

                    generation_timestamp = datetime.now().isoformat()

                    try:
                        # Generate response
                        response = model_client.generate(prompt, generator_model)

                        # Save response
                        response_file = save_response(output_dir, attempt_id, response)

                        # Evaluate response
                        rating, confidence, reasoning = model_client.evaluate(
                            prompt, response, evaluator_model
                        )

                        evaluation_timestamp = datetime.now().isoformat()

                        # Save result
                        result_data = {
                            "experiment_id": experiment_id,
                            "attempt_id": attempt_id,
                            "concept_names": "|".join(
                                c.name for c in concept_combination
                            ),
                            "concept_count": len(concept_combination),
                            "level": level,
                            "template_id": template_idx,
                            "response_file": response_file,
                            "automated_rating": int(rating),
                            "confidence_score": confidence,
                            "evaluation_reasoning": reasoning,
                            "evaluation_timestamp": evaluation_timestamp,
                            "generation_timestamp": generation_timestamp,
                            "generator_model": generator_model,
                            "evaluator_model": evaluator_model,
                        }

                        save_result_row(results_path, result_data)

                        if rating:
                            click.echo(
                                f"\\n✓ SUCCESS (attempt {attempt_id}): {confidence:.2f} confidence"
                            )
                            click.echo(
                                f"  Concepts: {', '.join(c.name for c in concept_combination)}"
                            )

                    except Exception as e:
                        click.echo(f"\\nError in attempt {attempt_id}: {e}")

                        # Save error result
                        result_data = {
                            "experiment_id": experiment_id,
                            "attempt_id": attempt_id,
                            "concept_names": "|".join(
                                c.name for c in concept_combination
                            ),
                            "concept_count": len(concept_combination),
                            "level": level,
                            "template_id": template_idx,
                            "response_file": "",
                            "automated_rating": 0,
                            "confidence_score": 0.0,
                            "evaluation_reasoning": f"Error: {str(e)}",
                            "evaluation_timestamp": "",
                            "generation_timestamp": generation_timestamp,
                            "generator_model": generator_model,
                            "evaluator_model": evaluator_model,
                        }

                        save_result_row(results_path, result_data)

                    bar.update(1)
                    time.sleep(0.1)  # Rate limiting

    click.echo(f"\\nExperiment completed!")
    click.echo(f"Results saved to: {results_path}")


if __name__ == "__main__":
    run_experiment()

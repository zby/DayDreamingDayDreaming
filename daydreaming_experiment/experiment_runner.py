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

# Constants
DEFAULT_CONCEPTS_DIR = "data/concepts"
CONCEPTS_MANIFEST_FILENAME = "day_dreaming_concepts.json"
DEFAULT_EXPERIMENTS_DIR = "data/experiments"
CONFIG_FILENAME = "config.json"
RESULTS_FILENAME = "results.csv"
RESPONSES_DIR_NAME = "responses"
RESPONSE_FILENAME_TEMPLATE = "response_{:03d}.txt"

# Default models and settings
DEFAULT_GENERATOR_MODEL = "openai/gpt-4"
DEFAULT_LEVEL = "paragraph"
DEFAULT_K_MAX = 4

# Rate limiting
RATE_LIMIT_DELAY = 0.1

# Experiment ID format
EXPERIMENT_ID_FORMAT = "experiment_{}"
EXPERIMENT_ID_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"


def generate_experiment_id() -> str:
    """Generate timestamp-based experiment ID."""
    return EXPERIMENT_ID_FORMAT.format(datetime.now().strftime(EXPERIMENT_ID_TIMESTAMP_FORMAT))


def save_response(output_dir: Path, attempt_id: int, response: str) -> str:
    """Save LLM response to file and return filename."""
    responses_dir = output_dir / RESPONSES_DIR_NAME
    responses_dir.mkdir(exist_ok=True)

    filename = RESPONSE_FILENAME_TEMPLATE.format(attempt_id)
    filepath = responses_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(response)

    return filename


def save_config(output_dir: Path, config: dict):
    """Save experiment configuration."""
    config_path = output_dir / CONFIG_FILENAME
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)


def get_csv_headers() -> list:
    """Get CSV headers for generation-only experiments."""
    return [
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


def initialize_results_csv(output_dir: Path) -> Path:
    """Initialize CSV file with headers."""
    results_path = output_dir / RESULTS_FILENAME
    headers = get_csv_headers()

    with open(results_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

    return results_path


def save_result_row(results_path: Path, result_data: dict):
    """Append result row to CSV."""
    headers = get_csv_headers()
    
    # Build row data based on headers order
    row_data = []
    for header in headers:
        row_data.append(result_data.get(header, ""))
    
    with open(results_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row_data)


@click.command()
@click.option(
    "--k-max", type=int, default=DEFAULT_K_MAX, help="Maximum number of concepts to combine"
)
@click.option(
    "--level",
    type=click.Choice(["sentence", "paragraph", "article"]),
    default=DEFAULT_LEVEL,
    help="Concept description level",
)
@click.option(
    "--generator-model", default=DEFAULT_GENERATOR_MODEL, help="Model for content generation"
)
@click.option(
    "--output", type=click.Path(), help="Output directory (default: auto-generated)"
)
@click.option(
    "--concepts-dir",
    type=click.Path(exists=True),
    default=DEFAULT_CONCEPTS_DIR,
    help="Concepts database directory",
)
@click.option(
    "--max-prompts",
    type=int,
    help="Maximum number of prompts to test (default: test all combinations)",
)
def run_experiment(
    k_max: int,
    level: str,
    generator_model: str,
    output: str,
    concepts_dir: str,
    max_prompts: int,
):
    """Run generation-only experiment. Use evaluation_runner.py for evaluation."""

    # Load environment variables from .env file
    load_dotenv()

    # Load concept database
    manifest_path = Path(concepts_dir) / CONCEPTS_MANIFEST_FILENAME
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
        output = f"{DEFAULT_EXPERIMENTS_DIR}/{generate_experiment_id()}"

    output_dir = Path(output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate experiment ID
    experiment_id = output_dir.name

    # Count total combinations (only k_max-sized combinations)
    total_combinations = len(list(combinations(concepts, k_max))) * len(prompt_factory.templates)
    
    # Apply max_prompts limit if specified
    if max_prompts:
        total_combinations = min(total_combinations, max_prompts)

    # Save configuration
    config = {
        "experiment_id": experiment_id,
        "timestamp": datetime.now().isoformat(),
        "k_max": k_max,
        "level": level,
        "generator_model": generator_model,
        "concept_count": len(concepts),
        "total_combinations": total_combinations,
        "templates_count": len(prompt_factory.templates),
        "max_prompts": max_prompts,
        "generation_only": True,
    }
    save_config(output_dir, config)

    # Initialize results CSV
    results_path = initialize_results_csv(output_dir)

    click.echo(f"Running experiment: {experiment_id}")
    click.echo(f"Total combinations to test: {total_combinations}")
    click.echo(f"Output directory: {output_dir}")

    # Run experiment
    attempt_id = 0
    max_reached = False

    with click.progressbar(
        length=total_combinations, label="Processing combinations"
    ) as bar:

        for concept_combination in combinations(concepts, k_max):
            if max_reached:
                break
            for template_idx in range(len(prompt_factory.templates)):
                attempt_id += 1
                
                # Check if we've reached the max_prompts limit
                if max_prompts and attempt_id > max_prompts:
                    max_reached = True
                    break

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

                    # Base result data (always present)
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
                        "generation_timestamp": generation_timestamp,
                        "generator_model": generator_model,
                    }

                    save_result_row(results_path, result_data)

                    # Show generation success message
                    click.echo(f"Generated response {attempt_id}")

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
                        "generation_timestamp": generation_timestamp,
                        "generator_model": generator_model,
                    }
                    
                    save_result_row(results_path, result_data)

                bar.update(1)
                time.sleep(RATE_LIMIT_DELAY)  # Rate limiting

    click.echo(f"\\nExperiment completed!")
    click.echo(f"Results saved to: {results_path}")


if __name__ == "__main__":
    run_experiment()

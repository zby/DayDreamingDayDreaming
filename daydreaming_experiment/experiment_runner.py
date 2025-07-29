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

# Default models and settings
# DEFAULT_GENERATOR_MODEL = "openai/gpt-4"
DEFAULT_GENERATOR_MODEL = "deepseek/deepseek-r1:free"
DEFAULT_LEVEL = "paragraph"
DEFAULT_K_MAX = 3

# Rate limiting
RATE_LIMIT_DELAY = 0.1

# Constants
DEFAULT_CONCEPTS_DIR = "data/concepts"
CONCEPTS_MANIFEST_FILENAME = "day_dreaming_concepts.json"
DEFAULT_EXPERIMENTS_DIR = "data/experiments"
CONFIG_FILENAME = "config.json"
RESULTS_FILENAME = "results.csv"
RESPONSES_DIR_NAME = "responses"
PROMPTS_DIR_NAME = "prompts"
RESPONSE_FILENAME_TEMPLATE = "response_{:03d}.txt"
PROMPT_FILENAME_TEMPLATE = "prompt_{:03d}.txt"

# Experiment ID format
EXPERIMENT_ID_FORMAT = "experiment_{}"
EXPERIMENT_ID_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"


def generate_experiment_id() -> str:
    """Generate timestamp-based experiment ID."""
    return EXPERIMENT_ID_FORMAT.format(
        datetime.now().strftime(EXPERIMENT_ID_TIMESTAMP_FORMAT)
    )


def save_response(output_dir: Path, attempt_id: int, response: str) -> str:
    """Save LLM response to file and return filename."""
    responses_dir = output_dir / RESPONSES_DIR_NAME
    responses_dir.mkdir(exist_ok=True)

    filename = RESPONSE_FILENAME_TEMPLATE.format(attempt_id)
    filepath = responses_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(response)

    return filename


def save_prompt(output_dir: Path, attempt_id: int, prompt: str) -> str:
    """Save prompt to file and return filename."""
    prompts_dir = output_dir / PROMPTS_DIR_NAME
    prompts_dir.mkdir(exist_ok=True)

    filename = PROMPT_FILENAME_TEMPLATE.format(attempt_id)
    filepath = prompts_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(prompt)

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
        "prompt_file",
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


def load_completed_attempts(results_path: Path) -> tuple[set, int]:
    """Load completed attempts from existing results CSV.

    Returns:
        tuple: (completed_combinations set, max_attempt_id)
    """
    completed = set()
    max_attempt_id = 0

    if not results_path.exists():
        return completed, max_attempt_id

    with open(results_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            concept_names = row["concept_names"]
            template_id = int(row["template_id"]) if row["template_id"] else 0
            attempt_id = int(row["attempt_id"]) if row["attempt_id"] else 0

            # Track completed combinations (including failed ones - we don't retry)
            completed.add((concept_names, template_id))
            max_attempt_id = max(max_attempt_id, attempt_id)

    return completed, max_attempt_id


@click.command()
@click.option(
    "--k-max",
    type=int,
    default=DEFAULT_K_MAX,
    help="Maximum number of concepts to combine",
)
@click.option(
    "--level",
    type=click.Choice(["sentence", "paragraph", "article"]),
    default=DEFAULT_LEVEL,
    help="Concept description level",
)
@click.option(
    "--generator-model",
    default=DEFAULT_GENERATOR_MODEL,
    help="Model for content generation",
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
@click.option(
    "--resume",
    is_flag=True,
    help="Resume interrupted experiment from existing output directory",
)
def run_experiment(
    k_max: int,
    level: str,
    generator_model: str,
    output: str,
    concepts_dir: str,
    max_prompts: int,
    resume: bool,
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
    if resume:
        if not output:
            click.echo("Error: --output directory is required when using --resume")
            return
        output_dir = Path(output)
        if not output_dir.exists():
            click.echo(f"Error: Resume directory {output_dir} does not exist")
            return
        if not (output_dir / RESULTS_FILENAME).exists():
            click.echo(f"Error: No {RESULTS_FILENAME} found in {output_dir}")
            return
    else:
        if not output:
            output = f"{DEFAULT_EXPERIMENTS_DIR}/{generate_experiment_id()}"
        output_dir = Path(output)
        output_dir.mkdir(parents=True, exist_ok=True)

    # Generate experiment ID
    experiment_id = output_dir.name

    # Count total combinations (only k_max-sized combinations)
    total_combinations = len(list(combinations(concepts, k_max))) * len(
        prompt_factory.templates
    )

    # Handle resume vs new experiment
    results_path = output_dir / RESULTS_FILENAME
    if resume:
        # Load completed attempts
        completed_attempts, max_attempt_id = load_completed_attempts(results_path)
        click.echo(f"Resuming experiment: {experiment_id}")
        click.echo(f"Found {len(completed_attempts)} completed attempts")

        # Load existing config to verify compatibility
        config_path = output_dir / CONFIG_FILENAME
        if config_path.exists():
            with open(config_path, "r") as f:
                existing_config = json.load(f)

            # Verify key parameters match
            if (
                existing_config.get("k_max") != k_max
                or existing_config.get("level") != level
                or existing_config.get("generator_model") != generator_model
            ):
                click.echo("Warning: Resume parameters differ from original experiment")

        attempt_id = max_attempt_id
    else:
        completed_attempts = set()
        attempt_id = 0

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

    # Calculate remaining work for progress bar
    if resume:
        remaining_combinations = total_combinations - len(completed_attempts)
        click.echo(f"Remaining combinations to test: {remaining_combinations}")
    else:
        remaining_combinations = total_combinations
        click.echo(f"Total combinations to test: {total_combinations}")

    click.echo(f"Output directory: {output_dir}")

    # Run experiment
    max_reached = False
    processed_count = 0

    with click.progressbar(
        length=remaining_combinations, label="Processing combinations"
    ) as bar:

        for concept_combination in combinations(concepts, k_max):
            if max_reached:
                break
            for template_idx in range(len(prompt_factory.templates)):
                # Build concept names key for checking completion
                concept_names = "|".join(c.name for c in concept_combination)
                combination_key = (concept_names, template_idx)

                # Skip if already completed (resume functionality)
                if combination_key in completed_attempts:
                    continue

                attempt_id += 1
                processed_count += 1

                # Check if we've reached the max_prompts limit
                if max_prompts and attempt_id > max_prompts:
                    max_reached = True
                    break

                # Generate prompt
                prompt = prompt_factory.generate_prompt(
                    list(concept_combination), level, template_idx
                )

                # Save prompt
                prompt_file = save_prompt(output_dir, attempt_id, prompt)

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
                        "concept_names": concept_names,
                        "concept_count": len(concept_combination),
                        "level": level,
                        "template_id": template_idx,
                        "prompt_file": prompt_file,
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
                        "concept_names": concept_names,
                        "concept_count": len(concept_combination),
                        "level": level,
                        "template_id": template_idx,
                        "prompt_file": prompt_file,
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

import json
import csv
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import click
from dotenv import load_dotenv

from daydreaming_experiment.model_client import SimpleModelClient, parse_llm_response
from daydreaming_experiment.evaluation_templates import EvaluationTemplateLoader

# Constants
CONFIG_FILENAME = "config.json"
RESULTS_FILENAME = "results.csv"
EVALUATION_RESULTS_FILENAME = "evaluation_results.csv"
RESPONSES_DIR_NAME = "responses"
EVAL_PROMPTS_DIR_NAME = "eval_prompts"
EVAL_RESPONSES_DIR_NAME = "eval_responses"
EVAL_PROMPT_FILENAME_TEMPLATE = "eval_prompt_{:03d}.txt"
EVAL_RESPONSE_FILENAME_TEMPLATE = "eval_response_{:03d}.txt"
EVALUATION_ERRORS_LOG = "evaluation_errors.log"
DEFAULT_EVALUATION_TEMPLATES_DIR = "data/evaluation_templates"

# Default settings
DEFAULT_EVALUATOR_MODEL = "deepseek/deepseek-r1:free"
DEFAULT_EVALUATION_TEMPLATE = "default"

# Rate limiting
RATE_LIMIT_DELAY = 0.1


def load_experiment_info(experiment_dir: Path) -> Tuple[Dict, List[Dict]]:
    """Load experiment configuration and generation results."""
    config_path = experiment_dir / CONFIG_FILENAME
    results_path = experiment_dir / RESULTS_FILENAME

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    if not results_path.exists():
        raise FileNotFoundError(f"Results file not found: {results_path}")

    # Load config
    with open(config_path, "r") as f:
        config = json.load(f)

    # Load generation results
    generation_results = []
    with open(results_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            generation_results.append(row)

    return config, generation_results


def load_response_content(experiment_dir: Path, response_file: str) -> str:
    """Load response content from file."""
    response_path = experiment_dir / RESPONSES_DIR_NAME / response_file

    if not response_path.exists():
        raise FileNotFoundError(f"Response file not found: {response_path}")

    with open(response_path, "r", encoding="utf-8") as f:
        return f.read().strip()


def save_evaluation_prompt(experiment_dir: Path, attempt_id: int, prompt: str) -> str:
    """Save evaluation prompt to file and return filename."""
    eval_prompts_dir = experiment_dir / EVAL_PROMPTS_DIR_NAME
    eval_prompts_dir.mkdir(exist_ok=True)
    filename = EVAL_PROMPT_FILENAME_TEMPLATE.format(attempt_id)
    filepath = eval_prompts_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(prompt)
    return filename


def save_evaluation_response(experiment_dir: Path, attempt_id: int, response: str) -> str:
    """Save evaluation response to file and return filename."""
    eval_responses_dir = experiment_dir / EVAL_RESPONSES_DIR_NAME
    eval_responses_dir.mkdir(exist_ok=True)
    filename = EVAL_RESPONSE_FILENAME_TEMPLATE.format(attempt_id)
    filepath = eval_responses_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(response)
    return filename


def log_evaluation_error(experiment_dir: Path, attempt_id: int, error_type: str, 
                        error_message: str, eval_prompt_file: str, response_file: str):
    """Log detailed evaluation error information to centralized log file."""
    log_path = experiment_dir / EVALUATION_ERRORS_LOG
    timestamp = datetime.now().isoformat()
    
    log_entry = f"""
================================================================================
EVALUATION ERROR - Attempt {attempt_id:03d} - {timestamp}
================================================================================
Error Type: {error_type}
Error Message: {error_message}

Evaluation Prompt File: {eval_prompt_file}
Response File: {response_file}

================================================================================

"""
    
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(log_entry)


def load_evaluation_response(experiment_dir: Path, attempt_id: int) -> Tuple[str, str]:
    """Load existing evaluation response from file.
    
    Returns:
        Tuple of (eval_response_content, eval_response_filename)
    
    Raises:
        FileNotFoundError: If evaluation response file doesn't exist
    """
    eval_responses_dir = experiment_dir / EVAL_RESPONSES_DIR_NAME
    filename = EVAL_RESPONSE_FILENAME_TEMPLATE.format(attempt_id)
    filepath = eval_responses_dir / filename
    
    if not filepath.exists():
        raise FileNotFoundError(f"Evaluation response file not found: {filepath}")
    
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read().strip()
    
    return content, filename


def create_evaluation_results_csv(experiment_dir: Path) -> Path:
    """Create evaluation results CSV file."""
    eval_results_path = experiment_dir / EVALUATION_RESULTS_FILENAME

    headers = [
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

    with open(eval_results_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

    return eval_results_path


def save_evaluation_result(results_path: Path, result_data: Dict):
    """Append evaluation result to CSV."""
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
                result_data["eval_prompt_file"],
                result_data["eval_response_file"],
                result_data["evaluation_status"],
                result_data["raw_score"],
                result_data["evaluation_timestamp"],
                result_data["evaluator_model"],
                result_data["evaluation_template"],
                result_data["generation_timestamp"],
                result_data["generator_model"],
            ]
        )


@click.command()
@click.argument("experiment_directory", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--evaluator-model",
    default=DEFAULT_EVALUATOR_MODEL,
    help="Model to use for evaluation",
)
@click.option(
    "--evaluation-template",
    default=DEFAULT_EVALUATION_TEMPLATE,
    help="Evaluation template to use (default: default)",
)
@click.option(
    "--reparse-responses",
    is_flag=True,
    help="Only reparse existing evaluation responses, skip model calls",
)
def evaluate_experiment(
    experiment_directory: Path, evaluator_model: str, evaluation_template: str, reparse_responses: bool
):
    """Evaluate responses from a generation-only experiment."""

    # Load environment variables
    load_dotenv()

    # Load experiment data
    try:
        config, generation_results = load_experiment_info(experiment_directory)
    except FileNotFoundError as e:
        click.echo(f"Error: {e}")
        return

    if not generation_results:
        click.echo("No generation results found to evaluate.")
        return

    # Check if this was a generation-only experiment
    if not config.get("generation_only", False):
        click.echo("Warning: This experiment already includes evaluation results.")
        click.echo("Continuing will create separate evaluation results.")

    # Create evaluation results CSV
    eval_results_path = create_evaluation_results_csv(experiment_directory)

    if reparse_responses:
        # Reparse existing evaluation responses
        click.echo(f"Reparsing existing evaluation responses for {len(generation_results)} attempts...")
        click.echo(f"Results will be saved to: {eval_results_path}")
        _reparse_existing_responses(experiment_directory, generation_results, eval_results_path, evaluator_model, evaluation_template)
    else:
        # Normal evaluation flow
        # Initialize model client
        try:
            model_client = SimpleModelClient()
        except ValueError as e:
            click.echo(f"Error initializing model client: {e}")
            return

        # Initialize evaluation template loader
        try:
            template_loader = EvaluationTemplateLoader()

            # Resolve "default" to actual default template
            if evaluation_template == DEFAULT_EVALUATION_TEMPLATE:
                evaluation_template = template_loader.get_default_template()

            # Validate template exists
            available_templates = template_loader.list_templates()
            if evaluation_template not in available_templates:
                click.echo(f"Error: Template '{evaluation_template}' not found.")
                click.echo(f"Available templates: {', '.join(available_templates)}")
                return

        except (FileNotFoundError, ValueError) as e:
            click.echo(f"Error loading evaluation templates: {e}")
            return

        click.echo(f"Evaluating {len(generation_results)} responses...")
        click.echo(f"Evaluation results will be saved to: {eval_results_path}")
        _run_full_evaluation(experiment_directory, generation_results, eval_results_path, model_client, template_loader, evaluator_model, evaluation_template)


def _run_full_evaluation(experiment_directory: Path, generation_results: List[Dict], eval_results_path: Path, 
                        model_client, template_loader, evaluator_model: str, evaluation_template: str):
    """Run full evaluation including model calls."""
    successful_evaluations = 0
    failed_evaluations = 0

    with click.progressbar(generation_results, label="Evaluating responses") as bar:
        for gen_result in bar:
            try:
                # Load response content
                response_content = load_response_content(
                    experiment_directory, gen_result["response_file"]
                )

                # Generate evaluation prompt using template
                evaluation_prompt = template_loader.render_evaluation_prompt(
                    evaluation_template, response_content
                )

                # Save evaluation prompt to file
                eval_prompt_file = save_evaluation_prompt(
                    experiment_directory, int(gen_result["attempt_id"]), evaluation_prompt
                )

                # Evaluate response with proper error handling
                try:
                    # Get raw evaluation response
                    full_response = model_client.evaluate(
                        evaluation_prompt, response_content, evaluator_model
                    )
                    
                    # Always save the full response regardless of parsing success
                    response_to_save = full_response
                    eval_response_file = save_evaluation_response(
                        experiment_directory, int(gen_result["attempt_id"]), response_to_save
                    )

                    
                    # Try to parse the response
                    try:
                        raw_score = parse_llm_response(full_response)
                        # Compute rating from raw score (>=5.0 is success)
                        rating = raw_score >= 5.0
                        evaluation_status = "success"
                        
                    except ValueError as parse_error:
                        # Parsing error - but we still have the full response saved
                        evaluation_status = "parsing_error"
                        raw_score = 0.0
                        rating = False
                        
                        # Log detailed error info
                        log_evaluation_error(
                            experiment_directory, int(gen_result["attempt_id"]),
                            "PARSING_ERROR", str(parse_error), eval_prompt_file, gen_result["response_file"]
                        )
                    
                except Exception as api_error:
                    # API error or other exception - no response file created
                    evaluation_status = "api_error"
                    raw_score = 0.0
                    rating = False
                    eval_response_file = ""  # No response file for API errors
                    
                    # Log detailed error info
                    log_evaluation_error(
                        experiment_directory, int(gen_result["attempt_id"]),
                        "API_ERROR", str(api_error), eval_prompt_file, gen_result["response_file"]
                    )
                    
                evaluation_timestamp = datetime.now().isoformat()

                # Prepare evaluation result
                eval_result = {
                    "experiment_id": gen_result["experiment_id"],
                    "attempt_id": gen_result["attempt_id"],
                    "concept_names": gen_result["concept_names"],
                    "concept_count": gen_result["concept_count"],
                    "level": gen_result["level"],
                    "template_id": gen_result["template_id"],
                    "response_file": gen_result["response_file"],
                    "eval_prompt_file": eval_prompt_file,
                    "eval_response_file": eval_response_file,
                    "evaluation_status": evaluation_status,
                    "raw_score": raw_score,
                    "evaluation_timestamp": evaluation_timestamp,
                    "evaluator_model": evaluator_model,
                    "evaluation_template": evaluation_template,
                    "generation_timestamp": gen_result["generation_timestamp"],
                    "generator_model": gen_result["generator_model"],
                }

                save_evaluation_result(eval_results_path, eval_result)

                if rating:
                    successful_evaluations += 1
                    click.echo(
                        f"\n✓ SUCCESS (attempt {gen_result['attempt_id']}): score {raw_score:.1f}/10"
                    )

            except Exception as e:
                failed_evaluations += 1
                click.echo(
                    f"\nError evaluating attempt {gen_result['attempt_id']}: {e}"
                )

                # Log the exception error
                # Log the exception error with available file info
                log_evaluation_error(
                    experiment_directory, int(gen_result["attempt_id"]),
                    "EXCEPTION", str(e), "N/A", gen_result["response_file"]
                )

                # Save error result
                eval_result = {
                    "experiment_id": gen_result["experiment_id"],
                    "attempt_id": gen_result["attempt_id"],
                    "concept_names": gen_result["concept_names"],
                    "concept_count": gen_result["concept_count"],
                    "level": gen_result["level"],
                    "template_id": gen_result["template_id"],
                    "response_file": gen_result["response_file"],
                    "eval_prompt_file": "",
                    "eval_response_file": "",
                    "evaluation_status": "exception",
                    "raw_score": 0.0,
                    "evaluation_timestamp": datetime.now().isoformat(),
                    "evaluator_model": evaluator_model,
                    "evaluation_template": evaluation_template,
                    "generation_timestamp": gen_result["generation_timestamp"],
                    "generator_model": gen_result["generator_model"],
                }

                save_evaluation_result(eval_results_path, eval_result)

            time.sleep(RATE_LIMIT_DELAY)  # Rate limiting

    click.echo("\nEvaluation completed!")
    click.echo(f"Successful evaluations: {successful_evaluations}")
    click.echo(f"Failed evaluations: {failed_evaluations}")
    click.echo(f"Results saved to: {eval_results_path}")


def _reparse_existing_responses(experiment_directory: Path, generation_results: List[Dict], eval_results_path: Path,
                               evaluator_model: str, evaluation_template: str):
    """Reparse existing evaluation responses without calling model."""
    successful_evaluations = 0
    failed_evaluations = 0

    with click.progressbar(generation_results, label="Reparsing responses") as bar:
        for gen_result in bar:
            try:
                attempt_id = int(gen_result["attempt_id"])
                
                # Load existing evaluation response
                try:
                    eval_response_content, eval_response_file = load_evaluation_response(experiment_directory, attempt_id)
                except FileNotFoundError:
                    # No evaluation response file exists for this attempt
                    evaluation_status = "missing_response"
                    raw_score = 0.0
                    rating = False
                    eval_response_file = ""
                    eval_prompt_file = ""
                    
                    click.echo(f"\n⚠ MISSING (attempt {attempt_id}): No evaluation response file found")
                else:
                    # Try to parse the existing response
                    try:
                        raw_score = parse_llm_response(eval_response_content)
                        rating = raw_score >= 5.0
                        evaluation_status = "success"
                        
                        # Look for corresponding eval prompt file
                        eval_prompt_file = EVAL_PROMPT_FILENAME_TEMPLATE.format(attempt_id)
                        eval_prompt_path = experiment_directory / EVAL_PROMPTS_DIR_NAME / eval_prompt_file
                        if not eval_prompt_path.exists():
                            eval_prompt_file = ""  # Prompt file doesn't exist
                        
                    except ValueError as parse_error:
                        # Parsing error
                        evaluation_status = "parsing_error"
                        raw_score = 0.0
                        rating = False
                        eval_prompt_file = EVAL_PROMPT_FILENAME_TEMPLATE.format(attempt_id)
                        
                        # Log detailed error info
                        log_evaluation_error(
                            experiment_directory, attempt_id,
                            "REPARSING_ERROR", str(parse_error), eval_prompt_file, gen_result["response_file"]
                        )
                        
                        click.echo(f"\n✗ PARSE ERROR (attempt {attempt_id}): {parse_error}")
                
                evaluation_timestamp = datetime.now().isoformat()

                # Prepare evaluation result
                eval_result = {
                    "experiment_id": gen_result["experiment_id"],
                    "attempt_id": gen_result["attempt_id"],
                    "concept_names": gen_result["concept_names"],
                    "concept_count": gen_result["concept_count"],
                    "level": gen_result["level"],
                    "template_id": gen_result["template_id"],
                    "response_file": gen_result["response_file"],
                    "eval_prompt_file": eval_prompt_file,
                    "eval_response_file": eval_response_file,
                    "evaluation_status": evaluation_status,
                    "raw_score": raw_score,
                    "evaluation_timestamp": evaluation_timestamp,
                    "evaluator_model": evaluator_model,
                    "evaluation_template": evaluation_template,
                    "generation_timestamp": gen_result["generation_timestamp"],
                    "generator_model": gen_result["generator_model"],
                }

                save_evaluation_result(eval_results_path, eval_result)

                if rating:
                    successful_evaluations += 1
                    click.echo(f"\n✓ SUCCESS (attempt {attempt_id}): score {raw_score:.1f}/10")
                elif evaluation_status != "missing_response":
                    failed_evaluations += 1

            except Exception as e:
                failed_evaluations += 1
                click.echo(f"\nError reparsing attempt {gen_result['attempt_id']}: {e}")

                # Log the exception error
                log_evaluation_error(
                    experiment_directory, int(gen_result["attempt_id"]),
                    "REPARSE_EXCEPTION", str(e), "N/A", gen_result["response_file"]
                )

                # Save error result
                eval_result = {
                    "experiment_id": gen_result["experiment_id"],
                    "attempt_id": gen_result["attempt_id"],
                    "concept_names": gen_result["concept_names"],
                    "concept_count": gen_result["concept_count"],
                    "level": gen_result["level"],
                    "template_id": gen_result["template_id"],
                    "response_file": gen_result["response_file"],
                    "eval_prompt_file": "",
                    "eval_response_file": "",
                    "evaluation_status": "exception",
                    "raw_score": 0.0,
                    "evaluation_timestamp": datetime.now().isoformat(),
                    "evaluator_model": evaluator_model,
                    "evaluation_template": evaluation_template,
                    "generation_timestamp": gen_result["generation_timestamp"],
                    "generator_model": gen_result["generator_model"],
                }

                save_evaluation_result(eval_results_path, eval_result)

    click.echo("\nReparsing completed!")
    click.echo(f"Successful reparsing: {successful_evaluations}")
    click.echo(f"Failed reparsing: {failed_evaluations}")
    click.echo(f"Results saved to: {eval_results_path}")


if __name__ == "__main__":
    evaluate_experiment()

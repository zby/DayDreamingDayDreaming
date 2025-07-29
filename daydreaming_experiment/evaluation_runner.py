import os
import json
import csv
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import click
from dotenv import load_dotenv

from daydreaming_experiment.model_client import SimpleModelClient
from daydreaming_experiment.evaluation_templates import EvaluationTemplateLoader


def load_experiment_info(experiment_dir: Path) -> Tuple[Dict, List[Dict]]:
    """Load experiment configuration and generation results."""
    config_path = experiment_dir / "config.json"
    results_path = experiment_dir / "results.csv"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    if not results_path.exists():
        raise FileNotFoundError(f"Results file not found: {results_path}")
    
    # Load config
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Load generation results
    generation_results = []
    with open(results_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            generation_results.append(row)
    
    return config, generation_results


def load_response_content(experiment_dir: Path, response_file: str) -> str:
    """Load response content from file."""
    response_path = experiment_dir / "responses" / response_file
    
    if not response_path.exists():
        raise FileNotFoundError(f"Response file not found: {response_path}")
    
    with open(response_path, 'r', encoding='utf-8') as f:
        return f.read().strip()


def create_evaluation_results_csv(experiment_dir: Path) -> Path:
    """Create evaluation results CSV file."""
    eval_results_path = experiment_dir / "evaluation_results.csv"
    
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
        "full_evaluation_response",
        "evaluation_timestamp",
        "evaluator_model",
        "evaluation_template",
        "generation_timestamp",
        "generator_model",
    ]
    
    with open(eval_results_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
    
    return eval_results_path


def save_evaluation_result(results_path: Path, result_data: Dict):
    """Append evaluation result to CSV."""
    with open(results_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
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
            result_data["full_evaluation_response"],
            result_data["evaluation_timestamp"],
            result_data["evaluator_model"],
            result_data["evaluation_template"],
            result_data["generation_timestamp"],
            result_data["generator_model"],
        ])


@click.command()
@click.argument('experiment_directory', type=click.Path(exists=True, path_type=Path))
@click.option(
    '--evaluator-model', 
    default='openai/gpt-4',
    help='Model to use for evaluation'
)
@click.option(
    '--evaluation-template',
    default='default',
    help='Evaluation template to use (default: iterative_loops)'
)
def evaluate_experiment(
    experiment_directory: Path,
    evaluator_model: str,
    evaluation_template: str
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
        if evaluation_template == "default":
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
    
    # Create evaluation results CSV
    eval_results_path = create_evaluation_results_csv(experiment_directory)
    
    click.echo(f"Evaluating {len(generation_results)} responses...")
    click.echo(f"Evaluation results will be saved to: {eval_results_path}")
    
    successful_evaluations = 0
    failed_evaluations = 0
    
    with click.progressbar(generation_results, label="Evaluating responses") as bar:
        for gen_result in bar:
            try:
                # Load response content
                response_content = load_response_content(
                    experiment_directory, 
                    gen_result["response_file"]
                )
                
                # Generate evaluation prompt using template
                evaluation_prompt = template_loader.render_evaluation_prompt(
                    evaluation_template, 
                    response_content
                )
                
                # Evaluate response
                rating, confidence, reasoning, full_response = model_client.evaluate(
                    evaluation_prompt, 
                    response_content,
                    evaluator_model
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
                    "automated_rating": int(rating),
                    "confidence_score": confidence,
                    "evaluation_reasoning": reasoning,
                    "full_evaluation_response": full_response,
                    "evaluation_timestamp": evaluation_timestamp,
                    "evaluator_model": evaluator_model,
                    "evaluation_template": evaluation_template,
                    "generation_timestamp": gen_result["generation_timestamp"],
                    "generator_model": gen_result["generator_model"],
                }
                
                save_evaluation_result(eval_results_path, eval_result)
                
                if rating:
                    successful_evaluations += 1
                    click.echo(f"\n✓ SUCCESS (attempt {gen_result['attempt_id']}): {confidence:.2f} confidence")
                
            except Exception as e:
                failed_evaluations += 1
                click.echo(f"\nError evaluating attempt {gen_result['attempt_id']}: {e}")
                
                # Save error result
                eval_result = {
                    "experiment_id": gen_result["experiment_id"],
                    "attempt_id": gen_result["attempt_id"],
                    "concept_names": gen_result["concept_names"],
                    "concept_count": gen_result["concept_count"],
                    "level": gen_result["level"],
                    "template_id": gen_result["template_id"],
                    "response_file": gen_result["response_file"],
                    "automated_rating": 0,
                    "confidence_score": 0.0,
                    "evaluation_reasoning": f"Evaluation error: {str(e)}",
                    "full_evaluation_response": "",
                    "evaluation_timestamp": datetime.now().isoformat(),
                    "evaluator_model": evaluator_model,
                    "evaluation_template": evaluation_template,
                    "generation_timestamp": gen_result["generation_timestamp"],
                    "generator_model": gen_result["generator_model"],
                }
                
                save_evaluation_result(eval_results_path, eval_result)
            
            time.sleep(0.1)  # Rate limiting
    
    click.echo(f"\nEvaluation completed!")
    click.echo(f"Successful evaluations: {successful_evaluations}")
    click.echo(f"Failed evaluations: {failed_evaluations}")
    click.echo(f"Results saved to: {eval_results_path}")


if __name__ == "__main__":
    evaluate_experiment()
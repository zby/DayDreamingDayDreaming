"""Standalone node functions copied directly to avoid kedro imports.

These are the essential functions needed for the Dagster migration.
"""

from typing import Any
import pandas as pd
import logging
from itertools import combinations
from jinja2 import Environment
import os

# Import utility classes from local copies
from .eval_response_parser import parse_llm_response, extract_template_name

logger = logging.getLogger(__name__)



def generate_concept_combinations(
    concepts_list: list[dict[str, str]], parameters: dict[str, Any]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate concept combinations and relationships using itertools.

    Args:
        concepts_list: List of concept dictionaries with concept_id and name
        parameters: Pipeline parameters including k_max

    Returns:
        - concept_combinations: DataFrame with combo metadata
        - concept_combo_relationships: DataFrame mapping combos to concepts
    """
    k_max = parameters.get("k_max", 3)

    logger.info(f"Generating concept combinations with k_max={k_max}")

    concept_combinations = []
    concept_combo_relationships = []

    combo_id = 1
    for combo in combinations(concepts_list, k_max):
        combo_id_str = f"combo_{combo_id:03d}"

        # Create description from concept names
        concept_names = [concept["name"] for concept in combo]
        description = " + ".join(concept_names)

        # Add to concept_combinations
        combo_data = {
            "combo_id": combo_id_str,
            "description": description,
            "num_concepts": len(combo),
            "created_date": pd.Timestamp.now().strftime("%Y-%m-%d"),
        }
        concept_combinations.append(combo_data)

        # Add concept-to-combo relationships
        for position, concept in enumerate(combo, 1):
            relationship_data = {
                "combo_id": combo_id_str,
                "concept_id": concept["concept_id"],
                "position": position,
            }
            concept_combo_relationships.append(relationship_data)

        combo_id += 1

    concept_combinations_df = pd.DataFrame(concept_combinations)
    concept_combo_relationships_df = pd.DataFrame(concept_combo_relationships)

    logger.info(
        f"Generated {len(concept_combinations_df)} combinations "
        f"with {len(concept_combo_relationships_df)} total concept relationships"
    )

    return concept_combinations_df, concept_combo_relationships_df


def create_generation_tasks_from_content_combinations(
    content_combinations: list,  # List[ContentCombination]
    generation_models: pd.DataFrame,
    generation_templates: dict[str, str],
) -> pd.DataFrame:
    """
    Create generation tasks directly from ContentCombination objects.

    Args:
        content_combinations: List of ContentCombination objects
        generation_models: DataFrame with active generation models
        generation_templates: Dict of template_id -> template_content

    Returns:
        generation_tasks: DataFrame with generation task definitions
    """
    logger.info("Creating generation tasks from ContentCombination objects")

    # Filter for active models only
    active_gen_models = generation_models[generation_models["active"] == True]

    logger.info(f"Found {len(active_gen_models)} active generation models")

    generation_tasks = []

    # Create generation tasks for each combination of combo + template + model
    for content_combo in content_combinations:
        combo_id = content_combo.combo_id

        for template_id in generation_templates.keys():
            for _, model_row in active_gen_models.iterrows():
                generation_model = model_row["model_name"]

                # Create unique task ID
                generation_task_id = f"{combo_id}_{template_id}_{generation_model}"

                generation_task = {
                    "generation_task_id": generation_task_id,
                    "combo_id": combo_id,
                    "generation_template": template_id,
                    "generation_model": generation_model,
                }
                generation_tasks.append(generation_task)

    generation_tasks_df = pd.DataFrame(generation_tasks)

    logger.info(f"Created {len(generation_tasks_df)} generation tasks")

    return generation_tasks_df


def create_evaluation_tasks_from_generation_tasks(
    generation_tasks: pd.DataFrame,
    evaluation_models: pd.DataFrame,
    evaluation_templates: dict[str, str],
) -> pd.DataFrame:
    """
    Create evaluation tasks from generation tasks.

    Args:
        generation_tasks: DataFrame with generation task definitions
        evaluation_models: DataFrame with active evaluation models
        evaluation_templates: Dict of template_id -> template_content

    Returns:
        evaluation_tasks: DataFrame with evaluation task definitions
    """
    logger.info("Creating evaluation tasks from generation tasks")

    # Filter for active models only
    active_eval_models = evaluation_models[evaluation_models["active"] == True]

    logger.info(f"Found {len(active_eval_models)} active evaluation models")

    evaluation_tasks = []

    # Create evaluation tasks for each evaluation template + model for each generation task
    for _, gen_task_row in generation_tasks.iterrows():
        generation_task_id = gen_task_row["generation_task_id"]

        for eval_template_id in evaluation_templates.keys():
            for _, eval_model_row in active_eval_models.iterrows():
                evaluation_model = eval_model_row["model_name"]

                # Create unique evaluation task ID
                evaluation_task_id = f"{generation_task_id}_{eval_template_id}_{evaluation_model}"

                evaluation_task = {
                    "evaluation_task_id": evaluation_task_id,
                    "generation_task_id": generation_task_id,
                    "evaluation_template": eval_template_id,
                    "evaluation_model": evaluation_model,
                }
                evaluation_tasks.append(evaluation_task)

    evaluation_tasks_df = pd.DataFrame(evaluation_tasks)

    logger.info(f"Created {len(evaluation_tasks_df)} evaluation tasks")

    return evaluation_tasks_df



def create_all_tasks(
    concept_combinations: tuple[pd.DataFrame, pd.DataFrame],
    generation_models: pd.DataFrame,
    evaluation_models: pd.DataFrame,
    generation_templates: dict[str, str],
    evaluation_templates: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Create generation and evaluation tasks from concept combinations and available models/templates.

    Args:
        concept_combinations: Tuple of (combinations_df, relationships_df)
        generation_models: DataFrame with active generation models
        evaluation_models: DataFrame with active evaluation models
        generation_templates: Dict of template_id -> template_content
        evaluation_templates: Dict of template_id -> template_content

    Returns:
        - generation_tasks: DataFrame with generation task definitions
        - evaluation_tasks: DataFrame with evaluation task definitions
    """
    combo_df, combo_relationships = concept_combinations

    logger.info("Creating generation and evaluation tasks")

    # Filter for active models only
    active_gen_models = generation_models[generation_models["active"] == True]
    active_eval_models = evaluation_models[evaluation_models["active"] == True]

    logger.info(
        f"Found {len(active_gen_models)} active generation models and {len(active_eval_models)} active evaluation models"
    )

    generation_tasks = []
    evaluation_tasks = []

    # Create generation tasks for each combination of combo + template + model
    for _, combo_row in combo_df.iterrows():
        combo_id = combo_row["combo_id"]

        for template_id in generation_templates.keys():
            for _, model_row in active_gen_models.iterrows():
                generation_model = model_row["model_name"]

                # Create unique task ID
                generation_task_id = f"{combo_id}_{template_id}_{generation_model}"

                generation_task = {
                    "generation_task_id": generation_task_id,
                    "combo_id": combo_id,
                    "generation_template": template_id,
                    "generation_model": generation_model,
                }
                generation_tasks.append(generation_task)

                # Create evaluation tasks for each evaluation template + model for this generation task
                for eval_template_id in evaluation_templates.keys():
                    for _, eval_model_row in active_eval_models.iterrows():
                        evaluation_model = eval_model_row["model_name"]

                        # Create unique evaluation task ID
                        evaluation_task_id = f"{generation_task_id}_{eval_template_id}_{evaluation_model}"

                        evaluation_task = {
                            "evaluation_task_id": evaluation_task_id,
                            "generation_task_id": generation_task_id,
                            "evaluation_template": eval_template_id,
                            "evaluation_model": evaluation_model,
                        }
                        evaluation_tasks.append(evaluation_task)

    generation_tasks_df = pd.DataFrame(generation_tasks)
    evaluation_tasks_df = pd.DataFrame(evaluation_tasks)

    logger.info(
        f"Created {len(generation_tasks_df)} generation tasks and {len(evaluation_tasks_df)} evaluation tasks"
    )

    return generation_tasks_df, evaluation_tasks_df


def generate_prompts(
    generation_tasks: pd.DataFrame,
    combo_df: pd.DataFrame,
    combo_relationships: pd.DataFrame,
    concept_contents: dict[str, str],
    generation_templates: dict[str, str],
    concepts_metadata: pd.DataFrame,
) -> dict[str, str]:
    """
    Generate prompts for all generation tasks.

    Args:
        generation_tasks: DataFrame of generation tasks
        combo_df: DataFrame of concept combinations
        combo_relationships: DataFrame of concept-combo relationships
        concept_contents: Dict mapping concept_id to content
        generation_templates: Dict of template_id -> template_content
        concepts_metadata: DataFrame with concept metadata for names

    Returns:
        Dictionary mapping generation_task_id to generated prompt text
    """
    logger.info("Generating prompts for all generation tasks")

    prompts = {}
    env = Environment()

    for _, task_row in generation_tasks.iterrows():
        generation_task_id = task_row["generation_task_id"]
        combo_id = task_row["combo_id"]
        template_id = task_row["generation_template"]

        # Get concepts for this combination
        combo_concept_ids = combo_relationships[
            combo_relationships["combo_id"] == combo_id
        ]["concept_id"].tolist()

        # Build concept data for template
        concepts_for_template = []
        for concept_id in combo_concept_ids:
            # Get concept name from metadata
            concept_name = concepts_metadata[
                concepts_metadata["concept_id"] == concept_id
            ]["name"].iloc[0]

            concept_data = {
                "concept_id": concept_id,
                "name": concept_name,
                "content": concept_contents[concept_id],
            }
            concepts_for_template.append(concept_data)

        # Render template
        template_content = generation_templates[template_id]
        template = env.from_string(template_content)
        prompt = template.render(concepts=concepts_for_template)

        prompts[generation_task_id] = prompt

    logger.info(f"Generated {len(prompts)} prompts")
    return prompts


def generate_evaluation_prompts(
    generation_responses: dict[str, str],
    evaluation_tasks: pd.DataFrame,
    evaluation_templates: dict[str, str],
) -> dict[str, str]:
    """
    Generate evaluation prompts from generation responses.

    Args:
        generation_responses: Dict mapping generation_task_id to response text
        evaluation_tasks: DataFrame of evaluation tasks
        evaluation_templates: Dict of template_id -> template_content

    Returns:
        Dictionary mapping evaluation_task_id to evaluation prompt text
    """
    logger.info("Generating evaluation prompts")

    eval_prompts = {}
    env = Environment()

    for _, task_row in evaluation_tasks.iterrows():
        evaluation_task_id = task_row["evaluation_task_id"]
        generation_task_id = task_row["generation_task_id"]
        template_id = task_row["evaluation_template"]

        # Get the generation response
        if generation_task_id not in generation_responses:
            logger.warning(
                f"No generation response found for {generation_task_id}, skipping evaluation task {evaluation_task_id}"
            )
            continue

        generation_response = generation_responses[generation_task_id]

        # Render evaluation template
        template_content = evaluation_templates[template_id]
        template = env.from_string(template_content)
        eval_prompt = template.render(response=generation_response)

        eval_prompts[evaluation_task_id] = eval_prompt

    logger.info(f"Generated {len(eval_prompts)} evaluation prompts")
    return eval_prompts


def parse_scores(evaluation_responses: dict[str, str]) -> str:
    """
    Parse evaluation responses to extract scores.

    Args:
        evaluation_responses: Dict mapping evaluation_task_id to response text

    Returns:
        Path to the saved parsed scores CSV file
    """
    logger.info("Parsing evaluation responses")

    parsed_scores = []

    for evaluation_task_id, response_text in evaluation_responses.items():
        try:
            # Extract template name from evaluation_task_id for template-aware parsing
            template_name = extract_template_name(evaluation_task_id)
            
            # Use template-aware parser
            score_data = parse_llm_response(response_text, template_name)
            score_data["evaluation_task_id"] = evaluation_task_id
            parsed_scores.append(score_data)
        except Exception as e:
            logger.error(
                f"Failed to parse response for {evaluation_task_id}: {e}"
            )
            # Add error record
            error_record = {
                "evaluation_task_id": evaluation_task_id,
                "score": None,
                "error": str(e),
            }
            parsed_scores.append(error_record)

    # Save to CSV - ensure proper column structure even if empty
    if not parsed_scores:
        # Create empty DataFrame with expected column structure
        parsed_scores_df = pd.DataFrame(columns=[
            "evaluation_task_id", 
            "score", 
            "error"
        ])
    else:
        parsed_scores_df = pd.DataFrame(parsed_scores)
    
    output_path = "data/05_parsing/parsed_scores.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    parsed_scores_df.to_csv(output_path, index=False)

    logger.info(f"Parsed {len(parsed_scores)} evaluation responses to {output_path}")
    return output_path
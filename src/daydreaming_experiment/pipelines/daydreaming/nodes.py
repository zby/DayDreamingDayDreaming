"""Node functions for the daydreaming experiment data processing pipeline.

These functions contain pure logic with no file I/O, following Kedro best practices
for separation of concerns. All data loading/saving is handled by the Data Catalog.
"""

from typing import Any
import pandas as pd
import logging
from itertools import combinations
from jinja2 import Environment

# Import utility classes
from daydreaming_experiment.utils.model_client import SimpleModelClient
from daydreaming_experiment.utils.eval_response_parser import parse_llm_response

logger = logging.getLogger(__name__)


def load_and_prepare_concepts(
    concepts_metadata: pd.DataFrame,
    concept_descriptions_sentence: dict[str, Any],
    concept_descriptions_paragraph: dict[str, Any],
    concept_descriptions_article: dict[str, Any],
    parameters: dict[str, Any],
) -> tuple[list[dict[str, str]], dict[str, str]]:
    """
    Load concepts from metadata and generate content with fallback strategy.

    Args:
        concepts_metadata: DataFrame with concept_id and name columns
        concept_descriptions_sentence: Dict of sentence-level concept descriptions
        concept_descriptions_paragraph: Dict of paragraph-level concept descriptions
        concept_descriptions_article: Dict of article-level concept descriptions
        parameters: Pipeline parameters including description_level

    Returns:
        - concepts_list: List of concept dictionaries with concept_id and name
        - concept_contents: Dict mapping concept_id to content
    """
    description_level = parameters.get("description_level", "paragraph")

    logger.info(
        f"Loading and preparing concepts with description_level={description_level}"
    )

    # Load concepts from the new filesystem-based structure
    concepts = []
    for _, row in concepts_metadata.iterrows():
        concept_id = row["concept_id"]
        concept_name = row["name"]
        concepts.append({"concept_id": concept_id, "name": concept_name})

    logger.info(f"Loaded {len(concepts)} concepts from filesystem")

    # First: Generate concept content with level flexibility and fallback strategy
    concept_contents = {}

    # Define fallback order: requested level -> paragraph -> sentence -> whatever's available
    level_preference = [description_level]
    if description_level != "paragraph":
        level_preference.append("paragraph")
    if description_level != "sentence":
        level_preference.append("sentence")

    # Add all levels to ensure we have fallbacks
    all_levels = ["article", "paragraph", "sentence"]
    for level in all_levels:
        if level not in level_preference:
            level_preference.append(level)

    level_datasets = {
        "sentence": concept_descriptions_sentence,
        "paragraph": concept_descriptions_paragraph,
        "article": concept_descriptions_article,
    }

    for concept in concepts:
        concept_id = concept["concept_id"]
        content = None

        # Try each level in preference order
        for level in level_preference:
            if level in level_datasets and concept_id in level_datasets[level]:
                content_dataset = level_datasets[level][concept_id]
                # Handle callable datasets (from partitioned datasets)
                content = (
                    content_dataset() if callable(content_dataset) else content_dataset
                )
                logger.debug(
                    f"Using {level} level description for concept {concept_id}"
                )
                break

        if content is None:
            raise ValueError(
                f"No description found for concept {concept_id} at any level"
            )

        concept_contents[concept_id] = content

    logger.info(
        f"Generated content for {len(concept_contents)} concepts using level {description_level} (with fallbacks)"
    )

    return concepts, concept_contents


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

    logger.info(f"Generated {len(concept_combinations)} concept combinations")
    logger.info(
        f"Generated {len(concept_combo_relationships)} concept-combo relationships"
    )

    # Create DataFrames
    concept_combinations_df = pd.DataFrame(concept_combinations)
    concept_combo_relationships_df = pd.DataFrame(concept_combo_relationships)

    return concept_combinations_df, concept_combo_relationships_df


def create_all_tasks(
    concept_combinations: pd.DataFrame,
    generation_models: pd.DataFrame,
    evaluation_models: pd.DataFrame,
    parameters: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate active models and create both generation and evaluation tasks.

    Args:
        concept_combinations: DataFrame with combo metadata
        generation_models: DataFrame with generation model configurations
        evaluation_models: DataFrame with evaluation model configurations
        parameters: Pipeline parameters including current templates

    Returns:
        - generation_tasks: DataFrame with generation task configs
        - evaluation_tasks: DataFrame with evaluation task configs
    """
    current_gen_template = parameters.get(
        "current_gen_template", "00_systematic_analytical"
    )
    current_eval_template = parameters.get(
        "current_eval_template", "creativity_metrics"
    )

    logger.info(
        f"Creating tasks with templates: {current_gen_template}, {current_eval_template}"
    )

    # Get active models
    active_gen_models = generation_models[generation_models["active"]]
    active_eval_models = evaluation_models[evaluation_models["active"]]

    if active_gen_models.empty:
        raise ValueError("No active generation models found")
    if active_eval_models.empty:
        raise ValueError("No active evaluation models found")

    # Generate generation tasks for all active models and current template
    generation_tasks = []
    for _, combo_row in concept_combinations.iterrows():
        combo_id_str = combo_row["combo_id"]

        # Create generation tasks for each active generation model
        for _, gen_model_row in active_gen_models.iterrows():
            gen_model_name = gen_model_row["model_name"]
            gen_model_short = gen_model_row["model_short"]
            generation_task_id = (
                f"{combo_id_str}_{current_gen_template}_{gen_model_short}"
            )

            gen_task_data = {
                "generation_task_id": generation_task_id,
                "combo_id": combo_id_str,
                "generation_template": current_gen_template,
                "generation_model": gen_model_name,
                "generation_model_short": gen_model_short,
            }
            generation_tasks.append(gen_task_data)

    # Generate evaluation tasks for all active evaluation models
    evaluation_tasks = []
    for gen_task_data in generation_tasks:
        generation_task_id = gen_task_data["generation_task_id"]

        # Create evaluation tasks for each active evaluation model
        for _, eval_model_row in active_eval_models.iterrows():
            eval_model_name = eval_model_row["model_name"]
            eval_model_short = eval_model_row["model_short"]
            evaluation_task_id = (
                f"{generation_task_id}_{current_eval_template}_{eval_model_short}"
            )

            eval_task_data = {
                "evaluation_task_id": evaluation_task_id,
                "generation_task_id": generation_task_id,
                "evaluation_template": current_eval_template,
                "evaluation_model": eval_model_name,
                "evaluation_model_short": eval_model_short,
            }
            evaluation_tasks.append(eval_task_data)

    logger.info(f"Generated {len(generation_tasks)} generation tasks")
    logger.info(f"Generated {len(evaluation_tasks)} evaluation tasks")

    # Create DataFrames
    generation_tasks_df = pd.DataFrame(generation_tasks)
    evaluation_tasks_df = pd.DataFrame(evaluation_tasks)

    return generation_tasks_df, evaluation_tasks_df


def create_task_list(
    generation_models: pd.DataFrame,
    evaluation_models: pd.DataFrame,
    generation_templates: dict[str, str],
    evaluation_templates: dict[str, str],
    concepts_metadata: pd.DataFrame,
    concept_descriptions_sentence: dict[str, Any],
    concept_descriptions_paragraph: dict[str, Any],
    concept_descriptions_article: dict[str, Any],
    parameters: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, str]]:
    """
    DEPRECATED: Use the new 3-node pipeline instead.

    This function is kept for backward compatibility but should be replaced
    with the new load_and_prepare_concepts -> generate_concept_combinations -> create_all_tasks pipeline.
    """
    logger.warning(
        "create_task_list is deprecated. Use the new 3-node pipeline instead."
    )

    # Call the new functions in sequence to maintain compatibility
    concepts_list, concept_contents = load_and_prepare_concepts(
        concepts_metadata,
        concept_descriptions_sentence,
        concept_descriptions_paragraph,
        concept_descriptions_article,
        parameters,
    )

    concept_combinations, concept_combo_relationships = generate_concept_combinations(
        concepts_list, parameters
    )

    generation_tasks, evaluation_tasks = create_all_tasks(
        concept_combinations, generation_models, evaluation_models, parameters
    )

    return (
        concept_combinations,
        concept_combo_relationships,
        generation_tasks,
        evaluation_tasks,
        concept_contents,
    )


def generate_prompts(
    generation_tasks: pd.DataFrame,
    concept_combinations: pd.DataFrame,
    concept_combo_relationships: pd.DataFrame,
    concept_contents: dict[str, str],
    generation_templates: dict[str, str],
    concepts_metadata: pd.DataFrame,
) -> dict[str, str]:
    """
    Generate prompts for all generation tasks.

    Args:
        generation_tasks: DataFrame with generation task configs
        concept_combinations: DataFrame with combo metadata
        concept_combo_relationships: DataFrame mapping combos to concepts
        concept_contents: Dictionary mapping concept_id to concept content
        generation_templates: Dictionary of generation templates
        concepts_metadata: DataFrame with concept_id and name columns

    Returns:
        Dict mapping generation_task_id to prompt content
    """
    logger.info(f"Generating prompts for {len(generation_tasks)} generation tasks")
    logger.info(f"Using concept contents for {len(concept_contents)} concepts")

    prompts = {}

    # Create concept name lookup from metadata
    concept_name_lookup = dict(
        zip(concepts_metadata["concept_id"], concepts_metadata["name"])
    )

    for _, gen_task in generation_tasks.iterrows():
        generation_task_id = gen_task["generation_task_id"]
        combo_id = gen_task["combo_id"]
        template_key = gen_task["generation_template"]

        # Get template content
        if template_key not in generation_templates:
            logger.warning(
                f"Template {template_key} not found, using first available template"
            )
            template_key = list(generation_templates.keys())[0]

        # Load the actual template content (partitioned datasets return callables)
        template_loader = generation_templates[template_key]
        template_content = (
            template_loader() if callable(template_loader) else template_loader
        )

        # Get concepts for this combo (ordered by position)
        combo_concepts = concept_combo_relationships[
            concept_combo_relationships["combo_id"] == combo_id
        ].sort_values("position")

        # Create concept objects for template rendering
        concepts = []
        for _, concept_row in combo_concepts.iterrows():
            concept_id = concept_row["concept_id"]

            if concept_id not in concept_contents:
                logger.error(
                    f"Concept ID '{concept_id}' not found in concept contents for task {generation_task_id}"
                )
                raise KeyError(
                    f"Concept ID '{concept_id}' not found in concept contents"
                )

            if concept_id not in concept_name_lookup:
                logger.error(
                    f"Concept ID '{concept_id}' not found in concept database for task {generation_task_id}"
                )
                raise KeyError(
                    f"Concept ID '{concept_id}' not found in concept database"
                )

            # Get the actual content (concept_contents may contain callables from partitioned dataset)
            content_dataset = concept_contents[concept_id]
            actual_content = (
                content_dataset() if callable(content_dataset) else content_dataset
            )
            concept_name = concept_name_lookup[concept_id]

            # Create a dictionary for template rendering (Jinja2 can access dict keys with dot notation)
            concept_dict = {
                "concept_id": concept_id,
                "name": concept_name,
                "content": actual_content,
            }
            concepts.append(concept_dict)

        # Create Jinja2 template and render with concept dictionaries
        # Templates expect: concepts (list of dicts with .name, .concept_id, .content keys)
        env = Environment()
        template = env.from_string(template_content)
        prompt = template.render(concepts=concepts)

        prompts[generation_task_id] = prompt

    logger.info(f"Generated {len(prompts)} prompts")
    return prompts


def get_llm_responses(
    generation_prompts: dict[str, str],
    generation_tasks: pd.DataFrame,
    generation_models: pd.DataFrame,
) -> dict[str, str]:
    """
    Get LLM responses for generation tasks.

    Args:
        generation_prompts: Dictionary of prompts to send to LLM
        generation_tasks: DataFrame with generation task configs
        generation_models: DataFrame with model configurations

    Returns:
        Dict mapping generation_task_id to response content
    """
    logger.info(
        f"Getting LLM responses for {len(generation_prompts)} generation prompts"
    )

    responses = {}

    # Initialize client with rate limiting
    client = SimpleModelClient()

    for generation_task_id, prompt_dataset in generation_prompts.items():
        gen_task = generation_tasks[
            generation_tasks["generation_task_id"] == generation_task_id
        ].iloc[0]

        # Load the actual prompt content from the dataset (prompts contains callable methods)
        if callable(prompt_dataset):
            prompt = prompt_dataset()  # Call the method to get the string
        else:
            prompt = prompt_dataset  # Fallback if it's already a string

        try:
            # API errors will propagate and fail the task
            model_name = gen_task["generation_model"]
            logger.info(
                f"Sending generation request for {generation_task_id} to model {model_name}"
            )
            response = client.generate(prompt, model=model_name)
            logger.info(
                f"Received generation response for {generation_task_id} ({len(response)} characters)"
            )
            responses[generation_task_id] = response

        except Exception as e:
            logger.error(f"Failed to generate response for {generation_task_id}: {e}")
            raise

    logger.info(f"Generated {len(responses)} LLM responses")
    return responses


def generate_evaluation_prompts(
    generation_responses: dict[str, str],
    evaluation_tasks: pd.DataFrame,
    evaluation_templates: dict[str, str],
) -> dict[str, str]:
    """
    Generate evaluation prompts for all evaluation tasks.

    Args:
        generation_responses: Dictionary of LLM responses to evaluate
        evaluation_tasks: DataFrame with evaluation task configs
        evaluation_templates: Dictionary of evaluation templates

    Returns:
        Dict mapping evaluation_task_id to evaluation prompt
    """
    logger.info(
        f"Generating evaluation prompts for {len(evaluation_tasks)} evaluation tasks"
    )

    eval_prompts = {}

    for _, eval_task in evaluation_tasks.iterrows():
        evaluation_task_id = eval_task["evaluation_task_id"]
        generation_task_id = eval_task["generation_task_id"]
        template_key = eval_task["evaluation_template"]

        # Get the generation response for this evaluation task
        if generation_task_id not in generation_responses:
            logger.error(
                f"Generation response for {generation_task_id} not found for evaluation task {evaluation_task_id}"
            )
            raise KeyError(f"Generation response for {generation_task_id} not found")

        response_dataset = generation_responses[generation_task_id]

        # Load the actual response content from the dataset (responses contains callable methods)
        if callable(response_dataset):
            response = response_dataset()  # Call the method to get the string
        else:
            response = response_dataset  # Fallback if it's already a string

        # Get evaluation template
        if template_key not in evaluation_templates:
            logger.warning(
                f"Evaluation template {template_key} not found, using first available"
            )
            template_key = list(evaluation_templates.keys())[0]

        # Load the actual template content (partitioned datasets return callables)
        eval_template_loader = evaluation_templates[template_key]
        eval_template_content = (
            eval_template_loader()
            if callable(eval_template_loader)
            else eval_template_loader
        )

        # Format the evaluation prompt with the response using Jinja2
        env = Environment()
        template = env.from_string(eval_template_content)
        eval_prompt = template.render(response=response)
        eval_prompts[evaluation_task_id] = eval_prompt

    logger.info(f"Generated {len(eval_prompts)} evaluation prompts")
    return eval_prompts


def query_evaluation_llm(
    evaluation_prompts: dict[str, str],
    evaluation_tasks: pd.DataFrame,
    evaluation_models: pd.DataFrame,
) -> dict[str, str]:
    """
    Get LLM responses for evaluation tasks.

    Args:
        evaluation_prompts: Dictionary of evaluation prompts
        evaluation_tasks: DataFrame with evaluation task configs
        evaluation_models: DataFrame with model configurations

    Returns:
        Dict mapping evaluation_task_id to evaluation response
    """
    logger.info(
        f"Getting evaluation responses for {len(evaluation_prompts)} evaluation prompts"
    )

    eval_responses = {}

    # Initialize client
    client = SimpleModelClient()

    for evaluation_task_id, eval_prompt_dataset in evaluation_prompts.items():
        eval_task = evaluation_tasks[
            evaluation_tasks["evaluation_task_id"] == evaluation_task_id
        ].iloc[0]

        # Load the actual prompt content from the dataset (eval_prompts contains callable methods)
        if callable(eval_prompt_dataset):
            eval_prompt = eval_prompt_dataset()  # Call the method to get the string
        else:
            eval_prompt = eval_prompt_dataset  # Fallback if it's already a string

        try:
            model_name = eval_task["evaluation_model"]
            logger.info(
                f"Sending evaluation request for {evaluation_task_id} to model {model_name}"
            )
            response = client.generate(eval_prompt, model=model_name)
            logger.info(
                f"Received evaluation response for {evaluation_task_id} ({len(response)} characters)"
            )
            eval_responses[evaluation_task_id] = response

        except Exception as e:
            logger.error(f"Failed to evaluate {evaluation_task_id}: {e}")
            raise

    logger.info(f"Generated {len(eval_responses)} evaluation responses")
    return eval_responses


def parse_scores(evaluation_responses: dict[str, str]) -> str:
    """
    Parse scores from evaluation responses and create a CSV file.

    Args:
        evaluation_responses: Dictionary of raw evaluation responses

    Returns:
        String path to the created CSV file
    """
    logger.info(f"Parsing scores from {len(evaluation_responses)} evaluation responses")

    parsed_scores = []
    failed_parses = []

    for evaluation_task_id, eval_response_dataset in evaluation_responses.items():
        # Load the actual response content from the dataset (eval_responses contains callable methods)
        if callable(eval_response_dataset):
            raw_eval = eval_response_dataset()  # Call the method to get the string
        else:
            raw_eval = eval_response_dataset  # Fallback if it's already a string

        try:
            # Use the existing parsing function
            score = parse_llm_response(raw_eval)

            parsed_score_data = {
                "evaluation_response_filename": evaluation_task_id,
                "score": score,
            }
            parsed_scores.append(parsed_score_data)

        except Exception as e:
            logger.error(f"Failed to parse evaluation for {evaluation_task_id}: {e}")
            failed_parses.append(evaluation_task_id)
            # Let parsing errors propagate to fail the task
            raise ValueError(
                f"Failed to parse evaluation for {evaluation_task_id}: {e}"
            )

    if failed_parses:
        logger.warning(
            f"Failed to parse {len(failed_parses)} evaluations: {failed_parses}"
        )

    logger.info(f"Successfully parsed {len(parsed_scores)} scores")

    # Convert to DataFrame and save as CSV
    result_df = pd.DataFrame(parsed_scores)
    csv_path = "data/04_evaluation/parsed_scores.csv"
    result_df.to_csv(csv_path, index=False)
    logger.info(f"Saved parsed scores to {csv_path}")

    return csv_path

"""Node functions for the daydreaming experiment data processing pipeline.

These functions contain pure logic with no file I/O, following Kedro best practices
for separation of concerns. All data loading/saving is handled by the Data Catalog.
"""

from typing import Dict, Any, List, Tuple
import pandas as pd
import logging
from itertools import combinations
from jinja2 import Environment
from pathlib import Path

# Import utility classes
from daydreaming_experiment.utils.model_client import SimpleModelClient, parse_llm_response
from daydreaming_experiment.utils.concept_db import ConceptDB
from daydreaming_experiment.utils.concept import Concept

logger = logging.getLogger(__name__)


def create_task_list(
    concepts_file_path: str = "data/01_raw/concepts/day_dreaming_concepts.json", 
    k_max: int = 2
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str]]:
    """Generate all concept combinations for processing and create concept content files.
    
    Args:
        concepts_file_path: Path to the concept database JSON file
        k_max: Size of concept combinations to generate
        
    Returns:
        Tuple of (tasks_df, task_concepts_df, concept_contents):
        - tasks_df: Core task metadata indexed by run_id
        - task_concepts_df: Task-concept relationships with run_id, concept_id, concept_order
        - concept_contents: Dictionary mapping concept_id to concept content for file generation
        
    TODO: Consider making template, generator_model, and evaluator_model configurable
    TODO: Add k_max validation (should be >= 1 and <= number of available concepts)
    TODO: Consider adding template selection strategies beyond the default
    """
    logger.info(f"Creating task list from concept database: {concepts_file_path}")
    
    # Load concepts using ConceptDB.load()
    concept_db = ConceptDB.load(concepts_file_path)
    logger.info(f"Loaded {len(concept_db)} concepts from database")
    
    # Generate concept content files for use in later nodes
    concept_contents = {}
    for concept in concept_db.get_concepts():
        # Use paragraph level by default, with fallback to less detailed levels
        content = concept.get_description("paragraph", strict=False)
        concept_contents[concept.concept_id] = content
    
    logger.info(f"Generated content for {len(concept_contents)} concepts")
    
    # Generate all k_max-combinations of concepts
    tasks = []
    task_concepts = []
    
    for combo in concept_db.get_combinations(k_max):
        run_id = f"run_{len(tasks)+1:05d}"
        
        # Create core task entry (no concept columns)
        task_data = {
            'run_id': run_id,
            'template': '00_systematic_analytical',  # Default template
            'generator_model': 'deepseek/deepseek-r1:free',
            'evaluator_model': 'meta-llama/llama-4-maverick:free',
            'k_max': k_max
        }
        tasks.append(task_data)
        
        # Create task-concept relationships using concept_id and name
        for i, concept in enumerate(combo, 1):
            task_concept_data = {
                'run_id': run_id,
                'concept_id': concept.concept_id,
                'concept_name': concept.name,
                'concept_order': i
            }
            task_concepts.append(task_concept_data)
    
    logger.info(f"Generated {len(tasks)} task combinations with k_max={k_max}")
    logger.info(f"Generated {len(task_concepts)} task-concept relationships")
    
    # Create DataFrames 
    # Keep run_id as a regular column, not index, so it saves properly to CSV
    tasks_df = pd.DataFrame(tasks)
    task_concepts_df = pd.DataFrame(task_concepts)
    
    return tasks_df, task_concepts_df, concept_contents


def generate_prompts(
    tasks_df: pd.DataFrame, 
    task_concepts_df: pd.DataFrame, 
    generation_templates: Dict[str, callable],
    concept_contents: Dict[str, str]
) -> Dict[str, str]:
    """Generate all prompts based on task definitions and generation templates.
    
    Args:
        tasks_df: DataFrame with task definitions
        task_concepts_df: DataFrame with task-concept relationships (using concept_id)
        generation_templates: Dictionary of template loaders from partitioned dataset
        concept_contents: Dictionary mapping concept_id to concept content
        
    Returns:
        Dictionary mapping run_id to generated prompt
    """
    logger.info(f"Generating prompts for {len(tasks_df)} tasks")
    logger.info(f"Using concept contents for {len(concept_contents)} concepts")
    
    prompts = {}
    
    for _, task in tasks_df.iterrows():
        run_id = task['run_id']
        # Get template content
        template_key = task["template"]
        if template_key not in generation_templates:
            logger.warning(f"Template {template_key} not found, using first available template")
            template_key = list(generation_templates.keys())[0]
        
        # Load the actual template content (partitioned datasets return callables)
        template_loader = generation_templates[template_key]
        template_content = template_loader()
        
        # Get concepts for this task (ordered by concept_order)
        task_concepts = task_concepts_df[task_concepts_df['run_id'] == run_id].sort_values('concept_order')
        
        # Create simple concept objects for template rendering
        concepts = []
        for _, task_concept_row in task_concepts.iterrows():
            concept_id = task_concept_row['concept_id']
            concept_name = task_concept_row['concept_name']
            
            if concept_id not in concept_contents:
                logger.error(f"Concept ID '{concept_id}' not found in concept contents for task {run_id}")
                raise KeyError(f"Concept ID '{concept_id}' not found in concept contents")
            
            # Load the actual content from the dataset (concept_contents contains TextDataset objects)
            content_dataset = concept_contents[concept_id]
            actual_content = content_dataset.load() if hasattr(content_dataset, 'load') else content_dataset
            
            # Create a dictionary for template rendering (Jinja2 can access dict keys with dot notation)
            concept_dict = {
                'concept_id': concept_id,
                'name': concept_name,
                'content': actual_content
            }
            concepts.append(concept_dict)
        
        # Create Jinja2 template and render with concept dictionaries
        # Templates expect: concepts (list of dicts with .name, .concept_id, .content keys)
        env = Environment()
        template = env.from_string(template_content)
        prompt = template.render(concepts=concepts)
        
        prompts[run_id] = prompt
        
    logger.info(f"Generated {len(prompts)} prompts")
    return prompts


def get_llm_responses(prompts: Dict[str, str], tasks_df: pd.DataFrame) -> Dict[str, str]:
    """Query LLM for responses to generated prompts.
    
    Args:
        prompts: Dictionary of prompts to send to LLM
        tasks_df: DataFrame with model configuration
        
    Returns:
        Dictionary mapping run_id to LLM response
    """
    logger.info(f"Getting LLM responses for {len(prompts)} prompts")
    
    responses = {}
    
    # Initialize client with rate limiting
    client = SimpleModelClient()
    
    for run_id, prompt in prompts.items():
        task = tasks_df[tasks_df['run_id'] == run_id].iloc[0]
        logger.debug(f"Generating response for {run_id} using model {task['generator_model']}")
        
        try:
            # API errors will propagate and fail the task
            response = client.generate(prompt, model=task["generator_model"])
            responses[run_id] = response
            
        except Exception as e:
            logger.error(f"Failed to generate response for {run_id}: {e}")
            raise
    
    logger.info(f"Generated {len(responses)} LLM responses")
    return responses


def generate_evaluation_prompts(
    responses: Dict[str, str], 
    eval_templates: Dict[str, callable], 
    tasks_df: pd.DataFrame
) -> Dict[str, str]:
    """Generate evaluation prompts from responses and templates.
    
    Args:
        responses: Dictionary of LLM responses to evaluate
        eval_templates: Dictionary of evaluation template loaders from partitioned dataset
        tasks_df: DataFrame with task configuration
        
    Returns:
        Dictionary mapping run_id to evaluation prompt
    """
    logger.info(f"Generating evaluation prompts for {len(responses)} responses")
    
    eval_prompts = {}
    
    for run_id, response in responses.items():
        task = tasks_df[tasks_df['run_id'] == run_id].iloc[0]
        
        # Use appropriate evaluation template (default to first available)
        template_key = task.get("eval_template", list(eval_templates.keys())[0])
        
        if template_key not in eval_templates:
            logger.warning(f"Evaluation template {template_key} not found, using first available")
            template_key = list(eval_templates.keys())[0]
        
        # Load the actual template content (partitioned datasets return callables)
        eval_template_loader = eval_templates[template_key]
        eval_template = eval_template_loader()
        
        # Format the evaluation prompt with the response
        eval_prompt = eval_template.format(response=response)
        eval_prompts[run_id] = eval_prompt
        
    logger.info(f"Generated {len(eval_prompts)} evaluation prompts")
    return eval_prompts


def query_evaluation_llm(eval_prompts: Dict[str, str], tasks_df: pd.DataFrame) -> Dict[str, str]:
    """Query LLM for evaluation scores.
    
    Args:
        eval_prompts: Dictionary of evaluation prompts
        tasks_df: DataFrame with evaluator model configuration
        
    Returns:
        Dictionary mapping run_id to evaluation response
    """
    logger.info(f"Getting evaluation responses for {len(eval_prompts)} prompts")
    
    eval_responses = {}
    
    # Initialize client
    client = SimpleModelClient()
    
    for run_id, eval_prompt in eval_prompts.items():
        task = tasks_df[tasks_df['run_id'] == run_id].iloc[0]
        logger.debug(f"Evaluating {run_id} using model {task['evaluator_model']}")
        
        try:
            response = client.generate(eval_prompt, model=task["evaluator_model"])
            eval_responses[run_id] = response
            
        except Exception as e:
            logger.error(f"Failed to evaluate {run_id}: {e}")
            raise
    
    logger.info(f"Generated {len(eval_responses)} evaluation responses")
    return eval_responses


def parse_scores(eval_responses: Dict[str, str]) -> pd.DataFrame:
    """Parse raw evaluation responses to extract scores.
    
    Args:
        eval_responses: Dictionary of raw evaluation responses
        
    Returns:
        DataFrame with parsed scores indexed by run_id
    """
    logger.info(f"Parsing scores from {len(eval_responses)} evaluation responses")
    
    parsed_scores = {}
    failed_parses = []
    
    for run_id, raw_eval in eval_responses.items():
        try:
            # Use the existing parsing function
            score = parse_llm_response(raw_eval)
            parsed_scores[run_id] = {
                'raw_score': score,
                'raw_evaluation': raw_eval
            }
            
        except Exception as e:
            logger.error(f"Failed to parse evaluation for {run_id}: {e}")
            failed_parses.append(run_id)
            # Let parsing errors propagate to fail the task
            raise ValueError(f"Failed to parse evaluation for {run_id}: {e}")
    
    if failed_parses:
        logger.warning(f"Failed to parse {len(failed_parses)} evaluations: {failed_parses}")
    
    logger.info(f"Successfully parsed {len(parsed_scores)} scores")
    
    # Convert to DataFrame
    result_df = pd.DataFrame.from_dict(parsed_scores, orient='index')
    return result_df
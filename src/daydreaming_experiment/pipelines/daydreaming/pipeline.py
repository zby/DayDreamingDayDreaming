"""Daydreaming experiment pipeline.

This pipeline connects the node functions into a directed acyclic graph (DAG)
that orchestrates the complete experiment flow from concept loading to final results."""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    create_task_list,
    generate_prompts,
    get_llm_responses,
    generate_evaluation_prompts,
    query_evaluation_llm,
    parse_scores
)


def create_pipeline(**kwargs) -> Pipeline:
    """Create the daydreaming experiment pipeline.
    
    Returns:
        Pipeline: Complete daydreaming experiment pipeline
    """
    return pipeline([
        # Stage 1: Task Generation
        node(
            func=create_task_list,
            inputs=None,
            outputs=["tasks_to_run", "task_concepts", "concept_contents"],
            name="create_initial_tasks"
        ),
        
        # Stage 2: Content Generation
        node(
            func=generate_prompts,
            inputs=["tasks_to_run", "task_concepts", "generation_templates", "concept_contents"],
            outputs="generation_prompts",
            name="generate_prompts_node"
        ),
        node(
            func=get_llm_responses,
            inputs=["generation_prompts", "tasks_to_run"],
            outputs="generation_responses",
            name="get_responses_node"
        ),
        
        # Stage 3: Evaluation
        node(
            func=generate_evaluation_prompts,
            inputs=["generation_responses", "evaluation_templates", "tasks_to_run"],
            outputs="evaluation_prompts",
            name="generate_eval_prompts_node"
        ),
        node(
            func=query_evaluation_llm,
            inputs=["evaluation_prompts", "tasks_to_run"],
            outputs="evaluation_responses",
            name="query_eval_llm_node"
        ),
        
        # Stage 4: Score Parsing and Final Results
        node(
            func=parse_scores,
            inputs="evaluation_responses",
            outputs="final_results",
            name="parse_scores_node"
        )
    ])
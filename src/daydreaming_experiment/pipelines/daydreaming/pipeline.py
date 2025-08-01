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
    """Create the updated daydreaming experiment pipeline."""
    return pipeline([
        # Stage 1: Task Configuration Generation
        node(
            func=create_task_list,
            inputs=[
                "generation_models",
                "evaluation_models", 
                "generation_templates",
                "evaluation_templates",
                "concepts_metadata",
                "concept_descriptions_sentence",
                "concept_descriptions_paragraph",
                "concept_descriptions_article",
                "parameters"
            ],
            outputs=[
                "concept_combinations",
                "concept_combo_relationships", 
                "generation_tasks",
                "evaluation_tasks",
                "concept_contents"
            ],
            name="create_task_configurations"
        ),
        
        # Stage 2: Generation Phase
        node(
            func=generate_prompts,
            inputs=[
                "generation_tasks",
                "concept_combinations",
                "concept_combo_relationships",
                "concept_contents", 
                "generation_templates",
                "concepts_metadata"
            ],
            outputs="generation_prompts",
            name="generate_prompts_node"
        ),
        node(
            func=get_llm_responses,
            inputs=[
                "generation_prompts",
                "generation_tasks",
                "generation_models"
            ],
            outputs="generation_responses",
            name="get_generation_responses_node"
        ),
        
        # Stage 3: Evaluation Phase  
        node(
            func=generate_evaluation_prompts,
            inputs=[
                "generation_responses",
                "evaluation_tasks",
                "evaluation_templates"
            ],
            outputs="evaluation_prompts",
            name="generate_evaluation_prompts_node"
        ),
        node(
            func=query_evaluation_llm,
            inputs=[
                "evaluation_prompts",
                "evaluation_tasks",
                "evaluation_models"
            ],
            outputs="evaluation_responses",
            name="get_evaluation_responses_node"
        ),
        
        # Stage 4: Results Processing
        node(
            func=parse_scores,
            inputs=[
                "evaluation_responses",
                "evaluation_tasks",
                "generation_tasks", 
                "concept_combinations"
            ],
            outputs="final_results",
            name="parse_scores_node"
        )
    ])
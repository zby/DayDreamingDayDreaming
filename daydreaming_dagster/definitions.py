from dagster import Definitions, multiprocess_executor
from daydreaming_dagster.assets.llm_prompts_responses import (
    generation_prompt,
    generation_response,
    evaluation_prompt,
    evaluation_response,
    parsed_scores,
    final_results,
    perfect_score_paths
)
from daydreaming_dagster.assets.raw_data import (
    concepts,
    concepts_metadata,
    generation_models,
    evaluation_models,
    generation_templates_metadata,
    generation_templates,
    evaluation_templates_metadata,
    evaluation_templates
)
from daydreaming_dagster.assets.core import (
    content_combinations,
    generation_tasks,
    evaluation_tasks
)
from daydreaming_dagster.assets.partitions import (
    task_definitions
)
from daydreaming_dagster.resources.llm_client import LLMClientResource
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from daydreaming_dagster.resources.io_managers import (
    PartitionedTextIOManager,
    CSVIOManager,
    PartitionedConceptIOManager
)

defs = Definitions(
    assets=[
        # Raw data assets
        concepts,
        concepts_metadata,
        generation_models,
        evaluation_models,
        generation_templates_metadata,
        generation_templates,
        evaluation_templates_metadata,
        evaluation_templates,
        
        # Core processing assets
        content_combinations,
        generation_tasks,
        evaluation_tasks,
        
        # Partition management
        task_definitions,
        
        # LLM prompt and response assets
        generation_prompt,
        generation_response,
        evaluation_prompt,
        evaluation_response,
        
        # Results processing assets
        parsed_scores,
        final_results,
        perfect_score_paths
    ],
    resources={
        "openrouter_client": LLMClientResource(),
        "config": ExperimentConfig(),
        "csv_io_manager": CSVIOManager("data/02_tasks"),
        "partitioned_concept_io_manager": PartitionedConceptIOManager("data/02_tasks/concept_contents"),
        "generation_prompt_io_manager": PartitionedTextIOManager("data/03_generation/generation_prompts"),
        "generation_response_io_manager": PartitionedTextIOManager("data/03_generation/generation_responses"),
        "evaluation_prompt_io_manager": PartitionedTextIOManager("data/04_evaluation/evaluation_prompts"),
        "evaluation_response_io_manager": PartitionedTextIOManager("data/04_evaluation/evaluation_responses"),
        "error_log_io_manager": CSVIOManager("data/07_reporting"),
        "parsing_results_io_manager": CSVIOManager("data/05_parsing"),
        "summary_results_io_manager": CSVIOManager("data/06_summary")
    },
    executor=multiprocess_executor.configured({"max_concurrent": 4})
    # Note: General operations can run in parallel, but LLM calls are limited by concurrency tags
)

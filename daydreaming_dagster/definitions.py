from dagster import Definitions
from daydreaming_dagster.assets.llm_prompts_responses import (
    generation_prompt,
    generation_response,
    evaluation_prompt,
    evaluation_response
)
from daydreaming_dagster.assets.raw_data import (
    concepts_metadata,
    concept_descriptions_sentence,
    concept_descriptions_paragraph,
    concept_descriptions_article,
    generation_models,
    evaluation_models,
    generation_templates,
    evaluation_templates
)
from daydreaming_dagster.assets.core import (
    concepts_and_content,
    concept_contents,
    concept_combinations,
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
        concepts_metadata,
        concept_descriptions_sentence,
        concept_descriptions_paragraph,
        concept_descriptions_article,
        generation_models,
        evaluation_models,
        generation_templates,
        evaluation_templates,
        
        # Core processing assets
        concepts_and_content,
        concept_contents,
        concept_combinations,
        generation_tasks,
        evaluation_tasks,
        
        # Partition management
        task_definitions,
        
        # LLM prompt and response assets
        generation_prompt,
        generation_response,
        evaluation_prompt,
        evaluation_response
    ],
    resources={
        "openrouter_client": LLMClientResource(),
        "experiment_config": ExperimentConfig(),
        "csv_io_manager": CSVIOManager("data/02_tasks"),
        "partitioned_concept_io_manager": PartitionedConceptIOManager("data/02_tasks/concept_contents"),
        "generation_prompt_io_manager": PartitionedTextIOManager("data/03_generation/generation_prompts"),
        "generation_response_io_manager": PartitionedTextIOManager("data/03_generation/generation_responses"),
        "evaluation_prompt_io_manager": PartitionedTextIOManager("data/04_evaluation/evaluation_prompts"),
        "evaluation_response_io_manager": PartitionedTextIOManager("data/04_evaluation/evaluation_responses"),
        "error_log_io_manager": CSVIOManager("data/07_reporting"),
        "parsing_results_io_manager": CSVIOManager("data/05_parsing"),
        "summary_results_io_manager": CSVIOManager("data/06_summary")
    }
)

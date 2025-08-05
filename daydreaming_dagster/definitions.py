from dagster import Definitions, multiprocess_executor
from daydreaming_dagster.assets.llm_generation import (
    generation_prompt,
    generation_response
)
from daydreaming_dagster.assets.llm_evaluation import (
    evaluation_prompt,
    evaluation_response
)
from daydreaming_dagster.assets.results_processing import (
    parsed_scores
)
from daydreaming_dagster.assets.results_analysis import (
    evaluator_agreement_analysis,
    comprehensive_variance_analysis
)
from daydreaming_dagster.assets.results_summary import (
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
from daydreaming_dagster.resources.llm_client import LLMClientResource
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from daydreaming_dagster.resources.io_managers import (
    PartitionedTextIOManager,
    CSVIOManager
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
        
        
        # LLM prompt and response assets
        generation_prompt,
        generation_response,
        evaluation_prompt,
        evaluation_response,
        
        # Results processing assets
        parsed_scores,
        evaluator_agreement_analysis,
        comprehensive_variance_analysis,
        final_results,
        perfect_score_paths
    ],
    resources={
        "openrouter_client": LLMClientResource(),
        "config": ExperimentConfig(),
        "csv_io_manager": CSVIOManager("data/2_tasks"),
        "generation_prompt_io_manager": PartitionedTextIOManager("data/3_generation/generation_prompts"),
        "generation_response_io_manager": PartitionedTextIOManager("data/3_generation/generation_responses"),
        "evaluation_prompt_io_manager": PartitionedTextIOManager("data/4_evaluation/evaluation_prompts"),
        "evaluation_response_io_manager": PartitionedTextIOManager("data/4_evaluation/evaluation_responses"),
        "error_log_io_manager": CSVIOManager("data/7_reporting"),
        "parsing_results_io_manager": CSVIOManager("data/5_parsing"),
        "summary_results_io_manager": CSVIOManager("data/6_summary")
    },
    executor=multiprocess_executor.configured({"max_concurrent": 10})
    # Note: Pool concurrency limits are set via CLI: dagster instance concurrency set llm_api 1
)

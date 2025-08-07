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
    llm_models,
    generation_templates,
    generation_templates_metadata,
    evaluation_templates,
    evaluation_templates_metadata
)
from daydreaming_dagster.assets.core import (
    content_combinations,
    content_combinations_csv,
    generation_tasks,
    evaluation_tasks
)
from daydreaming_dagster.resources.llm_client import LLMClientResource
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from daydreaming_dagster.resources.io_managers import (
    PartitionedTextIOManager,
    CSVIOManager
)
from pathlib import Path

defs = Definitions(
    assets=[
        # Raw data assets (now load all data, no filtering)
        concepts_metadata,              # Loads ALL concepts metadata
        concepts,                       # Filters for active concepts
        llm_models,                     # Loads ALL models
        generation_templates,           # Loads ALL generation templates
        generation_templates_metadata,  # Loads ALL generation template metadata
        evaluation_templates,           # Loads ALL evaluation templates
        evaluation_templates_metadata,  # Loads ALL evaluation template metadata
        
        # Core processing assets
        content_combinations,
        content_combinations_csv,
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
        
        # Simple data root path - no complex configuration needed
        "data_root": "data",
        
        # Simplified I/O managers - no complex source mappings or filtering
        "csv_io_manager": CSVIOManager(base_path=Path("data") / "2_tasks"),
        "generation_prompt_io_manager": PartitionedTextIOManager(base_path=Path("data") / "3_generation" / "generation_prompts"),
        "generation_response_io_manager": PartitionedTextIOManager(base_path=Path("data") / "3_generation" / "generation_responses"),
        "evaluation_prompt_io_manager": PartitionedTextIOManager(base_path=Path("data") / "4_evaluation" / "evaluation_prompts"),
        "evaluation_response_io_manager": PartitionedTextIOManager(base_path=Path("data") / "4_evaluation" / "evaluation_responses"),
        "error_log_io_manager": CSVIOManager(base_path=Path("data") / "7_reporting"),
        "parsing_results_io_manager": CSVIOManager(base_path=Path("data") / "5_parsing"),
        "summary_results_io_manager": CSVIOManager(base_path=Path("data") / "6_summary")
    },
    executor=multiprocess_executor.configured({"max_concurrent": 10})
    # Note: Pool concurrency limits are set via CLI: dagster instance concurrency set llm_api 1
)

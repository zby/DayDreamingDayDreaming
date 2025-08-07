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
    generation_templates,
    evaluation_templates
)
from daydreaming_dagster.assets.core import (
    content_combinations,
    content_combinations_csv,
    generation_tasks,
    evaluation_tasks
)
from daydreaming_dagster.resources.llm_client import LLMClientResource
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from daydreaming_dagster.resources.data_paths_config import DataPathsConfig
from daydreaming_dagster.resources.io_managers import (
    PartitionedTextIOManager,
    CSVIOManager,
    TemplateIOManager
)

defs = Definitions(
    assets=[
        # Raw data assets (now use enhanced I/O managers)
        concepts_metadata,  # Now source of truth, loads from CSV
        concepts,           # Now depends on concepts_metadata
        generation_models,
        evaluation_models,
        
        # Template assets (single-stage loading)
        generation_templates,
        evaluation_templates,
        
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
        "data_paths_config": DataPathsConfig(),
        
        # Enhanced CSV I/O manager with source mappings - now uses DataPathsConfig
        "enhanced_csv_io_manager": CSVIOManager(
            base_path=DataPathsConfig().tasks_dir,
            source_mappings={
                # Raw data assets that load from absolute paths
                "concepts_metadata": {
                    "source_file": str(DataPathsConfig().concepts_metadata_csv),
                    "filters": [{"column": "active", "value": True}]
                },
                "generation_models": {
                    "source_file": str(DataPathsConfig().llm_models_csv),
                    "filters": [{"column": "for_generation", "value": True}]
                },
                "evaluation_models": {
                    "source_file": str(DataPathsConfig().llm_models_csv),
                    "filters": [{"column": "for_evaluation", "value": True}]
                },
            }
        ),
        
        # Template I/O managers - now uses DataPathsConfig
        "generation_template_io_manager": TemplateIOManager(data_paths_config=DataPathsConfig()),
        "evaluation_template_io_manager": TemplateIOManager(data_paths_config=DataPathsConfig()),
        
        # Other I/O managers - now uses explicit paths
        "csv_io_manager": CSVIOManager(base_path=DataPathsConfig().tasks_dir),
        "generation_prompt_io_manager": PartitionedTextIOManager(base_path=DataPathsConfig().generation_prompts_dir),
        "generation_response_io_manager": PartitionedTextIOManager(base_path=DataPathsConfig().generation_responses_dir),
        "evaluation_prompt_io_manager": PartitionedTextIOManager(base_path=DataPathsConfig().evaluation_prompts_dir),
        "evaluation_response_io_manager": PartitionedTextIOManager(base_path=DataPathsConfig().evaluation_responses_dir),
        "error_log_io_manager": CSVIOManager(base_path=DataPathsConfig().reporting_dir),
        "parsing_results_io_manager": CSVIOManager(base_path=DataPathsConfig().parsing_results_dir),
        "summary_results_io_manager": CSVIOManager(base_path=DataPathsConfig().summary_results_dir)
    },
    executor=multiprocess_executor.configured({"max_concurrent": 10})
    # Note: Pool concurrency limits are set via CLI: dagster instance concurrency set llm_api 1
)

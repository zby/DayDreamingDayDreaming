from dagster import Definitions, multiprocess_executor
import os
from daydreaming_dagster.assets.two_phase_generation import (
    draft_prompt,
    draft_response,
    essay_prompt,
    essay_response,
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
    perfect_score_paths,
    generation_scores_pivot,
    evaluation_model_template_pivot
)
from daydreaming_dagster.assets.raw_data import RAW_SOURCE_ASSETS, TASK_SOURCE_ASSETS
from daydreaming_dagster.schedules.raw_schedule import raw_schedule
from daydreaming_dagster.assets.core import (
    selected_combo_mappings,
    content_combinations,
    draft_generation_tasks,
    essay_generation_tasks,
    document_index,
    evaluation_tasks,
)
from daydreaming_dagster.assets.cross_experiment import (
    filtered_evaluation_results,
    template_version_comparison_pivot,
    draft_generation_results_append,
    essay_generation_results_append,
    evaluation_results_append
)
from daydreaming_dagster.resources.llm_client import LLMClientResource
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from daydreaming_dagster.resources.io_managers import (
    PartitionedTextIOManager,
    CSVIOManager,
    VersionedTextIOManager,
    InMemoryIOManager,
)
from daydreaming_dagster.resources.cross_experiment_io_manager import CrossExperimentIOManager
from pathlib import Path


# Responses and prompts are versioned; overwrite flags are not used.

defs = Definitions(
    assets=[
        # Core processing assets
        selected_combo_mappings,
        content_combinations,
        draft_generation_tasks,
        essay_generation_tasks,
        document_index,
        evaluation_tasks,
        
        # Two-phase generation assets
        draft_prompt,
        draft_response,
        essay_prompt,
        essay_response,
        
        evaluation_prompt,
        evaluation_response,
        
        # Results processing assets
        parsed_scores,
        evaluator_agreement_analysis,
        comprehensive_variance_analysis,
        generation_scores_pivot,
        evaluation_model_template_pivot,
        final_results,
        perfect_score_paths,
        
        # Cross-experiment analysis assets
        filtered_evaluation_results,
        template_version_comparison_pivot,
        
        # Auto-materializing results tracking assets
        draft_generation_results_append,
        essay_generation_results_append,
        evaluation_results_append,
        # Source assets (CSV-only)
        *RAW_SOURCE_ASSETS,
        *TASK_SOURCE_ASSETS,
    ],
    schedules=[raw_schedule],
    resources={
        "openrouter_client": LLMClientResource(),
        "experiment_config": ExperimentConfig(),
        
        # Infrastructure configuration
        "data_root": "data",
        
        # Simplified I/O managers - no complex source mappings or filtering
        "csv_io_manager": CSVIOManager(base_path=Path("data") / "2_tasks"),
        "in_memory_io_manager": InMemoryIOManager(),
        
        # Versioned I/O managers for prompts and responses (non-destructive)
        "draft_prompt_io_manager": VersionedTextIOManager(base_path=Path("data") / "3_generation" / "draft_prompts"),
        "draft_response_io_manager": VersionedTextIOManager(base_path=Path("data") / "3_generation" / "draft_responses"),
        "essay_prompt_io_manager": VersionedTextIOManager(base_path=Path("data") / "3_generation" / "essay_prompts"),
        "essay_response_io_manager": VersionedTextIOManager(base_path=Path("data") / "3_generation" / "essay_responses"),
        "evaluation_prompt_io_manager": VersionedTextIOManager(base_path=Path("data") / "4_evaluation" / "evaluation_prompts"),
        "evaluation_response_io_manager": VersionedTextIOManager(base_path=Path("data") / "4_evaluation" / "evaluation_responses"),
        "error_log_io_manager": CSVIOManager(base_path=Path("data") / "7_reporting"),
        "parsing_results_io_manager": CSVIOManager(base_path=Path("data") / "5_parsing"),
        "summary_results_io_manager": CSVIOManager(base_path=Path("data") / "6_summary"),
        "cross_experiment_io_manager": CrossExperimentIOManager()
    },
    executor=multiprocess_executor.configured({"max_concurrent": 10})
    # Note: Pool concurrency is configured via dagster_home/dagster.yaml
)

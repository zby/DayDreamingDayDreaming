from dagster import Definitions, multiprocess_executor, in_process_executor
import os
from daydreaming_dagster.assets.group_generation_draft import (
    draft_prompt,
    draft_response,
)
from daydreaming_dagster.assets.group_generation_essays import (
    essay_prompt,
    essay_response,
)
from daydreaming_dagster.assets.group_evaluation import (
    evaluation_prompt,
    evaluation_response,
)
from daydreaming_dagster.assets.group_results_processing import (
    parsed_scores,
)
from daydreaming_dagster.assets.results_analysis import (
    evaluator_agreement_analysis,
    comprehensive_variance_analysis
)
from daydreaming_dagster.assets.group_results_summary import (
    final_results,
    perfect_score_paths,
    generation_scores_pivot,
    evaluation_model_template_pivot,
)
from daydreaming_dagster.assets.documents_reporting import (
    documents_latest_report,
    documents_consistency_report,
)
from daydreaming_dagster.checks.documents_checks import (
    draft_files_exist_check,
    essay_files_exist_check,
    evaluation_files_exist_check,
    draft_db_row_present_check,
    essay_db_row_present_check,
    evaluation_db_row_present_check,
)
from daydreaming_dagster.assets.raw_data import RAW_SOURCE_ASSETS, TASK_SOURCE_ASSETS
from daydreaming_dagster.schedules.raw_schedule import raw_schedule
from daydreaming_dagster.assets.group_task_definitions import (
    selected_combo_mappings,
    content_combinations,
    draft_generation_tasks,
    essay_generation_tasks,
    document_index,
    evaluation_tasks,
)
from daydreaming_dagster.assets.group_cross_experiment import (
    filtered_evaluation_results,
    template_version_comparison_pivot,
    draft_generation_results_append,
    essay_generation_results_append,
    evaluation_results_append,
)
from daydreaming_dagster.resources.llm_client import LLMClientResource
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from daydreaming_dagster.resources.io_managers import (
    PartitionedTextIOManager,
    CSVIOManager,
    VersionedTextIOManager,
    InMemoryIOManager,
)
from daydreaming_dagster.resources.documents_index import DocumentsIndexResource
from pathlib import Path


# Responses and prompts are versioned; overwrite flags are not used.

# Use in-process executor when DD_IN_PROCESS=1 (helpful for CI/restricted envs).
EXECUTOR = (
    in_process_executor
    if os.environ.get("DD_IN_PROCESS") == "1"
    else multiprocess_executor.configured({"max_concurrent": 10})
)

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
        documents_latest_report,
        documents_consistency_report,
        # Source assets (CSV-only)
        *RAW_SOURCE_ASSETS,
        *TASK_SOURCE_ASSETS,
    ],
    asset_checks=[
        draft_files_exist_check,
        essay_files_exist_check,
        evaluation_files_exist_check,
        draft_db_row_present_check,
        essay_db_row_present_check,
        evaluation_db_row_present_check,
    ],
    schedules=[raw_schedule],
    resources={
        "openrouter_client": LLMClientResource(),
        "experiment_config": ExperimentConfig(),
        
        # Infrastructure configuration
        "data_root": "data",
        "documents_index": DocumentsIndexResource(),
        
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
        "cross_experiment_io_manager": CSVIOManager(base_path=Path("data") / "7_cross_experiment")
    },
    executor=EXECUTOR
    # Note: Pool concurrency is configured via dagster_home/dagster.yaml
)

from dagster import Definitions, multiprocess_executor, in_process_executor
import os
from daydreaming_dagster.assets.group_draft import (
    draft_prompt,
    draft_raw,
    draft_parsed,
)
from daydreaming_dagster.assets.group_essay import (
    essay_prompt,
    essay_raw,
    essay_parsed,
)
from daydreaming_dagster.assets.group_evaluation import (
    evaluation_prompt,
    evaluation_raw,
    evaluation_parsed,
)
from daydreaming_dagster.assets.results_processing import (
    cohort_aggregated_scores,
)
from daydreaming_dagster.assets.results_analysis import (
    evaluator_agreement_analysis,
    comprehensive_variance_analysis
)
from daydreaming_dagster.assets.results_summary import (
    final_results,
    perfect_score_paths,
    generation_scores_pivot,
    evaluation_model_template_pivot,
)
from daydreaming_dagster.assets.group_cohorts import (
    cohort_membership,
    register_cohort_partitions,
)
from daydreaming_dagster.assets.maintenance import (
    prune_dynamic_partitions,
)
from daydreaming_dagster.assets.documents_reporting import (
    documents_latest_report,
    documents_consistency_report,
)
from daydreaming_dagster.checks.documents_checks import (
    draft_files_exist_check,
    essay_files_exist_check,
    evaluation_files_exist_check,
)
from daydreaming_dagster.assets.raw_data import RAW_SOURCE_ASSETS
from daydreaming_dagster.schedules.raw_schedule import raw_schedule
from daydreaming_dagster.assets.group_cohorts import (
    cohort_id,
    selected_combo_mappings,
    content_combinations,
)
from daydreaming_dagster.assets.cross_experiment import (
    filtered_evaluation_results,
    template_version_comparison_pivot,
)
from daydreaming_dagster.resources.llm_client import LLMClientResource
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from daydreaming_dagster.resources.io_managers import (
    CSVIOManager,
    InMemoryIOManager,
)
from daydreaming_dagster.resources.gens_prompt_io_manager import GensPromptIOManager
from daydreaming_dagster.resources.scores_aggregator import ScoresAggregatorResource
from daydreaming_dagster.resources.membership_service import MembershipServiceResource
from pathlib import Path
from daydreaming_dagster.types import Stage


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
        cohort_id,
        cohort_membership,
        register_cohort_partitions,
        selected_combo_mappings,
        content_combinations,
        
        # Two-phase generation assets
        draft_prompt,
        draft_raw,
        draft_parsed,

        essay_prompt,
        essay_raw,
        essay_parsed,

        evaluation_prompt,
        evaluation_raw,
        evaluation_parsed,
        
        # Results processing assets
        cohort_aggregated_scores,
        evaluator_agreement_analysis,
        comprehensive_variance_analysis,
        generation_scores_pivot,
        evaluation_model_template_pivot,
        final_results,
        perfect_score_paths,
        
        # Cross-experiment analysis assets (derive views on demand; no auto-appenders)
        filtered_evaluation_results,
        template_version_comparison_pivot,
        documents_latest_report,
        documents_consistency_report,
        prune_dynamic_partitions,
        # Source assets (CSV-only)
        *RAW_SOURCE_ASSETS,
    ],
    asset_checks=[
        draft_files_exist_check,
        essay_files_exist_check,
        evaluation_files_exist_check,
    ],
    schedules=[raw_schedule],
    resources={
        "openrouter_client": LLMClientResource(),
        "experiment_config": ExperimentConfig(),
        
        # Infrastructure configuration
        "data_root": "data",
        
        # IO managers
        "csv_io_manager": CSVIOManager(base_path=Path("data") / "2_tasks"),
        "in_memory_io_manager": InMemoryIOManager(fallback_data_root=Path("data")),
        # Prompts persist to gens store; responses are written to gens store by assets
        "draft_prompt_io_manager": GensPromptIOManager(
            gens_root=Path("data") / "gens",
            stage="draft",
        ),
        "essay_prompt_io_manager": GensPromptIOManager(
            gens_root=Path("data") / "gens",
            stage="essay",
        ),
        "evaluation_prompt_io_manager": GensPromptIOManager(
            gens_root=Path("data") / "gens",
            stage="evaluation",
        ),
        # Responses: no need to persist via IO manager — assets write to the gens store
        # Use in-memory manager only if downstream assets in-process; tests may override
        "draft_response_io_manager": InMemoryIOManager(fallback_data_root=Path("data")),
        "essay_response_io_manager": InMemoryIOManager(fallback_data_root=Path("data")),
        "evaluation_response_io_manager": InMemoryIOManager(fallback_data_root=Path("data")),
        "error_log_io_manager": CSVIOManager(base_path=Path("data") / "7_reporting"),
        "parsing_results_io_manager": CSVIOManager(base_path=Path("data") / "5_parsing"),
        "summary_results_io_manager": CSVIOManager(base_path=Path("data") / "6_summary"),
        "cross_experiment_io_manager": CSVIOManager(base_path=Path("data") / "7_cross_experiment"),
        # Services for results processing (dependency-injected for testability)
        "scores_aggregator": ScoresAggregatorResource(),
        "membership_service": MembershipServiceResource(),
    },
    executor=EXECUTOR
    # Note: Pool concurrency is configured via dagster_home/dagster.yaml
)

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

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
from daydreaming_dagster.assets.maintenance import (
    prune_dynamic_partitions,
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
    cohort_membership,
    selected_combo_mappings,
    content_combinations,
    register_cohort_partitions,
)
from daydreaming_dagster.assets.cross_experiment import (
    filtered_evaluation_results,
    template_version_comparison_pivot,
)
from daydreaming_dagster.resources import CohortSpecResource
from daydreaming_dagster.resources.llm_client import LLMClientResource
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from daydreaming_dagster.resources.io_managers import (
    CSVIOManager,
    CohortCSVIOManager,
    InMemoryIOManager,
)
from daydreaming_dagster.resources.gens_prompt_io_manager import GensPromptIOManager
from daydreaming_dagster.resources.scores_aggregator import ScoresAggregatorResource
from daydreaming_dagster.resources.membership_service import MembershipServiceResource
from daydreaming_dagster.data_layer.paths import Paths, COHORT_REPORT_ASSET_TARGETS
from daydreaming_dagster.types import Stage, STAGES as STAGE_ORDER


@dataclass(frozen=True)
class StageEntry:
    """Stage registry entry describing canonical assets and resource factories."""

    assets: tuple[Any, ...]
    resource_factories: Mapping[str, Callable[[Paths], Any]]

    def build_resources(self, paths: Paths) -> dict[str, Any]:
        return {key: factory(paths) for key, factory in self.resource_factories.items()}


# Responses and prompts are versioned; overwrite flags are not used.


def _build_stage_registry() -> dict[Stage, StageEntry]:
    """Construct the canonical stage registry used to wire assets and resources."""

    def prompt_manager(stage: Stage) -> Callable[[Paths], Any]:
        return lambda paths: GensPromptIOManager(gens_root=paths.gens_root, stage=stage)

    def response_manager() -> Callable[[Paths], Any]:
        return lambda paths: InMemoryIOManager(fallback_data_root=paths.data_root)

    return {
        "draft": StageEntry(
            assets=(draft_prompt, draft_raw, draft_parsed),
            resource_factories={
                "draft_prompt_io_manager": prompt_manager("draft"),
                "draft_response_io_manager": response_manager(),
            },
        ),
        "essay": StageEntry(
            assets=(essay_prompt, essay_raw, essay_parsed),
            resource_factories={
                "essay_prompt_io_manager": prompt_manager("essay"),
                "essay_response_io_manager": response_manager(),
            },
        ),
        "evaluation": StageEntry(
            assets=(evaluation_prompt, evaluation_raw, evaluation_parsed),
            resource_factories={
                "evaluation_prompt_io_manager": prompt_manager("evaluation"),
                "evaluation_response_io_manager": response_manager(),
            },
        ),
    }


STAGES: dict[Stage, StageEntry] = _build_stage_registry()


# Use in-process executor when DD_IN_PROCESS=1 (helpful for CI/restricted envs).
EXECUTOR = (
    in_process_executor
    if os.environ.get("DD_IN_PROCESS") == "1"
    else multiprocess_executor.configured({"max_concurrent": 10})
)


def _stage_assets() -> list:
    assets: list[Any] = []
    for stage in STAGE_ORDER:
        assets.extend(STAGES[stage].assets)
    return assets


CORE_ASSETS = (
    cohort_id,
    selected_combo_mappings,
    content_combinations,
    cohort_membership,
    register_cohort_partitions,
)

RESULT_ASSETS = (
    cohort_aggregated_scores,
    evaluator_agreement_analysis,
    comprehensive_variance_analysis,
    generation_scores_pivot,
    evaluation_model_template_pivot,
    final_results,
    perfect_score_paths,
    filtered_evaluation_results,
    template_version_comparison_pivot,
    prune_dynamic_partitions,
)


def _default_paths() -> Paths:
    data_root = os.environ.get("DAYDREAMING_DATA_ROOT", "data")
    return Paths.from_str(data_root)


def _shared_resources(paths: Paths) -> dict[str, Any]:
    data_root_path = paths.data_root
    return {
        "openrouter_client": LLMClientResource(),
        "experiment_config": ExperimentConfig(),
        "data_root": str(data_root_path),
        "csv_io_manager": CSVIOManager(base_path=paths.tasks_dir),
        "in_memory_io_manager": InMemoryIOManager(fallback_data_root=data_root_path),
        "error_log_io_manager": CSVIOManager(base_path=data_root_path / "7_reporting"),
        "parsing_results_io_manager": CohortCSVIOManager(
            paths,
            default_category="parsing",
            asset_map=COHORT_REPORT_ASSET_TARGETS,
        ),
        "summary_results_io_manager": CohortCSVIOManager(
            paths,
            default_category="summary",
            asset_map=COHORT_REPORT_ASSET_TARGETS,
        ),
        "cross_experiment_io_manager": CSVIOManager(
            base_path=paths.cross_experiment_dir
        ),
        "scores_aggregator": ScoresAggregatorResource(),
        "membership_service": MembershipServiceResource(),
        "cohort_spec": CohortSpecResource(),
    }


def _stage_resources(paths: Paths) -> dict[str, Any]:
    resources: dict[str, Any] = {}
    for stage in STAGE_ORDER:
        resources.update(STAGES[stage].build_resources(paths))
    return resources


def build_definitions(*, paths: Paths | None = None) -> Definitions:
    resolved_paths = paths or _default_paths()

    assets = [
        *CORE_ASSETS,
        *_stage_assets(),
        *RESULT_ASSETS,
        *RAW_SOURCE_ASSETS,
    ]

    resources = {
        **_shared_resources(resolved_paths),
        **_stage_resources(resolved_paths),
    }

    return Definitions(
        assets=assets,
        asset_checks=[
            draft_files_exist_check,
            essay_files_exist_check,
            evaluation_files_exist_check,
        ],
        schedules=[raw_schedule],
        resources=resources,
        executor=EXECUTOR,
    )


defs = build_definitions()

from __future__ import annotations

"""Data-layer aware path helpers for gens store and related artifacts."""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple
import os

from ..utils.errors import DDError, Err

# Canonical gens-store filenames (single source of truth)
PROMPT_FILENAME = "prompt.txt"
RAW_FILENAME = "raw.txt"
PARSED_FILENAME = "parsed.txt"
METADATA_FILENAME = "metadata.json"
RAW_METADATA_FILENAME = "raw_metadata.json"
PARSED_METADATA_FILENAME = "parsed_metadata.json"
GEN_ARTIFACT_FILENAMES = (
    PROMPT_FILENAME,
    RAW_FILENAME,
    PARSED_FILENAME,
    METADATA_FILENAME,
)


@dataclass(frozen=True)
class Paths:
    """Project path helper bound to a data root."""

    data_root: Path

    def __post_init__(self):
        if self.data_root is None or str(self.data_root).strip() == "":
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "paths_missing_data_root"},
            )
        object.__setattr__(self, "data_root", Path(self.data_root))

    # --- Core directories ---
    @property
    def gens_root(self) -> Path:
        return self.data_root / "gens"

    @property
    def raw_dir(self) -> Path:
        return self.data_root / "1_raw"

    @property
    def tasks_dir(self) -> Path:
        return self.data_root / "2_tasks"

    @property
    def parsing_dir(self) -> Path:
        return self.data_root / "5_parsing"

    @property
    def summary_dir(self) -> Path:
        return self.data_root / "6_summary"

    @property
    def cross_experiment_dir(self) -> Path:
        return self.data_root / "7_cross_experiment"

    @property
    def cohorts_dir(self) -> Path:
        return self.data_root / "cohorts"

    # --- Cohort reports helpers ---
    def _normalize_cohort_id(self, cohort_id: str) -> str:
        if cohort_id is None:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "paths_missing_cohort_id"},
            )
        cid = str(cohort_id).strip()
        if not cid:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "paths_missing_cohort_id"},
            )
        return cid

    def cohort_report_root(self, cohort_id: str) -> Path:
        return self.cohort_dir(self._normalize_cohort_id(cohort_id)) / "reports"

    def _cohort_report_category_dir(self, cohort_id: str, category: str) -> Path:
        cid = self._normalize_cohort_id(cohort_id)
        normalized = str(category).strip().lower()
        if normalized not in {"parsing", "summary", "analysis"}:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "paths_invalid_cohort_report_category",
                    "category": category,
                },
            )
        return self.cohort_report_root(cid) / normalized

    def cohort_report_path(self, cohort_id: str, category: str, name: str) -> Path:
        if not name or str(name).strip() == "":
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "reason": "paths_missing_cohort_report_name",
                    "category": category,
                },
            )
        return self._cohort_report_category_dir(cohort_id, category) / str(name)

    def cohort_parsing_csv(self, cohort_id: str, filename: str) -> Path:
        return self.cohort_report_path(cohort_id, "parsing", filename)

    def cohort_summary_csv(self, cohort_id: str, filename: str) -> Path:
        return self.cohort_report_path(cohort_id, "summary", filename)

    def cohort_analysis_csv(self, cohort_id: str, filename: str) -> Path:
        return self.cohort_report_path(cohort_id, "analysis", filename)

    @property
    def combo_mappings_csv(self) -> Path:
        return self.data_root / "combo_mappings.csv"

    # --- Gens store helpers ---
    def generation_dir(self, stage: str, gen_id: str) -> Path:
        return self.gens_root / str(stage) / str(gen_id)

    def prompt_path(self, stage: str, gen_id: str) -> Path:
        return self.generation_dir(stage, gen_id) / PROMPT_FILENAME

    def input_path(self, stage: str, gen_id: str) -> Path:
        return self.prompt_path(stage, gen_id)

    def raw_path(self, stage: str, gen_id: str) -> Path:
        return self.generation_dir(stage, gen_id) / RAW_FILENAME

    def parsed_path(self, stage: str, gen_id: str) -> Path:
        return self.generation_dir(stage, gen_id) / PARSED_FILENAME

    def metadata_path(self, stage: str, gen_id: str) -> Path:
        return self.generation_dir(stage, gen_id) / METADATA_FILENAME

    def raw_metadata_path(self, stage: str, gen_id: str) -> Path:
        return self.generation_dir(stage, gen_id) / RAW_METADATA_FILENAME

    def parsed_metadata_path(self, stage: str, gen_id: str) -> Path:
        return self.generation_dir(stage, gen_id) / PARSED_METADATA_FILENAME

    # --- Templates ---
    def templates_root(self) -> Path:
        env = os.environ.get("GEN_TEMPLATES_ROOT")
        return Path(env) if env else (self.raw_dir / "templates")

    def template_dir(self, stage: str) -> Path:
        return self.templates_root() / str(stage)

    def template_file(self, stage: str, template_id: str) -> Path:
        return self.template_dir(stage) / f"{template_id}.txt"

    # --- Raw CSVs ---
    @property
    def concepts_csv(self) -> Path:
        return self.raw_dir / "concepts_metadata.csv"

    @property
    def llm_models_csv(self) -> Path:
        return self.raw_dir / "llm_models.csv"

    def stage_templates_csv(self, stage: str) -> Path:
        return self.raw_dir / f"{stage}_templates.csv"

    # --- Cohorts ---
    def cohort_dir(self, cohort_id: str) -> Path:
        return self.cohorts_dir / str(cohort_id)

    def cohort_membership_csv(self, cohort_id: str) -> Path:
        return self.cohort_dir(cohort_id) / "membership.csv"

    def cohort_manifest_json(self, cohort_id: str) -> Path:
        return self.cohort_dir(cohort_id) / "manifest.json"

    # --- Processed outputs ---
    def evaluation_scores_normalized_csv(self, cohort_id: str) -> Path:
        return self.cohort_summary_csv(cohort_id, "evaluation_scores.csv")

    # --- Constructors ---
    @classmethod
    def from_context(cls, context) -> "Paths":
        resources = getattr(context, "resources", None)
        if resources is None or not hasattr(resources, "data_root"):
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "paths_requires_data_root_resource"},
            )
        return cls(Path(getattr(resources, "data_root")))

    @classmethod
    def from_str(cls, data_root: str | os.PathLike[str]) -> "Paths":
        return cls(Path(data_root))


COHORT_REPORT_ASSET_TARGETS: Dict[str, Tuple[str, str]] = {
    "cohort_aggregated_scores": ("parsing", "aggregated_scores.csv"),
    "generation_scores_pivot": ("summary", "generation_scores.csv"),
    "final_results": ("summary", "final_results.csv"),
    "perfect_score_paths": ("summary", "perfect_score_paths.csv"),
    "evaluation_model_template_pivot": ("summary", "evaluation_model_template_pivot.csv"),
    "evaluator_agreement_analysis": ("analysis", "evaluator_agreement.csv"),
    "comprehensive_variance_analysis": ("analysis", "variance_analysis.csv"),
}


__all__ = [
    "Paths",
    "PROMPT_FILENAME",
    "RAW_FILENAME",
    "PARSED_FILENAME",
    "METADATA_FILENAME",
    "RAW_METADATA_FILENAME",
    "PARSED_METADATA_FILENAME",
    "GEN_ARTIFACT_FILENAMES",
    "COHORT_REPORT_ASSET_TARGETS",
]

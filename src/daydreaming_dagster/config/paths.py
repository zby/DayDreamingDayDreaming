from __future__ import annotations

"""
Centralized path and storage conventions.

This module is the single source of truth for where project artifacts live on disk.
It mirrors and replaces scattered string concatenations across the codebase.

Conventions encapsulated here include:
- data root and well-known subdirectories
- gens store layout: data/gens/<stage>/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}
- templates layout with GEN_TEMPLATES_ROOT override
- raw CSV locations and cohort files
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import os

# Canonical gens-store filenames (single source of truth)
PROMPT_FILENAME = "prompt.txt"
RAW_FILENAME = "raw.txt"
PARSED_FILENAME = "parsed.txt"
METADATA_FILENAME = "metadata.json"


@dataclass(frozen=True)
class Paths:
    """Project path helper bound to a data root.

    Prefer constructing via `Paths.from_context(context)` inside assets to ensure
    consistency with Dagster-provided resources.
    """

    data_root: Path

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

    @property
    def combo_mappings_csv(self) -> Path:
        return self.data_root / "combo_mappings.csv"

    # --- Gens store helpers ---
    def generation_dir(self, stage: str, gen_id: str) -> Path:
        """Directory holding all files for a single generation.

        Layout: data/gens/<stage>/<gen_id>/
        """
        return self.gens_root / str(stage) / str(gen_id)

    def prompt_path(self, stage: str, gen_id: str) -> Path:
        return self.generation_dir(stage, gen_id) / PROMPT_FILENAME

    def raw_path(self, stage: str, gen_id: str) -> Path:
        return self.generation_dir(stage, gen_id) / RAW_FILENAME

    def parsed_path(self, stage: str, gen_id: str) -> Path:
        return self.generation_dir(stage, gen_id) / PARSED_FILENAME

    def metadata_path(self, stage: str, gen_id: str) -> Path:
        return self.generation_dir(stage, gen_id) / METADATA_FILENAME

    # --- Templates ---
    def templates_root(self) -> Path:
        """Root of Jinja templates. Honors GEN_TEMPLATES_ROOT override.

        Default: data/1_raw/templates
        """
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

    @property
    def selected_combo_mappings_csv(self) -> Path:
        return self.tasks_dir / "selected_combo_mappings.csv"

    # --- Cohorts ---
    def cohort_dir(self, cohort_id: str) -> Path:
        return self.cohorts_dir / str(cohort_id)

    def cohort_membership_csv(self, cohort_id: str) -> Path:
        return self.cohort_dir(cohort_id) / "membership.csv"

    def cohort_manifest_json(self, cohort_id: str) -> Path:
        return self.cohort_dir(cohort_id) / "manifest.json"

    # --- Processed outputs ---
    @property
    def aggregated_scores_csv(self) -> Path:
        return self.parsing_dir / "aggregated_scores.csv"

    # --- Constructors ---
    @classmethod
    def from_context(cls, context) -> "Paths":
        dr = Path(getattr(getattr(context, "resources", object()), "data_root", "data"))
        return cls(dr)

    @classmethod
    def from_str(cls, data_root: str | os.PathLike[str]) -> "Paths":
        return cls(Path(data_root))


__all__ = [
    "Paths",
    # Filename constants (for rare direct filename needs; prefer Paths methods)
    "PROMPT_FILENAME",
    "RAW_FILENAME",
    "PARSED_FILENAME",
    "METADATA_FILENAME",
]

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from .paths import Paths
from daydreaming_dagster.utils.errors import DDError, Err
from daydreaming_dagster.utils.ids import (
    DETERMINISTIC_GEN_IDS_ENABLED,
    compute_collision_resolved_gen_id,
    draft_signature,
    evaluation_signature,
    essay_signature,
    reserve_gen_id,
)


class GensDataLayer:
    """Minimal data-layer facade around gens store filesystem operations."""

    def __init__(self, data_root: Path):
        if data_root is None or str(data_root).strip() == "":
            raise DDError(Err.INVALID_CONFIG, ctx={"field": "data_root", "reason": "missing"})
        self.data_root = Path(data_root)
        self._paths = Paths.from_str(self.data_root)

    @classmethod
    def from_root(cls, data_root: Path | str) -> "GensDataLayer":
        return cls(Path(data_root))

    @property
    def paths(self) -> Paths:
        return self._paths

    def reserve_generation(self, stage: str, gen_id: str, *, create: bool = True) -> Path:
        target = self._paths.generation_dir(stage, gen_id)
        if create:
            target.mkdir(parents=True, exist_ok=True)
        return target

    def reserve_draft_id(
        self,
        *,
        combo_id: str,
        template_id: str,
        llm_model_id: str,
        cohort_id: str,
        replicate: int | str,
    ) -> str:
        """Return a deterministic draft gen_id for the provided combination."""

        signature = draft_signature(combo_id, template_id, llm_model_id, replicate)
        if DETERMINISTIC_GEN_IDS_ENABLED:
            return compute_collision_resolved_gen_id("draft", signature, self._paths.gens_root)

        replicate_index = signature[3]
        salt = None if replicate_index == 1 else f"rep{replicate_index}"
        task_id = f"{combo_id}__{template_id}__{llm_model_id}"
        return reserve_gen_id("draft", task_id, run_id=cohort_id, salt=salt)

    def reserve_essay_id(
        self,
        *,
        draft_gen_id: str,
        template_id: str,
        cohort_id: str,
        replicate: int | str,
    ) -> str:
        """Return a deterministic essay gen_id tied to a parent draft."""

        signature = essay_signature(draft_gen_id, template_id, replicate)
        if DETERMINISTIC_GEN_IDS_ENABLED:
            return compute_collision_resolved_gen_id("essay", signature, self._paths.gens_root)

        replicate_index = signature[2]
        salt = None if replicate_index == 1 else f"rep{replicate_index}"
        task_id = f"{draft_gen_id}__{template_id}"
        return reserve_gen_id("essay", task_id, run_id=cohort_id, salt=salt)

    def reserve_evaluation_id(
        self,
        *,
        essay_gen_id: str,
        template_id: str,
        llm_model_id: str,
        cohort_id: str,
        replicate: int | str,
    ) -> str:
        """Return a deterministic evaluation gen_id tied to an essay."""

        signature = evaluation_signature(essay_gen_id, template_id, llm_model_id, replicate)
        if DETERMINISTIC_GEN_IDS_ENABLED:
            return compute_collision_resolved_gen_id(
                "evaluation", signature, self._paths.gens_root
            )

        replicate_index = signature[3]
        salt = None if replicate_index == 1 else f"rep{replicate_index}"
        task_id = f"{essay_gen_id}__{template_id}__{llm_model_id}"
        return reserve_gen_id("evaluation", task_id, run_id=cohort_id, salt=salt)

    def write_input(self, stage: str, gen_id: str, text: str) -> Path:
        target = self._paths.input_path(stage, gen_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            target.write_text(str(text or ""), encoding="utf-8")
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc
        return target

    def read_input(self, stage: str, gen_id: str) -> str:
        target = self._paths.input_path(stage, gen_id)
        if not target.exists():
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "artifact": "input",
                    "path": str(target),
                },
            )
        try:
            return target.read_text(encoding="utf-8")
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc

    def write_raw(self, stage: str, gen_id: str, text: str) -> Path:
        target = self._paths.raw_path(stage, gen_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            target.write_text(str(text or ""), encoding="utf-8")
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc
        return target

    def write_parsed(self, stage: str, gen_id: str, text: str) -> Path:
        target = self._paths.parsed_path(stage, gen_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            target.write_text(str(text or ""), encoding="utf-8")
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc
        return target

    def write_main_metadata(self, stage: str, gen_id: str, metadata: Dict[str, Any]) -> Path:
        target = self._paths.metadata_path(stage, gen_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            target.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc
        return target

    def write_raw_metadata(self, stage: str, gen_id: str, metadata: Dict[str, Any]) -> Path:
        target = self._paths.raw_metadata_path(stage, gen_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            target.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc
        return target

    def write_parsed_metadata(self, stage: str, gen_id: str, metadata: Dict[str, Any]) -> Path:
        target = self._paths.parsed_metadata_path(stage, gen_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            target.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc
        return target

    def read_parsed(self, stage: str, gen_id: str) -> str:
        target = self._paths.parsed_path(stage, gen_id)
        if not target.exists():
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "artifact": "parsed",
                    "path": str(target),
                },
            )
        try:
            return target.read_text(encoding="utf-8")
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc

    def parsed_exists(self, stage: str, gen_id: str) -> bool:
        """Check if parsed.txt exists for the given stage and gen_id."""
        target = self._paths.parsed_path(stage, gen_id)
        return target.exists()

    def raw_exists(self, stage: str, gen_id: str) -> bool:
        """Check if raw.txt exists for the given stage and gen_id."""
        target = self._paths.raw_path(stage, gen_id)
        return target.exists()

    def raw_metadata_exists(self, stage: str, gen_id: str) -> bool:
        """Check if raw_metadata.json exists for the given stage and gen_id."""
        target = self._paths.raw_metadata_path(stage, gen_id)
        return target.exists()

    def read_raw(self, stage: str, gen_id: str) -> str:
        target = self._paths.raw_path(stage, gen_id)
        if not target.exists():
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "artifact": "raw",
                    "path": str(target),
                },
            )
        try:
            return target.read_text(encoding="utf-8")
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc

    def read_main_metadata(self, stage: str, gen_id: str) -> Dict[str, Any]:
        target = self._paths.metadata_path(stage, gen_id)
        if not target.exists():
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "artifact": "metadata",
                    "path": str(target),
                },
            )
        try:
            return json.loads(target.read_text(encoding="utf-8"))
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc
        except json.JSONDecodeError as exc:
            raise DDError(
                Err.PARSER_FAILURE,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "artifact": "metadata",
                    "path": str(target),
                },
                cause=exc,
            )

    def parsed_path(self, stage: str, gen_id: str) -> Path:
        return self._paths.parsed_path(stage, gen_id)

    def read_raw_metadata(self, stage: str, gen_id: str) -> Dict[str, Any]:
        target = self._paths.raw_metadata_path(stage, gen_id)
        if not target.exists():
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "artifact": "raw_metadata",
                    "path": str(target),
                },
            )
        try:
            return json.loads(target.read_text(encoding="utf-8"))
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc
        except json.JSONDecodeError as exc:
            raise DDError(
                Err.PARSER_FAILURE,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "artifact": "raw_metadata",
                    "path": str(target),
                },
                cause=exc,
            )

    def read_parsed_metadata(self, stage: str, gen_id: str) -> Dict[str, Any]:
        target = self._paths.parsed_metadata_path(stage, gen_id)
        if not target.exists():
            raise DDError(
                Err.DATA_MISSING,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "artifact": "parsed_metadata",
                    "path": str(target),
                },
            )
        try:
            return json.loads(target.read_text(encoding="utf-8"))
        except OSError as exc:
            raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc
        except json.JSONDecodeError as exc:
            raise DDError(
                Err.PARSER_FAILURE,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "artifact": "parsed_metadata",
                    "path": str(target),
                },
                cause=exc,
            )


@dataclass(frozen=True)
class GenerationMetadata:
    stage: str
    template_id: str
    parent_gen_id: str | None
    mode: str  # "llm" or "copy"
    combo_id: str | None
    origin_cohort_id: str | None


def resolve_generation_metadata(
    layer: GensDataLayer,
    stage: str,
    gen_id: str,
) -> GenerationMetadata:
    meta = layer.read_main_metadata(stage, gen_id)
    template_id = str(meta.get("template_id") or "").strip()
    mode = str(meta.get("mode") or "llm").strip().lower() or "llm"
    parent_gen_raw = str(meta.get("parent_gen_id") or "").strip() or None

    parent_required = stage in ("essay", "evaluation") or mode == "copy"
    if parent_required and not parent_gen_raw:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "stage": stage,
                "gen_id": gen_id,
                "mode": mode,
                "reason": "missing_parent",
            },
        )

    combo_val = meta.get("combo_id")
    cohort_val = meta.get("origin_cohort_id")
    return GenerationMetadata(
        stage=stage,
        template_id=template_id,
        parent_gen_id=parent_gen_raw,
        mode=mode,
        combo_id=str(combo_val).strip() if combo_val is not None else None,
        origin_cohort_id=str(cohort_val).strip() if cohort_val is not None else None,
    )


__all__ = [
    "GensDataLayer",
    "GenerationMetadata",
    "resolve_generation_metadata",
]

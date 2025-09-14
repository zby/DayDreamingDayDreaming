from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

from .stage_core import Stage
from .stage_policy import StageSpec


@dataclass
class GenerationEnvelope:
    stage: Stage
    gen_id: str
    template_id: str
    parent_gen_id: Optional[str]
    llm_model_id: Optional[str]
    mode: Literal["llm", "copy"]

    def validate(self, spec: StageSpec) -> None:
        if not (isinstance(self.template_id, str) and self.template_id.strip()):
            raise ValueError("template_id is required")
        # Parent required for child stages
        if spec.parent_stage is not None:
            if not (isinstance(self.parent_gen_id, str) and self.parent_gen_id.strip()):
                raise ValueError(f"parent_gen_id is required for stage '{self.stage}'")
        # Copy support
        if self.mode == "copy" and not spec.supports_copy_response:
            raise ValueError(f"Copy mode is unsupported for stage '{self.stage}'")
        # LLM model required when not copy and spec requires it
        if self.mode != "copy" and "llm_model_id" in spec.response_fields:
            if not (isinstance(self.llm_model_id, str) and self.llm_model_id.strip()):
                raise ValueError(f"llm_model_id is required for stage '{self.stage}'")

    def to_metadata_base(self) -> dict:
        md = {
            "stage": self.stage,
            "gen_id": str(self.gen_id),
            "template_id": str(self.template_id),
            "llm_model_id": str(self.llm_model_id) if self.llm_model_id else None,
            "mode": self.mode,
        }
        if isinstance(self.parent_gen_id, str) and self.parent_gen_id:
            md["parent_gen_id"] = str(self.parent_gen_id)
        return md


__all__ = ["GenerationEnvelope"]


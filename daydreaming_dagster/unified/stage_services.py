from __future__ import annotations

"""
Unified facade for stage functionality.

This module re-exports stage primitives, execution helpers, and asset-style
entrypoints from the dedicated submodules:

- stage_core: core types and execution/rendering utilities
- stage_prompts: prompt asset entrypoint
- stage_responses: response asset entrypoints (draft/essay/evaluation)
- stage_policy: stage-specific policy helpers (not re-exported by default)

Keeping this as a thin facade preserves the long-standing import path
`daydreaming_dagster.unified.stage_services` while allowing clearer internal
organization.
"""

from .stage_core import (
    Stage,
    ExecutionResult,
    LLMClientProto,
    ExecutionResultLike,
    render_template,
    generate_llm,
    resolve_parser_name,
    parse_text,
    write_generation,
    execute_copy,
    execute_llm,
)
from .stage_prompts import prompt_asset
from .stage_responses import (
    response_asset,
    essay_response_asset,
    evaluation_response_asset,
    draft_response_asset,
)

__all__ = [
    "Stage",
    "ExecutionResult",
    "LLMClientProto",
    "ExecutionResultLike",
    "render_template",
    "generate_llm",
    "resolve_parser_name",
    "parse_text",
    "write_generation",
    "execute_copy",
    "execute_llm",
    "prompt_asset",
    "response_asset",
    "essay_response_asset",
    "evaluation_response_asset",
    "draft_response_asset",
]


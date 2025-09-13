from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Tuple
import os
import time

from jinja2 import Environment, StrictUndefined
from dagster import Failure, MetadataValue

from daydreaming_dagster.utils.generation import Generation
from daydreaming_dagster.constants import DRAFT
# Evaluation parser config is now read uniformly via read_templates() in resolve_parser_name

# Types
Stage = Literal["draft", "essay", "evaluation"]


@dataclass
class ExecutionResult:
    prompt_text: Optional[str]
    raw_text: Optional[str]
    parsed_text: Optional[str]
    info: Optional[Dict[str, Any]]
    metadata: Dict[str, Any]


class LLMClientProto:
    def generate_with_info(
        self, prompt: str, *, model: str, max_tokens: Optional[int] = None
    ) -> Tuple[str, Dict[str, Any]]:  # pragma: no cover - protocol only
        raise NotImplementedError


ExecutionResultLike = ExecutionResult | Dict[str, Any]


# Module-level Jinja env (StrictUndefined) to mimic existing behavior
_JINJA = Environment(undefined=StrictUndefined)


def _templates_root(default: Optional[Path] = None) -> Path:
    root = default or Path(os.environ.get("GEN_TEMPLATES_ROOT", "data/1_raw/templates"))
    return Path(root)


def render_template(
    stage: Stage,
    template_id: str,
    values: Dict[str, Any],
    *,
    templates_root: Optional[Path] = None,
) -> str:
    base = _templates_root(templates_root) / stage
    path = base / f"{template_id}.txt"
    if not path.exists():
        raise FileNotFoundError(f"Template not found: {path}")
    tpl = _JINJA.from_string(path.read_text(encoding="utf-8"))
    return tpl.render(**values)


def generate_llm(
    llm: LLMClientProto,
    prompt: str,
    *,
    model: str,
    max_tokens: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    raw_text, info = llm.generate_with_info(prompt, model=model, max_tokens=max_tokens)
    normalized = str(raw_text).replace("\r\n", "\n")
    return normalized, info or {}


def _validate_min_lines(stage: Stage, raw_text: str, min_lines: Optional[int]) -> None:
    """Enforce a minimum number of non-empty lines across stages.

    Writes are performed before this check; on failure a ValueError is raised,
    keeping raw/metadata for debuggability and omitting parsed.txt.
    """
    if isinstance(min_lines, int) and min_lines > 0:
        response_lines = [ln for ln in str(raw_text).split("\n") if ln.strip()]
        if len(response_lines) < min_lines:
            raise ValueError(
                f"{str(stage).capitalize()} validation failed: only {len(response_lines)} non-empty lines, minimum required {min_lines}"
            )


def resolve_parser_name(
    data_root: Path,
    stage: Stage,
    template_id: str,
    provided: Optional[str] = None,
) -> Optional[str]:
    """Resolve parser for any stage by reading its templates CSV.

    Always consults data/1_raw/<stage>_templates.csv and returns the 'parser' value
    for the matching template_id. Returns None on any issue (missing file/column/id).
    The `provided` argument is ignored for unified behavior and kept for backcompat.
    """
    try:
        from daydreaming_dagster.utils.raw_readers import read_templates

        df = read_templates(Path(data_root), str(stage), filter_active=False)
        if df.empty or "parser" not in df.columns:
            return None
        row = df[df["template_id"].astype(str) == str(template_id)]
        if row.empty:
            return None
        val = row.iloc[0].get("parser")
        if val is None:
            return None
        s = str(val).strip()
        return s or None
    except Exception:
        return None


def parse_text(stage: Stage, raw_text: str, parser_name: Optional[str]) -> Optional[str]:
    """Unified parsing helper for all stages.

    - Returns None when parser_name is missing/empty or parser not found.
    - Catches parser exceptions and returns None for best-effort behavior.
    """
    if not (isinstance(parser_name, str) and parser_name.strip()):
        return None
    try:
        from daydreaming_dagster.utils.parser_registry import get_parser

        parser = get_parser(stage, parser_name)
        if parser is None:
            return None
        return parser(str(raw_text))
    except Exception:
        return None


def execute_llm(
    *,
    stage: Stage,
    llm: LLMClientProto,
    root_dir: Path,
    gen_id: str,
    template_id: str,
    prompt_text: str,
    model: str,
    max_tokens: Optional[int],
    min_lines: Optional[int] = None,
    fail_on_truncation: bool = True,
    parent_gen_id: Optional[str] = None,
    metadata_extra: Optional[Dict[str, Any]] = None,
) -> ExecutionResult:
    """Unified LLM execution for draft/essay/evaluation.

    Differences by stage:
    - draft: parent_gen_id optional; parser best-effort; parsed may be None.
    - essay: parent_gen_id required; parser resolved from CSV (typically 'identity'); parsed falls back to raw.
    - evaluation: parent_gen_id required; parser required; parsed must exist.
    """
    if stage in ("essay", "evaluation"):
        if not (isinstance(parent_gen_id, str) and parent_gen_id.strip()):
            raise ValueError("parent_gen_id is required for essay and evaluation stages")

    t0 = time.time()
    raw_text, info = generate_llm(llm, prompt_text, model=model, max_tokens=max_tokens)

    # Resolve parser uniformly from templates; default to identity hint for essay
    default_hint = "identity" if stage == "essay" else None
    parser_name = resolve_parser_name(Path(root_dir), stage, template_id, default_hint)
    if stage == "evaluation":
        if not (isinstance(parser_name, str) and parser_name.strip()):
            raise ValueError("parser_name is required for evaluation stage")

    # Parse via registry
    print(f"Using parser '{parser_name}' for stage '{stage}'")
    parsed = parse_text(stage, raw_text, parser_name)
    print(f"Parsed text (first 100 chars): {str(parsed)[:100]!r}")
    if stage == "essay" and not isinstance(parsed, str):
        parsed = str(raw_text)

    out_dir = Path(root_dir) / "gens"
    base = out_dir / str(stage) / str(gen_id)

    # Base metadata
    meta: Dict[str, Any] = _base_meta(
        stage=stage,
        gen_id=str(gen_id),
        template_id=template_id,
        model=model,
        parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
        mode="llm",
    )
    meta["files"] = {"raw": str((base / "raw.txt").resolve())}
    meta.update(
        {
            "parser_name": parser_name,
            "finish_reason": (info or {}).get("finish_reason") if isinstance(info, dict) else None,
            "truncated": bool((info or {}).get("truncated")) if isinstance(info, dict) else False,
            "usage": (info or {}).get("usage") if isinstance(info, dict) else None,
            "duration_s": round(time.time() - t0, 3),
        }
    )
    _merge_extras(meta, metadata_extra)

    # First write: raw + metadata
    write_generation(
        out_dir=out_dir,
        stage=stage,
        gen_id=str(gen_id),
        parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
        prompt_text=prompt_text,
        raw_text=raw_text,
        parsed_text=parsed,
        metadata=meta,
        write_raw=True,
        write_parsed=False,
        write_prompt=False,
        write_metadata=True,
    )

    # Validations
    _validate_min_lines(stage, raw_text, min_lines)
    if bool(fail_on_truncation) and isinstance(info, dict) and info.get("truncated"):
        raise ValueError("LLM response appears truncated (finish_reason=length or max_tokens hit)")

    # Success: write parsed if available
    if isinstance(parsed, str):
        meta["files"]["parsed"] = str((base / "parsed.txt").resolve())
        write_generation(
            out_dir=out_dir,
            stage=stage,
            gen_id=str(gen_id),
            parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
            prompt_text=None,
            raw_text=None,
            parsed_text=parsed,
            metadata=meta,
            write_raw=False,
            write_parsed=True,
            write_prompt=False,
            write_metadata=False,
        )

    return ExecutionResult(prompt_text=prompt_text, raw_text=raw_text, parsed_text=parsed, info=info, metadata=meta)


# parse_draft/parse_evaluation removed in favor of unified parse_text


def write_generation(
    *,
    out_dir: Path,
    stage: Stage,
    gen_id: str,
    parent_gen_id: Optional[str],
    prompt_text: Optional[str],
    raw_text: Optional[str],
    parsed_text: Optional[str],
    metadata: Dict[str, Any],
    write_raw: bool,
    write_parsed: bool,
    write_prompt: bool,
    write_metadata: bool,
) -> Path:
    gen = Generation(
        stage=stage,
        gen_id=str(gen_id),
        parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
        raw_text=str(raw_text or ""),
        parsed_text=str(parsed_text) if isinstance(parsed_text, str) else None,
        prompt_text=str(prompt_text) if isinstance(prompt_text, str) else None,
        metadata=metadata,
    )
    return gen.write_files(
        out_dir,
        write_raw=write_raw,
        write_parsed=write_parsed,
        write_prompt=write_prompt,
        write_metadata=write_metadata,
    )


def _base_meta(
    *,
    stage: Stage,
    gen_id: str,
    template_id: str,
    model: Optional[str],
    parent_gen_id: Optional[str] = None,
    mode: Literal["llm", "copy"] = "llm",
) -> Dict[str, Any]:
    return {
        "stage": stage,
        "gen_id": gen_id,
        "template_id": template_id,
        "llm_model_id": model,
        **({"parent_gen_id": parent_gen_id} if parent_gen_id else {}),
        "parser_name": None,
        "mode": mode,
        "files": {},
        "finish_reason": None,
        "truncated": False,
        "usage": None,
        "duration_s": None,
    }


def _merge_extras(meta: Dict[str, Any], extras: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if extras:
        for k, v in extras.items():
            if k not in meta:
                meta[k] = v
    return meta


def execute_copy(
    *,
    out_dir: Path,
    stage: Stage,
    gen_id: str,
    template_id: str,
    parent_gen_id: str,
    pass_through_from: Path,
    metadata_extra: Optional[Dict[str, Any]] = None,
) -> ExecutionResult:
    t0 = time.time()
    src = Path(pass_through_from)
    parsed = src.read_text(encoding="utf-8") if src.exists() else ""
    meta = _base_meta(
        stage=stage, gen_id=str(gen_id), template_id=template_id, model=None, parent_gen_id=str(parent_gen_id), mode="copy"
    )
    base = Path(out_dir) / str(stage) / str(gen_id)
    meta["files"] = {"parsed": str((base / "parsed.txt").resolve())}
    meta["duration_s"] = round(time.time() - t0, 3)
    _merge_extras(meta, metadata_extra)
    write_generation(
        out_dir=out_dir,
        stage=stage,
        gen_id=str(gen_id),
        parent_gen_id=str(parent_gen_id),
        prompt_text=None,
        raw_text=None,
        parsed_text=parsed,
        metadata=meta,
        write_raw=False,
        write_parsed=True,
        write_prompt=False,
        write_metadata=True,
    )
    return ExecutionResult(prompt_text=None, raw_text=None, parsed_text=parsed, info=None, metadata=meta)








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
    "response_asset",
]


def prompt_asset(context, stage: Stage, *, content_combinations=None) -> str:
    """Unified prompt entrypoint across stages.

    - Resolves membership and generator mode
    - Short-circuits copy mode with sentinel
    - Loads parent text (essay/evaluation) and renders the template
    - Emits standardized metadata including template_id, mode, prompt_length
    """
    gen_id = context.partition_key
    from daydreaming_dagster.assets._helpers import (
        require_membership_row,
        resolve_generator_mode,
        load_parent_parsed_text,
        build_prompt_metadata,
    )

    data_root = Path(getattr(context.resources, "data_root", "data"))

    required = {
        "draft": ["template_id", "combo_id"],
        "essay": ["template_id", "parent_gen_id"],
        "evaluation": ["template_id", "parent_gen_id"],
    }
    if stage not in required:
        raise Failure(description=f"Unsupported stage for prompt: {stage}")

    row, _cohort = require_membership_row(context, stage, str(gen_id), require_columns=required[stage])
    template_id = str(row.get("template_id") or "").strip()
    mode = resolve_generator_mode(kind=stage, data_root=data_root, template_id=template_id)

    # Copy-mode sentinel metadata
    if mode == "copy":
        extras = {}
        if stage in ("essay", "evaluation"):
            extras["parent_gen_id"] = str(row.get("parent_gen_id") or "")
        if stage == "draft":
            extras["combo_id"] = str(row.get("combo_id") or "")
        context.add_output_metadata(
            build_prompt_metadata(
                context,
                stage=stage,
                gen_id=str(gen_id),
                template_id=template_id,
                mode=mode,
                parent_gen_id=str(row.get("parent_gen_id") or "") if stage != "draft" else None,
                prompt_text=None,
                extras=extras,
            )
        )
        return "COPY_MODE: no prompt needed"

    # Values for template rendering per stage
    values = {}
    extras = {}
    parent_gen_id = None
    if stage == "draft":
        if content_combinations is None:
            raise Failure(
                description="content_combinations is required for draft prompts",
                metadata={
                    "function": MetadataValue.text("draft_prompt"),
                    "gen_id": MetadataValue.text(str(gen_id)),
                    "resolution": MetadataValue.text(
                        "Pass curated content_combinations to draft_prompt asset"
                    ),
                },
            )
        combo_id = str(row.get("combo_id") or "")
        content_combination = next((c for c in content_combinations if getattr(c, "combo_id", None) == combo_id), None)
        if content_combination is None:
            raise Failure(
                description=f"Content combination '{combo_id}' not found in combinations database",
                metadata={
                    "function": MetadataValue.text("draft_prompt"),
                    "combo_id": MetadataValue.text(combo_id),
                    "total_combinations": MetadataValue.int(len(content_combinations) if content_combinations else 0),
                },
            )
        values = {"concepts": content_combination.contents}
        extras["combo_id"] = combo_id
    elif stage == "essay":
        parent_gen_id, parent_text = load_parent_parsed_text(
            context, stage, str(gen_id), failure_fn_name="essay_prompt"
        )
        values = {"draft_block": parent_text, "links_block": parent_text}
        extras["draft_line_count"] = sum(1 for ln in parent_text.splitlines() if ln.strip())
    else:  # evaluation
        parent_gen_id, parent_text = load_parent_parsed_text(
            context, stage, str(gen_id), failure_fn_name="evaluation_prompt"
        )
        values = {"response": parent_text}

    # Render using shared renderer
    try:
        prompt = render_template(stage, template_id, values)
    except FileNotFoundError as e:
        raise Failure(
            description=f"{stage.capitalize()} template '{template_id}' not found",
            metadata={
                "function": MetadataValue.text(f"{stage}_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "template_id": MetadataValue.text(template_id),
                "error": MetadataValue.text(str(e)),
            },
        )
    except Exception as e:
        templates_root = Path(os.environ.get("GEN_TEMPLATES_ROOT", "data/1_raw/templates"))
        template_path = templates_root / stage / f"{template_id}.txt"
        raise Failure(
            description=f"Error rendering {stage} template '{template_id}'",
            metadata={
                "function": MetadataValue.text(f"{stage}_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "template_id": MetadataValue.text(template_id),
                "template_path": MetadataValue.path(str(template_path)),
                "jinja_message": MetadataValue.text(str(e)),
            },
        ) from e

    # Emit standardized metadata and return
    context.add_output_metadata(
        build_prompt_metadata(
            context,
            stage=stage,
            gen_id=str(gen_id),
            template_id=template_id,
            mode="llm",
            parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
            prompt_text=prompt,
            extras=extras,
        )
    )
    return prompt


def response_asset(context, prompt_text, stage: Stage) -> str:
    """Unified response asset for all stages.

    Handles stage-specific behavior (copy vs llm for essay, truncation/min-lines for draft,
    required columns, and metadata) while delegating execution to execute_copy/execute_llm.
    """
    gen_id = context.partition_key
    if not isinstance(prompt_text, str) or not prompt_text.strip():
        raise Failure(
            description=f"Upstream {stage}_prompt is missing or empty",
            metadata={
                "function": MetadataValue.text("response_asset"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text(
                    f"Ensure {stage}_prompt is materialized and wired as a dependency"
                ),
            },
        )

    # Lazy imports to avoid circular dependencies during module import
    from daydreaming_dagster.assets._helpers import (
        require_membership_row,
        load_generation_parsed_text,
        resolve_generator_mode,
        emit_standard_output_metadata,
        get_run_id,
    )

    data_root = Path(getattr(context.resources, "data_root", "data"))

    # Resolve membership row for all stages upfront with stage-specific requirements
    required_by_stage = {
        "essay": ["template_id", "parent_gen_id"],
        "evaluation": ["llm_model_id", "template_id", "parent_gen_id"],
        "draft": ["llm_model_id"],
    }
    if stage not in required_by_stage:  # pragma: no cover - defensive
        raise Failure(description=f"Unsupported stage: {stage}")
    row, cohort = require_membership_row(
        context,
        stage,
        str(gen_id),
        require_columns=required_by_stage[stage],
    )
    # Compute template id used for mode resolution uniformly from template_id
    template_id = str(row.get("template_id") or "")
    mode = resolve_generator_mode(kind=stage, data_root=data_root, template_id=template_id)
    # If not copy mode, require an LLM model across all stages
    model_id = str(row.get("llm_model_id") or "").strip()
    parent_gen_id = str(row.get("parent_gen_id") or "").strip() if "parent_gen_id" in row.index else None
    # Early copy path: pass-through from parent stage where applicable
    if mode == "copy":
        parent_map = {"essay": DRAFT, "evaluation": "essay"}
        pstage = parent_map.get(stage)
        if not pstage:
            raise Failure(
                description=f"Copy mode is unsupported for stage '{stage}'",
                metadata={
                    "function": MetadataValue.text(f"{stage}_response"),
                    "gen_id": MetadataValue.text(str(gen_id)),
                    "resolution": MetadataValue.text("Use generator=llm for draft or provide a parent_gen_id for pass-through"),
                },
            )
        if not parent_gen_id:
            raise Failure(
                description=f"Copy mode requires parent_gen_id for stage '{stage}'",
                metadata={
                    "function": MetadataValue.text(f"{stage}_response"),
                    "gen_id": MetadataValue.text(str(gen_id)),
                    "resolution": MetadataValue.text("Ensure cohort membership has a valid parent_gen_id"),
                },
            )
        result = execute_copy(
            out_dir=data_root / "gens",
            stage=stage,
            gen_id=str(gen_id),
            template_id=template_id,
            parent_gen_id=str(parent_gen_id),
            pass_through_from=(data_root / "gens" / pstage / str(parent_gen_id) / "parsed.txt"),
            metadata_extra={
                "function": f"{stage}_response",
                "run_id": get_run_id(context),
            },
        )
        emit_standard_output_metadata(
            context,
            function=f"{stage}_response",
            gen_id=str(gen_id),
            result=result,
            extras={"mode": "copy", "parent_gen_id": str(parent_gen_id)},
        )
        return result.parsed_text or ""
    elif not model_id:
        raise Failure(
            description=f"Missing generation model for {stage} task",
            metadata={
                "function": MetadataValue.text(f"{stage}_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text(
                    f"Ensure cohort membership includes an llm_model_id for this {stage}"
                ),
            },
        )
    # Tokens/min-lines via constructed keys
    exp_cfg = getattr(context.resources, "experiment_config", object())
    max_tokens_key = "evaluation_max_tokens" if stage == "evaluation" else f"{stage}_generation_max_tokens"
    max_tokens = getattr(exp_cfg, max_tokens_key, None)
    min_lines = int(getattr(exp_cfg, "min_draft_lines", 3)) if stage == "draft" else None

    result = execute_llm(
        stage=stage,
        llm=context.resources.openrouter_client,
        root_dir=data_root,
        gen_id=str(gen_id),
        template_id=template_id,
        prompt_text=str(prompt_text),
        model=model_id,
        max_tokens=max_tokens,
        min_lines=min_lines,
        parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
        metadata_extra={
            "function": f"{stage}_response",
            "run_id": get_run_id(context),
        },
    )
    emit_standard_output_metadata(
        context,
        function=f"{stage}_response",
        gen_id=str(gen_id),
        result=result,
    )
    context.log.info(f"Generated {stage} response for gen {gen_id}")
    # Uniform return shape across stages: raw model output
    return result.raw_text or ""

def essay_response_asset(context, essay_prompt) -> str:
    return response_asset(context, essay_prompt, "essay")


# Export new entrypoints
__all__ += ["prompt_asset", "essay_response_asset"]


# --------------------
# Asset-style entrypoints for Evaluation stage
# --------------------

def evaluation_prompt_asset(context) -> str:
    return prompt_asset(context, "evaluation")


def evaluation_response_asset(context, evaluation_prompt) -> str:
    return response_asset(context, evaluation_prompt, "evaluation")


__all__ += ["evaluation_prompt_asset", "evaluation_response_asset"]


# --------------------
# Asset-style entrypoints for Draft stage
# --------------------

def draft_prompt_asset(context, content_combinations) -> str:
    return prompt_asset(context, "draft", content_combinations=content_combinations)


def draft_response_asset(context, draft_prompt) -> str:
    return response_asset(context, draft_prompt, "draft")


__all__ += ["draft_prompt_asset", "draft_response_asset"]

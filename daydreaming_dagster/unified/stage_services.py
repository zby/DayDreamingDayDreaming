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
from daydreaming_dagster.utils.evaluation_parsing_config import (
    load_parser_map,
    require_parser_for_template,
)

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


def resolve_parser_name(
    data_root: Path,
    stage: Stage,
    template_id: str,
    provided: Optional[str] = None,
) -> Optional[str]:
    """Resolve a parser name for a given stage/template.

    Behavior:
    - If `provided` is a non-empty string, return it.
    - For stage "draft": best-effort CSV fallback from draft_templates.csv (returns None on any issue).
    - For stage "evaluation": best-effort lookup via evaluation_templates.csv using evaluation_parsing_config.
    - For other stages: return None.
    """
    if isinstance(provided, str) and provided.strip():
        return provided.strip()
    try:
        if stage == "draft":
            from daydreaming_dagster.utils.raw_readers import read_templates

            df = read_templates(Path(data_root), "draft", filter_active=False)
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
        if stage == "evaluation":
            # Best-effort: use evaluation_parsing_config; swallow errors and return None
            from daydreaming_dagster.utils.evaluation_parsing_config import (
                load_parser_map,
                require_parser_for_template,
            )

            parser_map = load_parser_map(Path(data_root))
            return require_parser_for_template(str(template_id), parser_map)
    except Exception:
        return None
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


def execute_draft_llm(
    *,
    llm: LLMClientProto,
    root_dir: Path,
    gen_id: str,
    template_id: str,
    prompt_text: str,
    model: str,
    max_tokens: Optional[int],
    min_lines: Optional[int],
    fail_on_truncation: bool = True,
    parser_name: Optional[str] = None,
    parent_gen_id: Optional[str] = None,
    metadata_extra: Optional[Dict[str, Any]] = None,
) -> ExecutionResult:
    t0 = time.time()
    # Call LLM
    raw_text, info = generate_llm(llm, prompt_text, model=model, max_tokens=max_tokens)

    # Resolve effective parser name via CSV fallback when not provided
    effective_parser = resolve_parser_name(Path(root_dir), "draft", template_id, parser_name)

    # Parse draft (best-effort)
    parsed = parse_text("draft", raw_text, effective_parser)

    # Build metadata early
    out_dir = Path(root_dir) / "gens"
    base = out_dir / "draft" / str(gen_id)
    meta: Dict[str, Any] = _base_meta(
        stage="draft",
        gen_id=str(gen_id),
        template_id=template_id,
        model=model,
        parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
        mode="llm",
    )
    meta["files"] = {
        "raw": str((base / "raw.txt").resolve()),
    }
    meta.update(
        {
            "parser_name": effective_parser,
            "finish_reason": (info or {}).get("finish_reason") if isinstance(info, dict) else None,
            "truncated": bool((info or {}).get("truncated")) if isinstance(info, dict) else False,
            "usage": (info or {}).get("usage") if isinstance(info, dict) else None,
            "duration_s": round(time.time() - t0, 3),
        }
    )
    _merge_extras(meta, metadata_extra)

    # First write: prompt/raw/metadata for debuggability
    write_generation(
        out_dir=out_dir,
        stage="draft",
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

    # Validations after raw write
    if isinstance(min_lines, int) and min_lines > 0:
        response_lines = [ln for ln in str(raw_text).split("\n") if ln.strip()]
        if len(response_lines) < min_lines:
            raise ValueError(
                f"Draft validation failed: only {len(response_lines)} non-empty lines, minimum required {min_lines}"
            )
    if bool(fail_on_truncation) and isinstance(info, dict) and info.get("truncated"):
        raise ValueError("LLM response appears truncated (finish_reason=length or max_tokens hit)")

    # Success: write parsed if available
    if isinstance(parsed, str):
        meta["files"]["parsed"] = str((base / "parsed.txt").resolve())
        write_generation(
            out_dir=out_dir,
            stage="draft",
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

    return ExecutionResult(
        prompt_text=prompt_text, raw_text=raw_text, parsed_text=parsed, info=info, metadata=meta
    )


def execute_essay_llm(
    *,
    llm: LLMClientProto,
    root_dir: Path,
    gen_id: str,
    template_id: str,
    prompt_text: str,
    model: str,
    max_tokens: Optional[int],
    min_lines: Optional[int] = None,
    parent_gen_id: str,
    metadata_extra: Optional[Dict[str, Any]] = None,
) -> ExecutionResult:
    t0 = time.time()
    raw_text, info = generate_llm(llm, prompt_text, model=model, max_tokens=max_tokens)
    # Essay identity parse via unified parse_text for consistency
    parsed = parse_text("essay", raw_text, "identity") or str(raw_text)

    out_dir = Path(root_dir) / "gens"
    base = out_dir / "essay" / str(gen_id)
    meta: Dict[str, Any] = _base_meta(
        stage="essay", gen_id=str(gen_id), template_id=template_id, model=model, parent_gen_id=str(parent_gen_id), mode="llm"
    )
    meta["files"] = {
        "raw": str((base / "raw.txt").resolve()),
    }
    meta.update(
        {
            "finish_reason": (info or {}).get("finish_reason") if isinstance(info, dict) else None,
            "truncated": bool((info or {}).get("truncated")) if isinstance(info, dict) else False,
            "usage": (info or {}).get("usage") if isinstance(info, dict) else None,
            "duration_s": round(time.time() - t0, 3),
        }
    )
    _merge_extras(meta, metadata_extra)
    # First write: prompt/raw/metadata for debuggability
    write_generation(
        out_dir=out_dir,
        stage="essay",
        gen_id=str(gen_id),
        parent_gen_id=str(parent_gen_id),
        prompt_text=prompt_text,
        raw_text=raw_text,
        parsed_text=None,
        metadata=meta,
        write_raw=True,
        write_parsed=False,
        write_prompt=False,
        write_metadata=True,
    )

    # Validations after raw write (like draft)
    if isinstance(min_lines, int) and min_lines > 0:
        response_lines = [ln for ln in str(raw_text).split("\n") if ln.strip()]
        if len(response_lines) < min_lines:
            raise ValueError(
                f"Essay validation failed: only {len(response_lines)} non-empty lines, minimum required {min_lines}"
            )

    # Success: write parsed identity if available
    if isinstance(parsed, str):
        meta["files"]["parsed"] = str((base / "parsed.txt").resolve())
        write_generation(
            out_dir=out_dir,
            stage="essay",
            gen_id=str(gen_id),
            parent_gen_id=str(parent_gen_id),
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


def execute_evaluation_llm(
    *,
    llm: LLMClientProto,
    root_dir: Path,
    gen_id: str,
    template_id: str,
    prompt_text: str,
    model: str,
    max_tokens: Optional[int],
    min_lines: Optional[int] = None,
    parent_gen_id: str,
    metadata_extra: Optional[Dict[str, Any]] = None,
) -> ExecutionResult:
    # Resolve parser via unified resolver; require a valid name for evaluation.
    parser_name = resolve_parser_name(Path(root_dir), "evaluation", template_id, None)
    if not (isinstance(parser_name, str) and parser_name.strip()):
        raise ValueError("parser_name is required for evaluation stage")
    t0 = time.time()
    raw_text, info = generate_llm(llm, prompt_text, model=model, max_tokens=max_tokens)
    parsed = parse_text("evaluation", raw_text, parser_name)

    out_dir = Path(root_dir) / "gens"
    base = out_dir / "evaluation" / str(gen_id)
    meta: Dict[str, Any] = _base_meta(
        stage="evaluation",
        gen_id=str(gen_id),
        template_id=template_id,
        model=model,
        parent_gen_id=str(parent_gen_id),
        mode="llm",
    )
    meta["files"] = {
        "raw": str((base / "raw.txt").resolve()),
    }
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
    # First write: prompt/raw/metadata for debuggability
    write_generation(
        out_dir=out_dir,
        stage="evaluation",
        gen_id=str(gen_id),
        parent_gen_id=str(parent_gen_id),
        prompt_text=prompt_text,
        raw_text=raw_text,
        parsed_text=None,
        metadata=meta,
        write_raw=True,
        write_parsed=False,
        write_prompt=False,
        write_metadata=True,
    )

    # Validations after raw write (like draft)
    if isinstance(min_lines, int) and min_lines > 0:
        response_lines = [ln for ln in str(raw_text).split("\n") if ln.strip()]
        if len(response_lines) < min_lines:
            raise ValueError(
                f"Evaluation validation failed: only {len(response_lines)} non-empty lines, minimum required {min_lines}"
            )

    # Success: write parsed if available
    if isinstance(parsed, str):
        meta["files"]["parsed"] = str((base / "parsed.txt").resolve())
        write_generation(
            out_dir=out_dir,
            stage="evaluation",
            gen_id=str(gen_id),
            parent_gen_id=str(parent_gen_id),
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
    "execute_draft_llm",
    "execute_essay_llm",
    "execute_evaluation_llm",
]


# --------------------
# Asset-style entrypoints for Essay stage
# --------------------

def essay_prompt_asset(context) -> str:
    """
    Asset-compatible prompt entrypoint for the essay stage.
    Mirrors behavior of the previous asset implementation without writing files here
    (IO manager handles persistence for prompts). Emits the same Dagster metadata.
    """
    gen_id = context.partition_key
    # Validate membership and required fields
    # Import helpers lazily to avoid circular imports during module initialization
    from daydreaming_dagster.assets._helpers import (
        require_membership_row,
        load_generation_parsed_text,
        resolve_essay_generator_mode,
    )

    row, _cohort = require_membership_row(context, "essay", str(gen_id), require_columns=["template_id", "parent_gen_id"])
    template_name = str(row.get("template_id") or row.get("essay_template") or "")
    parent_gen_id = row.get("parent_gen_id")

    # Resolve generator mode
    generator_mode = resolve_essay_generator_mode(Path(context.resources.data_root), template_name)
    if generator_mode == "copy":
        context.add_output_metadata(
            {
                "function": MetadataValue.text("essay_prompt"),
                "mode": MetadataValue.text(generator_mode),
                "essay_template": MetadataValue.text(template_name),
            }
        )
        return "COPY_MODE: no prompt needed"

    # Load upstream draft text strictly via parent_gen_id
    if not (isinstance(parent_gen_id, str) and parent_gen_id.strip()):
        raise Failure(
            description="Missing parent_doc_id for essay doc",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure essay row exists in cohort membership with a valid parent_gen_id"),
            },
        )
    draft_text = load_generation_parsed_text(context, "draft", str(parent_gen_id), failure_fn_name="essay_prompt")
    used_source = "draft_gens_parent"
    draft_lines = [line.strip() for line in draft_text.split("\n") if line.strip()]
    min_lines = int(context.resources.experiment_config.min_draft_lines)
    if len(draft_lines) < max(1, min_lines):
        data_root = Path(getattr(context.resources, "data_root", "data"))
        draft_dir = data_root / "gens" / DRAFT / str(parent_gen_id)
        raise Failure(
            description="Upstream draft text is empty/too short for essay prompt",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                "draft_line_count": MetadataValue.int(len(draft_lines)),
                "min_required_lines": MetadataValue.int(min_lines),
                "draft_gen_dir": MetadataValue.path(str(draft_dir)),
            },
        )

    try:
        prompt = render_template("essay", template_name, {"draft_block": draft_text, "links_block": draft_text})
    except FileNotFoundError as e:
        raise Failure(
            description=f"Essay template '{template_name}' not found",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "essay_template": MetadataValue.text(template_name),
                "error": MetadataValue.text(str(e)),
            },
        )
    except Exception as e:
        raise Failure(
            description=f"Error rendering essay template '{template_name}'",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "essay_template": MetadataValue.text(template_name),
                "jinja_message": MetadataValue.text(str(e)),
            },
        )

    context.add_output_metadata(
        {
            "function": MetadataValue.text("essay_prompt"),
            "gen_id": MetadataValue.text(str(gen_id)),
            "essay_template": MetadataValue.text(template_name),
            "draft_line_count": MetadataValue.int(len(draft_lines)),
            "phase1_source": MetadataValue.text(used_source),
        }
    )
    return prompt


def essay_response_asset(context, essay_prompt) -> str:
    """
    Asset-compatible response entrypoint for the essay stage.
    Delegates to execute_copy/execute_essay_llm and mirrors the asset logic/metadata.
    """
    gen_id = context.partition_key
    # Import helpers lazily to avoid circular imports during module initialization
    from daydreaming_dagster.assets._helpers import (
        require_membership_row,
        load_generation_parsed_text,
        resolve_essay_generator_mode,
        emit_standard_output_metadata,
        get_run_id,
    )

    row, _cohort = require_membership_row(context, "essay", str(gen_id), require_columns=["template_id", "parent_gen_id"])
    template_name = str(row.get("template_id") or row.get("essay_template") or "")
    parent_gen_id = row.get("parent_gen_id")

    # Determine mode based on prompt or CSV
    if isinstance(essay_prompt, str) and essay_prompt.strip().upper().startswith("COPY_MODE"):
        mode = "copy"
    else:
        mode = resolve_essay_generator_mode(Path(context.resources.data_root), template_name)

    # Load upstream draft text
    if not (isinstance(parent_gen_id, str) and parent_gen_id.strip()):
        raise Failure(
            description="Missing parent_gen_id for essay doc",
            metadata={
                "function": MetadataValue.text("_essay_response_impl"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure essay row exists in cohort membership with a valid parent_gen_id"),
            },
        )
    draft_text = load_generation_parsed_text(context, "draft", str(parent_gen_id), failure_fn_name="_essay_response_impl")

    # Enforce min lines
    min_lines = int(context.resources.experiment_config.min_draft_lines)
    dlines = [line.strip() for line in str(draft_text).split("\n") if line.strip()]
    if len(dlines) < max(1, min_lines):
        data_root = Path(getattr(context.resources, "data_root", "data"))
        draft_dir = data_root / "gens" / DRAFT / str(parent_gen_id)
        raise Failure(
            description="Upstream draft text is empty/too short for essay generation",
            metadata={
                "function": MetadataValue.text("_essay_response_impl"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                "draft_line_count": MetadataValue.int(len(dlines)),
                "min_required_lines": MetadataValue.int(min_lines),
                "draft_gen_dir": MetadataValue.path(str(draft_dir)),
            },
        )

    values = {"draft_block": draft_text, "links_block": draft_text}
    data_root = Path(getattr(context.resources, "data_root", "data"))

    if mode == "copy":
        result = execute_copy(
            out_dir=data_root / "gens",
            stage="essay",
            gen_id=str(gen_id),
            template_id=template_name,
            parent_gen_id=str(parent_gen_id),
            pass_through_from=(data_root / "gens" / DRAFT / str(parent_gen_id) / "parsed.txt"),
            metadata_extra={
                "function": "essay_response",
                "run_id": get_run_id(context),
            },
        )
        emit_standard_output_metadata(
            context,
            function="essay_response",
            gen_id=str(gen_id),
            result=result,
            extras={"mode": "copy", "parent_gen_id": str(parent_gen_id)},
        )
        return result.parsed_text or draft_text

    # LLM path
    model_id = str(row.get("llm_model_id") or "").strip()
    if not model_id:
        raise Failure(
            description="Missing generation model for essay task",
            metadata={
                "function": MetadataValue.text("_essay_response_impl"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure cohort membership includes an llm_model_id for this essay"),
            },
        )
    result = execute_essay_llm(
        llm=context.resources.openrouter_client,
        root_dir=data_root,
        gen_id=str(gen_id),
        template_id=template_name,
        prompt_text=str(essay_prompt) if isinstance(essay_prompt, str) else render_template("essay", template_name, values),
        model=model_id,
        max_tokens=getattr(context.resources.experiment_config, "essay_generation_max_tokens", None),
        min_lines=None,
        parent_gen_id=str(parent_gen_id),
        metadata_extra={
            "function": "essay_response",
            "run_id": get_run_id(context),
        },
    )
    emit_standard_output_metadata(
        context,
        function="essay_response",
        gen_id=str(gen_id),
        result=result,
    )
    return result.raw_text or ""


# Export new entrypoints
__all__ += [
    "essay_prompt_asset",
    "essay_response_asset",
]


# --------------------
# Asset-style entrypoints for Evaluation stage
# --------------------

def evaluation_prompt_asset(context) -> str:
    """Asset-compatible evaluation prompt generation.

    Mirrors existing asset behavior: loads parent essay text, renders template,
    and emits output metadata. Does not write files here; the IO manager handles
    persistence for prompts.
    """
    gen_id = context.partition_key
    from pathlib import Path as _Path
    from dagster import Failure as _Failure, MetadataValue as _MV
    # Lazy imports to avoid circular imports at module import time
    from daydreaming_dagster.utils.membership_lookup import find_membership_row_by_gen
    from daydreaming_dagster.assets._helpers import load_generation_parsed_text

    data_root = _Path(getattr(context.resources, "data_root", "data"))
    mrow, _cohort = find_membership_row_by_gen(data_root, "evaluation", str(gen_id))
    parent_gen_id = mrow.get("parent_gen_id") if mrow is not None else None
    evaluation_template = mrow.get("template_id") if mrow is not None else None
    if not (isinstance(parent_gen_id, str) and parent_gen_id.strip()):
        raise _Failure(
            description="Missing parent_gen_id for evaluation task",
            metadata={
                "function": _MV.text("evaluation_prompt"),
                "gen_id": _MV.text(str(gen_id)),
                "resolution": _MV.text("Ensure evaluation row exists in cohort membership with a valid parent_gen_id"),
            },
        )
    # Load target essay parsed text via shared helper (emits helpful Failure metadata)
    doc_text = load_generation_parsed_text(context, "essay", str(parent_gen_id), failure_fn_name="evaluation_prompt")
    used_source = "essay_gens"

    # Render via shared stage_services (StrictUndefined semantics preserved)
    try:
        eval_prompt = render_template("evaluation", str(evaluation_template), {"response": doc_text})
    except FileNotFoundError as e:
        raise _Failure(
            description=f"Evaluation template '{evaluation_template}' not found",
            metadata={
                "function": _MV.text("evaluation_prompt"),
                "gen_id": _MV.text(str(gen_id)),
                "template_used": _MV.text(str(evaluation_template)),
                "error": _MV.text(str(e)),
            },
        )
    except Exception as e:
        raise _Failure(
            description=f"Error rendering evaluation template '{evaluation_template}'",
            metadata={
                "function": _MV.text("evaluation_prompt"),
                "gen_id": _MV.text(str(gen_id)),
                "template_used": _MV.text(str(evaluation_template)),
                "jinja_message": _MV.text(str(e)),
            },
        )
    context.add_output_metadata(
        {
            "gen_id": _MV.text(str(gen_id)),
            "parent_gen_id": _MV.text(str(parent_gen_id)),
            "document_content_length": _MV.int(len(doc_text or "")),
            "evaluation_prompt_length": _MV.int(len(eval_prompt)),
            "source_used": _MV.text(used_source or ""),
            "template_used": _MV.text(str(evaluation_template) if evaluation_template else ""),
        }
    )
    return eval_prompt


def evaluation_response_asset(context, evaluation_prompt) -> str:
    """Asset-compatible evaluation response execution.

    Delegates to execute_evaluation_llm with internal parser resolution; emits
    the same output metadata as the previous asset.
    """
    gen_id = context.partition_key
    from pathlib import Path as _Path
    from daydreaming_dagster.assets._helpers import (
        require_membership_row,
        load_generation_parsed_text,
        emit_standard_output_metadata,
        get_run_id,
    )

    data_root = _Path(getattr(context.resources, "data_root", "data"))
    row, _cohort = require_membership_row(
        context,
        "evaluation",
        str(gen_id),
        require_columns=["llm_model_id", "template_id", "parent_gen_id"],
    )
    model_name = str(row.get("llm_model_id") or "").strip()
    evaluation_template = str(row.get("template_id") or "").strip()
    parent_gen_id = str(row.get("parent_gen_id") or "").strip()

    # Execute via stage_services; prefer prompt text passed from dependency
    result = execute_evaluation_llm(
        llm=context.resources.openrouter_client,
        root_dir=data_root,
        gen_id=str(gen_id),
        template_id=evaluation_template,
        prompt_text=str(evaluation_prompt)
        if isinstance(evaluation_prompt, str)
        else render_template(
            "evaluation",
            evaluation_template,
            {"response": load_generation_parsed_text(context, "essay", parent_gen_id, failure_fn_name="evaluation_response")},
        ),
        model=model_name,
        max_tokens=getattr(context.resources.experiment_config, "evaluation_max_tokens", None),
        min_lines=None,
        parent_gen_id=parent_gen_id,
        metadata_extra={
            "function": "evaluation_response",
            "run_id": get_run_id(context),
        },
    )

    # Emit standardized metadata for Dagster UI
    emit_standard_output_metadata(context, function="evaluation_response", gen_id=str(gen_id), result=result)
    context.log.info(f"Generated evaluation response for gen {gen_id}")

    return result.raw_text or ""


__all__ += [
    "evaluation_prompt_asset",
    "evaluation_response_asset",
]


# --------------------
# Asset-style entrypoints for Draft stage
# --------------------

def draft_prompt_asset(context, content_combinations) -> str:
    """Asset-compatible draft prompt generation.

    Mirrors existing asset behavior: resolves membership row, locates the
    content combination, renders the draft template, returns prompt text, and
    logs context info. IO manager persists the prompt, no file writes here.
    """
    gen_id = context.partition_key
    from pathlib import Path as _Path
    from dagster import Failure as _Failure, MetadataValue as _MV
    from daydreaming_dagster.utils.membership_lookup import find_membership_row_by_gen

    # Read membership to resolve combo/template
    data_root = _Path(getattr(context.resources, "data_root", "data"))
    row, _cohort = find_membership_row_by_gen(data_root, "draft", str(gen_id))
    if row is None:
        raise _Failure(
            description="Cohort membership row not found for draft gen_id",
            metadata={
                "function": _MV.text("draft_prompt"),
                "gen_id": _MV.text(str(gen_id)),
                "resolution": _MV.text(
                    "Materialize cohort_id,cohort_membership to register this gen_id; use that partition key"
                ),
            },
        )
    combo_id = str(row.get("combo_id") or "")
    template_name = str(row.get("template_id") or row.get("draft_template") or "")

    # Resolve content combination; curated combos are provided via content_combinations
    content_combination = next((c for c in content_combinations if c.combo_id == combo_id), None)
    if content_combination is None:
        available_combos = [combo.combo_id for combo in content_combinations[:5]]
        raise _Failure(
            description=f"Content combination '{combo_id}' not found in combinations database",
            metadata={
                "combo_id": _MV.text(combo_id),
                "available_combinations_sample": _MV.text(str(available_combos)),
                "total_combinations": _MV.int(len(content_combinations)),
            },
        )

    try:
        prompt = render_template("draft", template_name, {"concepts": content_combination.contents})
    except FileNotFoundError as e:
        raise _Failure(
            description=f"Draft template '{template_name}' not found",
            metadata={
                "template_name": _MV.text(template_name),
                "phase": _MV.text("draft"),
                "error": _MV.text(str(e)),
                "resolution": _MV.text("Ensure the template exists in data/1_raw/templates/draft/"),
            },
        )
    except Exception as e:
        templates_root = Path(os.environ.get("GEN_TEMPLATES_ROOT", "data/1_raw/templates"))
        template_path = templates_root / "draft" / f"{template_name}.txt"
        raise _Failure(
            description=f"Error rendering draft template '{template_name}'",
            metadata={
                "template_name": _MV.text(template_name),
                "phase": _MV.text("draft"),
                "template_path": _MV.path(str(template_path)),
                "jinja_message": _MV.text(str(e)),
            },
        ) from e

    context.log.info(f"Generated draft prompt for gen {gen_id} using template {template_name}")
    return prompt


def draft_response_asset(context, draft_prompt) -> str:
    """Asset-compatible draft response generation.

    Delegates to execute_draft_llm and emits the same output metadata.
    """
    gen_id = context.partition_key
    from pathlib import Path as _Path
    from dagster import Failure as _Failure, MetadataValue as _MV
    from daydreaming_dagster.assets._helpers import (
        require_membership_row,
        emit_standard_output_metadata,
        get_run_id,
    )

    # Resolve model/template from cohort membership (required columns enforced)
    data_root = _Path(getattr(context.resources, "data_root", "data"))
    row, cohort = require_membership_row(
        context,
        "draft",
        str(gen_id),
        require_columns=["llm_model_id"],
    )
    model_id = str(row.get("llm_model_id") or "").strip()
    template_id = str(row.get("template_id") or row.get("draft_template") or "").strip()
    if not model_id:
        raise _Failure(
            description="Missing generation model for draft task",
            metadata={
                "function": _MV.text("draft_response"),
                "gen_id": _MV.text(str(gen_id)),
                "resolution": _MV.text(
                    "Ensure cohort membership contains llm_model_id for this draft"
                ),
            },
        )

    # Execute via stage_services (writes files; parses via CSV fallback when configured)
    result = execute_draft_llm(
        llm=context.resources.openrouter_client,
        root_dir=data_root,
        gen_id=str(gen_id),
        template_id=template_id,
        prompt_text=str(draft_prompt) if isinstance(draft_prompt, str) else "",
        model=model_id,
        max_tokens=getattr(context.resources.experiment_config, "draft_generation_max_tokens", None),
        min_lines=int(getattr(context.resources.experiment_config, "min_draft_lines", 3)),
        fail_on_truncation=True,
        parser_name=None,
        parent_gen_id=None,
        metadata_extra={
            "function": "draft_response",
            "cohort_id": str(cohort) if isinstance(cohort, str) and cohort else None,
            "combo_id": str(row.get("combo_id") or "") or None,
            "run_id": get_run_id(context),
        },
    )

    # Emit standard Dagster metadata for UI
    emit_standard_output_metadata(
        context,
        function="draft_response",
        gen_id=str(gen_id),
        result=result,
        extras={
            "parser_name": (result.metadata or {}).get("parser_name"),
        },
    )

    return str(result.parsed_text or result.raw_text or "")


__all__ += [
    "draft_prompt_asset",
    "draft_response_asset",
]

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
    parsed = parse_text(stage, raw_text, parser_name)
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


def response_asset(context, prompt_text, stage: Stage) -> str:
    """Unified response asset for all stages.

    Handles stage-specific behavior (copy vs llm for essay, truncation/min-lines for draft,
    required columns, and metadata) while delegating execution to execute_copy/execute_llm.
    """
    gen_id = context.partition_key
    # Lazy imports to avoid circular dependencies during module import
    from daydreaming_dagster.assets._helpers import (
        require_membership_row,
        load_generation_parsed_text,
        resolve_essay_generator_mode,
        emit_standard_output_metadata,
        get_run_id,
    )

    data_root = Path(getattr(context.resources, "data_root", "data"))
    fn_label = f"_{stage}_response_impl"

    if stage == "essay":
        # Membership: need template and link to parent draft; model may be optional for copy mode
        row, _cohort = require_membership_row(
            context, "essay", str(gen_id), require_columns=["template_id", "parent_gen_id"]
        )
        template_name = str(row.get("template_id") or row.get("essay_template") or "")
        parent_gen_id = str(row.get("parent_gen_id") or "").strip()

        # Determine mode from prompt override or templates CSV
        if isinstance(prompt_text, str) and prompt_text.strip().upper().startswith("COPY_MODE"):
            mode = "copy"
        else:
            mode = resolve_essay_generator_mode(data_root, template_name)

        # Load upstream draft text and enforce minimum lines
        if not parent_gen_id:
            raise Failure(
                description=f"Missing parent_gen_id for {stage} doc",
                metadata={
                    "function": MetadataValue.text(fn_label),
                    "gen_id": MetadataValue.text(str(gen_id)),
                    "resolution": MetadataValue.text(
                        f"Ensure {stage} row exists in cohort membership with a valid parent_gen_id"
                    ),
                },
            )
        draft_text = load_generation_parsed_text(context, "draft", parent_gen_id, failure_fn_name=fn_label)
        min_lines = int(getattr(context.resources.experiment_config, "min_draft_lines", 1))
        dlines = [line.strip() for line in str(draft_text).split("\n") if line.strip()]
        if len(dlines) < max(1, min_lines):
            draft_dir = data_root / "gens" / DRAFT / parent_gen_id
            raise Failure(
                description=f"Upstream draft text is empty/too short for {stage} generation",
                metadata={
                    "function": MetadataValue.text(fn_label),
                    "gen_id": MetadataValue.text(str(gen_id)),
                    "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                    "draft_line_count": MetadataValue.int(len(dlines)),
                    "min_required_lines": MetadataValue.int(min_lines),
                    "draft_gen_dir": MetadataValue.path(str(draft_dir)),
                },
            )

        # Copy path: pass through parsed draft to essay/parsed.txt
        if mode == "copy":
            result = execute_copy(
                out_dir=data_root / "gens",
                stage=stage,
                gen_id=str(gen_id),
                template_id=template_name,
                parent_gen_id=parent_gen_id,
                pass_through_from=(data_root / "gens" / DRAFT / parent_gen_id / "parsed.txt"),
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
                extras={"mode": "copy", "parent_gen_id": parent_gen_id},
            )
            return result.parsed_text or draft_text

        # LLM path for essay
        model_id = str(row.get("llm_model_id") or "").strip()
        if not model_id:
            raise Failure(
                description=f"Missing generation model for {stage} task",
                metadata={
                    "function": MetadataValue.text(fn_label),
                    "gen_id": MetadataValue.text(str(gen_id)),
                    "resolution": MetadataValue.text(
                        f"Ensure cohort membership includes an llm_model_id for this {stage}"
                    ),
                },
            )
        if not isinstance(prompt_text, str) or not prompt_text.strip():
            raise Failure(
                description=f"Upstream {stage}_prompt is missing or empty",
                metadata={
                    "function": MetadataValue.text(fn_label),
                    "gen_id": MetadataValue.text(str(gen_id)),
                    "resolution": MetadataValue.text(
                        f"Ensure {stage}_prompt is materialized and wired as a dependency"
                    ),
                },
            )

        result = execute_llm(
            stage=stage,
            llm=context.resources.openrouter_client,
            root_dir=data_root,
            gen_id=str(gen_id),
            template_id=template_name,
            prompt_text=str(prompt_text),
            model=model_id,
            max_tokens=getattr(context.resources.experiment_config, f"{stage}_generation_max_tokens", None),
            min_lines=None,
            parent_gen_id=parent_gen_id,
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
        return result.raw_text or ""

    elif stage == "evaluation":
        # Membership: need model, template, and parent essay
        from dagster import Failure as _Failure, MetadataValue as _MV

        row, _cohort = require_membership_row(
            context,
            stage,
            str(gen_id),
            require_columns=["llm_model_id", "template_id", "parent_gen_id"],
        )
        model_name = str(row.get("llm_model_id") or "").strip()
        evaluation_template = str(row.get("template_id") or "").strip()
        parent_gen_id = str(row.get("parent_gen_id") or "").strip()

        if not isinstance(prompt_text, str) or not prompt_text.strip():
            raise _Failure(
                description=f"Upstream {stage}_prompt is missing or empty",
                metadata={
                    "function": _MV.text(f"{stage}_response"),
                    "gen_id": _MV.text(str(gen_id)),
                    "resolution": _MV.text(
                        f"Ensure {stage}_prompt is materialized and wired as a dependency"
                    ),
                },
            )

        result = execute_llm(
            stage=stage,
            llm=context.resources.openrouter_client,
            root_dir=data_root,
            gen_id=str(gen_id),
            template_id=evaluation_template,
            prompt_text=str(prompt_text),
            model=model_name,
            max_tokens=getattr(context.resources.experiment_config, f"{stage}_max_tokens", None),
            min_lines=None,
            parent_gen_id=parent_gen_id,
            metadata_extra={
                "function": f"{stage}_response",
                "run_id": get_run_id(context),
            },
        )
        emit_standard_output_metadata(
            context, function=f"{stage}_response", gen_id=str(gen_id), result=result
        )
        context.log.info(f"Generated {stage} response for gen {gen_id}")
        return result.raw_text or ""

    elif stage == "draft":
        from dagster import Failure as _Failure, MetadataValue as _MV

        # Resolve model/template from cohort membership (required columns enforced)
        row, cohort = require_membership_row(
            context,
            stage,
            str(gen_id),
            require_columns=["llm_model_id"],
        )
        model_id = str(row.get("llm_model_id") or "").strip()
        template_id = str(row.get("template_id") or row.get("draft_template") or "").strip()
        if not model_id:
            raise _Failure(
                description=f"Missing generation model for {stage} task",
                metadata={
                    "function": _MV.text(f"{stage}_response"),
                    "gen_id": _MV.text(str(gen_id)),
                    "resolution": _MV.text(
                        f"Ensure cohort membership contains llm_model_id for this {stage}"
                    ),
                },
            )

        result = execute_llm(
            stage=stage,
            llm=context.resources.openrouter_client,
            root_dir=data_root,
            gen_id=str(gen_id),
            template_id=template_id,
            prompt_text=str(prompt_text) if isinstance(prompt_text, str) else "",
            model=model_id,
            max_tokens=getattr(context.resources.experiment_config, f"{stage}_generation_max_tokens", None),
            min_lines=int(getattr(context.resources.experiment_config, "min_draft_lines", 3)),
            fail_on_truncation=True,
            parent_gen_id=None,
            metadata_extra={
                "function": f"{stage}_response",
                "cohort_id": str(cohort) if isinstance(cohort, str) and cohort else None,
                "combo_id": str(row.get("combo_id") or "") or None,
                "run_id": get_run_id(context),
            },
        )

        # Emit standard Dagster metadata for UI
        emit_standard_output_metadata(
            context,
            function=f"{stage}_response",
            gen_id=str(gen_id),
            result=result,
            extras={
                "parser_name": (result.metadata or {}).get("parser_name"),
            },
        )

        return str(result.parsed_text or result.raw_text or "")

    else:  # pragma: no cover - defensive
        raise Failure(description=f"Unsupported stage: {stage}")


def essay_response_asset(context, essay_prompt) -> str:
    return response_asset(context, essay_prompt, "essay")


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
    from dagster import Failure as _Failure, MetadataValue as _MV
    # Lazy imports to avoid circular imports at module import time
    from daydreaming_dagster.assets._helpers import (
        require_membership_row,
        load_generation_parsed_text,
        resolve_evaluation_generator_mode,
    )

    row, _cohort = require_membership_row(
        context,
        "evaluation",
        str(gen_id),
        require_columns=["parent_gen_id", "template_id"],
    )
    parent_gen_id = str(row.get("parent_gen_id") or "").strip()
    evaluation_template = str(row.get("template_id") or "").strip()
    if not parent_gen_id:
        raise _Failure(
            description="Missing parent_gen_id for evaluation task",
            metadata={
                "function": _MV.text("evaluation_prompt"),
                "gen_id": _MV.text(str(gen_id)),
                "resolution": _MV.text(
                    "Ensure evaluation row exists in cohort membership with a valid parent_gen_id"
                ),
            },
        )
    # Copy mode: short-circuit with sentinel prompt
    mode = resolve_evaluation_generator_mode(Path(context.resources.data_root), evaluation_template)
    if mode == "copy":
        context.add_output_metadata(
            {
                "function": _MV.text("evaluation_prompt"),
                "gen_id": _MV.text(str(gen_id)),
                "parent_gen_id": _MV.text(str(parent_gen_id)),
                "mode": _MV.text(mode),
                "template_used": _MV.text(str(evaluation_template)),
            }
        )
        return "COPY_MODE: no prompt needed"

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
    return response_asset(context, evaluation_prompt, "evaluation")


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
    from daydreaming_dagster.assets._helpers import (
        require_membership_row,
        resolve_draft_generator_mode,
    )

    # Read membership to resolve combo/template
    row, _cohort = require_membership_row(context, "draft", str(gen_id))
    combo_id = str(row.get("combo_id") or "")
    template_name = str(row.get("template_id") or row.get("draft_template") or "")

    # Copy mode: short-circuit prompt generation
    mode = resolve_draft_generator_mode(Path(getattr(context.resources, "data_root", "data")), template_name)
    if mode == "copy":
        context.add_output_metadata(
            {
                "function": _MV.text("draft_prompt"),
                "gen_id": _MV.text(str(gen_id)),
                "mode": _MV.text(mode),
                "template_name": _MV.text(template_name),
                "combo_id": _MV.text(combo_id),
            }
        )
        return "COPY_MODE: no prompt needed"

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
    return response_asset(context, draft_prompt, "draft")


__all__ += [
    "draft_prompt_asset",
    "draft_response_asset",
]

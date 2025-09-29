from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Literal
import time
import math

from jinja2 import Environment, StrictUndefined

from daydreaming_dagster.types import Stage
from daydreaming_dagster.data_layer.paths import Paths, RAW_FILENAME, PARSED_FILENAME
from daydreaming_dagster.utils.raw_readers import read_templates
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer


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

_PARENT_STAGE_MAP: Dict[Stage, Stage] = {
    "essay": "draft",
    "evaluation": "essay",
}

_JINJA = Environment(undefined=StrictUndefined)


def parent_stage_of(stage: Stage) -> Optional[Stage]:
    return _PARENT_STAGE_MAP.get(stage)


def _templates_root(default: Optional[Path] = None) -> Path:
    """Resolve templates root, honoring optional override and GEN_TEMPLATES_ROOT.

    Kept for back-compat; new code should use Paths(...).templates_root().
    """
    if default:
        return Path(default)
    return Paths.from_str("data").templates_root()


def render_template(
    stage: Stage,
    template_id: str,
    values: Dict[str, Any],
    *,
    templates_root: Optional[Path] = None,
) -> str:
    base = (_templates_root(templates_root) / stage)
    path = base / f"{template_id}.txt"
    if not path.exists():
        raise FileNotFoundError(f"Template not found: {path}")
    tpl = _JINJA.from_string(path.read_text(encoding="utf-8"))
    return tpl.render(**values)


def effective_parent_stage(stage: Stage) -> Stage:
    parent = parent_stage_of(stage)
    if parent is None:
        raise ValueError(f"Stage {stage} does not have a parent stage")
    return parent


def effective_parser_name(
    data_root: Path,
    stage: Stage,
    template_id: str,
    override: Optional[str] = None,
) -> Optional[str]:
    if isinstance(override, str) and override.strip():
        return override.strip()

    stage_str = str(stage)
    if stage_str not in {"draft", "essay", "evaluation"}:
        raise ValueError(f"Unsupported stage for parser resolution: {stage}")

    df = read_templates(Path(data_root), stage_str, filter_active=False)
    row = df[df["template_id"].astype(str) == str(template_id)]
    if row.empty:
        raise ValueError(f"Template '{template_id}' not found in {stage_str}_templates.csv")

    raw_val = row.iloc[0].get("parser")
    if raw_val is None or (isinstance(raw_val, float) and math.isnan(raw_val)):
        parsed_name = ""
    else:
        parsed_name = str(raw_val).strip()

    return parsed_name or "identity"


def resolve_generator_mode(
    *,
    kind: Literal["draft", "essay", "evaluation"],
    data_root: Path,
    template_id: str,
    override_from_prompt: Optional[str] = None,
    filter_active: Optional[bool] = None,
) -> Literal["llm", "copy"]:
    if isinstance(override_from_prompt, str) and override_from_prompt.strip().upper().startswith("COPY_MODE"):
        return "copy"

    if filter_active is None:
        filter_active = False

    df = read_templates(Path(data_root), kind, filter_active=bool(filter_active))
    if df.empty:
        raise ValueError(f"{kind.capitalize()} templates table is empty; cannot resolve generator mode")
    if "generator" not in df.columns:
        raise ValueError(f"{kind.capitalize()} templates CSV missing required 'generator' column")

    row = df[df["template_id"].astype(str) == str(template_id)]
    if row.empty:
        raise ValueError(f"{kind.capitalize()} template not found: {template_id}")

    val = row.iloc[0].get("generator")
    if not isinstance(val, str) or not val.strip():
        raise ValueError(f"{kind.capitalize()} template has empty/invalid generator value")

    mode = val.strip().lower()
    if mode not in ("llm", "copy"):
        raise ValueError(f"{kind.capitalize()} template declares unsupported generator '{mode}'")
    return mode  # type: ignore[return-value]


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
    if isinstance(min_lines, int) and min_lines > 0:
        response_lines = [ln for ln in str(raw_text).split("\n") if ln.strip()]
        if len(response_lines) < min_lines:
            raise ValueError(
                f"{str(stage).capitalize()} validation failed: only {len(response_lines)} non-empty lines, minimum required {min_lines}"
            )


def validate_result(
    stage: Stage,
    raw_text: str,
    info: Dict[str, Any] | None,
    *,
    min_lines: Optional[int] = None,
    fail_on_truncation: bool = True,
) -> None:
    """Pure validation wrapper for response text and call info.

    - Enforces minimum non-empty line count when provided.
    - Raises on truncation when fail_on_truncation is True and info['truncated'] is truthy.
    """
    _validate_min_lines(stage, raw_text, min_lines)
    if bool(fail_on_truncation) and isinstance(info, dict) and info.get("truncated"):
        raise ValueError(
            "LLM response appears truncated (finish_reason=length or max_tokens hit)"
        )


def resolve_parser_name(
    data_root: Path,
    stage: Stage,
    template_id: str,
    provided: Optional[str] = None,
) -> Optional[str]:
    """Resolve the effective parser name per centralized policy with pragmatic defaults.

    - Explicit override wins when provided and non-empty.
    - For essay/draft in test contexts without CSVs, fall back to "identity" if CSV is absent.
    - Otherwise, apply the centralized parser policy defined in this module (raises on config errors).
    """
    if isinstance(provided, str) and provided.strip():
        return provided.strip()
    try:
        return effective_parser_name(Path(data_root), stage, template_id, None)
    except Exception:
        # Pragmatic fallback: unit tests often avoid CSV; essay/draft can safely use identity
        if stage in ("essay", "draft"):
            return "identity"
        raise


def parse_text(stage: Stage, raw_text: str, parser_name: Optional[str]) -> Optional[str]:
    """Parse raw_text using the named parser.

    Behavior:
    - If parser_name is falsy/empty, return None (no parsing requested).
    - If a parser_name is provided but not registered for the stage, raise ParserError.
    - If the parser is found but raises during parsing, return None (soft-fail to allow early writes + validation to proceed).
    """
    if not (isinstance(parser_name, str) and parser_name.strip()):
        return None
    from daydreaming_dagster.utils.parser_registry import get_parser, ParserError

    parser = get_parser(stage, parser_name)
    if parser is None:
        raise ParserError(f"Missing parser '{parser_name}' for stage '{stage}'")
    try:
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
    parser_name: Optional[str] = None,
    # IO injection points (defaults are canonical helpers)
    write_raw=None,
    write_parsed=None,
    write_metadata=None,
) -> ExecutionResult:
    if stage in ("essay", "evaluation"):
        if not (isinstance(parent_gen_id, str) and parent_gen_id.strip()):
            raise ValueError("parent_gen_id is required for essay and evaluation stages")

    paths = Paths.from_str(root_dir)
    gens_root = paths.gens_root
    data_layer = GensDataLayer.from_root(root_dir)
    data_layer.reserve_generation(stage, gen_id, create=True)

    t0 = time.time()
    raw_text, info = generate_llm(llm, prompt_text, model=model, max_tokens=max_tokens)

    # Resolve parser name using centralized policy
    eff_parser_name: Optional[str] = resolve_parser_name(Path(root_dir), stage, template_id, parser_name)

    parsed = parse_text(stage, raw_text, eff_parser_name)
    if stage == "essay" and not isinstance(parsed, str):
        parsed = str(raw_text)

    base = gens_root / str(stage) / str(gen_id)

    meta: Dict[str, Any] = _base_meta(
        stage=stage,
        gen_id=str(gen_id),
        template_id=template_id,
        model=model,
        parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
        mode="llm",
    )
    meta["files"] = {"raw": str((paths.raw_path(stage, str(gen_id))).resolve())}
    meta.update(
        {
            "parser_name": eff_parser_name,
            "finish_reason": (info or {}).get("finish_reason") if isinstance(info, dict) else None,
            "truncated": bool((info or {}).get("truncated")) if isinstance(info, dict) else False,
            "usage": (info or {}).get("usage") if isinstance(info, dict) else None,
            "duration_s": round(time.time() - t0, 3),
        }
    )
    # Add essential metrics and parameters for downstream observability
    # Basic resource metrics
    meta["max_tokens"] = max_tokens if isinstance(max_tokens, int) else None

    duration_s = meta.get("duration_s")
    if isinstance(duration_s, (int, float)):
        meta["duration_ms"] = int(round(duration_s * 1000))
    else:
        meta["duration_ms"] = None
    # Character counts for prompt/raw/parsed
    meta["prompt_chars"] = len(prompt_text) if isinstance(prompt_text, str) else 0
    meta["raw_chars"] = len(raw_text) if isinstance(raw_text, str) else 0
    meta["parsed_chars"] = len(parsed) if isinstance(parsed, str) else 0
    # Total tokens if provided by the provider info
    usage = meta.get("usage")
    if isinstance(usage, dict):
        token_value = usage.get("total_tokens") or usage.get("totalTokens") or usage.get("total")
        meta["total_tokens"] = int(token_value) if isinstance(token_value, (int, float)) else None
    else:
        meta["total_tokens"] = None
    _merge_extras(meta, metadata_extra)
    # Add default replicate=1 if not provided
    if "replicate" not in meta:
        meta["replicate"] = 1

    # First write raw and metadata for debuggability.
    # NOTE: We intentionally perform early writes here instead of using an IO manager
    # for responses. IO managers write only after an asset returns successfully, but
    # execute_llm may raise after generating (e.g., truncation/min-lines validation).
    # Early writes ensure raw.txt and metadata.json are available on failures for
    # postmortem debugging and several tests rely on this behavior.
    if write_raw is None:
        data_layer.write_raw(stage, gen_id, str(raw_text or ""))
    else:
        write_raw(gens_root, stage, str(gen_id), str(raw_text or ""))

    if write_metadata is None:
        data_layer.write_main_metadata(stage, gen_id, meta)
    else:
        write_metadata(gens_root, stage, str(gen_id), meta)

    validate_result(stage, raw_text, info, min_lines=min_lines, fail_on_truncation=bool(fail_on_truncation))

    if isinstance(parsed, str):
        meta["files"]["parsed"] = str((paths.parsed_path(stage, str(gen_id))).resolve())
        if write_parsed is None:
            data_layer.write_parsed(stage, gen_id, str(parsed))
        else:
            write_parsed(gens_root, stage, str(gen_id), str(parsed))

    return ExecutionResult(prompt_text=prompt_text, raw_text=raw_text, parsed_text=parsed, info=info, metadata=meta)


## write_generation helper removed; explicit write_* calls are used inline


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
    out_root = Path(out_dir)
    data_root = out_root.parent if out_root.name == "gens" else out_root
    paths = Paths.from_str(data_root)
    data_layer = GensDataLayer.from_root(data_root)
    data_layer.reserve_generation(stage, gen_id, create=True)
    meta = _base_meta(
        stage=stage, gen_id=str(gen_id), template_id=template_id, model=None, parent_gen_id=str(parent_gen_id), mode="copy"
    )
    meta["files"] = {"parsed": str(paths.parsed_path(stage, str(gen_id)).resolve())}
    meta["duration_s"] = round(time.time() - t0, 3)
    _merge_extras(meta, metadata_extra)
    if "replicate" not in meta:
        meta["replicate"] = 1
    data_layer.write_parsed(stage, gen_id, str(parsed))
    data_layer.write_main_metadata(stage, gen_id, meta)
    return ExecutionResult(prompt_text=None, raw_text=None, parsed_text=parsed, info=None, metadata=meta)


__all__ = [
    "Stage",
    "ExecutionResult",
    "LLMClientProto",
    "ExecutionResultLike",
    "parent_stage_of",
    "render_template",
    "effective_parent_stage",
    "effective_parser_name",
    "resolve_generator_mode",
    "generate_llm",
    "resolve_parser_name",
    "parse_text",
    "execute_copy",
    "execute_llm",
]

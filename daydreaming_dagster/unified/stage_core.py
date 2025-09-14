from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Tuple
import os
import time

from jinja2 import Environment, StrictUndefined

from daydreaming_dagster.utils.generation import (
    write_raw as write_raw_file,
    write_parsed as write_parsed_file,
    write_prompt as write_prompt_file,
    write_metadata as write_metadata_file,
)
from daydreaming_dagster.constants import DRAFT

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
    """Resolve parser name with a provided override and CSV fallback.

    Behavior:
    - If `provided` is a non-empty string, return it.
    - Otherwise, try to read data/1_raw/<stage>_templates.csv via raw_readers.
      Return the 'parser' value for the matching template_id, or None if not found.
      Any read/parse errors are swallowed and result in None.
    """
    if isinstance(provided, str) and provided.strip():
        return provided.strip()
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


def parse_text(stage: Stage, raw_text: str, parser_name: Optional[str]) -> Optional[str]:
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
    if stage in ("essay", "evaluation"):
        if not (isinstance(parent_gen_id, str) and parent_gen_id.strip()):
            raise ValueError("parent_gen_id is required for essay and evaluation stages")

    t0 = time.time()
    raw_text, info = generate_llm(llm, prompt_text, model=model, max_tokens=max_tokens)

    default_hint = "identity" if stage == "essay" else None
    try:
        parser_name = resolve_parser_name(Path(root_dir), stage, template_id, default_hint)
    except Exception:
        parser_name = default_hint if isinstance(default_hint, str) else None
    if stage == "evaluation":
        if not (isinstance(parser_name, str) and parser_name.strip()):
            raise ValueError("parser_name is required for evaluation stage")

    parsed = parse_text(stage, raw_text, parser_name)
    if stage == "essay" and not isinstance(parsed, str):
        parsed = str(raw_text)

    out_dir = Path(root_dir) / "gens"
    base = out_dir / str(stage) / str(gen_id)

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

    # First write raw and metadata for debuggability
    write_raw_file(out_dir, stage, str(gen_id), str(raw_text or ""))
    write_metadata_file(out_dir, stage, str(gen_id), meta)

    _validate_min_lines(stage, raw_text, min_lines)
    if bool(fail_on_truncation) and isinstance(info, dict) and info.get("truncated"):
        raise ValueError("LLM response appears truncated (finish_reason=length or max_tokens hit)")

    if isinstance(parsed, str):
        meta["files"]["parsed"] = str((base / "parsed.txt").resolve())
        write_parsed_file(out_dir, stage, str(gen_id), str(parsed))

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
    meta = _base_meta(
        stage=stage, gen_id=str(gen_id), template_id=template_id, model=None, parent_gen_id=str(parent_gen_id), mode="copy"
    )
    base = Path(out_dir) / str(stage) / str(gen_id)
    meta["files"] = {"parsed": str((base / "parsed.txt").resolve())}
    meta["duration_s"] = round(time.time() - t0, 3)
    _merge_extras(meta, metadata_extra)
    write_parsed_file(out_dir, stage, str(gen_id), str(parsed))
    write_metadata_file(out_dir, stage, str(gen_id), meta)
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
    "execute_copy",
    "execute_llm",
]

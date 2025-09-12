from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Tuple
import os
import time

from jinja2 import Environment, StrictUndefined

from daydreaming_dagster.utils.generation import Generation

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


def resolve_draft_parser_name(
    data_root: Path,
    template_id: str,
    provided: Optional[str] = None,
) -> Optional[str]:
    if isinstance(provided, str) and provided.strip():
        return provided.strip()
    # CSV fallback (best-effort, never raise)
    try:
        from daydreaming_dagster.utils.raw_readers import read_draft_templates

        df = read_draft_templates(Path(data_root), filter_active=False)
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


def parse_draft(raw_text: str, parser_name: Optional[str]) -> Optional[str]:
    if not (isinstance(parser_name, str) and parser_name.strip()):
        return None
    try:
        from daydreaming_dagster.utils.draft_parsers import get_draft_parser

        parser = get_draft_parser(parser_name)
        if parser is None:
            return None
        return parser(str(raw_text))
    except Exception:
        return None


def parse_evaluation(raw_text: str, parser_name: str) -> Optional[str]:
    if not (isinstance(parser_name, str) and parser_name.strip()):
        return None
    try:
        from daydreaming_dagster.utils.eval_response_parser import parse_llm_response

        res = parse_llm_response(str(raw_text), parser_name)
        score = res.get("score")
        if isinstance(score, (int, float)):
            return f"{float(score)}\n"
        return None
    except Exception:
        return None


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


def execute_essay_copy(
    *,
    out_dir: Path,
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
        stage="essay", gen_id=str(gen_id), template_id=template_id, model=None, parent_gen_id=str(parent_gen_id), mode="copy"
    )
    base = Path(out_dir) / "essay" / str(gen_id)
    meta["files"] = {"parsed": str((base / "parsed.txt").resolve())}
    meta["duration_s"] = round(time.time() - t0, 3)
    _merge_extras(meta, metadata_extra)
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
        write_metadata=True,
    )
    return ExecutionResult(prompt_text=None, raw_text=None, parsed_text=parsed, info=None, metadata=meta)


def execute_draft_llm(
    *,
    llm: LLMClientProto,
    out_dir: Path,
    gen_id: str,
    template_id: str,
    prompt_text: str,
    model: str,
    data_root: Path,
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
    effective_parser = resolve_draft_parser_name(Path(data_root), template_id, parser_name)

    # Parse draft (best-effort)
    parsed = parse_draft(raw_text, effective_parser)

    # Build metadata early
    base = Path(out_dir) / "draft" / str(gen_id)
    meta: Dict[str, Any] = _base_meta(
        stage="draft",
        gen_id=str(gen_id),
        template_id=template_id,
        model=model,
        parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
        mode="llm",
    )
    meta["files"] = {
        "prompt": str((base / "prompt.txt").resolve()),
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
        write_prompt=True,
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
    out_dir: Path,
    gen_id: str,
    template_id: str,
    prompt_text: str,
    model: str,
    max_tokens: Optional[int],
    parent_gen_id: str,
    metadata_extra: Optional[Dict[str, Any]] = None,
) -> ExecutionResult:
    t0 = time.time()
    raw_text, info = generate_llm(llm, prompt_text, model=model, max_tokens=max_tokens)
    # Essay identity parse: parsed == normalized raw
    parsed = str(raw_text)

    base = Path(out_dir) / "essay" / str(gen_id)
    meta: Dict[str, Any] = _base_meta(
        stage="essay", gen_id=str(gen_id), template_id=template_id, model=model, parent_gen_id=str(parent_gen_id), mode="llm"
    )
    meta["files"] = {
        "prompt": str((base / "prompt.txt").resolve()),
        "raw": str((base / "raw.txt").resolve()),
        "parsed": str((base / "parsed.txt").resolve()),
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

    write_generation(
        out_dir=out_dir,
        stage="essay",
        gen_id=str(gen_id),
        parent_gen_id=str(parent_gen_id),
        prompt_text=prompt_text,
        raw_text=raw_text,
        parsed_text=parsed,
        metadata=meta,
        write_raw=True,
        write_parsed=True,
        write_prompt=True,
        write_metadata=True,
    )
    return ExecutionResult(prompt_text=prompt_text, raw_text=raw_text, parsed_text=parsed, info=info, metadata=meta)


def execute_evaluation_llm(
    *,
    llm: LLMClientProto,
    out_dir: Path,
    gen_id: str,
    template_id: str,
    prompt_text: str,
    model: str,
    parser_name: str,
    max_tokens: Optional[int],
    parent_gen_id: str,
    metadata_extra: Optional[Dict[str, Any]] = None,
) -> ExecutionResult:
    if not (isinstance(parser_name, str) and parser_name.strip()):
        raise ValueError("parser_name is required for evaluation stage")
    t0 = time.time()
    raw_text, info = generate_llm(llm, prompt_text, model=model, max_tokens=max_tokens)
    parsed = parse_evaluation(raw_text, parser_name)

    base = Path(out_dir) / "evaluation" / str(gen_id)
    meta: Dict[str, Any] = _base_meta(
        stage="evaluation",
        gen_id=str(gen_id),
        template_id=template_id,
        model=model,
        parent_gen_id=str(parent_gen_id),
        mode="llm",
    )
    meta["files"] = {
        "prompt": str((base / "prompt.txt").resolve()),
        "raw": str((base / "raw.txt").resolve()),
        "parsed": str((base / "parsed.txt").resolve()),
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

    write_generation(
        out_dir=out_dir,
        stage="evaluation",
        gen_id=str(gen_id),
        parent_gen_id=str(parent_gen_id),
        prompt_text=prompt_text,
        raw_text=raw_text,
        parsed_text=parsed,
        metadata=meta,
        write_raw=True,
        write_parsed=True,
        write_prompt=True,
        write_metadata=True,
    )
    return ExecutionResult(prompt_text=prompt_text, raw_text=raw_text, parsed_text=parsed, info=info, metadata=meta)


__all__ = [
    "Stage",
    "ExecutionResult",
    "LLMClientProto",
    "ExecutionResultLike",
    "render_template",
    "generate_llm",
    "resolve_draft_parser_name",
    "parse_draft",
    "parse_evaluation",
    "write_generation",
    "execute_essay_copy",
    "execute_draft_llm",
    "execute_essay_llm",
    "execute_evaluation_llm",
]


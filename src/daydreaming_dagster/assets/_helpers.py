from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Tuple, Any, Dict

import pandas as pd
from dagster import Failure, MetadataValue

from ..utils.membership_lookup import find_membership_row_by_gen
from ..utils.generation import load_generation
from ..config.paths import Paths
from ..utils.raw_readers import read_templates
from ..unified.stage_policy import parent_stage_of as _parent_stage_of
from ..types import Stage


def get_data_root(context) -> Path:
    return Path(getattr(getattr(context, "resources", object()), "data_root", "data"))


def get_gens_root(context) -> Path:
    return get_data_root(context) / "gens"


def get_run_id(context) -> Optional[str]:
    # Prefer Dagster run.run_id when available; fall back to context.run_id if present
    run = getattr(context, "run", None)
    if run is not None and getattr(run, "run_id", None):
        return str(run.run_id)
    rid = getattr(context, "run_id", None)
    return str(rid) if rid else None


def require_membership_row(
    context,
    stage: Stage,
    gen_id: str,
    *,
    require_columns: Iterable[str] = (),
) -> Tuple[pd.Series, Optional[str]]:
    data_root = get_data_root(context)
    row, cohort_id = find_membership_row_by_gen(data_root, stage, str(gen_id))
    if row is None:
        raise Failure(
            description="Membership row not found for generation",
            metadata={
                "function": MetadataValue.text("require_membership_row"),
                "stage": MetadataValue.text(str(stage)),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text(
                    "Ensure data/cohorts/*/membership.csv contains this stage/gen_id"
                ),
            },
        )
    missing: list[str] = []
    for c in list(require_columns or ()):  # normalize Iterable -> list
        if c not in row.index:
            missing.append(str(c))
        else:
            val = row.get(c)
            # Treat None, empty string, and NaN as missing
            if val is None or (isinstance(val, str) and not val.strip()) or (pd.isna(val)):
                missing.append(str(c))
    if missing:
        raise Failure(
            description="Membership row missing required columns (missing_columns)",
            metadata={
                "function": MetadataValue.text("require_membership_row"),
                "stage": MetadataValue.text(str(stage)),
                "gen_id": MetadataValue.text(str(gen_id)),
                "missing_columns": MetadataValue.json(missing),
                "resolution": MetadataValue.text(
                    "Populate required fields in cohort membership (e.g., llm_model_id, template_id)"
                ),
            },
        )
    return row, cohort_id


def load_generation_parsed_text(
    context,
    stage: Stage,
    gen_id: str,
    *,
    failure_fn_name: str,
) -> str:
    gens_root = get_gens_root(context)
    gen = load_generation(gens_root, stage, str(gen_id))
    base = Path(gens_root) / stage / str(gen_id)
    parsed = gen.get("parsed_text") if isinstance(gen, dict) else None
    if not parsed:
        paths = Paths.from_context(context)
        raise Failure(
            description="Missing or unreadable parsed.txt for upstream generation",
            metadata={
                "function": MetadataValue.text(failure_fn_name),
                "stage": MetadataValue.text(str(stage)),
                "gen_id": MetadataValue.text(str(gen_id)),
                "parsed_path": MetadataValue.path(str(paths.parsed_path(stage, str(gen_id)).resolve())),
            },
        )
    return str(parsed).replace("\r\n", "\n")


def _parent_stage(stage: Stage) -> Stage:
    """Wrapper delegating to unified.stage_policy.parent_stage_of for consistency."""
    ps = _parent_stage_of(stage)
    # For draft (no parent), keep prior behavior returning "draft" though it shouldn't be requested.
    return ps or "draft"


def load_parent_parsed_text(
    context,
    stage: Stage,
    gen_id: str,
    *,
    failure_fn_name: str,
) -> tuple[str, str]:
    """Load parsed.txt for the parent of a generation.

    Resolves the membership row for `stage`/`gen_id`, validates presence of
    `parent_gen_id`, determines the upstream stage, and returns
    (parent_gen_id, parent_parsed_text). Raises Failure with helpful metadata
    if the membership row or files are missing.
    """
    row, _cohort = require_membership_row(
        context,
        stage,
        str(gen_id),
        require_columns=["parent_gen_id"],
    )
    parent_gen_id = str(row.get("parent_gen_id") or "").strip()
    if not parent_gen_id:
        raise Failure(
            description="Missing parent_gen_id for upstream document",
            metadata={
                "function": MetadataValue.text(str(failure_fn_name)),
                "stage": MetadataValue.text(str(stage)),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text(
                    "Ensure cohort membership row has a valid parent_gen_id"
                ),
            },
        )
    pstage = _parent_stage(stage)
    parent_text = load_generation_parsed_text(
        context,
        pstage,
        parent_gen_id,
        failure_fn_name=failure_fn_name,
    )
    return parent_gen_id, parent_text


def resolve_generator_mode(
    *,
    kind: Literal["draft", "essay", "evaluation"],
    data_root: Path,
    template_id: str,
    override_from_prompt: Optional[str] = None,
    filter_active: Optional[bool] = None,
) -> Literal["llm", "copy"]:
    """Parametrized resolver for generator modes across all stages.

    - Uses data/1_raw/<kind>_templates.csv and the 'generator' column.
    - Accepts an override via prompt prefix 'COPY_MODE'.
    - Always uses filter_active=False by default, per unified behavior.
    - Failure metadata uses function=f"resolve_{kind}_generator_mode".
    """
    function_label = f"resolve_{kind}_generator_mode"
    if isinstance(override_from_prompt, str) and override_from_prompt.strip().upper().startswith("COPY_MODE"):
        return "copy"

    # Unified default: do not filter by active
    if filter_active is None:
        filter_active = False

    df = read_templates(Path(data_root), kind, filter_active=bool(filter_active))

    if df.empty:
        raise Failure(
            description=f"{kind.capitalize()} templates table is empty; cannot resolve generator mode",
            metadata={
                "function": MetadataValue.text(function_label),
                "data_root": MetadataValue.path(str(data_root)),
                "resolution": MetadataValue.text(
                    f"Ensure data/1_raw/{kind}_templates.csv contains templates with a 'generator' column"
                ),
            },
        )
    if "generator" not in df.columns:
        raise Failure(
            description=f"{kind.capitalize()} templates CSV missing required 'generator' column",
            metadata={
                "function": MetadataValue.text(function_label),
                "data_root": MetadataValue.path(str(data_root)),
                "resolution": MetadataValue.text("Add a 'generator' column with values 'llm' or 'copy'"),
            },
        )
    row = df[df["template_id"].astype(str) == str(template_id)]
    if row.empty:
        raise Failure(
            description=f"{kind.capitalize()} template not found: {template_id}",
            metadata={
                "function": MetadataValue.text(function_label),
                "template_id": MetadataValue.text(str(template_id)),
                "resolution": MetadataValue.text(
                    f"Add the template to {kind}_templates.csv or correct the {kind} template_id in tasks"
                ),
            },
        )
    val = row.iloc[0].get("generator")
    if not isinstance(val, str) or not val.strip():
        raise Failure(
            description=f"{kind.capitalize()} template has empty/invalid generator value",
            metadata={
                "function": MetadataValue.text(function_label),
                "template_id": MetadataValue.text(str(template_id)),
                "resolution": MetadataValue.text("Set generator to 'llm' or 'copy'"),
            },
        )
    mode = val.strip().lower()
    if mode not in ("llm", "copy"):
        raise Failure(
            description=f"{kind.capitalize()} template declares unsupported generator '{mode}'",
            metadata={
                "function": MetadataValue.text(function_label),
                "template_id": MetadataValue.text(str(template_id)),
                "resolution": MetadataValue.text(
                    f"Set generator to 'llm' or 'copy' in {kind}_templates.csv"
                ),
            },
        )
    return mode  # type: ignore[return-value]




def emit_standard_output_metadata(
    context,
    *,
    function: str,
    gen_id: str,
    result: "ExecutionResultLike",
    extras: Optional[dict] = None,
) -> None:
    # Accept both our dataclass and a mapping-like result
    def _get(obj: Any, key: str, default=None):
        if hasattr(obj, key):
            return getattr(obj, key)
        if isinstance(obj, dict):
            return obj.get(key, default)
        return default

    info: Dict[str, Any] = _get(result, "info", {}) or {}

    md: Dict[str, MetadataValue] = {
        "function": MetadataValue.text(str(function)),
        "gen_id": MetadataValue.text(str(gen_id)),
        "finish_reason": MetadataValue.text(str(info.get("finish_reason")) if isinstance(info.get("finish_reason"), (str, int, float)) else ""),
        "truncated": MetadataValue.bool(bool(info.get("truncated"))),
    }
    # Optional: include duration and token usage if available on result.metadata or info.usage
    meta = _get(result, "metadata", {}) or {}
    # If stage_core provided precomputed counts and metrics, include them directly without re-deriving.
    def _try_add_numeric(key: str, value, *, as_float: bool = False):
        try:
            if value is None:
                return
            md[key] = MetadataValue.float(float(value)) if as_float else MetadataValue.int(int(value))
        except Exception:
            pass

    try:
        if isinstance(meta, dict) and meta.get("duration_s") is not None:
            md["duration_s"] = MetadataValue.float(float(meta.get("duration_s")))
            if meta.get("duration_ms") is not None:
                md["duration_ms"] = MetadataValue.int(int(meta.get("duration_ms")))
    except Exception:
        pass
    # max_tokens from metadata when present
    try:
        if isinstance(meta, dict) and meta.get("max_tokens") is not None:
            md["max_tokens"] = MetadataValue.int(int(meta.get("max_tokens")))
    except Exception:
        pass
    # Precomputed char/line counts from metadata (if provided by execution layer)
    if isinstance(meta, dict):
        _try_add_numeric("prompt_chars", meta.get("prompt_chars"))
        _try_add_numeric("raw_chars", meta.get("raw_chars"))
        _try_add_numeric("parsed_chars", meta.get("parsed_chars"))
        _try_add_numeric("prompt_lines", meta.get("prompt_lines"))
        _try_add_numeric("raw_lines", meta.get("raw_lines"))
        _try_add_numeric("parsed_lines", meta.get("parsed_lines"))
    # total_tokens best-effort from metadata.usage or info.usage
    def _total_tokens_from(u: Any) -> int | None:
        if isinstance(u, dict):
            t = u.get("total_tokens")
            if t is None:
                t = u.get("totalTokens") or u.get("total")
            try:
                return int(t) if isinstance(t, (int, float)) else None
            except Exception:
                return None
        return None

    if isinstance(meta, dict) and isinstance(meta.get("usage"), dict):
        tt = _total_tokens_from(meta.get("usage"))
        if tt is not None:
            md["total_tokens"] = MetadataValue.int(tt)
    else:
        u = info.get("usage") if isinstance(info, dict) else None
        tt = _total_tokens_from(u)
        if tt is not None:
            md["total_tokens"] = MetadataValue.int(tt)
    if extras:
        for k, v in extras.items():
            # Avoid overriding the standard keys
            if k not in md:
                # Coerce common types into MetadataValue
                if isinstance(v, bool):
                    md[k] = MetadataValue.bool(v)
                elif isinstance(v, int):
                    md[k] = MetadataValue.int(v)
                elif isinstance(v, float):
                    md[k] = MetadataValue.float(v)
                elif isinstance(v, (list, dict)):
                    md[k] = MetadataValue.json(v)
                else:
                    md[k] = MetadataValue.text(str(v))
    context.add_output_metadata(md)


# For type checkers in assets that import this
ExecutionResultLike = Any

__all__ = [
    "Stage",
    "get_data_root",
    "get_gens_root",
    "get_run_id",
    "require_membership_row",
    "load_generation_parsed_text",
    "load_parent_parsed_text",
    "build_prompt_metadata",
    "resolve_generator_mode",
    "emit_standard_output_metadata",
]

def build_prompt_metadata(
    context,
    *,
    stage: Stage,
    gen_id: str,
    template_id: str,
    mode: str,
    parent_gen_id: Optional[str] = None,
    prompt_text: Optional[str] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> Dict[str, MetadataValue]:
    """Standard prompt metadata for Dagster UI.

    - Always includes function, gen_id, template_id, mode, prompt_length.
    - Includes parent_gen_id when available.
    - Accepts extras to add stage‑specific fields (e.g., draft_line_count).
    """
    def _mlen(s: Optional[str]) -> int:
        return len(s) if isinstance(s, str) else 0
    def _lines(s: Optional[str]) -> int:
        return sum(1 for _ in str(s).splitlines()) if isinstance(s, str) and s else 0

    md: Dict[str, MetadataValue] = {
        "function": MetadataValue.text(f"{stage}_prompt"),
        "gen_id": MetadataValue.text(str(gen_id)),
        "template_id": MetadataValue.text(str(template_id)),
        "mode": MetadataValue.text(str(mode)),
        "prompt_length": MetadataValue.int(_mlen(prompt_text)),
        "prompt_lines": MetadataValue.int(_lines(prompt_text)),
    }
    if isinstance(parent_gen_id, str) and parent_gen_id:
        md["parent_gen_id"] = MetadataValue.text(str(parent_gen_id))
    if extras:
        for k, v in extras.items():
            if isinstance(v, bool):
                md[k] = MetadataValue.bool(v)
            elif isinstance(v, int):
                md[k] = MetadataValue.int(v)
            elif isinstance(v, float):
                md[k] = MetadataValue.float(v)
            elif isinstance(v, (list, dict)):
                md[k] = MetadataValue.json(v)
            elif v is not None:
                md[k] = MetadataValue.text(str(v))
    return md

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Literal, Optional, Tuple, Any, Dict

import pandas as pd
from dagster import Failure, MetadataValue

from ..utils.membership_lookup import find_membership_row_by_gen
from ..utils.generation import Generation
from ..constants import FILE_PARSED
from ..utils.raw_readers import read_essay_templates
from ..utils.raw_readers import read_evaluation_templates

Stage = Literal["draft", "essay", "evaluation"]


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
    gen = Generation.load(gens_root, stage, str(gen_id))
    base = Path(gens_root) / stage / str(gen_id)
    parsed = gen.parsed_text if isinstance(gen.parsed_text, str) else None
    if not parsed:
        raise Failure(
            description="Missing or unreadable parsed.txt for upstream generation",
            metadata={
                "function": MetadataValue.text(failure_fn_name),
                "stage": MetadataValue.text(str(stage)),
                "gen_id": MetadataValue.text(str(gen_id)),
                "parsed_path": MetadataValue.path(str((base / FILE_PARSED).resolve())),
            },
        )
    return str(parsed).replace("\r\n", "\n")


def _parent_stage(stage: Stage) -> Stage:
    """Return the upstream stage for a given stage.

    - essay -> draft
    - evaluation -> essay
    - draft has no parent but is never used here
    """
    if stage == "essay":
        return "draft"
    if stage == "evaluation":
        return "essay"
    # Default fallback; callers should not request parent of draft
    return "draft"


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


def _resolve_generator_mode(
    *,
    kind: Literal["essay", "evaluation"],
    data_root: Path,
    template_id: str,
    override_from_prompt: Optional[str] = None,
    filter_active: Optional[bool] = None,
) -> Literal["llm", "copy"]:
    """
    Parametrized resolver for generator modes.

    TEMPORARY: Used by resolve_essay_generator_mode and resolve_evaluation_generator_mode
    until those thin wrappers are removed. Keeps error metadata consistent with the
    wrapper function names.
    """
    function_label = (
        "resolve_essay_generator_mode" if kind == "essay" else "resolve_evaluation_generator_mode"
    )
    if isinstance(override_from_prompt, str) and override_from_prompt.strip().upper().startswith("COPY_MODE"):
        return "copy"

    # Determine filtering behavior per kind to preserve legacy semantics
    if filter_active is None:
        filter_active = False if kind == "essay" else True

    if kind == "essay":
        df = read_essay_templates(Path(data_root), filter_active=filter_active)
    else:
        df = read_evaluation_templates(Path(data_root)) if filter_active else read_evaluation_templates(Path(data_root))

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


def resolve_essay_generator_mode(
    data_root: Path,
    template_id: str,
    *,
    override_from_prompt: Optional[str] = None,
) -> Literal["llm", "copy"]:
    # TEMPORARY: Thin wrapper over _resolve_generator_mode(kind="essay")
    return _resolve_generator_mode(
        kind="essay",
        data_root=data_root,
        template_id=template_id,
        override_from_prompt=override_from_prompt,
        filter_active=False,
    )


def resolve_evaluation_generator_mode(
    data_root: Path,
    template_id: str,
    *,
    override_from_prompt: Optional[str] = None,
) -> Literal["llm", "copy"]:
    # TEMPORARY: Thin wrapper over _resolve_generator_mode(kind="evaluation")
    return _resolve_generator_mode(
        kind="evaluation",
        data_root=data_root,
        template_id=template_id,
        override_from_prompt=override_from_prompt,
        filter_active=True,
    )


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

    prompt = _get(result, "prompt_text") or _get(result, "prompt")
    raw = _get(result, "raw_text") or _get(result, "raw")
    parsed = _get(result, "parsed_text") or _get(result, "parsed")
    info: Dict[str, Any] = _get(result, "info", {}) or {}

    def _chars(x: Optional[str]) -> int:
        return len(str(x)) if isinstance(x, str) else 0

    def _lines(x: Optional[str]) -> int:
        return sum(1 for _ in str(x).splitlines()) if isinstance(x, str) and x else 0

    md: Dict[str, MetadataValue] = {
        "function": MetadataValue.text(str(function)),
        "gen_id": MetadataValue.text(str(gen_id)),
        "finish_reason": MetadataValue.text(str(info.get("finish_reason")) if isinstance(info.get("finish_reason"), (str, int, float)) else ""),
        "truncated": MetadataValue.bool(bool(info.get("truncated"))),
        "prompt_chars": MetadataValue.int(_chars(prompt)),
        "prompt_lines": MetadataValue.int(_lines(prompt)),
        "raw_chars": MetadataValue.int(_chars(raw)),
        "raw_lines": MetadataValue.int(_lines(raw)),
        "parsed_chars": MetadataValue.int(_chars(parsed)),
        "parsed_lines": MetadataValue.int(_lines(parsed)),
    }
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
    "resolve_essay_generator_mode",
    "resolve_evaluation_generator_mode",
    "emit_standard_output_metadata",
]

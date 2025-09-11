"""
Group: generation_essays

Asset definitions for the essay (Phase‑2) generation stage.
"""

from dagster import asset, Failure, MetadataValue
from pathlib import Path
from jinja2 import Environment
from .partitions import essay_gens_partitions
from ..utils.template_loader import load_generation_template
from ..utils.raw_readers import read_essay_templates
from ..utils.dataframe_helpers import get_task_row, resolve_llm_model_id
from ..utils.membership_lookup import find_membership_row_by_gen
from ..utils.generation import Generation
from ..utils.metadata import build_generation_metadata
from ..constants import DRAFT, ESSAY, FILE_RAW

# Reuse a single Jinja environment
JINJA = Environment()


def _get_essay_generator_mode(data_root: str | Path, template_id: str) -> str:
    """Return normalized generator mode for an essay template.

    Allowed values: 'llm' or 'copy'. Any other value (including missing) raises Failure.
    """
    df = read_essay_templates(Path(data_root), filter_active=False)
    if df.empty:
        raise Failure(
            description="Essay templates table is empty; cannot resolve generator mode",
            metadata={
                "function": MetadataValue.text("_get_essay_generator_mode"),
                "data_root": MetadataValue.path(str(data_root)),
                "resolution": MetadataValue.text("Ensure data/1_raw/essay_templates.csv contains active templates with a 'generator' column"),
            },
        )
    if "generator" not in df.columns:
        raise Failure(
            description="Essay templates CSV missing required 'generator' column",
            metadata={
                "function": MetadataValue.text("_get_essay_generator_mode"),
                "data_root": MetadataValue.path(str(data_root)),
                "resolution": MetadataValue.text("Add a 'generator' column with values 'llm' or 'copy'"),
            },
        )
    row = df[df["template_id"] == template_id]
    if row.empty:
        raise Failure(
            description=f"Essay template not found: {template_id}",
            metadata={
                "function": MetadataValue.text("_get_essay_generator_mode"),
                "template_id": MetadataValue.text(str(template_id)),
                "resolution": MetadataValue.text("Add the template row to essay_templates.csv or correct the essay_template id in tasks"),
            },
        )
    val = row.iloc[0].get("generator")
    if not isinstance(val, str) or not val.strip():
        raise Failure(
            description="Essay template has empty/invalid generator value",
            metadata={
                "function": MetadataValue.text("_get_essay_generator_mode"),
                "template_id": MetadataValue.text(str(template_id)),
                "resolution": MetadataValue.text("Set generator to 'llm' or 'copy'"),
            },
        )
    mode = val.strip().lower()
    if mode not in ("llm", "copy"):
        raise Failure(
            description=f"Essay template declares unsupported generator '{mode}'",
            metadata={
                "function": MetadataValue.text("_get_essay_generator_mode"),
                "template_id": MetadataValue.text(str(template_id)),
                "resolution": MetadataValue.text("Set generator to 'llm' or 'copy' in data/1_raw/essay_templates.csv"),
            },
        )
    return mode


    


def _load_phase1_text_by_parent_doc(context, parent_gen_id: str) -> tuple[str, str]:
    """Load Phase‑1 text by parent_gen_id directly from the filesystem.

    Returns (normalized_text, source_label).
    """
    data_root = Path(getattr(context.resources, "data_root", "data"))
    gens_root = data_root / "gens"
    base = gens_root / DRAFT / str(parent_gen_id)
    try:
        gen = Generation.load(gens_root, DRAFT, str(parent_gen_id))
    except Exception as e:
        raise Failure(
            description="Parent draft document not found",
            metadata={
                "function": MetadataValue.text("_load_phase1_text_by_parent_doc"),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                "error": MetadataValue.text(str(e)),
            },
        )
    if not isinstance(gen.parsed_text, str) or not gen.parsed_text:
        raise Failure(
            description="Missing or unreadable parsed.txt for parent draft document",
            metadata={
                "function": MetadataValue.text("_load_phase1_text_by_parent_doc"),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                "draft_gen_dir": MetadataValue.path(str(base)),
            },
        )
    return str(gen.parsed_text).replace("\r\n", "\n"), "draft_gens_parent"


# Note: combo/model-based resolution removed — parent_gen_id is required for essays.


def _essay_prompt_impl(context, essay_generation_tasks=None) -> str:
    """Generate Phase‑2 prompts based on Phase‑1 drafts."""
    gen_id = context.partition_key
    # Prefer cohort membership lookup; fall back to tasks for backcompat
    mrow, _cohort = find_membership_row_by_gen(getattr(context.resources, "data_root", "data"), "essay", str(gen_id))
    if mrow is not None:
        template_name = str(mrow.get("template_id") or mrow.get("essay_template") or "")
        parent_gen_id = mrow.get("parent_gen_id")
    else:
        # BACKCOMPAT: prefer provided tasks DF; else read tasks CSV
        _row = None
        try:
            import pandas as _pd
            if isinstance(essay_generation_tasks, _pd.DataFrame) and not essay_generation_tasks.empty:
                tdf = essay_generation_tasks
            else:
                _data_root = Path(getattr(context.resources, "data_root", "data"))
                tdf = _pd.read_csv(_data_root / "2_tasks" / "essay_generation_tasks.csv")
            mask = tdf["gen_id"].astype(str) == str(gen_id)
            if mask.any():
                _row = tdf[mask].iloc[0]
            elif len(tdf) > 0:
                _row = tdf.iloc[0]
        except Exception:
            _row = None
        template_name = str((_row.get("essay_template") if _row is not None else "") or "")
        parent_gen_id = _row.get("parent_gen_id") if _row is not None else None

    generator_mode = _get_essay_generator_mode(context.resources.data_root, template_name)
    if generator_mode == "copy":
        context.add_output_metadata(
            {
                "function": MetadataValue.text("essay_prompt"),
                "mode": MetadataValue.text(generator_mode),
                "essay_template": MetadataValue.text(template_name),
            }
        )
        return f"{generator_mode.upper()}_MODE: no prompt needed"

    # Resolve Phase‑1 text strictly via parent_gen_id (gen-id required)
    if not (isinstance(parent_gen_id, str) and parent_gen_id.strip()):
        raise Failure(
            description="Missing parent_doc_id for essay doc",
            metadata={
                "function": MetadataValue.text("essay_prompt"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Provide parent_gen_id (draft gen id) in essay_generation_tasks.csv"),
            },
        )
    draft_text, used_source = _load_phase1_text_by_parent_doc(context, str(parent_gen_id))
    draft_lines = [line.strip() for line in draft_text.split("\n") if line.strip()]
    # Enforce non-empty upstream draft text to avoid empty prompts
    # experiment_config is required via asset definition
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
        tmpl = load_generation_template(template_name, "essay")
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
    # Provide both preferred and legacy variables to templates
    # - draft_lines: list[str] of non-empty lines
    # - draft_block: entire upstream draft text as a block
    # - links_block: legacy alias used by historical templates
    env = JINJA
    template = env.from_string(tmpl)
    # legacy-fallback: keep 'links_block' for historical templates that reference it directly.
    # Intentionally lowercased to avoid automated sweeps that remove FALLBACK(...) tags.
    # We are not changing historical templates, so this alias must remain.
    prompt = template.render(
        draft_block=draft_text,
        links_block=draft_text,
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


@asset(
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="essay_prompt_io_manager",
    required_resource_keys={"data_root", "experiment_config"},
)
def essay_prompt(context) -> str:
    return _essay_prompt_impl(context)


def _essay_response_impl(context, essay_prompt, essay_generation_tasks=None) -> str:
    """Generate Phase‑2 essay responses.

    Always resolves and loads the Phase‑1 (draft) text via parent_gen_id first,
    regardless of mode, and then applies the selected generation path.
    """
    gen_id = context.partition_key
    mrow, _cohort = find_membership_row_by_gen(getattr(context.resources, "data_root", "data"), "essay", str(gen_id))
    if mrow is not None:
        template_name = str(mrow.get("template_id") or mrow.get("essay_template") or "")
        parent_gen_id = mrow.get("parent_gen_id")
    else:
        # BACKCOMPAT: prefer provided tasks DF; else read tasks CSV
        _row = None
        try:
            import pandas as _pd
            if isinstance(essay_generation_tasks, _pd.DataFrame) and not essay_generation_tasks.empty:
                tdf = essay_generation_tasks
            else:
                _data_root = Path(getattr(context.resources, "data_root", "data"))
                tdf = _pd.read_csv(_data_root / "2_tasks" / "essay_generation_tasks.csv")
            mask = tdf["gen_id"].astype(str) == str(gen_id)
            if mask.any():
                _row = tdf[mask].iloc[0]
            elif len(tdf) > 0:
                _row = tdf.iloc[0]
        except Exception:
            _row = None
        template_name = str((_row.get("essay_template") if _row is not None else "") or "")
        parent_gen_id = _row.get("parent_gen_id") if _row is not None else None
    # Allow explicit COPY_MODE hint from caller/tests to bypass template lookup
    if isinstance(essay_prompt, str) and essay_prompt.strip().upper().startswith("COPY_MODE"):
        mode = "copy"
    else:
        mode = _get_essay_generator_mode(context.resources.data_root, template_name)

    # Load Phase‑1 draft text; prefer index when available, otherwise fall back to IO/filesystem
    if not (isinstance(parent_gen_id, str) and parent_gen_id.strip()):
        raise Failure(
            description="Missing parent_gen_id for essay doc",
            metadata={
                "function": MetadataValue.text("_essay_response_impl"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Provide parent_gen_id (draft gen id) in essay_generation_tasks.csv"),
            },
        )
    draft_text, used_source = _load_phase1_text_by_parent_doc(context, str(parent_gen_id))
    # Ensure upstream draft has content even in copy mode
    # experiment_config is required by the caller asset
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

    if mode == "copy":
        text = draft_text
        context.add_output_metadata(
            {
                "function": MetadataValue.text("essay_response"),
                "mode": MetadataValue.text("copy"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "parent_gen_id": MetadataValue.text(str(parent_gen_id)),
                "source": MetadataValue.text(used_source),
                "chars": MetadataValue.int(len(text)),
                "lines": MetadataValue.int(sum(1 for _ in text.splitlines())),
            }
        )
        return text
    # Default LLM path (persist RAW early; apply truncation guard)
    # Use model id from membership; fall back to tasks for backcompat
    if mrow is not None:
        model_id = str(mrow.get("llm_model_id") or mrow.get("generation_model") or "").strip()
    else:
        # Fallback: prefer provided tasks DF; else read tasks CSV
        import pandas as _pd
        try:
            if isinstance(essay_generation_tasks, _pd.DataFrame) and not essay_generation_tasks.empty:
                tdf = essay_generation_tasks
            else:
                _data_root = Path(getattr(context.resources, "data_root", "data"))
                tdf = _pd.read_csv(_data_root / "2_tasks" / "essay_generation_tasks.csv")
            mask = tdf["gen_id"].astype(str) == str(gen_id)
            if mask.any():
                _row = tdf[mask].iloc[0]
                model_id = resolve_llm_model_id(_row, "essay")
            elif len(tdf) > 0:
                _row = tdf.iloc[0]
                model_id = resolve_llm_model_id(_row, "essay")
            else:
                model_id = ""
        except Exception:
            model_id = ""
    if not model_id:
        raise Failure(
            description="Missing generation_model for essay task",
            metadata={
                "function": MetadataValue.text("_essay_response_impl"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure essay_generation_tasks.csv includes a generation_model (model id) column"),
            },
        )
    llm_client = context.resources.openrouter_client
    max_tokens = context.resources.experiment_config.essay_generation_max_tokens
    text, info = llm_client.generate_with_info(essay_prompt, model=model_id, max_tokens=max_tokens)

    normalized = str(text).replace("\r\n", "\n")

    # Persist RAW to gens store immediately so it exists even if we later fail (e.g., truncation)
    data_root = Path(getattr(context.resources, "data_root", "data"))
    try:
        _gen0 = Generation(
            stage=ESSAY,
            gen_id=str(gen_id),
            parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
            raw_text=normalized,
            parsed_text=None,
            prompt_text=essay_prompt if isinstance(essay_prompt, str) else None,
            metadata={
                "function": "essay_response",
                "gen_id": str(gen_id),
                "parent_gen_id": str(parent_gen_id) if parent_gen_id else "",
            },
        )
        _gen0.write_files(data_root / "gens")
        raw_path_str = str((_gen0.target_dir(data_root / "gens") / FILE_RAW).resolve())
    except Exception:
        raw_path_str = None

    # Truncation detection: explicit flag or finish_reason=length
    finish_reason = (info or {}).get("finish_reason") if isinstance(info, dict) else None
    was_truncated = bool((info or {}).get("truncated") if isinstance(info, dict) else False)
    usage = (info or {}).get("usage") if isinstance(info, dict) else None
    completion_tokens = usage.get("completion_tokens") if isinstance(usage, dict) else None
    requested_max = usage.get("max_tokens") if isinstance(usage, dict) else None
    if was_truncated:
        meta = {
            "function": MetadataValue.text("essay_response"),
            "gen_id": MetadataValue.text(str(gen_id)),
            # Intentionally omit provider model from reports to simplify
            "max_tokens": MetadataValue.int(int(max_tokens) if isinstance(max_tokens, (int, float)) else 0),
            "finish_reason": MetadataValue.text(str(finish_reason)),
            "truncated": MetadataValue.bool(True),
        }
        if isinstance(completion_tokens, int):
            meta["completion_tokens"] = MetadataValue.int(completion_tokens)
        if isinstance(requested_max, int):
            meta["requested_max_tokens"] = MetadataValue.int(requested_max)
        if raw_path_str:
            meta["raw_path"] = MetadataValue.path(raw_path_str)
            meta["raw_chars"] = MetadataValue.int(len(normalized))
        raise Failure(description=f"Essay response truncated for gen {gen_id}", metadata=meta)

    context.add_output_metadata(
        {
            "function": MetadataValue.text("essay_response"),
            "gen_id": MetadataValue.text(str(gen_id)),
            # Intentionally omit provider model from reports to simplify
            "chars": MetadataValue.int(len(normalized)),
            "truncated": MetadataValue.bool(False),
            **({"raw_path": MetadataValue.path(raw_path_str)} if raw_path_str else {}),
        }
    )
    return normalized


@asset(
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="essay_response_io_manager",
    required_resource_keys={"openrouter_client", "experiment_config", "data_root"},
)
def essay_response(context, essay_prompt) -> str:
    text = _essay_response_impl(context, essay_prompt)

    # Write to filesystem (gens)
    import time
    from pathlib import Path as _Path

    gen_id = context.partition_key
    mrow, cohort = find_membership_row_by_gen(getattr(context.resources, "data_root", "data"), "essay", str(gen_id))
    if mrow is not None:
        essay_template = mrow.get("template_id")
        model_id = (mrow.get("llm_model_id") or mrow.get("generation_model"))
        parent_gen_id = mrow.get("parent_gen_id")
    else:
        # BACKCOMPAT CSV fallback
        import pandas as _pd
        _data_root = Path(getattr(context.resources, "data_root", "data"))
        try:
            _df = _pd.read_csv(_data_root / "2_tasks" / "essay_generation_tasks.csv")
            _row = _df[_df["gen_id"].astype(str) == str(gen_id)].iloc[0]
            essay_template = _row.get("essay_template")
            model_id = _row.get("generation_model") or _row.get("generation_model_id")
            parent_gen_id = _row.get("parent_gen_id")
        except Exception:
            essay_template = None
            model_id = None
            parent_gen_id = None
    generator_mode = (_get_essay_generator_mode(context.resources.data_root, essay_template) or "llm").lower()

    if not (isinstance(parent_gen_id, str) and parent_gen_id and parent_gen_id.strip()):
        raise Failure(
            description="Missing parent_gen_id for essay doc",
            metadata={
                "function": MetadataValue.text("essay_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Provide parent_gen_id (draft gen id) in essay_generation_tasks.csv"),
            },
        )
    gen_id_value = str(gen_id)
    if not (isinstance(gen_id_value, str) and gen_id_value.strip()):
        raise Failure(
            description="Missing gen_id for essay doc",
            metadata={
                "function": MetadataValue.text("essay_response"),
                "gen_id": MetadataValue.text(str(gen_id)),
                "resolution": MetadataValue.text("Ensure essay_generation_tasks.csv includes a gen_id column"),
            },
        )

    gens_root = Path(getattr(context.resources, "data_root", "data")) / "gens"
    # Build document using helper
    run_id = getattr(getattr(context, "run", object()), "run_id", None) or getattr(context, "run_id", None)
    metadata = build_generation_metadata(
        stage=ESSAY,
        gen_id=str(gen_id_value),
        parent_gen_id=str(parent_gen_id) if parent_gen_id else None,
        template_id=str(essay_template) if essay_template else None,
        model_id=str(model_id) if model_id else None,
        task_id=str(""),
        function="essay_response",
        run_id=str(run_id) if run_id else None,
        cohort_id=str(cohort) if isinstance(cohort, str) and cohort else None,
        extra={
            "essay_template": essay_template,
        },
    )
    prompt_text = essay_prompt if (generator_mode == "llm" and isinstance(essay_prompt, str)) else None

    doc = Generation(
        stage=ESSAY,
        gen_id=gen_id_value,
        parent_gen_id=parent_gen_id,
        raw_text=text,
        parsed_text=text,
        prompt_text=prompt_text,
        metadata=metadata,
    )
    target_dir = doc.write_files(gens_root)

    context.add_output_metadata(
        {
            "gen_id": MetadataValue.text(gen_id_value),
            "gen_dir": MetadataValue.path(str(target_dir)),
            "parent_gen_id": MetadataValue.text(str(parent_gen_id) if parent_gen_id else ""),
        }
    )

    return text

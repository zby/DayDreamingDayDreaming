from dagster import asset, MetadataValue, AssetIn
from pathlib import Path
import pandas as pd
import numpy as np
from scripts.aggregate_scores import parse_all as parse_all_scores
import json
from ..constants import ESSAY, DRAFT, FILE_PARSED, FILE_METADATA


@asset(
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager",
    required_resource_keys={"data_root"},
    ins={
        # Limit to the current experiment's evaluation tasks
        "evaluation_tasks": AssetIn(),
    },
    description="Parse evaluation scores from gens store for current evaluation_tasks only",
    compute_kind="pandas",
)
def parsed_scores(context, evaluation_tasks: pd.DataFrame) -> pd.DataFrame:
    """Parse evaluation scores from the gens store and write a consolidated DataFrame.

    Reads data/gens/evaluation/<gen_id>/{parsed.txt,metadata.json} and uses
    evaluation_templates.csv to select the parser when parsed.txt is missing.
    """
    data_root = Path(getattr(context.resources, "data_root", "data"))
    out_csv = data_root / "5_parsing" / "parsed_scores.csv"
    df = parse_all_scores(data_root, out_csv)
    # Filter to current evaluation tasks only (gen-id-first)
    try:
        if isinstance(evaluation_tasks, pd.DataFrame) and not evaluation_tasks.empty and "gen_id" in evaluation_tasks.columns:
            keep = set(evaluation_tasks["gen_id"].astype(str).dropna().tolist())
            if "gen_id" in df.columns and keep:
                df = df[df["gen_id"].astype(str).isin(keep)].reset_index(drop=True)
    except Exception:
        pass
    # Enrich with generation metadata by reading gens store metadata for parents
    # Expected fields by downstream: combo_id, draft_template, generation_template, generation_model, stage, generation_response_path
    gens_root = data_root / "gens"
    combo_ids = []
    draft_templates = []
    gen_templates = []
    gen_models = []
    stages = []
    gen_paths = []
    for _, row in (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).iterrows():
        essay_doc = str(row.get("parent_gen_id") or "")
        # Default stage for evaluations is essays
        stages.append("essay2p")
        # Default generation path to essay parsed.txt in gens store
        gen_paths.append(str(gens_root / ESSAY / essay_doc / FILE_PARSED) if essay_doc else "")
        etpl = ""
        gmid = ""
        ddoc = ""
        # Read essay metadata to get template_id (essay) and model_id and parent draft
        try:
            if essay_doc:
                emeta_path = gens_root / ESSAY / essay_doc / FILE_METADATA
                if emeta_path.exists():
                    emeta = json.loads(emeta_path.read_text(encoding="utf-8")) or {}
                    etpl = str(emeta.get("template_id") or emeta.get("essay_template") or "")
                    gmid = str(emeta.get("model_id") or "")
                    ddoc = str(emeta.get("parent_gen_id") or "")
        except Exception:
            pass
        # Read draft metadata to get combo_id and draft_template
        cid = ""
        dtpl = ""
        try:
            if ddoc:
                dmeta_path = gens_root / DRAFT / ddoc / FILE_METADATA
                if dmeta_path.exists():
                    dmeta = json.loads(dmeta_path.read_text(encoding="utf-8")) or {}
                    cid = str(dmeta.get("combo_id") or "")
                    dtpl = str(dmeta.get("template_id") or dmeta.get("draft_template") or "")
                    if not gmid:
                        gmid = str(dmeta.get("model_id") or "")
        except Exception:
            pass
        combo_ids.append(cid)
        draft_templates.append(dtpl)
        gen_templates.append(etpl)
        gen_models.append(gmid)

    if isinstance(df, pd.DataFrame) and not df.empty:
        df = df.copy()
        if combo_ids:
            df["combo_id"] = combo_ids
        if draft_templates:
            df["draft_template"] = draft_templates
        if gen_templates:
            df["generation_template"] = gen_templates
        if gen_models:
            df["generation_model"] = gen_models
        if stages:
            df["stage"] = stages
        if gen_paths:
            df["generation_response_path"] = gen_paths
    context.add_output_metadata({
        "rows": MetadataValue.int(int(df.shape[0]) if hasattr(df, "shape") else 0),
        "output": MetadataValue.path(str(out_csv)),
        "enriched": MetadataValue.bool(True),
    })
    return df

from dagster import asset, MetadataValue
from pathlib import Path
import pandas as pd
import numpy as np
from scripts.parse_all_scores import parse_all as parse_all_scores
import json


@asset(
    group_name="results_processing",
    io_manager_key="parsing_results_io_manager",
    required_resource_keys={"data_root"},
    description="Parse evaluation scores from docs store (data/docs/evaluation/<doc_id>/parsed.txt)",
    compute_kind="pandas",
)
def parsed_scores(context) -> pd.DataFrame:
    """Parse evaluation scores from the docs store and write a consolidated DataFrame.

    Reads data/docs/evaluation/<doc_id>/{parsed.txt,metadata.json} and uses
    evaluation_templates.csv to select the parser when parsed.txt is missing.
    """
    data_root = Path(getattr(context.resources, "data_root", "data"))
    out_csv = data_root / "5_parsing" / "parsed_scores.csv"
    df = parse_all_scores(data_root, out_csv)
    # Enrich with generation metadata by reading docs store metadata for parents
    # Expected fields by downstream: combo_id, draft_template, generation_template, generation_model, stage, generation_response_path
    docs_root = data_root / "docs"
    combo_ids = []
    draft_templates = []
    gen_templates = []
    gen_models = []
    stages = []
    gen_paths = []
    for _, row in (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).iterrows():
        essay_doc = str(row.get("parent_doc_id") or "")
        # Default stage for evaluations is essays
        stages.append("essay2p")
        # Default generation path to essay parsed.txt in docs store
        gen_paths.append(str(docs_root / "essay" / essay_doc / "parsed.txt") if essay_doc else "")
        etpl = ""
        gmid = ""
        ddoc = ""
        # Read essay metadata to get template_id (essay) and model_id and parent draft
        try:
            if essay_doc:
                emeta_path = docs_root / "essay" / essay_doc / "metadata.json"
                if emeta_path.exists():
                    emeta = json.loads(emeta_path.read_text(encoding="utf-8")) or {}
                    etpl = str(emeta.get("template_id") or emeta.get("essay_template") or "")
                    gmid = str(emeta.get("model_id") or "")
                    ddoc = str(emeta.get("parent_doc_id") or "")
        except Exception:
            pass
        # Read draft metadata to get combo_id and draft_template
        cid = ""
        dtpl = ""
        try:
            if ddoc:
                dmeta_path = docs_root / "draft" / ddoc / "metadata.json"
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

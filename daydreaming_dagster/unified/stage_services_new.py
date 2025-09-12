from __future__ import annotations

from pathlib import Path
from typing import Literal

import json
from daydreaming_dagster.unified.stage_services import render_template
from daydreaming_dagster.assets._helpers import upstage


Stage = Literal["draft", "essay", "evaluation"]


def build_prompt(doc_root: str | Path, gen_id: str, stage: Stage) -> str:
    """Compute the prompt text for a given generation.

    Parameters
    - doc_root: path to the project data root (directory containing 1_raw/, gens/, cohorts/)
    - gen_id: generation identifier (partition key)
    - stage: target stage ("essay" or "evaluation")

    Returns
    - The rendered prompt text for the given stage/gen_id.
    """
    if stage not in ("draft", "essay", "evaluation"):
        raise ValueError(f"Unsupported stage for prompt rendering: {stage}")

    base = Path(doc_root)

    # Draft stage: render from template using combo mapping and concepts
    gen_dir = base / "gens" / stage / str(gen_id)
    if stage == "draft":
        md_path = gen_dir / "metadata.json"
        if not md_path.exists():
            raise FileNotFoundError(f"metadata.json not found for draft gen_id={gen_id}: {md_path}")
        try:
            meta = json.loads(md_path.read_text(encoding="utf-8"))
        except Exception as e:
            raise ValueError(f"Invalid metadata.json for draft gen_id={gen_id}: {e}")
        template_id = str(meta.get("template_id") or meta.get("draft_template") or "").strip()
        if not template_id:
            raise ValueError(f"Missing template_id in metadata for draft gen_id={gen_id}")
        combo_id = str(meta.get("combo_id") or "").strip()
        if not combo_id:
            raise ValueError(f"Missing combo_id in metadata for draft gen_id={gen_id}")

        # Load selected mapping first, then fall back to global combo_mappings.csv
        import pandas as _pd
        sel_path = base / "2_tasks" / "selected_combo_mappings.csv"
        group = None
        if sel_path.exists():
            try:
                df = _pd.read_csv(sel_path)
                grp = df[df["combo_id"].astype(str) == combo_id]
                if not grp.empty:
                    group = grp
            except Exception:
                group = None
        if group is None:
            superset = base / "combo_mappings.csv"
            if not superset.exists():
                raise FileNotFoundError(
                    f"No mapping found for combo_id={combo_id}; missing {sel_path} and {superset}"
                )
            df2 = _pd.read_csv(superset)
            group = df2[df2["combo_id"].astype(str) == combo_id]
            if group.empty:
                raise ValueError(f"combo_id {combo_id} not present in {superset}")
        # Determine uniform description level (first row), default 'paragraph'
        level = str(group.iloc[0].get("description_level") or "paragraph")
        concept_ids = [str(x) for x in group["concept_id"].astype(str).tolist()]

        # Load concepts and build ordered Concept list
        from daydreaming_dagster.utils.raw_readers import read_concepts as _read_concepts
        all_concepts = {c.concept_id: c for c in _read_concepts(base, filter_active=False)}
        missing = [cid for cid in concept_ids if cid not in all_concepts]
        if missing:
            raise ValueError(f"Missing concepts for combo {combo_id}: {missing}")
        from daydreaming_dagster.models.content_combination import ContentCombination as _CC
        ordered_concepts = [all_concepts[cid] for cid in concept_ids]
        combo = _CC.from_concepts(ordered_concepts, level=level, combo_id=combo_id)
        values = {"concepts": combo.contents}
        templates_root = base / "1_raw" / "templates"
        return render_template("draft", template_id, values, templates_root=templates_root)

    # Read template_id and parent_gen_id from this generation's metadata.json (essay/evaluation)
    md_path = gen_dir / "metadata.json"
    if not md_path.exists():
        raise FileNotFoundError(f"metadata.json not found for {stage} gen_id={gen_id}: {md_path}")
    try:
        meta = json.loads(md_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise ValueError(f"Invalid metadata.json for {stage} gen_id={gen_id}: {e}")
    template_id = str(meta.get("template_id") or "").strip()
    if not template_id:
        raise ValueError(f"Missing template_id in metadata for {stage} gen_id={gen_id}")
    parent_gen_id = str(meta.get("parent_gen_id") or "").strip()
    if not parent_gen_id:
        raise ValueError(f"Missing parent_gen_id in metadata for {stage} gen_id={gen_id}")

    # Load upstream parsed text
    parent_stage = upstage(stage)  # essay->draft, evaluation->essay
    parent_parsed_path = base / "gens" / parent_stage / str(parent_gen_id) / "parsed.txt"
    if not parent_parsed_path.exists():
        raise FileNotFoundError(f"Upstream parsed.txt not found: {parent_parsed_path}")
    parent_text = parent_parsed_path.read_text(encoding="utf-8").replace("\r\n", "\n")

    # Build template values by stage
    if stage == "essay":
        values = {"draft_block": parent_text, "links_block": parent_text}
    else:  # evaluation
        values = {"response": parent_text}

    # Render using templates under <doc_root>/1_raw/templates/<stage>/<template_id>.txt
    templates_root = base / "1_raw" / "templates"
    return render_template(stage, template_id, values, templates_root=templates_root)


__all__ = ["build_prompt"]

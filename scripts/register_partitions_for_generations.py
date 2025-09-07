#!/usr/bin/env python3
"""
Create draft tasks and register Dagster dynamic partitions from a curated
essay_generation_tasks.csv. Also generates evaluation tasks for all active
evaluation templates and evaluation-capable models.

Input:
- data/2_tasks/essay_generation_tasks.csv (source of truth)

Outputs:
- data/2_tasks/draft_generation_tasks.csv (deduped by draft_task_id)
- data/2_tasks/selected_combo_mappings.csv (filtered from combo_mappings.csv)
- data/2_tasks/evaluation_tasks.csv (deduped by evaluation_task_id)
- Registers dynamic partitions (draft, essay, evaluation); by default resets before adding

Usage example:
  uv run python scripts/register_partitions_for_generations.py \
    --input data/2_tasks/essay_generation_tasks.csv

Doc-ID pinning (default):
- evaluation_tasks.csv is generated with a pinned parent_doc_id for each evaluation task,
  resolved from the documents SQLite index (data/db/documents.sqlite). No flags needed.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import List

import pandas as pd
import re
from daydreaming_dagster.utils.document_locator import find_document_path
from daydreaming_dagster.utils.evaluation_parsing_config import load_parser_map
import sqlite3


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # Source of truth for curated generations
    p.add_argument("--input", type=Path, default=Path("data/2_tasks/essay_generation_tasks.csv"), help="Path to essay_generation_tasks.csv")
    p.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root (default: data)")
    p.add_argument("--dry-run", action="store_true", help="Compute and print actions but make no changes")
    # Reset by default; use --add-only to avoid clearing existing partitions
    p.add_argument("--add-only", action="store_true", help="Do not clear existing dynamic partitions (add-only mode)")
    return p.parse_args()


# Legacy input helpers removed; script now requires essay_generation_tasks.csv


# Legacy template/model parsing removed; rely on curated essay_generation_tasks.csv


def _load_models_map(data_root: Path) -> dict:
    models_csv = data_root / "1_raw" / "llm_models.csv"
    model_map: dict[str, str] = {}
    if models_csv.exists():
        try:
            mdf = pd.read_csv(models_csv)
            if {"id","model"}.issubset(mdf.columns):
                model_map = dict(zip(mdf["id"].astype(str), mdf["model"].astype(str)))
        except Exception as e:
            print(f"Warning: failed to read models CSV ({e}); using model ids verbatim", file=sys.stderr)
    return model_map

def _load_model_ids(data_root: Path) -> set[str]:
    models_csv = data_root / "1_raw" / "llm_models.csv"
    ids: set[str] = set()
    if models_csv.exists():
        try:
            mdf = pd.read_csv(models_csv)
            if "id" in mdf.columns:
                ids = set(mdf["id"].astype(str).tolist())
        except Exception as e:
            print(f"Warning: failed to read models CSV ({e}); model-id parsing may be less accurate", file=sys.stderr)
    return ids

# Legacy draft id parsing removed; derive from curated CSV only

_COMBO_PREFIX_RE = re.compile(r"^combo_v\d+_[0-9a-f]{12}")  # retained for sanity checks if needed

def _write_selected_combo_mappings(data_root: Path, combo_ids: List[str], dry_run: bool = False) -> int:
    """Filter data/combo_mappings.csv to selected combo_ids and write selected file under 2_tasks.

    Returns number of rows written (or that would be written in dry-run).
    Output path: data/2_tasks/selected_combo_mappings.csv
    """
    mapping_csv = data_root / "combo_mappings.csv"
    if not mapping_csv.exists():
        print(f"Warning: {mapping_csv} not found; cannot write selected_combo_mappings.csv", file=sys.stderr)
        return 0
    try:
        mdf = pd.read_csv(mapping_csv)
    except Exception as e:
        print(f"Warning: failed to read {mapping_csv}: {e}", file=sys.stderr)
        return 0

    if "combo_id" not in mdf.columns or "concept_id" not in mdf.columns:
        print(f"Warning: {mapping_csv} missing required columns combo_id/concept_id", file=sys.stderr)
        return 0

    need = set(map(str, combo_ids))
    subset = mdf[mdf["combo_id"].astype(str).isin(need)].copy()
    if subset.empty:
        print("Warning: none of the selected combo_ids were found in combo_mappings.csv", file=sys.stderr)
        return 0

    missing = sorted(list(need - set(subset["combo_id"].astype(str))))
    if missing:
        print(f"Warning: missing {len(missing)} combo_ids in combo_mappings.csv (first 5): {missing[:5]}", file=sys.stderr)

    # Keep full schema from superset to preserve parity
    curated = subset.copy()

    out = data_root / "2_tasks" / "selected_combo_mappings.csv"
    if dry_run:
        print(f"[dry-run] Would write {len(curated)} selected combo-mapping rows to {out}")
        return len(curated)
    out.parent.mkdir(parents=True, exist_ok=True)
    curated.to_csv(out, index=False)
    print(f"Wrote selected combo mappings: {out} ({len(curated)} rows)")
    return len(curated)

def _load_active_evaluation_axes(data_root: Path) -> tuple[list[str], list[str]]:
    """Return (evaluation_template_ids, evaluation_model_ids) that are active."""
    eval_tpl_csv = data_root / "1_raw" / "evaluation_templates.csv"
    models_csv = data_root / "1_raw" / "llm_models.csv"
    tpl_ids: list[str] = []
    model_ids: list[str] = []
    try:
        if eval_tpl_csv.exists():
            df = pd.read_csv(eval_tpl_csv)
            if "template_id" in df.columns:
                tpl_ids = df[df.get("active", True) == True]["template_id"].astype(str).tolist()
    except Exception as e:
        print(f"Warning: failed to read evaluation_templates.csv ({e})", file=sys.stderr)
    try:
        if models_csv.exists():
            df = pd.read_csv(models_csv)
            if "id" in df.columns:
                # Filter for_evaluation==True if present; else include none to be safe
                if "for_evaluation" in df.columns:
                    df = df[df["for_evaluation"] == True]
                model_ids = df["id"].astype(str).tolist()
    except Exception as e:
        print(f"Warning: failed to read llm_models.csv ({e})", file=sys.stderr)
    return tpl_ids, model_ids


def _open_documents_db(data_root: Path) -> sqlite3.Connection | None:
    """Open the documents SQLite DB if present; return None if missing/unavailable."""
    db_path = Path(data_root) / "db" / "documents.sqlite"
    try:
        if not db_path.exists():
            return None
        con = sqlite3.connect(str(db_path))
        con.row_factory = sqlite3.Row
        return con
    except Exception:
        return None


def _resolve_latest_essay_doc_id(con: sqlite3.Connection, essay_task_id: str) -> str | None:
    """Return latest essay doc_id for a given essay_task_id from the documents DB."""
    try:
        row = con.execute(
            (
                "SELECT doc_id FROM documents "
                "WHERE stage='essay' AND task_id=? "
                "ORDER BY created_at DESC, rowid DESC LIMIT 1"
            ),
            (str(essay_task_id),),
        ).fetchone()
        return str(row["doc_id"]) if row and row["doc_id"] else None
    except Exception:
        return None


def _write_table(out_csv: Path, rows: List[dict], key: str, columns: List[str], dry_run: bool = False) -> int:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
    if key in df.columns and not df.empty:
        df = df.drop_duplicates(subset=[key])
    if dry_run:
        print(f"[dry-run] Would write {len(df)} rows to {out_csv}")
        return len(df)
    df.to_csv(out_csv, index=False)
    return len(df)


def main() -> int:
    args = parse_args()
    data_root = args.data_root
    # New mode: derive everything from existing essay_generation_tasks.csv
    essay_rows: List[dict] = []
    draft_rows: List[dict] = []
    combo_ids: set[str] = set()

    # Require curated essay_generation_tasks.csv
    if args.input.suffix.lower() != ".csv":
        print(f"Expected a CSV input (essay_generation_tasks.csv). Got: {args.input}", file=sys.stderr)
        return 1
    try:
        df = pd.read_csv(args.input)
    except Exception as e:
        print(f"Failed to read {args.input}: {e}", file=sys.stderr)
        return 1
    required = {"essay_task_id", "draft_task_id", "combo_id", "draft_template", "generation_model"}
    missing = sorted(list(required - set(df.columns)))
    if missing:
        print(f"Input CSV does not look like essay_generation_tasks.csv (missing columns: {missing}).", file=sys.stderr)
        return 1
    # Use the curated essay tasks directly
    model_map = _load_models_map(data_root)
    for _, r in df.iterrows():
        essay_rows.append({
            "essay_task_id": str(r["essay_task_id"]),
            "draft_task_id": str(r["draft_task_id"]),
            "combo_id": str(r["combo_id"]),
            "draft_template": str(r["draft_template"]),
            "essay_template": str(r.get("essay_template", "")) if "essay_template" in df.columns else "",
            "generation_model": str(r["generation_model"]),
            "generation_model_name": model_map.get(str(r["generation_model"]), str(r["generation_model"]))
        })
        draft_rows.append({
            "draft_task_id": str(r["draft_task_id"]),
            "combo_id": str(r["combo_id"]),
            "draft_template": str(r["draft_template"]),
            "generation_model": str(r["generation_model"]),
            "generation_model_name": model_map.get(str(r["generation_model"]), str(r["generation_model"]))
        })
        combo_ids.add(str(r["combo_id"]))

    # Write/merge outputs
    essay_out_csv = data_root / "2_tasks" / "essay_generation_tasks.csv"
    link_out_csv = data_root / "2_tasks" / "draft_generation_tasks.csv"

    # Ensure output directory exists
    two_tasks = data_root / "2_tasks"
    two_tasks.mkdir(parents=True, exist_ok=True)

    # Write selected combo mappings for chosen combinations
    _write_selected_combo_mappings(data_root, sorted(combo_ids), dry_run=args.dry_run)

    added_essays = 0
    added_drafts = 0
    # Do not write essay_generation_tasks.csv here; treat it as source of truth

    if draft_rows:
        added_drafts = _write_table(
            link_out_csv,
            draft_rows,
            key="draft_task_id",
            columns=[
                "draft_task_id","combo_id","draft_template","generation_model","generation_model_name",
            ],
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            print(f"Wrote {link_out_csv} ({added_drafts} rows)")

    # Note: Do not write cross-experiment task tables here; cross-experiment tracking is driven by results appenders

    # Register dynamic partitions (add-only)
    # Prepare partition keys
    draft_ids = [r["draft_task_id"] for r in draft_rows] if draft_rows else []
    essay_ids = [r["essay_task_id"] for r in essay_rows] if essay_rows else []

    # Evaluation axes (with optional overrides)
    eval_tpls, eval_models = _load_active_evaluation_axes(data_root)
    # Load strict parser map for evaluation templates
    parser_map: dict[str, str] = {}
    try:
        parser_map = load_parser_map(data_root)
    except Exception as e:
        print(f"Warning: could not load parser map from evaluation_templates.csv: {e}", file=sys.stderr)
    # Build evaluation tasks pinned by doc_id (default)
    evaluation_rows: List[dict] = []
    if essay_rows and eval_tpls and eval_models:
        model_name_map = _load_models_map(data_root)
        con = _open_documents_db(data_root)
        for r in essay_rows:
            essay_task_id = r["essay_task_id"]
            # Resolve the latest essay doc_id for this task (pinned lineage)
            parent_doc_id = _resolve_latest_essay_doc_id(con, essay_task_id) if con else None
            if not parent_doc_id:
                print(
                    f"Warning: no essay doc_id found in DB for task_id={essay_task_id}; parent_doc_id will be empty",
                    file=sys.stderr,
                )
            # Locate source document path (optional, for ops UX)
            fp, src = find_document_path(essay_task_id, data_root)
            file_path = str(fp) if fp else ""
            for tpl in eval_tpls:
                for model in eval_models:
                    evaluation_rows.append({
                        "evaluation_task_id": f"{essay_task_id}__{tpl}__{model}",
                        "parent_doc_id": parent_doc_id or "",
                        # task context (kept for compatibility in reports)
                        "document_id": essay_task_id,
                        "essay_task_id": essay_task_id,
                        "draft_task_id": r.get("draft_task_id"),
                        "combo_id": r.get("combo_id"),
                        "draft_template": r.get("draft_template"),
                        "essay_template": r.get("essay_template"),
                        "generation_model": r.get("generation_model"),
                        "generation_model_name": r.get("generation_model_name"),
                        "evaluation_template": tpl,
                        "evaluation_model": model,
                        "evaluation_model_name": model_name_map.get(model, model),
                        "parser": parser_map.get(tpl),
                        "file_path": file_path,
                        "source_dir": src,
                        "source_asset": "essay_response",
                    })
    # Write evaluation tasks CSV
    eval_out_csv = data_root / "2_tasks" / "evaluation_tasks.csv"
    added_evals = 0
    if evaluation_rows:
        added_evals = _write_table(
            eval_out_csv,
            evaluation_rows,
            key="evaluation_task_id",
            columns=[
                "evaluation_task_id",
                # pinned lineage
                "parent_doc_id",
                # legacy/task context (kept during migration)
                "document_id","essay_task_id","draft_task_id","combo_id",
                "draft_template","essay_template","generation_model","generation_model_name",
                # evaluation axes and optional parser and file hints
                "evaluation_template","evaluation_model","evaluation_model_name","parser","file_path","source_dir","source_asset",
            ],
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            print(f"Wrote {eval_out_csv} ({added_evals} rows)")
    else:
        if not args.dry_run:
            print("No evaluation rows to write (check active evaluation templates/models)")

    # Partition keys for evaluation
    eval_ids = [row["evaluation_task_id"] for row in evaluation_rows] if evaluation_rows else []

    # Optionally write partition lists
    # No key-list outputs in simplified mode

    if not args.dry_run:
        try:
            from dagster import DagsterInstance
            instance = DagsterInstance.get()
            
            def _unique_str(values: list[str]) -> list[str]:
                seen, out = set(), []
                for v in values:
                    if isinstance(v, str) and v and v not in seen:
                        seen.add(v); out.append(v)
                return out

            # Optionally clear existing partitions first
            if not args.add_only:
                for name in ("draft_tasks", "essay_tasks", "evaluation_tasks"):
                    existing = list(instance.get_dynamic_partitions(name))
                    for p in existing:
                        instance.delete_dynamic_partition(name, p)
                    print(f"Cleared {len(existing)} partitions from {name}")

            if draft_ids:
                draft_ids = _unique_str(draft_ids)
                existing = set(instance.get_dynamic_partitions("draft_tasks"))
                new = [k for k in draft_ids if k not in existing]
                if new:
                    instance.add_dynamic_partitions("draft_tasks", new)
                print(f"Registered draft partitions: +{len(new)} (total ~{len(existing)+len(new)})")

            if essay_ids:
                essay_ids = _unique_str(essay_ids)
                existing = set(instance.get_dynamic_partitions("essay_tasks"))
                new = [k for k in essay_ids if k not in existing]
                if new:
                    instance.add_dynamic_partitions("essay_tasks", new)
                print(f"Registered essay partitions: +{len(new)} (total ~{len(existing)+len(new)})")

            # Evaluation partitions for active evaluation templates × models
            if eval_ids:
                existing = set(instance.get_dynamic_partitions("evaluation_tasks"))
                to_add = [k for k in eval_ids if k not in existing]
                if to_add:
                    instance.add_dynamic_partitions("evaluation_tasks", to_add)
                print(f"Registered evaluation partitions: +{len(to_add)} (total ~{len(existing)+len(to_add)})")

            print(f"DAGSTER_HOME={os.environ.get('DAGSTER_HOME','(unset)')}")
        except Exception as e:
            print(f"Warning: could not register Dagster partitions automatically: {e}", file=sys.stderr)

    # Simplified mode: no auto-refresh of assets here

    if args.dry_run:
        print(f"[dry-run] Would write drafts={len(draft_rows)}, evals={len(evaluation_rows)}; register draft={len(draft_ids)}, essay={len(essay_ids)}, eval={len(eval_ids)} (add_only={args.add_only})")
    else:
        print(f"Done. drafts={added_drafts}, evals={added_evals}; registered draft={len(draft_ids)}, essay={len(essay_ids)}, eval={len(eval_ids)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

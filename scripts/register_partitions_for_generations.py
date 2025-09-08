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
- evaluation_tasks.csv is generated with a pinned parent_doc_id (the essay doc id) for each evaluation task,
  taken directly from the curated essay_generation_tasks.csv. The input MUST include the essay document id
  column named `essay_doc_id` (temporary alias `parent_doc_id` accepted for backcompat). The script also
  reads the essay's metadata.json to optionally include `draft_doc_id`.
- This script reserves and writes `doc_id` for draft_generation_tasks.csv and evaluation_tasks.csv only.
  It does NOT modify essay_generation_tasks.csv. It FAILS if the curated essay tasks are missing a `doc_id`
  column or if any `doc_id` is empty. Recommended: set doc_id = parent_doc_id in curated essays.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import List

import pandas as pd
import re
from daydreaming_dagster.utils.evaluation_parsing_config import load_parser_map
import json
import pandas as pd
from daydreaming_dagster.utils.ids import reserve_doc_id


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # Source of truth for curated generations
    p.add_argument("--input", type=Path, default=Path("data/2_tasks/essay_generation_tasks.csv"), help="Path to essay_generation_tasks.csv")
    p.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root (default: data)")
    p.add_argument("--dry-run", action="store_true", help="Compute and print actions but make no changes")
    # Reset by default; use --add-only to avoid clearing existing partitions
    p.add_argument("--add-only", action="store_true", help="Do not clear existing dynamic partitions (add-only mode)")
    # Optionally materialize content_combinations after updating selection
    materialize_group = p.add_mutually_exclusive_group()
    materialize_group.add_argument(
        "--materialize-combos",
        dest="materialize_combos",
        action="store_true",
        help="Materialize content_combinations (and core task assets) after updating selection in the ACTIVE Dagster instance (default: on)",
    )
    materialize_group.add_argument(
        "--no-materialize-combos",
        dest="materialize_combos",
        action="store_false",
        help="Do not materialize content_combinations automatically",
    )
    p.set_defaults(materialize_combos=True)
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


def _report_missing_concepts_in_selection(data_root: Path) -> None:
    """Scan selected_combo_mappings.csv for concept_ids not present in concepts_metadata.csv.

    Always prints a short report if the selected file exists. Does not exit non-zero.
    """
    sel_path = data_root / "2_tasks" / "selected_combo_mappings.csv"
    meta_path = data_root / "1_raw" / "concepts_metadata.csv"
    if not sel_path.exists() or not meta_path.exists():
        return
    try:
        sel = pd.read_csv(sel_path)
        meta = pd.read_csv(meta_path)
    except Exception as e:
        print(f"Warning: could not read selection/metadata CSVs for missing-concepts report: {e}", file=sys.stderr)
        return
    if "concept_id" not in sel.columns or "combo_id" not in sel.columns or "concept_id" not in meta.columns:
        return
    selected_ids = set(sel["concept_id"].astype(str))
    known_ids = set(meta["concept_id"].astype(str))
    missing_ids = sorted(selected_ids - known_ids)
    if not missing_ids:
        print("Selected combos: no missing concept_ids detected (selection is consistent with concepts_metadata.csv)")
        return
    bad_combos = (
        sel[sel["concept_id"].astype(str).isin(missing_ids)]["combo_id"].astype(str).dropna().unique().tolist()
    )
    bad_combos.sort()
    print("WARNING: Missing concept_ids in selection detected:")
    print(" - missing concept_ids (first 20):", missing_ids[:20])
    print(f" - total missing concept_ids: {len(missing_ids)}")
    print(f" - combos referencing missing concepts ({len(bad_combos)}):", bad_combos[:20])

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


# Deprecated DB helpers removed — filesystem + curated CSVs are the only sources now.


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
    input_abs = str(Path(args.input).resolve())
    try:
        df = pd.read_csv(args.input)
    except Exception as e:
        print(f"Failed to read input CSV: {input_abs} — {e}", file=sys.stderr)
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

    # EARLY VALIDATION: require essay document ids for evaluation lineage before any writes
    docs_col = "essay_doc_id" if "essay_doc_id" in df.columns else ("parent_doc_id" if "parent_doc_id" in df.columns else None)
    if docs_col is None:
        print(
            "Error: input CSV is missing required column 'essay_doc_id' (temporary alias: 'parent_doc_id').\n"
            "       Each row must include the essay document id to pin evaluation lineage (doc-id first).\n"
            f"       File: {input_abs}",
            file=sys.stderr,
        )
        return 1
    # Check non-empty values (treat 'nan' as empty)
    missing_mask = df[docs_col].astype(str).map(lambda s: not bool(s.strip()) or s.lower() == "nan")
    missing_count = int(missing_mask.sum())
    if missing_count:
        sample = df.loc[missing_mask, "essay_task_id"].astype(str).head(5).tolist() if "essay_task_id" in df.columns else []
        print(
            "Error: essay document ids are required but missing for some rows.\n"
            f"       Column '{docs_col}' has {missing_count} empty/missing values.\n"
            f"       Sample essay_task_id(s) missing ids: {sample}.\n"
            "       Ensure essay responses were produced and recorded, then populate 'essay_doc_id' in the input CSV.\n"
            "       Tip: you can read doc IDs from data/docs/essay/*/metadata.json or from essay_response run metadata.\n"
            f"       File: {input_abs}",
            file=sys.stderr,
        )
        return 1

    # NEW: Require doc_id column for essay tasks to support prompt persistence via DocsPromptIOManager
    if "doc_id" not in df.columns:
        print(
            "Error: input CSV is missing required column 'doc_id'.\n"
            "       Essay tasks must include their own doc_id so prompts can be persisted to data/docs/essay/<doc_id>.\n"
            "       Tip: for curated tasks, set doc_id = parent_doc_id.\n"
            f"       File: {input_abs}",
            file=sys.stderr,
        )
        return 1
    doc_missing_mask = df["doc_id"].astype(str).map(lambda s: not bool(s.strip()) or s.lower() == "nan")
    if int(doc_missing_mask.sum()) > 0:
        sample = df.loc[doc_missing_mask, "essay_task_id"].astype(str).head(5).tolist() if "essay_task_id" in df.columns else []
        print(
            "Error: some rows in essay_generation_tasks.csv have empty 'doc_id' values.\n"
            f"       Missing count: {int(doc_missing_mask.sum())}. Sample essay_task_id(s): {sample}\n"
            f"       File: {input_abs}",
            file=sys.stderr,
        )
        return 1

    # Write/merge outputs
    essay_out_csv = data_root / "2_tasks" / "essay_generation_tasks.csv"
    link_out_csv = data_root / "2_tasks" / "draft_generation_tasks.csv"

    # Ensure output directory exists
    two_tasks = data_root / "2_tasks"
    two_tasks.mkdir(parents=True, exist_ok=True)

    # Write selected combo mappings for chosen combinations
    _write_selected_combo_mappings(data_root, sorted(combo_ids), dry_run=args.dry_run)
    # Do NOT modify essay_generation_tasks.csv here (BACKCOMPAT: scripted curation is the source of truth)
    # Always report missing concepts in the current selection to catch mismatches early
    _report_missing_concepts_in_selection(data_root)
    # Optionally materialize content_combinations to ensure Dagster sees the updated selection
    if args.materialize_combos and not args.dry_run:
        # Materialize into the ACTIVE instance (DAGSTER_HOME), so the UI sees updated assets
        try:
            from dagster import materialize, DagsterInstance
            from daydreaming_dagster.assets.group_task_definitions import (
                content_combinations as combos_asset,
                draft_generation_tasks as draft_tasks_asset,
                essay_generation_tasks as essay_tasks_asset,
                evaluation_tasks as eval_tasks_asset,
            )
            from daydreaming_dagster.resources.experiment_config import ExperimentConfig
            from daydreaming_dagster.resources.io_managers import CSVIOManager

            resources = {
                "experiment_config": ExperimentConfig(),
                "data_root": str(data_root),
                # Ensure CSV outputs for task assets land under data_root/2_tasks
                "csv_io_manager": CSVIOManager(base_path=data_root / "2_tasks"),
            }
            instance = DagsterInstance.get()
            # First ensure content_combinations is refreshed for the active selection
            res1 = materialize([combos_asset], resources=resources, instance=instance)
            if not res1.success:
                print("Warning: materialize(content_combinations) did not succeed", file=sys.stderr)
            # Then refresh the task assets so dynamic partitions and CSVs are consistent
            res2 = materialize([draft_tasks_asset, essay_tasks_asset, eval_tasks_asset], resources=resources, instance=instance)
            if not res2.success:
                print("Warning: materialize(task assets) did not fully succeed", file=sys.stderr)
        except Exception as e:
            print(f"Warning: failed to materialize assets in active instance: {e}", file=sys.stderr)

    added_essays = 0
    added_drafts = 0
    # Do not write essay_generation_tasks.csv here; treat it as source of truth

    if draft_rows:
        # Integrity WARNINGS ONLY (no filtering here): report missing template files for awareness
        try:
            tpl_root = data_root / "1_raw" / "generation_templates" / "draft"
            missing_tpls = sorted({
                str(r.get("draft_template") or "")
                for r in draft_rows
                if r.get("draft_template") and not (tpl_root / f"{r.get('draft_template')}.txt").exists()
            })
            if missing_tpls and not args.dry_run:
                print("Warning: draft task rows reference draft templates without files present:")
                print(" - missing template_ids (first 10):", missing_tpls[:10])
                print(" - total missing template_ids:", len(missing_tpls))
        except Exception:
            pass
        # Reserve doc_ids for drafts
        for r in draft_rows:
            r["doc_id"] = reserve_doc_id("draft", str(r["draft_task_id"]))
        added_drafts = _write_table(
            link_out_csv,
            draft_rows,
            key="draft_task_id",
            columns=[
                "draft_task_id","combo_id","draft_template","generation_model","generation_model_name","doc_id",
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
        docs_root = Path(data_root) / "docs"
        # Require essay doc id column; accept either 'essay_doc_id' or 'parent_doc_id' (temporary naming)
        essay_doc_col = 'essay_doc_id' if 'essay_doc_id' in df.columns else ('parent_doc_id' if 'parent_doc_id' in df.columns else None)
        if essay_doc_col is None:
            print("Input CSV must contain 'essay_doc_id' (or temporary alias 'parent_doc_id') to build evaluation_tasks.csv with pinned doc IDs", file=sys.stderr)
            return 1
        for _, r in df.iterrows():
            essay_task_id = str(r["essay_task_id"]) if "essay_task_id" in r else ""
            essay_doc_id = str(r.get(essay_doc_col) or "").strip()
            if not essay_doc_id:
                print(f"Error: missing essay doc id in column '{essay_doc_col}' for essay_task_id={essay_task_id}", file=sys.stderr)
                return 1
            # Prefer docs store path for essay (parsed.txt if present, else raw.txt)
            doc_dir = docs_root / "essay" / essay_doc_id
            parsed_fp = doc_dir / "parsed.txt"
            raw_fp = doc_dir / "raw.txt"
            if parsed_fp.exists():
                file_path = str(parsed_fp)
            elif raw_fp.exists():
                file_path = str(raw_fp)
            else:
                file_path = ""
            # Also attempt to read the draft parent doc id from the essay's metadata.json
            draft_doc_id = None
            meta_path = docs_root / "essay" / essay_doc_id / "metadata.json"
            try:
                if meta_path.exists():
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    if isinstance(meta, dict):
                        val = meta.get("parent_doc_id")
                        if isinstance(val, str) and val.strip():
                            draft_doc_id = val.strip()
            except Exception:
                draft_doc_id = None
            for tpl in eval_tpls:
                for model in eval_models:
                    evaluation_rows.append({
                        # Use essay doc id as the evaluation target (parent for evaluation stage)
                        "evaluation_task_id": f"{essay_doc_id}__{tpl}__{model}",
                        "parent_doc_id": essay_doc_id,
                        # Optional visibility of essay->draft lineage for ops/debugging
                        "draft_doc_id": draft_doc_id or "",
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
                        "source_dir": "docs/essay" if doc_dir.exists() else "",
                        "source_asset": "essay_response",
                    })
    # Write evaluation tasks CSV
    eval_out_csv = data_root / "2_tasks" / "evaluation_tasks.csv"
    added_evals = 0
    if evaluation_rows:
        # Reserve doc_ids for evaluations
        for r in evaluation_rows:
            r["doc_id"] = reserve_doc_id("evaluation", str(r["evaluation_task_id"]))
        added_evals = _write_table(
            eval_out_csv,
            evaluation_rows,
            key="evaluation_task_id",
            columns=[
                "evaluation_task_id",
                # pinned lineage
                "parent_doc_id",
                "draft_doc_id",
                "doc_id",
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

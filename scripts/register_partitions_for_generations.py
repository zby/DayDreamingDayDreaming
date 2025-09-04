#!/usr/bin/env python3
"""
Create curated generation task rows and register Dagster dynamic partitions
for a provided list of generation document IDs (drafts and essays). Also
register evaluation partitions for all active evaluation templates and
evaluation-capable models.

Input formats:
- Line-delimited text file (one document_id per line)
- CSV with a 'document_id' column (or 'essay_task_id'/'draft_task_id')

Outputs:
- data/2_tasks/draft_generation_tasks.csv (merged; deduped by draft_task_id)
- data/2_tasks/essay_generation_tasks.csv (merged; deduped by essay_task_id)
- Registers dynamic partitions add-only (does not remove existing)
- Registers evaluation partitions for selected documents across active
  evaluation templates and models

Usage examples:
  # Use the output of scripts/find_top_prior_art.py
  uv run python scripts/register_partitions_for_generations.py \
    --input data/2_tasks/selected_generations.txt

  # CSV input with document_id
  uv run python scripts/register_partitions_for_generations.py \
    --input data/2_tasks/selected_generations.csv

Flags:
- --no-write-drafts: do not write draft_generation_tasks.csv (essays only)
- --no-register: do not register Dagster partitions (CSV only)
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import Iterable, List, Tuple

import pandas as pd
import re


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", type=Path, default=Path("data/2_tasks/selected_generations.txt"), help="Path to list/CSV of document IDs")
    p.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root (default: data)")
    p.add_argument("--write-drafts", dest="write_drafts", action="store_true", default=True, help="Write draft_generation_tasks rows (default: on)")
    p.add_argument("--no-write-drafts", dest="write_drafts", action="store_false", help="Do not write draft_generation_tasks rows")
    p.add_argument("--register", dest="register", action="store_true", default=True, help="Register dynamic partitions in Dagster (default: on)")
    p.add_argument("--no-register", dest="register", action="store_false", help="Do not register dynamic partitions")
    p.add_argument("--reset-partitions", dest="reset_partitions", action="store_true", default=True, help="Reset (clear) existing dynamic partitions before adding new ones (default: on)")
    p.add_argument("--no-reset-partitions", dest="reset_partitions", action="store_false", help="Do not clear existing dynamic partitions (add-only)")
    p.add_argument("--clean-2-tasks", dest="clean_2_tasks", action="store_true", default=True, help="Clean data/2_tasks before writing curated CSVs (default: on)")
    p.add_argument("--no-clean-2-tasks", dest="clean_2_tasks", action="store_false", help="Do not clean data/2_tasks (write only curated files)")
    p.add_argument("--eval-templates", nargs="*", default=None, help="Evaluation templates to register (override active)")
    p.add_argument("--eval-models", nargs="*", default=None, help="Evaluation model ids to register (override active for_evaluation)")
    p.add_argument("--dry-run", action="store_true", help="Compute and print what would be written/registered, but make no changes")
    p.add_argument("--write-keys-dir", type=Path, default=None, help="If set, write draft/essay/evaluation partition keys into this directory as text files")
    return p.parse_args()


def _read_list(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Input not found: {path}")
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
        for col in ("document_id", "essay_task_id", "draft_task_id"):
            if col in df.columns:
                vals = df[col].dropna().astype(str).tolist()
                if vals:
                    return vals
        raise ValueError("CSV must include one of: document_id, essay_task_id, draft_task_id")
    # Treat as line-delimited
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _load_templates(data_root: Path) -> Tuple[set[str], set[str]]:
    draft_tpls: set[str] = set()
    essay_tpls: set[str] = set()
    d_csv = data_root / "1_raw" / "draft_templates.csv"
    l_csv = data_root / "1_raw" / "link_templates.csv"
    e_csv = data_root / "1_raw" / "essay_templates.csv"
    try:
        if d_csv.exists():
            df = pd.read_csv(d_csv)
            if "template_id" in df.columns:
                draft_tpls = set(df["template_id"].astype(str))
        elif l_csv.exists():
            df = pd.read_csv(l_csv)
            if "template_id" in df.columns:
                draft_tpls = set(df["template_id"].astype(str))
        if e_csv.exists():
            df = pd.read_csv(e_csv)
            if "template_id" in df.columns:
                essay_tpls = set(df["template_id"].astype(str))
    except Exception as e:
        print(f"Warning: failed to read template CSVs ({e}); falling back to naive parsing", file=sys.stderr)
    return draft_tpls, essay_tpls


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

def _parse_draft_tokens(doc: str, draft_tpls: set[str], model_ids: set[str]) -> tuple[str|None, str|None, str|None]:
    """Parse combo_id, draft_template, generation_model from a draft-like id.

    Robust to underscores in model ids by matching known model ids as a suffix first,
    then matching a known draft template as the next suffix.
    """
    # 1) Match model id by longest-suffix
    gen_model = None
    if model_ids:
        for mid in sorted(model_ids, key=len, reverse=True):
            suffix = "_" + mid
            if doc.endswith(suffix):
                gen_model = mid
                base = doc[: -len(suffix)]
                break
        else:
            base = doc
    else:
        base = doc

    # 2) Match draft template as suffix
    draft_template = None
    combo_id = None
    if draft_tpls:
        for tpl in sorted(draft_tpls, key=len, reverse=True):
            suffix = "_" + tpl
            if base.endswith(suffix):
                draft_template = tpl
                combo_id = base[: -len(suffix)]
                break

    # Fallback naive parsing if sets unavailable
    if combo_id is None or draft_template is None or gen_model is None:
        parts = doc.split("_")
        if len(parts) >= 3:
            draft_template = draft_template or parts[-2]
            gen_model = gen_model or parts[-1]
            combo_id = combo_id or "_".join(parts[:-2])
        else:
            return None, None, None
    # Validate against known ids/templates when available; if invalid, force parse failure
    if model_ids and gen_model not in model_ids:
        gen_model = None
    if draft_tpls and draft_template not in draft_tpls:
        draft_template = None
    if combo_id and draft_template and gen_model:
        return combo_id, draft_template, gen_model
    return None, None, None

_COMBO_PREFIX_RE = re.compile(r"^combo_v\d+_[0-9a-f]{12}")

def _extract_combo_prefix(doc: str) -> str | None:
    """Extract stable combo_id prefix (combo_vN_<12-hex>) from any doc string."""
    m = _COMBO_PREFIX_RE.match(doc)
    if m:
        return m.group(0)
    # Fallback token check: first three underscore-separated tokens
    parts = doc.split("_")
    if len(parts) >= 3 and parts[0] == "combo" and parts[1].startswith("v") and len(parts[2]) == 12:
        # ensure hex
        try:
            int(parts[2], 16)
            return "_".join([parts[0], parts[1], parts[2]])
        except Exception:
            return None
    return None

def _write_curated_combo_mappings(data_root: Path, combo_ids: List[str], dry_run: bool = False) -> int:
    """Filter data/combo_mappings.csv to selected combo_ids and write curated file under 2_tasks.

    Returns number of rows written (or that would be written in dry-run).
    """
    mapping_csv = data_root / "combo_mappings.csv"
    if not mapping_csv.exists():
        print(f"Warning: {mapping_csv} not found; cannot write curated combo mappings", file=sys.stderr)
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

    # Keep minimal columns expected by the content_combinations asset
    cols = [c for c in ["combo_id", "description_level", "concept_id"] if c in subset.columns]
    curated = subset[cols].copy()

    out = data_root / "2_tasks" / "curated_combo_mappings.csv"
    if dry_run:
        print(f"[dry-run] Would write {len(curated)} curated combo-mapping rows to {out}")
        return len(curated)
    out.parent.mkdir(parents=True, exist_ok=True)
    curated.to_csv(out, index=False)
    print(f"Wrote curated combo mappings: {out} ({len(curated)} rows)")
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
    input_ids = _read_list(args.input)
    if not input_ids:
        print("No document IDs to process.")
        return 0

    draft_tpls, essay_tpls = _load_templates(data_root)
    model_map = _load_models_map(data_root)
    model_ids = _load_model_ids(data_root)

    essay_rows: List[dict] = []
    draft_rows: List[dict] = []
    combo_ids: set[str] = set()

    for doc in input_ids:
        essay_template = None
        draft_template = None
        generation_model = None
        combo_id = None

        # Essay doc if endswith _<essay_template>
        matched_essay = None
        if essay_tpls:
            for tpl in sorted(essay_tpls, key=len, reverse=True):
                if doc.endswith("_" + tpl):
                    matched_essay = tpl
                    break

        if matched_essay is not None:
            essay_template = matched_essay
            draft_task_id = doc[: -(len(essay_template) + 1)]
            combo_id, draft_template, generation_model = _parse_draft_tokens(draft_task_id, draft_tpls, model_ids)
            if not (combo_id and draft_template and generation_model):
                # Attempt to salvage combo_id from prefix only; allow curated combos to be written even if templates/models are unknown
                combo_id = _extract_combo_prefix(doc)
                if not combo_id:
                    print(f"Skipping malformed essay id (unable to parse draft part): {doc}", file=sys.stderr)
                    continue
                # Do not append task rows if we cannot parse draft side reliably; rely on curated combos
                combo_ids.add(combo_id)
                continue
            combo_ids.add(combo_id)
            essay_rows.append({
                "essay_task_id": doc,
                "draft_task_id": draft_task_id,
                "combo_id": combo_id,
                "draft_template": draft_template,
                "essay_template": essay_template,
                "generation_model": generation_model,
                "generation_model_name": model_map.get(generation_model, generation_model),
            })
            # Also ensure draft task exists when requested
            if args.write_drafts:
                draft_rows.append({
                    "draft_task_id": draft_task_id,
                    "combo_id": combo_id,
                    "draft_template": draft_template,
                    "generation_model": generation_model,
                    "generation_model_name": model_map.get(generation_model, generation_model),
                })
        else:
            # Treat as a draft document id (or legacy single-phase id without essay template suffix)
            combo_id, draft_template, generation_model = _parse_draft_tokens(doc, draft_tpls, model_ids)
            if not (combo_id and draft_template and generation_model):
                # Salvage combo from prefix
                prefix = _extract_combo_prefix(doc)
                if prefix:
                    combo_ids.add(prefix)
                else:
                    print(f"Skipping malformed draft id (parsed): {doc}", file=sys.stderr)
                continue
            if args.write_drafts:
                draft_rows.append({
                    "draft_task_id": doc,
                    "combo_id": combo_id,
                    "draft_template": draft_template,
                    "generation_model": generation_model,
                    "generation_model_name": model_map.get(generation_model, generation_model),
                })
            combo_ids.add(combo_id)

    # Write/merge outputs
    essay_out_csv = data_root / "2_tasks" / "essay_generation_tasks.csv"
    link_out_csv = data_root / "2_tasks" / "draft_generation_tasks.csv"

    # Optionally clean data/2_tasks (non-cube selection)
    two_tasks = data_root / "2_tasks"
    if args.clean_2_tasks and not args.dry_run:
        if two_tasks.exists():
            removed = 0
            preserve = {"selected_generations.txt", "selected_generations.csv"}
            for p in two_tasks.iterdir():
                if p.is_file() and p.name not in preserve:
                    try:
                        p.unlink()
                        removed += 1
                    except Exception as e:
                        print(f"Warning: failed to remove {p}: {e}", file=sys.stderr)
            print(f"Cleaned data/2_tasks (removed {removed} files; preserved {', '.join(sorted(preserve))})")
        else:
            two_tasks.mkdir(parents=True, exist_ok=True)
    elif args.clean_2_tasks and args.dry_run:
        print("[dry-run] Would clean data/2_tasks (remove existing files)")

    # Write curated combo mappings for selected combinations
    _write_curated_combo_mappings(data_root, sorted(combo_ids), dry_run=args.dry_run)

    added_essays = 0
    added_drafts = 0
    if essay_rows:
        added_essays = _write_table(
            essay_out_csv,
            essay_rows,
            key="essay_task_id",
            columns=[
                "essay_task_id","draft_task_id","combo_id","draft_template",
                "essay_template","generation_model","generation_model_name",
            ],
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            print(f"Wrote curated {essay_out_csv} ({added_essays} rows)")

    if args.write_drafts and draft_rows:
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
            print(f"Wrote curated {link_out_csv} ({added_drafts} rows)")

    # Note: Do not write cross-experiment task tables here; cross-experiment tracking is driven by results appenders

    # Register dynamic partitions (add-only)
    # Prepare partition keys
    draft_ids = [r["draft_task_id"] for r in draft_rows] if args.write_drafts and draft_rows else []
    essay_ids = [r["essay_task_id"] for r in essay_rows] if essay_rows else []

    # Evaluation axes (with optional overrides)
    eval_tpls, eval_models = _load_active_evaluation_axes(data_root)
    if args.eval_templates is not None:
        eval_tpls = list(args.eval_templates)
    if args.eval_models is not None:
        eval_models = list(args.eval_models)
    eval_docs = []
    if essay_rows:
        eval_docs.extend([r["essay_task_id"] for r in essay_rows])
    if args.write_drafts and draft_rows:
        eval_docs.extend([r["draft_task_id"] for r in draft_rows])
    eval_docs = list({d for d in eval_docs if isinstance(d, str) and d})
    eval_ids = [f"{doc}__{tpl}__{model}" for doc in eval_docs for tpl in eval_tpls for model in eval_models] if (eval_docs and eval_tpls and eval_models) else []

    # Optionally write partition lists
    if args.write_keys_dir:
        args.write_keys_dir.mkdir(parents=True, exist_ok=True)
        (args.write_keys_dir / "draft_partitions.txt").write_text("\n".join(draft_ids), encoding="utf-8")
        (args.write_keys_dir / "essay_partitions.txt").write_text("\n".join(essay_ids), encoding="utf-8")
        (args.write_keys_dir / "evaluation_partitions.txt").write_text("\n".join(eval_ids), encoding="utf-8")
        print(f"Wrote partition key lists under {args.write_keys_dir}")

    if args.register and not args.dry_run:
        try:
            from dagster import DagsterInstance
            instance = DagsterInstance.get()
            
            def _unique_str(values: list[str]) -> list[str]:
                seen, out = set(), []
                for v in values:
                    if isinstance(v, str) and v and v not in seen:
                        seen.add(v); out.append(v)
                return out

            # Optionally clear existing partitions first (non-cube curated selection)
            if args.reset_partitions:
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

    if not args.dry_run and not (added_essays or added_drafts):
        print("No new rows were added (all selected items already present)")
    if args.dry_run:
        print(f"[dry-run] Drafts: {len(draft_ids)}, Essays: {len(essay_ids)}, Evaluations: {len(eval_ids)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

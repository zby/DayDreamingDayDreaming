#!/usr/bin/env python3
"""
Build evaluation_results.csv from existing evaluation response files (two-phase and legacy).

Usage:
    python scripts/build_evaluation_results_table.py

What it does:
- Two-phase (current):
  - Scans `data/4_evaluation/evaluation_responses/` for `.txt` files
  - Matches files to tasks in `data/2_tasks/evaluation_tasks.csv` and joins
    to `data/2_tasks/essay_generation_tasks.csv` to gather combo/template/model metadata
  - Appends rows to `data/7_cross_experiment/evaluation_results.csv` with the same
    columns produced by the auto-tracking asset
- Legacy (fallback):
  - If task CSVs are missing, parses filenames heuristically and fills a best-effort row

When to use:
- Same as generation table rebuild: initial migration, recovery, or historical analysis

Notes:
- The active pipeline evaluates by essay_task_id (two-phase). This script prefers
  task CSV joins and falls back to legacy filename parsing only if needed.

Error handling:
- Continues on individual file errors and prints a summary at the end
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import sys
import os

# Add project root to path to import dagster modules
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from daydreaming_dagster.assets.cross_experiment import append_to_results_csv
from filename_parser import parse_evaluation_filename


def _safe_read_csv(path: Path):
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        return None


def rebuild_evaluation_results():
    """Scan all existing evaluation responses and build the table (two-phase preferred)."""
    
    print("🔍 Loading task metadata...")
    
    # Load task metadata (prefer two-phase joins)
    evaluation_tasks = _safe_read_csv(Path("data/2_tasks/evaluation_tasks.csv"))
    essay_generation_tasks = _safe_read_csv(Path("data/2_tasks/essay_generation_tasks.csv"))
    generation_tasks = _safe_read_csv(Path("data/2_tasks/generation_tasks.csv"))  # legacy only
    if evaluation_tasks is not None:
        print(f"✅ Loaded {len(evaluation_tasks)} evaluation tasks for matching")
    else:
        print("⚠️  Missing data/2_tasks/evaluation_tasks.csv")
    if essay_generation_tasks is not None:
        print(f"✅ Loaded {len(essay_generation_tasks)} essay generation tasks for joining")
    else:
        print("⚠️  Missing data/2_tasks/essay_generation_tasks.csv (two-phase join unavailable)")
    if generation_tasks is not None:
        print(f"ℹ️  Loaded {len(generation_tasks)} legacy generation tasks (fallback only)")
    
    # Scan evaluation response files
    response_dir = Path("data/4_evaluation/evaluation_responses/")
    
    if not response_dir.exists():
        print(f"❌ Error: {response_dir} directory not found")
        return False
    
    print(f"🔍 Scanning {response_dir} for response files...")
    response_files = list(response_dir.glob("*.txt"))
    print(f"📁 Found {len(response_files)} response files")
    
    processed_count = 0
    errors = []
    rows: list[dict] = []
    
    for response_file in response_files:
        task_id = response_file.stem  # filename without extension
        
        # Preferred path: two-phase join via evaluation_tasks -> essay_generation_tasks
        new_row = None
        if evaluation_tasks is not None and essay_generation_tasks is not None:
            et = evaluation_tasks[evaluation_tasks["evaluation_task_id"] == task_id]
            if not et.empty:
                et_row = et.iloc[0]
                essay_task_id = et_row["essay_task_id"] if "essay_task_id" in et_row else None
                if essay_task_id is not None:
                    es = essay_generation_tasks[essay_generation_tasks["essay_task_id"] == essay_task_id]
                    if not es.empty:
                        es_row = es.iloc[0]
                        new_row = {
                            "evaluation_task_id": task_id,
                            "essay_task_id": essay_task_id,
                            "combo_id": es_row.get("combo_id", "unknown"),
                            "link_template": es_row.get("link_template", "unknown"),
                            "essay_template": es_row.get("essay_template", "unknown"),
                            "generation_model": es_row.get("generation_model_name", "unknown"),
                            "evaluation_template": et_row.get("evaluation_template", "unknown"),
                            "evaluation_model": et_row.get("evaluation_model_name", "unknown"),
                            "evaluation_status": "success",
                            "evaluation_timestamp": datetime.fromtimestamp(response_file.stat().st_mtime).isoformat(),
                            "eval_response_file": f"evaluation_responses/{task_id}.txt",
                            "eval_response_size_bytes": response_file.stat().st_size,
                        }

        # Fallback to legacy parsing if two-phase join failed
        if new_row is None:
            parsed = parse_evaluation_filename(response_file.name)
            if parsed is None:
                error_msg = f"⚠️  Could not match CSVs or parse filename: {response_file.name}"
                print(error_msg)
                errors.append(error_msg)
                continue
            print(f"📝 Parsed (legacy): {response_file.name} -> {parsed['combo_id']}, {parsed['generation_template']}, {parsed['evaluation_template']}")
            new_row = {
                "evaluation_task_id": task_id,
                "generation_task_id": parsed.get("generation_task_id", "unknown"),
                "combo_id": parsed["combo_id"],
                "generation_template": parsed["generation_template"],
                "generation_model": parsed["generation_model"],
                "evaluation_template": parsed["evaluation_template"],
                "evaluation_model": parsed["evaluation_model"],
                "evaluation_status": "success",
                "evaluation_timestamp": datetime.fromtimestamp(response_file.stat().st_mtime).isoformat(),
                "eval_response_file": f"evaluation_responses/{task_id}.txt",
                "eval_response_size_bytes": response_file.stat().st_size,
            }
        
        # Append to table
        # Collect row; we will write a single consistent CSV at the end
        rows.append(new_row)
        processed_count += 1
        if processed_count % 200 == 0:
            print(f"✅ Collected {processed_count} files...")
    
    print(f"\n🎉 Completed! Processed {processed_count} evaluation responses")

    # Write a single CSV with a canonical schema to avoid column mismatch
    out_path = Path("data/7_cross_experiment/evaluation_results.csv")
    canonical_columns = [
        "evaluation_task_id",
        "essay_task_id",
        "generation_task_id",
        "combo_id",
        "link_template",
        "essay_template",
        "generation_template",
        "generation_model",
        "evaluation_template",
        "evaluation_model",
        "evaluation_status",
        "evaluation_timestamp",
        "eval_response_file",
        "eval_response_size_bytes",
    ]
    if rows:
        df_out = pd.DataFrame(rows)
        # Ensure all canonical columns exist; preserve order
        for col in canonical_columns:
            if col not in df_out.columns:
                df_out[col] = None
        df_out = df_out[canonical_columns]
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(out_path, index=False)
        print(f"📁 Wrote {len(df_out)} rows to {out_path}")
    
    if errors:
        print(f"\n⚠️  Encountered {len(errors)} errors:")
        for error in errors[:10]:  # Show first 10 errors
            print(f"   {error}")
        if len(errors) > 10:
            print(f"   ... and {len(errors) - 10} more errors")
    
    return processed_count > 0


if __name__ == "__main__":
    print("🚀 Building evaluation_results.csv from existing data...")
    
    # Check if we're in the right directory
    if not Path("data").exists():
        print("❌ Error: 'data' directory not found")
        print("   Please run this script from the project root directory")
        sys.exit(1)
    
    success = rebuild_evaluation_results()
    
    if success:
        print("✅ evaluation_results.csv built successfully")
        
        # Show summary of created table
        results_file = Path("data/7_cross_experiment/evaluation_results.csv")
        if results_file.exists():
            df = pd.read_csv(results_file)
            print(f"📊 Table summary: {len(df)} rows, {len(df.columns)} columns")
            print(f"📁 Table location: {results_file}")
        
        sys.exit(0)
    else:
        print("❌ Failed to build evaluation_results.csv")
        sys.exit(1)

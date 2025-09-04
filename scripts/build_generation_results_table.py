#!/usr/bin/env python3
"""
Build cross-experiment generation tracking tables from existing response files.

Usage:
    python scripts/build_generation_results_table.py

What it does:
- Two-phase (current):
  - Scans draft responses in `data/3_generation/draft_responses/` (falls back to legacy `links_responses/`) and essays in `data/3_generation/essay_responses/`
  - Matches files to tasks in `data/2_tasks/draft_generation_tasks.csv` (falls back to legacy `link_generation_tasks.csv`) and `data/2_tasks/essay_generation_tasks.csv`
  - Appends rows to:
      • `data/7_cross_experiment/draft_generation_results.csv`
      • `data/7_cross_experiment/essay_generation_results.csv`
    with metadata consistent with auto-tracking assets
- Legacy single-phase (optional):
  - Scans `data/3_generation/generation_responses/` for `.txt` files
  - Matches files to `data/2_tasks/generation_tasks.csv` when available; otherwise parses filenames
  - Appends rows to `data/7_cross_experiment/generation_results.csv`

When to use:
- Initial migration to cross-experiment tracking
- Recovery after table corruption
- Rebuilding tables when adding new columns
- Analyzing existing historical single-phase data

Notes:
- The active pipeline uses a two-phase system with auto-materializing tracking. This
  script backfills the same tables from existing files (useful for historical data
  or when the daemon wasn’t running).

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
from filename_parser import parse_generation_filename


def _safe_read_csv(path: Path):
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        return None


def rebuild_two_phase_generation_results() -> int:
    """Backfill two-phase tracking tables (drafts and essays). Returns rows appended."""
    total_appended = 0

    # Load task metadata (prefer draft; fallback to link)
    draft_tasks = _safe_read_csv(Path("data/2_tasks/draft_generation_tasks.csv"))
    link_tasks = _safe_read_csv(Path("data/2_tasks/link_generation_tasks.csv"))
    essay_tasks = _safe_read_csv(Path("data/2_tasks/essay_generation_tasks.csv"))

    # Drafts
    # Support both new draft and legacy links locations; prefer draft files if both exist
    draft_dir = Path("data/3_generation/draft_responses")
    legacy_dir = Path("data/3_generation/links_responses")
    seen: set[str] = set()
    if draft_dir.exists() or legacy_dir.exists():
        if draft_dir.exists():
            print(f"🔍 Scanning {draft_dir} for draft responses...")
            for f in draft_dir.glob("*.txt"):
                link_task_id = f.stem  # equals draft_task_id in new schema
                seen.add(link_task_id)
                row = None
                # Prefer draft_generation_tasks.csv
                if draft_tasks is not None:
                    m = draft_tasks[draft_tasks["draft_task_id"] == link_task_id]
                    if not m.empty:
                        row = m.iloc[0]
                # Fallback: legacy link_generation_tasks.csv
                if row is None and link_tasks is not None:
                    m = link_tasks[link_tasks["link_task_id"] == link_task_id]
                    if not m.empty:
                        row = m.iloc[0]
                if row is None:
                    # Last-resort: parse filename heuristically to recover metadata
                    from scripts.filename_parser import parse_generation_filename
                    parsed = parse_generation_filename(f.name)
                    if parsed is None:
                        print(f"⚠️  No task metadata for draft {link_task_id} and filename parse failed; skipping")
                        continue
                    combo_id = parsed.get("combo_id")
                    draft_template = parsed.get("generation_template")
                    generation_model = parsed.get("generation_model")
                else:
                    combo_id = row["combo_id"]
                    draft_template = row["draft_template"] if "draft_template" in row else row.get("link_template")
                    generation_model = row.get("generation_model_name") or row.get("generation_model")

                new_row = {
                    "draft_task_id": link_task_id,
                    "combo_id": combo_id,
                    "draft_template_id": draft_template,
                    "generation_model": generation_model,
                    "generation_status": "success",
                    "generation_timestamp": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
                    "response_file": f"{draft_dir.name}/{link_task_id}.txt",
                    "response_size_bytes": f.stat().st_size,
                }
                append_to_results_csv("data/7_cross_experiment/draft_generation_results.csv", new_row)
                total_appended += 1
        if legacy_dir.exists():
            print(f"🔍 Scanning {legacy_dir} for legacy draft responses...")
            for f in legacy_dir.glob("*.txt"):
                link_task_id = f.stem
                if link_task_id in seen:
                    continue
                row = None
                if draft_tasks is not None:
                    m = draft_tasks[draft_tasks["draft_task_id"] == link_task_id]
                    if not m.empty:
                        row = m.iloc[0]
                if row is None and link_tasks is not None:
                    m = link_tasks[link_tasks["link_task_id"] == link_task_id]
                    if not m.empty:
                        row = m.iloc[0]
                if row is None:
                    from scripts.filename_parser import parse_generation_filename
                    parsed = parse_generation_filename(f.name)
                    if parsed is None:
                        print(f"⚠️  No task metadata for legacy draft {link_task_id} and filename parse failed; skipping")
                        continue
                    combo_id = parsed.get("combo_id")
                    draft_template = parsed.get("generation_template")
                    generation_model = parsed.get("generation_model")
                else:
                    combo_id = row["combo_id"]
                    draft_template = row["draft_template"] if "draft_template" in row else row.get("link_template")
                    generation_model = row.get("generation_model_name") or row.get("generation_model")
                new_row = {
                    "draft_task_id": link_task_id,
                    "combo_id": combo_id,
                    "draft_template_id": draft_template,
                    "generation_model": generation_model,
                    "generation_status": "success",
                    "generation_timestamp": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
                    "response_file": f"{legacy_dir.name}/{link_task_id}.txt",
                    "response_size_bytes": f.stat().st_size,
                }
                append_to_results_csv("data/7_cross_experiment/draft_generation_results.csv", new_row)
                total_appended += 1

    # Essays
    essays_dir = Path("data/3_generation/essay_responses")
    if essays_dir.exists():
        print(f"🔍 Scanning {essays_dir} for essay responses...")
        for f in essays_dir.glob("*.txt"):
            essay_task_id = f.stem
            row = None
            if essay_tasks is not None:
                m = essay_tasks[essay_tasks["essay_task_id"] == essay_task_id]
                if not m.empty:
                    row = m.iloc[0]
            if row is None:
                print(f"⚠️  No task metadata for essay {essay_task_id}; skipping")
                continue
            new_row = {
                "essay_task_id": essay_task_id,
                "combo_id": row["combo_id"],
                "essay_template_id": row["essay_template"],
                "generation_model": row["generation_model_name"],
                "generation_status": "success",
                "generation_timestamp": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
                "response_file": f"essay_responses/{essay_task_id}.txt",
                "response_size_bytes": f.stat().st_size,
            }
            append_to_results_csv("data/7_cross_experiment/essay_generation_results.csv", new_row)
            total_appended += 1

    return total_appended


def rebuild_legacy_single_phase_results() -> int:
    """Backfill legacy single-phase generation_results.csv. Returns rows appended."""
    
    print("🔍 Loading task metadata...")
    
    # Load task metadata (optional - we'll use filename parsing as fallback)
    generation_tasks = _safe_read_csv(Path("data/2_tasks/generation_tasks.csv"))
    if generation_tasks is not None:
        print(f"✅ Loaded {len(generation_tasks)} generation tasks for matching")
    else:
        print("⚠️  data/2_tasks/generation_tasks.csv not found; will parse filenames")
    
    # Scan generation response files
    response_dir = Path("data/3_generation/generation_responses/")
    
    if not response_dir.exists():
        print(f"ℹ️  Legacy directory not found: {response_dir} (skipping)")
        return 0
    
    print(f"🔍 Scanning {response_dir} for response files...")
    response_files = list(response_dir.glob("*.txt"))
    print(f"📁 Found {len(response_files)} response files")
    
    processed_count = 0
    errors = []
    
    for response_file in response_files:
        task_id = response_file.stem  # filename without extension
        
        # Try to find matching task first (if tasks CSV exists)
        task_row = None
        if generation_tasks is not None:
            task_rows = generation_tasks[generation_tasks["generation_task_id"] == task_id]
            if not task_rows.empty:
                task_row = task_rows.iloc[0]
        
        # If no task found, try filename parsing
        if task_row is None:
            parsed = parse_generation_filename(response_file.name)
            if parsed is None:
                error_msg = f"⚠️  Could not parse filename: {response_file.name}"
                print(error_msg)
                errors.append(error_msg)
                continue
            
            # Create synthetic task row from parsed data
            task_row = {
                "combo_id": parsed["combo_id"],
                "generation_template": parsed["generation_template"],
                "generation_model_name": parsed["generation_model"],
            }
            print(f"📝 Parsed filename: {response_file.name} -> {parsed['combo_id']}, {parsed['generation_template']}, {parsed['generation_model']}")
        
        # Create row data
        new_row = {
            "generation_task_id": task_id,
            "combo_id": task_row["combo_id"],
            "generation_template": task_row["generation_template"],
            "generation_model": task_row["generation_model_name"] if "generation_model_name" in task_row else task_row["generation_model"],
            "generation_status": "success",
            "generation_timestamp": datetime.fromtimestamp(response_file.stat().st_mtime).isoformat(),
            "response_file": f"generation_responses/{task_id}.txt",
            "response_size_bytes": response_file.stat().st_size
        }
        
        # Append to table
        try:
            append_to_results_csv("data/7_cross_experiment/generation_results.csv", new_row)
            processed_count += 1
            if processed_count % 100 == 0:
                print(f"✅ Processed {processed_count} files...")
        except Exception as e:
            error_msg = f"❌ Error processing {task_id}: {e}"
            print(error_msg)
            errors.append(error_msg)
    
    print(f"\n🎉 Completed! Processed {processed_count} generation responses")
    
    if errors:
        print(f"\n⚠️  Encountered {len(errors)} errors:")
        for error in errors[:10]:  # Show first 10 errors
            print(f"   {error}")
        if len(errors) > 10:
            print(f"   ... and {len(errors) - 10} more errors")
    
    return processed_count


if __name__ == "__main__":
    print("🚀 Backfilling cross-experiment generation tables from existing data...")
    
    # Check if we're in the right directory
    if not Path("data").exists():
        print("❌ Error: 'data' directory not found")
        print("   Please run this script from the project root directory")
        sys.exit(1)
    
    two_phase = rebuild_two_phase_generation_results()
    legacy = rebuild_legacy_single_phase_results()

    print(f"✅ Appended rows — two-phase: {two_phase}, legacy: {legacy}")

    # Summaries
    link_file = Path("data/7_cross_experiment/draft_generation_results.csv")
    essay_file = Path("data/7_cross_experiment/essay_generation_results.csv")
    legacy_file = Path("data/7_cross_experiment/generation_results.csv")

    if link_file.exists():
        df = pd.read_csv(link_file)
        print(f"📊 draft_generation_results.csv: {len(df)} rows")
    if essay_file.exists():
        df = pd.read_csv(essay_file)
        print(f"📊 essay_generation_results.csv: {len(df)} rows")
    if legacy_file.exists():
        df = pd.read_csv(legacy_file)
        print(f"📊 generation_results.csv (legacy): {len(df)} rows")

    sys.exit(0)

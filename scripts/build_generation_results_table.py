#!/usr/bin/env python3
"""
Build generation_results.csv from existing generation response files.

Usage:
    python scripts/build_generation_results_table.py

What it does:
- Scans legacy single-phase responses in `data/3_generation/generation_responses/` for all `.txt` files
- Matches files to tasks in `data/2_tasks/generation_tasks.csv` when available; otherwise parses filenames
- Appends rows to `data/7_cross_experiment/generation_results.csv` with:
  - generation_task_id, combo_id, generation_template, generation_model
  - generation_status, generation_timestamp, response_file, response_size_bytes

When to use:
- Initial migration to cross-experiment tracking
- Recovery after table corruption
- Rebuilding tables when adding new columns
- Analyzing existing historical single-phase data

Notes:
- The active pipeline uses a two-phase system (links/essay) with auto-materializing
  tracking assets. This script targets legacy single-phase outputs and is safe to
  keep for historical data.

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


def rebuild_generation_results():
    """Scan all existing generation responses and build the table."""
    
    print("🔍 Loading task metadata...")
    
    # Load task metadata (optional - we'll use filename parsing as fallback)
    generation_tasks = None
    try:
        generation_tasks = pd.read_csv("data/2_tasks/generation_tasks.csv")
        print(f"✅ Loaded {len(generation_tasks)} generation tasks for matching")
    except FileNotFoundError:
        print("⚠️  data/2_tasks/generation_tasks.csv not found")
        print("   Will rely on filename parsing for all files")
    
    # Scan generation response files
    response_dir = Path("data/3_generation/generation_responses/")
    
    if not response_dir.exists():
        print(f"❌ Error: {response_dir} directory not found")
        return False
    
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
    
    return processed_count > 0


if __name__ == "__main__":
    print("🚀 Building generation_results.csv from existing data...")
    
    # Check if we're in the right directory
    if not Path("data").exists():
        print("❌ Error: 'data' directory not found")
        print("   Please run this script from the project root directory")
        sys.exit(1)
    
    success = rebuild_generation_results()
    
    if success:
        print("✅ generation_results.csv built successfully")
        
        # Show summary of created table
        results_file = Path("data/7_cross_experiment/generation_results.csv")
        if results_file.exists():
            df = pd.read_csv(results_file)
            print(f"📊 Table summary: {len(df)} rows, {len(df.columns)} columns")
            print(f"📁 Table location: {results_file}")
        
        sys.exit(0)
    else:
        print("❌ Failed to build generation_results.csv")
        sys.exit(1)

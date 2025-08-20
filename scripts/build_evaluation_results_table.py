#!/usr/bin/env python3
"""
Build evaluation_results.csv from existing evaluation response files.

Usage:
    python scripts/build_evaluation_results_table.py

What it does:
- Scans legacy evaluation responses in `data/4_evaluation/evaluation_responses/` for all `.txt` files
- Matches files to tasks in `data/2_tasks/evaluation_tasks.csv` and `data/2_tasks/generation_tasks.csv`
  when available; otherwise parses filenames
- Appends rows to `data/7_cross_experiment/evaluation_results.csv` with:
  - evaluation_task_id, generation_task_id, combo_id
  - generation_template, generation_model, evaluation_template, evaluation_model
  - evaluation_status, evaluation_timestamp, eval_response_file, eval_response_size_bytes

When to use:
- Same as generation table rebuild: initial migration, recovery, or historical analysis

Notes:
- The active pipeline now evaluates by essay_task_id (two-phase). This script focuses on
  legacy single-phase linkage and filename parsing for existing data.

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


def rebuild_evaluation_results():
    """Scan all existing evaluation responses and build the table."""
    
    print("🔍 Loading task metadata...")
    
    # Load task metadata (optional - we'll use filename parsing as fallback)
    evaluation_tasks = None
    generation_tasks = None
    try:
        evaluation_tasks = pd.read_csv("data/2_tasks/evaluation_tasks.csv")
        generation_tasks = pd.read_csv("data/2_tasks/generation_tasks.csv")
        print(f"✅ Loaded {len(evaluation_tasks)} evaluation tasks for matching")
        print(f"✅ Loaded {len(generation_tasks)} generation tasks for matching")
    except FileNotFoundError:
        print("⚠️  Task CSV files not found")
        print("   Will rely on filename parsing for all files")
    
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
    
    for response_file in response_files:
        task_id = response_file.stem  # filename without extension
        
        # Try to find matching task first (if tasks CSV exists)
        eval_task_row = None
        gen_task_row = None
        
        if evaluation_tasks is not None and generation_tasks is not None:
            eval_task_rows = evaluation_tasks[evaluation_tasks["evaluation_task_id"] == task_id]
            if not eval_task_rows.empty:
                eval_task_row = eval_task_rows.iloc[0]
                
                # Get generation metadata via foreign key
                gen_task_id = eval_task_row["generation_task_id"]
                gen_task_rows = generation_tasks[generation_tasks["generation_task_id"] == gen_task_id]
                if not gen_task_rows.empty:
                    gen_task_row = gen_task_rows.iloc[0]
        
        # If no task found, try filename parsing
        if eval_task_row is None or gen_task_row is None:
            parsed = parse_evaluation_filename(response_file.name)
            if parsed is None:
                error_msg = f"⚠️  Could not parse filename: {response_file.name}"
                print(error_msg)
                errors.append(error_msg)
                continue
            
            # Create synthetic task rows from parsed data
            gen_task_id = parsed["generation_task_id"]
            print(f"📝 Parsed filename: {response_file.name} -> {parsed['combo_id']}, {parsed['generation_template']}, {parsed['evaluation_template']}")
            
            # Use parsed data directly
            new_row = {
                "evaluation_task_id": task_id,
                "generation_task_id": gen_task_id,
                "combo_id": parsed["combo_id"],
                "generation_template": parsed["generation_template"],
                "generation_model": parsed["generation_model"],
                "evaluation_template": parsed["evaluation_template"],
                "evaluation_model": parsed["evaluation_model"],
                "evaluation_status": "success",
                "evaluation_timestamp": datetime.fromtimestamp(response_file.stat().st_mtime).isoformat(),
                "eval_response_file": f"evaluation_responses/{task_id}.txt",
                "eval_response_size_bytes": response_file.stat().st_size
            }
        else:
            # Use task data from CSV files
            new_row = {
                "evaluation_task_id": task_id,
                "generation_task_id": eval_task_row["generation_task_id"],
                "combo_id": gen_task_row["combo_id"],
                "generation_template": gen_task_row["generation_template"],
                "generation_model": gen_task_row["generation_model_name"],
                "evaluation_template": eval_task_row["evaluation_template"],
                "evaluation_model": eval_task_row["evaluation_model_name"],
                "evaluation_status": "success",
                "evaluation_timestamp": datetime.fromtimestamp(response_file.stat().st_mtime).isoformat(),
                "eval_response_file": f"evaluation_responses/{task_id}.txt",
                "eval_response_size_bytes": response_file.stat().st_size
            }
        
        # Append to table
        try:
            append_to_results_csv("data/7_cross_experiment/evaluation_results.csv", new_row)
            processed_count += 1
            if processed_count % 100 == 0:
                print(f"✅ Processed {processed_count} files...")
        except Exception as e:
            error_msg = f"❌ Error processing {task_id}: {e}"
            print(error_msg)
            errors.append(error_msg)
    
    print(f"\n🎉 Completed! Processed {processed_count} evaluation responses")
    
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

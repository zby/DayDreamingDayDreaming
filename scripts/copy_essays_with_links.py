#!/usr/bin/env python3
"""
Copy top-scoring essays (and their links + templates) for manual review.

Usage:
    python scripts/copy_essays_with_links.py [N]
    # Or use cross-experiment pivot and current experiment eval tasks
    python scripts/copy_essays_with_links.py --use-big-pivot --n 10 \
        --big-pivot data/7_cross_experiment/evaluation_scores_by_template_model.csv \
        --evaluation-tasks data/2_tasks/evaluation_tasks.csv

Examples:
    python scripts/copy_essays_with_links.py       # top 5 (default)
    python scripts/copy_essays_with_links.py 3     # top 3
    python scripts/copy_essays_with_links.py 10    # top 10

What it does:
1) Reads `data/6_summary/generation_scores_pivot.csv`
2) Sorts by `sum_scores` and selects top N
3) Copies to `to_analyse/`:
   - Essay responses from `data/3_generation/essay_responses/`
   - Links responses from `data/3_generation/links_responses/` (as `<name>_links_response.txt`)
   - Template files from the two-phase generation system:
       * Essay templates: `{essay_template}_essay_template.txt` from `data/1_raw/generation_templates/essay/`
       * Links templates: `{links_template}_links_template.txt` from `data/1_raw/generation_templates/links/`

Notes:
- Supports the two-phase generation system where links and essays use different templates
- Links files get a `_links_response` postfix to avoid naming conflicts with essay files
- Links template names are extracted from the link task ID structure
- Evaluation responses by Sonnet (`evaluation_model_id='sonnet-4'`) are copied into
  a subdirectory named `evaluations_sonnet/` alongside the other files
"""

import os
import shutil
import sys
import pandas as pd
from pathlib import Path
from typing import List, Tuple
import argparse


def get_top_scoring_generations(n: int = 5, scores_csv: str = "data/6_summary/generation_scores_pivot.csv") -> Tuple[List[str], List[dict]]:
    """
    Get the top N scoring generations from the generation_scores_pivot.csv file.
    
    Args:
        n: Number of top scoring generations to return (default: 5)
        scores_csv: Path to the generation scores CSV file
        
    Returns:
        Tuple of (essay_filenames, generation_info) where generation_info contains score details
    """
    if not os.path.exists(scores_csv):
        raise FileNotFoundError(f"Scores CSV file not found: {scores_csv}")
    
    # Read the CSV file
    df = pd.read_csv(scores_csv)
    
    # Sort by sum_scores in descending order and get top N
    top_generations = df.nlargest(n, 'sum_scores')
    
    # Extract filenames and generation info
    essay_filenames = []
    generation_info = []
    
    for _, row in top_generations.iterrows():
        filename = os.path.basename(row['generation_response_path'])
        essay_filenames.append(filename)
        generation_info.append({
            'filename': filename,
            'combo_id': row['combo_id'],
            'template': row['generation_template'],  # Essay template
            'link_template': row['link_template'],   # Links template
            'model': row['generation_model'],
            'score': row['sum_scores']
        })
    
    return essay_filenames, generation_info


def get_top_from_big_pivot(
    n: int = 5,
    pivot_csv: str = "data/7_cross_experiment/evaluation_scores_by_template_model.csv",
    evaluation_tasks_csv: str = "data/2_tasks/evaluation_tasks.csv",
) -> Tuple[List[str], List[dict]]:
    """
    Compute top N essays using the cross-experiment pivot, summing only the columns
    corresponding to the current experiment's evaluation templates and models.

    - pivot has rows per essay_task_id with metadata, columns per evaluation_template__evaluation_model
    - evaluation_tasks_csv provides which template+model pairs to sum.
    """
    if not os.path.exists(pivot_csv):
        raise FileNotFoundError(f"Big pivot CSV not found: {pivot_csv}")
    if not os.path.exists(evaluation_tasks_csv):
        raise FileNotFoundError(f"Evaluation tasks CSV not found: {evaluation_tasks_csv}")

    df = pd.read_csv(pivot_csv)
    et = pd.read_csv(evaluation_tasks_csv)

    # Determine required evaluation columns as template__model
    et_cols = (
        et[["evaluation_template", "evaluation_model"]]
        .dropna()
        .drop_duplicates()
        .assign(col=lambda d: d["evaluation_template"].astype(str) + "__" + d["evaluation_model"].astype(str))
    )["col"].tolist()

    # Ensure required columns exist (missing treated as NaN => excluded in sum)
    for c in et_cols:
        if c not in df.columns:
            df[c] = float("nan")

    # Compute total score across current experiment's eval template+model pairs
    df["sum_scores"] = df[et_cols].sum(axis=1, skipna=True)

    # Sort and pick top N
    top = df.nlargest(n, "sum_scores")

    # Build outputs
    essay_filenames: List[str] = []
    generation_info: List[dict] = []
    for _, row in top.iterrows():
        essay_task_id = row.get("essay_task_id")
        if not isinstance(essay_task_id, str) or not essay_task_id:
            continue
        filename = f"{essay_task_id}.txt"
        essay_filenames.append(filename)
        generation_info.append({
            "filename": filename,
            "combo_id": row.get("combo_id"),
            "template": row.get("generation_template"),
            "link_template": row.get("link_template"),  # May be None for old data
            "model": row.get("generation_model"),
            "score": row.get("sum_scores", 0.0),
        })

    return essay_filenames, generation_info


def copy_essays_with_links(essay_paths: List[str], generation_info: List[dict],
                          essay_responses_dir: str = "data/3_generation/essay_responses",
                          links_responses_dir: str = "data/3_generation/links_responses", 
                          essay_templates_dir: str = "data/1_raw/generation_templates/essay",
                          links_templates_dir: str = "data/1_raw/generation_templates/links",
                          evaluation_responses_dir: str = "data/4_evaluation/evaluation_responses",
                          evaluation_model_id: str = "sonnet-4",
                          evaluation_output_subdir: str = "evaluations_sonnet",
                          output_dir: str = "to_analyse"):
    """
    Copy essays, links responses, and templates to the analysis folder.
    
    Args:
        essay_paths: List of essay file paths (basenames)
        generation_info: List of generation info dicts containing template names
        essay_responses_dir: Directory containing essay response files
        links_responses_dir: Directory containing links response files  
        essay_templates_dir: Directory containing essay template files
        links_templates_dir: Directory containing links template files
        output_dir: Output directory to copy files to
    """
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert to Path objects for easier manipulation
    essay_responses_path = Path(essay_responses_dir)
    links_responses_path = Path(links_responses_dir)
    essay_templates_path = Path(essay_templates_dir)
    links_templates_path = Path(links_templates_dir)
    evaluation_responses_path = Path(evaluation_responses_dir)
    output_path = Path(output_dir)
    eval_output_path = output_path / evaluation_output_subdir
    
    copied_files = []
    missing_files = []
    copied_evals = 0
    
    # Create a mapping from filename to generation info
    filename_to_info = {info['filename']: info for info in generation_info}
    
    # Track unique templates that have been copied
    copied_templates = set()
    
    for essay_path in essay_paths:
        essay_file = Path(essay_path)
        
        # If it's just a filename, construct full path
        if not essay_file.is_absolute() and len(essay_file.parts) == 1:
            essay_filename = Path(essay_file.name)
        else:
            essay_filename = Path(essay_file.name)
        
        # Get the generation info for this file
        info = filename_to_info.get(str(essay_filename))
        if not info:
            print(f"\nWarning: No generation info found for {essay_filename}")
            continue
            
        essay_template_name = info['template']
        links_template_name = info.get('link_template', essay_template_name)  # Use link_template if available
        
        # Define source file paths
        essay_response_src = essay_responses_path / essay_filename
        # Link responses are saved by link_task_id, not essay_task_id.
        # essay_task_id = f"{link_task_id}_{essay_template}" so we drop the trailing
        # "_{essay_template_name}" from the essay filename stem to get the link filename.
        link_filename_stem = essay_filename.stem
        suffix_to_remove = f"_{essay_template_name}"
        if link_filename_stem.endswith(suffix_to_remove):
            link_filename_stem = link_filename_stem[: -len(suffix_to_remove)]
        links_response_src = links_responses_path / f"{link_filename_stem}{essay_filename.suffix}"
        
        essay_template_src = essay_templates_path / f"{essay_template_name}.txt"
        links_template_src = links_templates_path / f"{links_template_name}.txt"
        
        # Define destination file paths
        essay_response_dst = output_path / essay_filename
        links_response_dst = output_path / f"{essay_filename.stem}_links_response{essay_filename.suffix}"
        essay_template_dst = output_path / f"{essay_template_name}_essay_template.txt"
        links_template_dst = output_path / f"{links_template_name}_links_template.txt"
        
        print(f"\nProcessing: {essay_filename} (essay_template: {essay_template_name}, links_template: {links_template_name})")
        
        # Copy essay response
        if essay_response_src.exists():
            shutil.copy2(essay_response_src, essay_response_dst)
            print(f"  ✓ Copied essay response: {essay_response_dst.name}")
            copied_files.append(str(essay_response_dst))
        else:
            print(f"  ✗ Essay response not found: {essay_response_src}")
            missing_files.append(str(essay_response_src))
            
        # Copy links response
        if links_response_src.exists():
            shutil.copy2(links_response_src, links_response_dst)
            print(f"  ✓ Copied links response: {links_response_dst.name}")
            copied_files.append(str(links_response_dst))
        else:
            print(f"  ✗ Links response not found: {links_response_src}")
            missing_files.append(str(links_response_src))

        # Copy evaluation responses by the specified evaluation model (default: sonnet-4)
        # Evaluation files are named: {essay_task_id}_{evaluation_template}_{evaluation_model_id}.txt
        essay_task_id = essay_filename.stem
        eval_glob = f"{essay_task_id}_*_{evaluation_model_id}{essay_filename.suffix}"
        eval_files = list(evaluation_responses_path.glob(eval_glob))
        if eval_files:
            eval_output_path.mkdir(parents=True, exist_ok=True)
            for ef in eval_files:
                dst = eval_output_path / ef.name
                shutil.copy2(ef, dst)
                copied_evals += 1
            print(f"  ✓ Copied {len(eval_files)} evaluation(s) by {evaluation_model_id} -> {evaluation_output_subdir}/")
        else:
            print(f"  - No evaluations by {evaluation_model_id} found for {essay_task_id}")
            
        # Copy templates (only if not already copied)
        # Use a compound key for both essay and links templates
        template_key = f"{essay_template_name}+{links_template_name}"
        if template_key not in copied_templates:
            # Copy essay template
            if essay_template_src.exists():
                shutil.copy2(essay_template_src, essay_template_dst)
                print(f"  ✓ Copied essay template: {essay_template_dst.name}")
                copied_files.append(str(essay_template_dst))
            else:
                print(f"  ✗ Essay template not found: {essay_template_src}")
                missing_files.append(str(essay_template_src))
                
            # Copy links template
            if links_template_src.exists():
                shutil.copy2(links_template_src, links_template_dst)
                print(f"  ✓ Copied links template: {links_template_dst.name}")
                copied_files.append(str(links_template_dst))
            else:
                print(f"  ✗ Links template not found: {links_template_src}")
                missing_files.append(str(links_template_src))
            
            # Mark this template combination as copied
            copied_templates.add(template_key)
        else:
            print(f"  - Templates for '{essay_template_name}' + '{links_template_name}' already copied, skipping")
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"SUMMARY:")
    print(f"  Successfully copied: {len(copied_files)} files")
    print(f"  Missing files: {len(missing_files)} files")
    print(f"  Copied evaluations: {copied_evals} files -> {evaluation_output_subdir}/")
    
    if missing_files:
        print(f"\nMissing files:")
        for missing in missing_files:
            print(f"  - {missing}")
    
    return copied_files, missing_files


def main():
    """Main function to handle command line usage."""
    parser = argparse.ArgumentParser(description="Copy top-scoring essays with links, templates, and evaluations")
    parser.add_argument("pos_n", nargs="?", type=int, help="Top N to copy (back-compat positional)")
    parser.add_argument("--n", type=int, default=None, help="Top N to copy (overrides positional)")
    parser.add_argument("--use-big-pivot", action="store_true", help="Use cross-experiment pivot + current eval tasks to compute totals")
    parser.add_argument("--scores-csv", type=str, default="data/6_summary/generation_scores_pivot.csv", help="Path to experiment pivot (default mode)")
    parser.add_argument("--big-pivot", type=str, default="data/7_cross_experiment/evaluation_scores_by_template_model.csv", help="Path to cross-experiment pivot table")
    parser.add_argument("--evaluation-tasks", type=str, default="data/2_tasks/evaluation_tasks.csv", help="Path to evaluation_tasks.csv (to select eval template+model columns)")
    args = parser.parse_args()

    # Resolve N with backward compatibility
    n = args.n if args.n is not None else (args.pos_n if args.pos_n is not None else 5)
    if n <= 0:
        print("Error: N must be positive")
        sys.exit(1)

    print(f"Finding top {n} scoring generations...")

    # Get top scoring generations from selected source
    try:
        if args.use_big_pivot:
            essay_filenames, generation_info = get_top_from_big_pivot(
                n=n, pivot_csv=args.big_pivot, evaluation_tasks_csv=args.evaluation_tasks
            )
        else:
            essay_filenames, generation_info = get_top_scoring_generations(n=n, scores_csv=args.scores_csv)
    except Exception as e:
        print(f"Error computing top-scoring essays: {e}")
        sys.exit(1)
    
    if not essay_filenames:
        if 'args' in locals() and getattr(args, 'use_big_pivot', False):
            print("No essays found using big pivot mode.")
            print("- Ensure the big pivot exists and is populated: data/7_cross_experiment/evaluation_scores_by_template_model.csv")
            print("- If missing or empty, run: ./scripts/rebuild_results.sh")
            print("- Also confirm evaluation_tasks.csv lists the template+model pairs for this experiment.")
        else:
            print("No generations found in scores CSV (data/6_summary/generation_scores_pivot.csv)")
        sys.exit(1)
    
    print(f"Selected {len(essay_filenames)} top scoring generations:")
    for i, info in enumerate(generation_info, 1):
        link_template = info.get('link_template', 'N/A')
        print(f"  {i}. {info['model']} | Essay: {info['template']} | Links: {link_template} | Score: {info['score']} | {info['combo_id']}")
        print(f"     -> {info['filename']}")
    
    print(f"\nCopying files...")
    
    copied_files, missing_files = copy_essays_with_links(essay_filenames, generation_info)
    
    if missing_files:
        sys.exit(1)  # Exit with error if any files were missing


if __name__ == "__main__":
    main()

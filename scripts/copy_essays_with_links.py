#!/usr/bin/env python3
"""
Copy top-scoring essays (and their links + templates) for manual review.

Usage:
    python scripts/copy_essays_with_links.py [N]

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
   - One copy per unique template used:
       * Essay template: `{template}_essay_template.txt` from `data/1_raw/generation_templates/essay/`
       * Links template: `{template}_links_template.txt` from `data/1_raw/generation_templates/links/`

Notes:
- Links files get a `_links_response` postfix to avoid naming conflicts with essay files.
- Evaluation responses by Sonnet (`evaluation_model_id='sonnet-4'`) are copied into
  a subdirectory named `evaluations_sonnet/` alongside the other files.
"""

import os
import shutil
import sys
import pandas as pd
from pathlib import Path
from typing import List


def get_top_scoring_generations(n: int = 5, scores_csv: str = "data/6_summary/generation_scores_pivot.csv"):
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
            'template': row['generation_template'],
            'model': row['generation_model'],
            'score': row['sum_scores']
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
            
        template_name = info['template']
        
        # Define source file paths
        essay_response_src = essay_responses_path / essay_filename
        # Link responses are saved by link_task_id, not essay_task_id.
        # essay_task_id = f"{link_task_id}_{essay_template}" so we drop the trailing
        # "_{template_name}" from the essay filename stem to get the link filename.
        link_filename_stem = essay_filename.stem
        suffix_to_remove = f"_{template_name}"
        if link_filename_stem.endswith(suffix_to_remove):
            link_filename_stem = link_filename_stem[: -len(suffix_to_remove)]
        links_response_src = links_responses_path / f"{link_filename_stem}{essay_filename.suffix}"
        essay_template_src = essay_templates_path / f"{template_name}.txt"
        links_template_src = links_templates_path / f"{template_name}.txt"
        
        # Define destination file paths
        essay_response_dst = output_path / essay_filename
        links_response_dst = output_path / f"{essay_filename.stem}_links_response{essay_filename.suffix}"
        essay_template_dst = output_path / f"{template_name}_essay_template.txt"
        links_template_dst = output_path / f"{template_name}_links_template.txt"
        
        print(f"\nProcessing: {essay_filename} (template: {template_name})")
        
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
            
        # Copy essay template (only if not already copied)
        if template_name not in copied_templates:
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
            
            # Mark this template as copied
            copied_templates.add(template_name)
        else:
            print(f"  - Templates for '{template_name}' already copied, skipping")
    
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
    # Parse command line arguments
    if len(sys.argv) > 2:
        print("Usage: python copy_essays_with_links.py [N]")
        print("  N: Number of top scoring generations to copy (default: 5)")
        print("Example: python copy_essays_with_links.py 10")
        print("Example: python copy_essays_with_links.py")
        sys.exit(1)
    
    # Get number of generations to copy (default: 5)
    n = 5
    if len(sys.argv) == 2:
        try:
            n = int(sys.argv[1])
            if n <= 0:
                print("Error: Number of generations must be positive")
                sys.exit(1)
        except ValueError:
            print("Error: Please provide a valid number")
            sys.exit(1)
    
    print(f"Finding top {n} scoring generations...")
    
    # Get top scoring generations from CSV
    try:
        essay_filenames, generation_info = get_top_scoring_generations(n)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading scores CSV: {e}")
        sys.exit(1)
    
    if not essay_filenames:
        print("No generations found in scores CSV")
        sys.exit(1)
    
    print(f"Selected {len(essay_filenames)} top scoring generations:")
    for i, info in enumerate(generation_info, 1):
        print(f"  {i}. {info['model']} | {info['template']} | Score: {info['score']} | {info['combo_id']}")
        print(f"     -> {info['filename']}")
    
    print(f"\nCopying files...")
    
    copied_files, missing_files = copy_essays_with_links(essay_filenames, generation_info)
    
    if missing_files:
        sys.exit(1)  # Exit with error if any files were missing


if __name__ == "__main__":
    main()

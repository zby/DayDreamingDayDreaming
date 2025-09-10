#!/usr/bin/env python3
"""
Copy top-scoring essays (and their drafts + templates) for manual review.

Usage:
    python scripts/copy_essays_with_drafts.py [N]
    # Or use cross-experiment pivot and current experiment eval tasks
    python scripts/copy_essays_with_drafts.py --use-big-pivot --n 10 \
        --big-pivot data/7_cross_experiment/evaluation_scores_by_template_model.csv \
        --evaluation-tasks data/2_tasks/evaluation_tasks.csv
    # Or copy all current generation files (ignores pivot tables)
    python scripts/copy_essays_with_drafts.py --use-all-generations

Examples:
    python scripts/copy_essays_with_drafts.py       # top 5 (default)
    python scripts/copy_essays_with_drafts.py 3     # top 3
    python scripts/copy_essays_with_drafts.py 10    # top 10
    python scripts/copy_essays_with_drafts.py --use-all-generations  # all current files

What it does:
1) Reads `data/6_summary/generation_scores_pivot.csv`
2) Sorts by `sum_scores` and selects top N
3) Copies to `to_analyse/`:
   - Essay responses from `data/3_generation/essay_responses/`
   - Draft responses from `data/3_generation/draft_responses/` (falls back to legacy `links_responses/`) as `<name>_draft.txt`
   - Template files from the two-phase generation system:
       * Essay templates: `{essay_template}.txt` from `data/1_raw/generation_templates/essay/`
       * Draft templates: `{draft_template}.txt` from `data/1_raw/generation_templates/draft/`

Notes:
- Supports the two-phase generation system where drafts and essays use different templates
- Draft files get a `_draft` postfix to avoid naming conflicts with essay files
- Draft template names are extracted from the draft task ID structure
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


def find_common_prefix(filenames: List[str]) -> str:
    """
    Find the common prefix among a list of filenames.
    
    Args:
        filenames: List of filenames to analyze
        
    Returns:
        Common prefix string (empty if no common prefix found)
    """
    if not filenames:
        return ""
    
    # Extract just the stems (without extensions) for prefix analysis
    stems = [Path(f).stem for f in filenames]
    
    if len(stems) == 1:
        return ""  # Single file, no common prefix to remove
    
    # Find common prefix by comparing characters
    common_prefix = ""
    min_length = min(len(stem) for stem in stems)
    
    for i in range(min_length):
        char = stems[0][i]
        if all(stem[i] == char for stem in stems):
            common_prefix += char
        else:
            break
    
    # Only return prefix if it's meaningful (longer than a few characters)
    # Find the last underscore in the prefix to avoid cutting words in half
    if len(common_prefix) > 5:
        last_underscore = common_prefix.rfind('_')
        if last_underscore >= 5:  # Ensure we keep a meaningful prefix
            return common_prefix[:last_underscore + 1]
    
    return ""


def remove_common_prefix(filename: str, common_prefix: str) -> str:
    """
    Remove common prefix from a filename while preserving the extension.
    
    Args:
        filename: Original filename
        common_prefix: Prefix to remove
        
    Returns:
        Filename with prefix removed
    """
    if not common_prefix:
        return filename
    
    file_path = Path(filename)
    stem = file_path.stem
    suffix = file_path.suffix
    
    if stem.startswith(common_prefix):
        new_stem = stem[len(common_prefix):]
        # Ensure we don't create an empty filename
        if new_stem:
            return new_stem + suffix
    
    return filename


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
        # Derive draft_task_id robustly from combo_id, draft_template, model
        draft_template = row.get('draft_template')
        generation_model = row.get('generation_model')
        combo_id = row.get('combo_id')
        draft_task_id = None
        if isinstance(combo_id, str) and isinstance(draft_template, str) and isinstance(generation_model, str):
            draft_task_id = f"{combo_id}_{draft_template}_{generation_model}"
        generation_info.append({
            'filename': filename,
            'combo_id': row['combo_id'],
            'template': row['generation_template'],  # Essay template
            'draft_template': draft_template,         # Draft template (renamed from legacy link_template)
            'draft_task_id': draft_task_id,           # For finding corresponding draft response
            'model': row['generation_model'],
            'score': row['sum_scores']
        })
    
    return essay_filenames, generation_info


def get_all_current_generations(
    essay_responses_dir: str = "data/3_generation/essay_responses",
    concepts_metadata_path: str = "data/1_raw/concepts_metadata.csv",
    content_combinations_path: str = "data/2_tasks/selected_combo_mappings.csv",
    essay_tasks_path: str = "data/2_tasks/essay_generation_tasks.csv",
    essay_templates_path: str = "data/1_raw/essay_templates.csv",
    draft_templates_path: str = "data/1_raw/draft_templates.csv"
) -> Tuple[List[str], List[dict]]:
    """
    Get all current generation files from the active cube (active concepts and templates).
    
    Args:
        essay_responses_dir: Directory containing essay response files
        concepts_metadata_path: Path to concepts metadata CSV with active column
        content_combinations_path: Path to content combinations CSV mapping combo_id to concept_id
        essay_tasks_path: Path to essay generation tasks CSV
        essay_templates_path: Path to essay templates CSV with active column
        draft_templates_path: Path to draft templates CSV with active column (falls back to legacy link_templates.csv)
        
    Returns:
        Tuple of (essay_filenames, generation_info) where generation_info contains proper names from CSV
    """
    essay_responses_path = Path(essay_responses_dir)
    
    if not essay_responses_path.exists():
        raise FileNotFoundError(f"Essay responses directory not found: {essay_responses_dir}")
    
    # Load active concepts
    concepts_df = pd.read_csv(concepts_metadata_path)
    active_concepts = set(concepts_df[concepts_df['active'] == True]['concept_id'].tolist())
    
    if not active_concepts:
        print("Warning: No active concepts found in concepts metadata")
        return [], []
    
    # Load active templates
    essay_templates_df = pd.read_csv(essay_templates_path)
    active_essay_templates = set(essay_templates_df[essay_templates_df['active'] == True]['template_id'].tolist())
    
    # Load draft templates; fall back to legacy link_templates.csv if needed
    if not os.path.exists(draft_templates_path):
        legacy = Path(draft_templates_path).parent / "link_templates.csv"
        if legacy.exists():
            draft_templates_path = str(legacy)
    link_templates_df = pd.read_csv(draft_templates_path)
    active_draft_templates = set(link_templates_df[link_templates_df['active'] == True]['template_id'].tolist())
    
    print(f"Active concepts: {sorted(active_concepts)}")
    print(f"Active essay templates: {sorted(active_essay_templates)}")
    print(f"Active draft templates: {sorted(active_draft_templates)}")
    
    # Load combo to concept mappings (curated or superset or deprecated export)
    if Path(content_combinations_path).exists():
        combinations_df = pd.read_csv(content_combinations_path)
    else:
        superset = Path("data") / "combo_mappings.csv"
        deprecated = Path("data") / "2_tasks" / "content_combinations_csv.csv"
        if superset.exists():
            print(f"Using superset mapping: {superset}")
            combinations_df = pd.read_csv(superset)
        elif deprecated.exists():
            print(f"Deprecated: reading {deprecated}; prefer selected_combo_mappings.csv or combo_mappings.csv")
            combinations_df = pd.read_csv(deprecated)
        else:
            raise FileNotFoundError(
                f"No combinations mapping found. Provide curated 'data/2_tasks/selected_combo_mappings.csv' or ensure 'data/combo_mappings.csv' exists."
            )
    
    # Find combo_ids that contain only active concepts
    active_combo_ids = set()
    for combo_id in combinations_df['combo_id'].unique():
        if pd.isna(combo_id):
            continue
        combo_concepts = set(combinations_df[combinations_df['combo_id'] == combo_id]['concept_id'].tolist())
        # Remove any NaN values
        combo_concepts = {c for c in combo_concepts if pd.notna(c)}
        
        # Check if all concepts in this combo are active
        if combo_concepts and combo_concepts.issubset(active_concepts):
            active_combo_ids.add(combo_id)
    
    if not active_combo_ids:
        print("Warning: No active combo_ids found")
        return [], []
        
    print(f"Active combo_ids: {sorted(active_combo_ids)}")
    
    # Load essay tasks to get the proper mapping of filenames to metadata
    essay_tasks_df = pd.read_csv(essay_tasks_path)
    
    # Filter essay tasks to only active combinations and templates
    # Support both 'draft_template' (canonical) and legacy 'link_template' in tasks
    draft_tpl_col = 'draft_template' if 'draft_template' in essay_tasks_df.columns else 'link_template'
    active_tasks = essay_tasks_df[
        (essay_tasks_df['combo_id'].isin(active_combo_ids)) &
        (essay_tasks_df['essay_template'].isin(active_essay_templates)) &
        (essay_tasks_df[draft_tpl_col].isin(active_draft_templates))
    ]
    
    if active_tasks.empty:
        print("Warning: No active tasks found matching active concepts and templates")
        return [], []
    
    print(f"Found {len(active_tasks)} active essay tasks")
    
    # Create template name mappings
    essay_template_names = dict(zip(essay_templates_df['template_id'], essay_templates_df['template_name']))
    draft_template_names = dict(zip(link_templates_df['template_id'], link_templates_df['template_name']))
    
    essay_filenames = []
    generation_info = []
    
    # For each active task, check if the corresponding file exists
    for _, task_row in active_tasks.iterrows():
        essay_task_id = task_row['essay_task_id']
        expected_filename = f"{essay_task_id}.txt"
        file_path = essay_responses_path / expected_filename
        
        if file_path.exists():
            essay_filenames.append(expected_filename)
            # Prefer canonical draft fields; fall back to legacy link_* when absent
            draft_tpl = task_row.get('draft_template') if 'draft_template' in task_row else task_row.get('link_template')
            draft_task_id = task_row.get('draft_task_id') if 'draft_task_id' in task_row else task_row.get('link_task_id')
            generation_info.append({
                'filename': expected_filename,
                'combo_id': task_row['combo_id'],
                'template': task_row['essay_template'],  # Use template ID for file paths
                'template_name': essay_template_names.get(task_row['essay_template'], task_row['essay_template']),  # Display name
                'draft_template': draft_tpl,  # Use template ID for file paths
                'draft_template_name': draft_template_names.get(draft_tpl, draft_tpl),  # Display name
                'draft_task_id': draft_task_id,  # For finding corresponding draft response
                'model': task_row['generation_model_name'],
                'score': 0.0  # No score available when bypassing pivot tables
            })
    
    # Sort by filename for consistent ordering
    combined = list(zip(essay_filenames, generation_info))
    combined.sort(key=lambda x: x[0])
    essay_filenames, generation_info = zip(*combined) if combined else ([], [])
    
    return list(essay_filenames), list(generation_info)


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
        # Derive draft_task_id when possible
        draft_template = row.get("draft_template") or row.get("link_template")
        generation_model = row.get("generation_model")
        combo_id = row.get("combo_id")
        draft_task_id = None
        if isinstance(combo_id, str) and isinstance(draft_template, str) and isinstance(generation_model, str):
            draft_task_id = f"{combo_id}_{draft_template}_{generation_model}"
        generation_info.append({
            "filename": filename,
            "combo_id": row.get("combo_id"),
            "template": row.get("generation_template"),
            "draft_template": draft_template,  # May be None for old data
            "draft_task_id": draft_task_id,
            "model": row.get("generation_model"),
            "score": row.get("sum_scores", 0.0),
        })

    return essay_filenames, generation_info


def copy_essays_with_drafts(
    essay_paths: List[str],
    generation_info: List[dict],
    essay_responses_dir: str = "data/3_generation/essay_responses",
    draft_responses_dir: str = "data/3_generation/draft_responses",
    essay_templates_dir: str = "data/1_raw/generation_templates/essay",
    draft_templates_dir: str = "data/1_raw/generation_templates/draft",
    evaluation_responses_dir: str = "data/4_evaluation/evaluation_responses",
    evaluation_model_id: str = "sonnet-4",
    evaluation_output_subdir: str = "evaluations_sonnet",
    output_dir: str = "to_analyse",
):
    """
    Copy essays, draft responses, and templates to the analysis folder.

    Args:
        essay_paths: List of essay file paths (basenames)
        generation_info: List of generation info dicts containing template names
        essay_responses_dir: Directory containing essay response files
        draft_responses_dir: Directory containing draft response files (falls back to legacy links_responses)
        essay_templates_dir: Directory containing essay template files
        draft_templates_dir: Directory containing draft template files (falls back to legacy generation_templates/links)
        output_dir: Output directory to copy files to
    """
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert to Path objects for easier manipulation
    essay_responses_path = Path(essay_responses_dir)
    draft_responses_path = Path(draft_responses_dir)
    if not draft_responses_path.exists():
        legacy = Path("data/3_generation/links_responses")
        if legacy.exists():
            draft_responses_path = legacy
    essay_templates_path = Path(essay_templates_dir)
    draft_templates_path = Path(draft_templates_dir)
    if not draft_templates_path.exists():
        legacy_tpl = Path("data/1_raw/generation_templates/links")
        if legacy_tpl.exists():
            draft_templates_path = legacy_tpl
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
    
    # Find common prefix among all essay filenames to remove from destination names
    common_prefix = find_common_prefix(essay_paths)
    if common_prefix:
        print(f"Detected common prefix '{common_prefix}' - will be removed from copied filenames")
    
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
        draft_template_name = info.get('draft_template', essay_template_name)  # Use draft_template if available
        
        # Define source file paths
        essay_response_src = essay_responses_path / essay_filename
        # Draft responses are saved by draft_task_id (legacy link_task_id)
        draft_task_id = info.get('draft_task_id') or info.get('link_task_id') or ''
        if draft_task_id:
            draft_response_src = draft_responses_path / f"{draft_task_id}.txt"
        else:
            # Fallback: derive from essay filename by stripping suffix _{essay_template}
            draft_filename_stem = essay_filename.stem
            suffix_to_remove = f"_{essay_template_name}"
            if draft_filename_stem.endswith(suffix_to_remove):
                draft_filename_stem = draft_filename_stem[: -len(suffix_to_remove)]
            draft_response_src = draft_responses_path / f"{draft_filename_stem}{essay_filename.suffix}"

        essay_template_src = essay_templates_path / f"{essay_template_name}.txt"
        draft_template_src = draft_templates_path / f"{draft_template_name}.txt"
        
        # Define destination file paths with common prefix removed
        clean_essay_filename = remove_common_prefix(str(essay_filename), common_prefix)
        clean_essay_filename_path = Path(clean_essay_filename)
        
        # Use proper suffixes for generation files: *_essay.txt and *_draft.txt
        base_name = clean_essay_filename_path.stem
        essay_response_dst = output_path / f"{base_name}_essay{clean_essay_filename_path.suffix}"
        draft_response_dst = output_path / f"{base_name}_draft{clean_essay_filename_path.suffix}"
        essay_template_dst = output_path / f"{essay_template_name}_essay_template.txt"
        draft_template_dst = output_path / f"{draft_template_name}_draft_template.txt"

        print(f"\nProcessing: {essay_filename} (essay_template: {essay_template_name}, draft_template: {draft_template_name})")
        
        # Copy essay response
        if essay_response_src.exists():
            shutil.copy2(essay_response_src, essay_response_dst)
            print(f"  ✓ Copied essay response: {essay_response_dst.name}")
            copied_files.append(str(essay_response_dst))
        else:
            print(f"  ✗ Essay response not found: {essay_response_src}")
            missing_files.append(str(essay_response_src))
            
        # Copy draft response
        if draft_response_src.exists():
            shutil.copy2(draft_response_src, draft_response_dst)
            print(f"  ✓ Copied draft response: {draft_response_dst.name}")
            copied_files.append(str(draft_response_dst))
        else:
            print(f"  ✗ Draft response not found: {draft_response_src}")
            missing_files.append(str(draft_response_src))

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
        # Use a compound key for both essay and draft templates
        template_key = f"{essay_template_name}+{draft_template_name}"
        if template_key not in copied_templates:
            # Copy essay template
            if essay_template_src.exists():
                shutil.copy2(essay_template_src, essay_template_dst)
                print(f"  ✓ Copied essay template: {essay_template_dst.name}")
                copied_files.append(str(essay_template_dst))
            else:
                print(f"  ✗ Essay template not found: {essay_template_src}")
                missing_files.append(str(essay_template_src))
                
            # Copy draft template
            if draft_template_src.exists():
                shutil.copy2(draft_template_src, draft_template_dst)
                print(f"  ✓ Copied draft template: {draft_template_dst.name}")
                copied_files.append(str(draft_template_dst))
            else:
                print(f"  ✗ Draft template not found: {draft_template_src}")
                missing_files.append(str(draft_template_src))
            
            # Mark this template combination as copied
            copied_templates.add(template_key)
        else:
            print(f"  - Templates for '{essay_template_name}' + '{draft_template_name}' already copied, skipping")
    
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
    parser = argparse.ArgumentParser(description="Copy top-scoring essays with drafts, templates, and evaluations")
    parser.add_argument("pos_n", nargs="?", type=int, help="Top N to copy (back-compat positional, ignored with --use-all-generations)")
    parser.add_argument("--n", type=int, default=None, help="Top N to copy (overrides positional, ignored with --use-all-generations)")
    parser.add_argument("--use-big-pivot", action="store_true", help="Use cross-experiment pivot + current eval tasks to compute totals")
    parser.add_argument("--use-all-generations", action="store_true", help="Copy all current generation files (ignores N and pivot tables)")
    parser.add_argument("--scores-csv", type=str, default="data/6_summary/generation_scores_pivot.csv", help="Path to experiment pivot (default mode)")
    parser.add_argument("--big-pivot", type=str, default="data/7_cross_experiment/evaluation_scores_by_template_model.csv", help="Path to cross-experiment pivot table")
    parser.add_argument("--evaluation-tasks", type=str, default="data/2_tasks/evaluation_tasks.csv", help="Path to evaluation_tasks.csv (to select eval template+model columns)")
    parser.add_argument("--essay-responses-dir", type=str, default="data/3_generation/essay_responses", help="Directory containing essay response files")
    parser.add_argument("--concepts-metadata", type=str, default="data/1_raw/concepts_metadata.csv", help="Path to concepts metadata CSV")
    parser.add_argument(
        "--content-combinations",
        type=str,
        default="data/2_tasks/selected_combo_mappings.csv",
        help="Path to curated combinations CSV (selected_combo_mappings.csv). Falls back to data/combo_mappings.csv; legacy content_combinations_csv.csv supported with warning.")
    parser.add_argument("--essay-tasks", type=str, default="data/2_tasks/essay_generation_tasks.csv", help="Path to essay generation tasks CSV")
    parser.add_argument("--essay-templates", type=str, default="data/1_raw/essay_templates.csv", help="Path to essay templates CSV")
    parser.add_argument("--draft-templates", type=str, default="data/1_raw/draft_templates.csv", help="Path to draft templates CSV")
    # Back-compat (deprecated): allow --link-templates to override if provided
    parser.add_argument("--link-templates", type=str, default=None, help="[Deprecated] Path to legacy link templates CSV")
    args = parser.parse_args()

    # Handle conflicting options
    if args.use_all_generations and args.use_big_pivot:
        print("Error: --use-all-generations and --use-big-pivot are mutually exclusive")
        sys.exit(1)

    # Get generation files from selected source
    try:
        if args.use_all_generations:
            print("Finding all current generation files from active cube...")
            draft_templates_csv = args.draft_templates if args.link_templates is None else args.link_templates
            essay_filenames, generation_info = get_all_current_generations(
                essay_responses_dir=args.essay_responses_dir,
                concepts_metadata_path=args.concepts_metadata,
                content_combinations_path=args.content_combinations,
                essay_tasks_path=args.essay_tasks,
                essay_templates_path=args.essay_templates,
                draft_templates_path=draft_templates_csv,
            )
        else:
            # Resolve N with backward compatibility
            n = args.n if args.n is not None else (args.pos_n if args.pos_n is not None else 5)
            if n <= 0:
                print("Error: N must be positive")
                sys.exit(1)

            print(f"Finding top {n} scoring generations...")
            
            if args.use_big_pivot:
                essay_filenames, generation_info = get_top_from_big_pivot(
                    n=n, pivot_csv=args.big_pivot, evaluation_tasks_csv=args.evaluation_tasks
                )
            else:
                essay_filenames, generation_info = get_top_scoring_generations(n=n, scores_csv=args.scores_csv)
    except Exception as e:
        print(f"Error getting generation files: {e}")
        sys.exit(1)
    
    if not essay_filenames:
        if args.use_all_generations:
            print(f"No generation files found in directory: {args.essay_responses_dir}")
        elif args.use_big_pivot:
            print("No essays found using big pivot mode.")
            print("- Ensure the big pivot exists and is populated: data/7_cross_experiment/evaluation_scores_by_template_model.csv")
            print("- If missing or empty, run:")
            print("    uv run python scripts/aggregate_scores.py --output data/7_cross_experiment/parsed_scores.csv")
            print("    uv run python scripts/build_pivot_tables.py --parsed-scores data/7_cross_experiment/parsed_scores.csv")
            print("- Also confirm evaluation_tasks.csv lists the template+model pairs for this experiment.")
        else:
            print("No generations found in scores CSV (data/6_summary/generation_scores_pivot.csv)")
        sys.exit(1)
    
    if args.use_all_generations:
        print(f"Found {len(essay_filenames)} generation files:")
        for i, info in enumerate(generation_info, 1):
            essay_name = info.get('template_name', info.get('template', 'N/A'))
            draft_name = info.get('draft_template_name', info.get('draft_template', 'N/A'))
            print(f"  {i}. {info['model']} | Essay: {essay_name} | Draft: {draft_name} | {info['combo_id']}")
            print(f"     -> {info['filename']}")
    else:
        print(f"Selected {len(essay_filenames)} top scoring generations:")
        for i, info in enumerate(generation_info, 1):
            draft_template = info.get('draft_template', 'N/A')
            print(f"  {i}. {info['model']} | Essay: {info['template']} | Draft: {draft_template} | Score: {info['score']} | {info['combo_id']}")
            print(f"     -> {info['filename']}")
    
    print(f"\nCopying files...")
    
    copied_files, missing_files = copy_essays_with_drafts(essay_filenames, generation_info)
    
    if missing_files:
        sys.exit(1)  # Exit with error if any files were missing


if __name__ == "__main__":
    main()

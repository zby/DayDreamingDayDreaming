from dagster import asset, AssetCheckResult, MetadataValue, Failure
from typing import Dict, List, Tuple
import pandas as pd
from collections import defaultdict
import hashlib
from .two_phase_generation import links_prompt
from .llm_evaluation import evaluation_prompt


@asset(
    group_name="data_validation",
    deps=[links_prompt],
    io_manager_key="summary_results_io_manager"
)
def generation_prompt_consistency_report(context, generation_tasks) -> pd.DataFrame:
    """
    Check that generation prompts are identical when combo_id and generation_template are the same,
    regardless of the model used. This validation runs after all generation_prompt partitions are complete.
    
    FAILS the pipeline if inconsistencies are detected.
    """
    # Group tasks by (combo_id, generation_template) - these should produce identical prompts
    prompt_groups = defaultdict(list)
    
    for _, task_row in generation_tasks.iterrows():
        key = (task_row["combo_id"], task_row["generation_template"])
        prompt_groups[key].append({
            "task_id": task_row["generation_task_id"],
            "model": task_row["generation_model_name"]
        })
    
    inconsistencies = []
    total_groups_checked = 0
    total_prompts_checked = 0
    groups_skipped = 0
    
    prompt_io_manager = context.resources.links_prompt_io_manager
    
    class MockLoadContext:
        def __init__(self, partition_key):
            self.partition_key = partition_key
    
    # Check each group that has multiple models
    for (combo_id, template), tasks in prompt_groups.items():
        if len(tasks) <= 1:
            continue  # Skip groups with only one task
            
        total_groups_checked += 1
        
        # Load prompts for all tasks in this group
        prompt_hashes = {}
        missing_prompts = []
        
        for task in tasks:
            task_id = task["task_id"]
            model = task["model"]
            
            try:
                prompt_content = prompt_io_manager.load_input(MockLoadContext(task_id))
                prompt_hash = hashlib.sha256(prompt_content.encode()).hexdigest()
                prompt_hashes[task_id] = {
                    "hash": prompt_hash,
                    "model": model,
                    "combo_id": combo_id,
                    "template": template
                }
                total_prompts_checked += 1
                
            except FileNotFoundError:
                missing_prompts.append(task_id)
                continue
        
        # If we have missing prompts, that's an error
        if missing_prompts:
            raise Failure(
                description=f"Missing generation prompts for {len(missing_prompts)} tasks",
                metadata={
                    "combo_id": MetadataValue.text(combo_id),
                    "template": MetadataValue.text(template), 
                    "missing_tasks": MetadataValue.text(str(missing_prompts[:10])),
                    "resolution": MetadataValue.text("Run: dagster asset materialize --select generation_prompt")
                }
            )
        
        # Skip this group if we don't have at least 2 prompts to compare
        if len(prompt_hashes) < 2:
            groups_skipped += 1
            continue
        
        # Check if all prompts in this group have the same hash
        unique_hashes = set(data["hash"] for data in prompt_hashes.values())
        
        if len(unique_hashes) > 1:
            # Found inconsistency - collect details
            for task_id, data in prompt_hashes.items():
                inconsistencies.append({
                    "task_id": task_id,
                    "combo_id": combo_id,
                    "template": template,
                    "model": data["model"],
                    "prompt_hash": data["hash"][:12],  # First 12 chars for identification
                    "status": "inconsistent"
                })
    
    # Create results DataFrame
    if inconsistencies:
        results_df = pd.DataFrame(inconsistencies)
        
        # Add metadata about the validation
        context.add_output_metadata({
            "total_groups_checked": MetadataValue.int(total_groups_checked),
            "groups_skipped": MetadataValue.int(groups_skipped),
            "total_prompts_checked": MetadataValue.int(total_prompts_checked),
            "inconsistent_tasks": MetadataValue.int(len(inconsistencies)),
            "validation_status": MetadataValue.text("FAILED - Inconsistencies detected")
        })
        
        # FAIL the pipeline due to inconsistencies
        inconsistency_summary = results_df.groupby(['combo_id', 'template']).agg({
            'prompt_hash': 'nunique',
            'task_id': 'count'
        }).rename(columns={'prompt_hash': 'unique_hashes', 'task_id': 'task_count'})
        
        raise Failure(
            description=f"Generation prompt consistency check FAILED: Found {len(inconsistencies)} tasks with inconsistent prompts across {len(inconsistency_summary)} groups",
            metadata={
                "inconsistent_groups": MetadataValue.int(len(inconsistency_summary)),
                "inconsistent_tasks": MetadataValue.int(len(inconsistencies)),
                "first_few_issues": MetadataValue.md(
                    "### Examples of Inconsistent Prompts\n\n" +
                    "\n".join([
                        f"- **{row['combo_id']} + {row['template']}**: {row['unique_hashes']} different hashes across {row['task_count']} tasks"
                        for _, row in inconsistency_summary.head(5).iterrows()
                    ])
                ),
                "resolution": MetadataValue.md("""
                ### Fix Steps:
                1. Check template rendering logic for non-deterministic behavior
                2. Verify content_combinations data consistency
                3. Re-materialize: `dagster asset materialize --select links_prompt`
            """)
            }
        )
    else:
        # All prompts are consistent - create success report
        results_df = pd.DataFrame([{
            "validation_type": "generation_prompt_consistency",
            "status": "PASSED",
            "groups_checked": total_groups_checked,
            "prompts_checked": total_prompts_checked,
            "inconsistencies_found": 0
        }])
        
        context.add_output_metadata({
            "total_groups_checked": MetadataValue.int(total_groups_checked),
            "groups_skipped": MetadataValue.int(groups_skipped),
            "total_prompts_checked": MetadataValue.int(total_prompts_checked),
            "validation_status": MetadataValue.text("PASSED - All prompts consistent")
        })
        
        context.log.info(f"✓ Generation prompt consistency check PASSED: {total_groups_checked} groups, {total_prompts_checked} prompts checked")
    
    return results_df


@asset(
    group_name="data_validation", 
    deps=[evaluation_prompt],
    io_manager_key="summary_results_io_manager"
)
def evaluation_prompt_consistency_report(context, evaluation_tasks) -> pd.DataFrame:
    """
    Check that evaluation prompts are identical when generation_task_id and evaluation_template are the same,
    regardless of the evaluation model used. This validation runs after all evaluation_prompt partitions are complete.
    
    FAILS the pipeline if inconsistencies are detected.
    """
    # Group tasks by (generation_task_id, evaluation_template) - these should produce identical prompts
    prompt_groups = defaultdict(list)
    
    for _, task_row in evaluation_tasks.iterrows():
        key = (task_row["generation_task_id"], task_row["evaluation_template"])
        prompt_groups[key].append({
            "task_id": task_row["evaluation_task_id"],
            "model": task_row["evaluation_model_name"]
        })
    
    inconsistencies = []
    total_groups_checked = 0
    total_prompts_checked = 0
    groups_skipped = 0
    
    prompt_io_manager = context.resources.evaluation_prompt_io_manager
    
    class MockLoadContext:
        def __init__(self, partition_key):
            self.partition_key = partition_key
    
    # Check each group that has multiple models
    for (gen_task_id, template), tasks in prompt_groups.items():
        if len(tasks) <= 1:
            continue  # Skip groups with only one task
            
        total_groups_checked += 1
        
        # Load prompts for all tasks in this group
        prompt_hashes = {}
        missing_prompts = []
        
        for task in tasks:
            task_id = task["task_id"]
            model = task["model"]
            
            try:
                prompt_content = prompt_io_manager.load_input(MockLoadContext(task_id))
                prompt_hash = hashlib.sha256(prompt_content.encode()).hexdigest()
                prompt_hashes[task_id] = {
                    "hash": prompt_hash,
                    "model": model,
                    "generation_task_id": gen_task_id,
                    "template": template
                }
                total_prompts_checked += 1
                
            except FileNotFoundError:
                missing_prompts.append(task_id)
                continue
        
        # If we have missing prompts, that's an error
        if missing_prompts:
            raise Failure(
                description=f"Missing evaluation prompts for {len(missing_prompts)} tasks",
                metadata={
                    "generation_task_id": MetadataValue.text(gen_task_id),
                    "template": MetadataValue.text(template), 
                    "missing_tasks": MetadataValue.text(str(missing_prompts[:10])),
                    "resolution": MetadataValue.text("Run: dagster asset materialize --select evaluation_prompt")
                }
            )
        
        # Skip this group if we don't have at least 2 prompts to compare
        if len(prompt_hashes) < 2:
            groups_skipped += 1
            continue
        
        # Check if all prompts in this group have the same hash
        unique_hashes = set(data["hash"] for data in prompt_hashes.values())
        
        if len(unique_hashes) > 1:
            # Found inconsistency - collect details
            for task_id, data in prompt_hashes.items():
                inconsistencies.append({
                    "task_id": task_id,
                    "generation_task_id": gen_task_id,
                    "template": template,
                    "model": data["model"],
                    "prompt_hash": data["hash"][:12],  # First 12 chars for identification
                    "status": "inconsistent"
                })
    
    # Create results DataFrame
    if inconsistencies:
        results_df = pd.DataFrame(inconsistencies)
        
        # Add metadata about the validation
        context.add_output_metadata({
            "total_groups_checked": MetadataValue.int(total_groups_checked),
            "groups_skipped": MetadataValue.int(groups_skipped),
            "total_prompts_checked": MetadataValue.int(total_prompts_checked),
            "inconsistent_tasks": MetadataValue.int(len(inconsistencies)),
            "validation_status": MetadataValue.text("FAILED - Inconsistencies detected")
        })
        
        # FAIL the pipeline due to inconsistencies
        inconsistency_summary = results_df.groupby(['generation_task_id', 'template']).agg({
            'prompt_hash': 'nunique',
            'task_id': 'count'
        }).rename(columns={'prompt_hash': 'unique_hashes', 'task_id': 'task_count'})
        
        raise Failure(
            description=f"Evaluation prompt consistency check FAILED: Found {len(inconsistencies)} tasks with inconsistent prompts across {len(inconsistency_summary)} groups",
            metadata={
                "inconsistent_groups": MetadataValue.int(len(inconsistency_summary)),
                "inconsistent_tasks": MetadataValue.int(len(inconsistencies)),
                "first_few_issues": MetadataValue.md(
                    "### Examples of Inconsistent Evaluation Prompts\n\n" +
                    "\n".join([
                        f"- **Gen Task {row.name[0]} + Template {row.name[1]}**: {row['unique_hashes']} different hashes across {row['task_count']} tasks"
                        for row in inconsistency_summary.head(5).itertuples()
                    ])
                ),
                "resolution": MetadataValue.md("""
                ### Fix Steps:
                1. Check evaluation template rendering logic for non-deterministic behavior
                2. Verify generation_response data consistency for same generation_task_id
                3. Re-materialize: `dagster asset materialize --select evaluation_prompt`
                """)
            }
        )
    else:
        # All prompts are consistent - create success report
        results_df = pd.DataFrame([{
            "validation_type": "evaluation_prompt_consistency",
            "status": "PASSED", 
            "groups_checked": total_groups_checked,
            "prompts_checked": total_prompts_checked,
            "inconsistencies_found": 0
        }])
        
        context.add_output_metadata({
            "total_groups_checked": MetadataValue.int(total_groups_checked),
            "groups_skipped": MetadataValue.int(groups_skipped),
            "total_prompts_checked": MetadataValue.int(total_prompts_checked),
            "validation_status": MetadataValue.text("PASSED - All prompts consistent")
        })
        
        context.log.info(f"✓ Evaluation prompt consistency check PASSED: {total_groups_checked} groups, {total_prompts_checked} prompts checked")
    
    return results_df

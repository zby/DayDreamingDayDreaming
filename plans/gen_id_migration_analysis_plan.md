# Gen ID Migration Analysis Execution Plan

## Objective
Operationalize the guidance from `docs/guides/gen_id_migration_analysis.md` so we can safely validate legacy generations, confirm metadata completeness, and be prepared if a full deterministic ID migration ever becomes necessary.

## Prerequisites
- Repository cloned with full `data/gens/` history available (read/write).
- Python environment via `uv` or `.venv` matching project expectations.
- Dagster instance idle; no concurrent runs modifying `data/gens/` while audits execute.

## Plan Overview

1. **Inventory Current Generations**
   - Script: `python scripts/gen_id_inventory.py` (write if missing).
   - Outputs: counts by stage, length distribution, list of legacy IDs (<18 chars).
   - Store report under `reports/gen_id_inventory_{date}.json`.

2. **Metadata Validation Pass**
   - Script: `python scripts/validate_migration_metadata.py`.
   - Assert required fields per stage (draft: combo/template/model/replicate; essay: parent/template/replicate; eval: parent/template/model/replicate).
   - Flag extraneous fields (e.g., `combo_id` on essays/evals) for cleanup.

3. **Parent Chain Consistency Check**
   - Verify every essay’s parent draft directory exists and metadata is readable.
   - Verify every evaluation’s parent essay exists; compute reachability graph to detect orphaned generations.
   - Optional: generate CSV of broken references (if any) for manual remediation.

4. **Deterministic ID Dry Run**
   - Script: `python scripts/recompute_gen_ids.py --check-only`.
   - For each generation, recompute deterministic ID using signatures; compare with existing IDs.
   - Collect mappings (`old_id -> recomputed_id`) for legacy entries without applying changes.

5. **Cross-Artifact Update Impact Assessment**
   - Identify all files referencing gen_ids (`rg "gen_id" data/7_cross_experiment`, cohort memberships, scripts).
   - Enumerate updates required if a rename were performed (per doc sections on cascading updates).

6. **Optional: Migration Simulation (non-destructive)**
   - Use dry-run scripts to rename into a temp directory structure, applying the phased approach described in the guide (drafts → essays → evaluations) and confirming consistency checks.
   - Validate Dagster partition updates in a sandbox instance if renames occur.

7. **Documentation & Sign-off**
   - Publish findings (inventory, metadata validation, parent chain status) under `docs/reports/`.
   - If no blockers, note that full migration remains optional and high risk, matching the guide’s recommendation to accept mixed IDs.

## Tooling Notes
- Scripts mentioned may need to be authored; each should support `--dry-run` and `--output` options.
- Prefer `Path` utilities from `daydreaming_dagster.data_layer.paths` for consistency.
- Logging should highlight items requiring manual intervention.

## Risks & Mitigations
- **Accidental Overwrite**: Keep dry-run default; require explicit flag to mutate files.
- **Incomplete Parent Chains**: Validation step surfaces orphans before migration attempts.
- **Partition Drift**: If renaming is simulated, ensure Dagster dynamic partitions are updated within the same transaction block.


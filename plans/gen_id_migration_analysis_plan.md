# Gen ID Migration Analysis Execution Plan

## Objective
Operationalize (and critically reassess) the guidance from `docs/guides/gen_id_migration_analysis.md` so we:
1. confirm metadata completeness and parent-chain integrity for existing generations;
2. document how deterministic IDs interact with legacy assets during cohort rebuilds; and
3. scope any mitigation work (e.g., new curated modes) needed so “reuse existing evaluations” is reliable without surprise re-generation.

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

4. **Deterministic ID Dry Run (with Rebuild Impact)**
   - Script: `python scripts/recompute_gen_ids.py --check-only`.
   - For each generation, recompute deterministic ID using signatures; compare with existing IDs.
   - Highlight cases where recomputed IDs differ from legacy IDs **and** the curated cohort logic would regenerate downstream stages (draft/essay/eval). Document the practical effect: rerunning `mode: regenerate` will schedule new LLM calls because the deterministic ID does not match the stored legacy asset.
   - Collect mappings (`old_id -> recomputed_id`) for legacy entries; flag any records missing signature inputs.

5. **Cohort Rebuild Behaviour Audit**
   - Exercise each curated mode (`regenerate`, `reuse-drafts`, `reuse-essays`, `reuse-essays + skip-existing-evaluations`) against a sample of legacy essays to observe whether existing generations are reused or regenerated.
   - Capture the exact conditions required to reuse legacy IDs (e.g., only `reuse-essays` keeps evaluations untouched) and where regeneration is unavoidable.
   - Produce recommendations for user-facing docs (and possible new `selected_essays.txt` directives) so operators can intentionally “backfill only missing evaluations” versus “re-run everything”.

6. **Cross-Artifact Update Impact Assessment**
   - Identify all files referencing gen_ids (`rg "gen_id" data/7_cross_experiment`, cohort memberships, scripts).
   - Enumerate updates required if a rename were performed (per doc sections on cascading updates) and note any automation gaps surfaced by the cohort audit.

7. **Optional: Migration Simulation (non-destructive)**
   - Use dry-run scripts to rename into a temp directory structure, applying the phased approach described in the guide (drafts → essays → evaluations) and confirming consistency checks.
   - Validate Dagster partition updates in a sandbox instance if renames occur.

8. **Documentation & Sign-off**
   - Publish findings (inventory, metadata validation, parent-chain status, cohort mode behaviour) under `docs/reports/`.
   - Update `docs/guides/gen_id_migration_analysis.md` with a clarified conclusion: mixed IDs are operationally safe only when replay paths use reuse modes; “regenerate” will not recover legacy assets because deterministic signatures yield new IDs.
   - If no blockers, note that full migration remains optional and high risk, and outline any follow-up (e.g., spec for a new curated mode) discovered during the audit.

## Tooling Notes
- Scripts mentioned may need to be authored; each should support `--dry-run` and `--output` options.
- Prefer `Path` utilities from `daydreaming_dagster.data_layer.paths` for consistency.
- Logging should highlight items requiring manual intervention.

## Risks & Mitigations
- **Accidental Overwrite**: Keep dry-run default; require explicit flag to mutate files.
- **Incomplete Parent Chains**: Validation step surfaces orphans before migration attempts.
- **Partition Drift**: If renaming is simulated, ensure Dagster dynamic partitions are updated within the same transaction block.

# Cohort Reuse & Deterministic Gen IDs

## Goals
1. Reuse existing generations whenever the requested task (combo/template/model/replicate chain) already has outputs in the gens store.
2. Mint new generation IDs only for task/replicate combinations that have never been materialized.
3. Move toward deterministic generation IDs so cohorts act purely as selectors over a global task universe.

## Phase 1 – Signature-Based Reuse
- **Task Signatures**
  - Define canonical signatures per stage:
    - Draft: `(combo_id, draft_template_id, generation_model_id, replicate_index)`
    - Essay: `(draft_gen_id, essay_template_id, replicate_index)`
    - Evaluation: `(essay_gen_id, eval_template_id, eval_model_id, replicate_index)`
  - Add helpers to derive signatures from cohort inputs and from existing metadata.
- **Cohort Membership**
  - For each desired signature, look up existing gens (`data/gens/<stage>/<gen_id>/metadata.json`).
  - Reuse the gen id if a match exists; otherwise, mint a new one (preserving `origin_cohort_id`).
  - Respect replication_config per stage: reuse up to the requested replicate count, then mint new IDs for any remaining slots.
  - Emit diagnostics (counts, reused vs created) for operator visibility.
- **CohortScope**
  - Surface whether a gen is reused based on `origin_cohort_id` vs current cohort.
  - Provide utilities to enumerate missing signatures (potential future manifest use).

## Phase 2 – Deterministic Generation IDs
- **Deterministic ID Function**
  - Implement a stable hashing/salting scheme that maps each signature (including replicate index) to its deterministic gen id.
- **Migration**
  - Once validated, switch `reserve_gen_id` (and any other ID generation sites) to use the deterministic function for all new cohorts.
  - Develop a migration procedure that renames existing gens to their deterministic IDs (renaming directories and metadata), then regenerate downstream tables (aggregated scores, pivots, reports) instead of patching them in place.
  - Rollout sequence: deterministic implementation ➔ staging dry-run ➔ production migration with monitoring/rollback.
- **Fallback**
  - Keep a legacy→deterministic mapping and rollback plan during rollout.

## Phase 3 – Optional Manifest Cache
- Emit `cohorts/<cohort_id>/manifest.json` summarizing signatures → gen metadata.
- Let `CohortScope` load the manifest when present, falling back to direct metadata reads.
- Add CLI/maintenance tasks to regenerate manifests for historical cohorts.

## Testing & Documentation
- Expand unit/integration tests to cover reuse, replication counts, and deterministic ID validation.
- Update architecture documentation to describe `CohortScope`, signature-based reuse, and origin vs current cohort semantics.

# Action Plan: Slim the Cohort Membership Schema

## Goal
- Replace the denormalised `cohort_membership` CSV with a minimal three-column table (`stage`, `gen_id`, `cohort_id`) while keeping generation metadata authoritative in the gens store.

## Desired Outcomes
- Membership assets stay the sole source for "which generations belong to a cohort".
- Prompt/raw/parsed assets continue to find template/combo/model/parent metadata in `data/gens/**/metadata.json` (or an equivalent manifest) without regressions.
- Dagster pivot assets and external scripts rely on the same enriched scores pipeline.
- Legacy cohorts either regenerate cleanly or remain readable during the transition.

## Workstreams & Tasks

### 1. Metadata manifest foundation
- [ ] Add a `StageManifest` helper (likely `src/.../data_layer/`) that maps `gen_id -> template_id/combo_id/llm_model_id/parent_gen_id/replicate`.
- [ ] Populate the manifest inside `cohort_membership` before writing CSV output (source data comes from the existing DataFrame or task tables).
- [ ] Update `_seed_generation_metadata` to read from the manifest (fall back to existing behaviour behind a feature flag until rollout completes).
- [ ] Add validations: fail fast if required metadata is missing when seeding.
- [ ] Unit-test manifest creation and the seeding flow using fixture gens.

### 2. Slim the membership CSV
- [ ] After manifest + metadata seeding succeeds, write `cohort_membership.csv` with only `stage`, `gen_id`, `cohort_id`.
- [ ] Ensure `MembershipService.stage_gen_ids` and `register_cohort_partitions` continue to work unchanged.
- [ ] Implement a compatibility reader that can ingest both wide and slim CSVs (used by resources/tests until all cohorts regenerate).
- [ ] Document the new schema in `docs/` or `AGENTS.md`.

### 3. Refactor lookups, helpers, and tests
- [ ] Rework `membership_lookup.find_membership_row_by_gen` to return the key tuple and fetch extra metadata from the manifest (or gens metadata).
- [ ] Simplify `MembershipService.require_row` to rely on manifest lookups instead of CSV columns.
- [ ] Update `tests/helpers/membership.write_membership_csv` and dependent tests to use the slim schema + manifest fixtures.
- [ ] Backfill new manifest fixtures under `tests/` or `data/gens/*` so unit tests remain fast and isolated.

### 4. Clean up scripts & align pipelines
- [ ] Retire or rewrite `scripts/backfill_draft_combo_ids.py` to operate on manifest or metadata files.
- [ ] Confirm `aggregated_scores` still scopes to cohort via `stage_gen_ids` and reads enrichment from metadata.
- [ ] Publicise a shared helper (Python module or CLI) that both Dagster assets and external pivot scripts can import to resolve manifest + metadata.
- [ ] Add regression tests or smoke checks for `generation_scores_pivot` to ensure only active cohort rows appear after the change.

## Sequencing & Rollout Checks
1. Build manifest helper and dual-read `_seed_generation_metadata` (integration test with a temporary cohort).
2. Switch helper/tests/scripts to manifest-backed metadata.
3. Flip the feature flag to emit slim CSVs; regenerate current cohorts via Dagster materializations.
4. Remove wide-CSV compatibility and delete obsolete scripts.

## Risks & Mitigations
- Metadata might be missing for historical cohorts → provide a migration script to backfill manifest entries from existing `metadata.json` files before slimming the CSV.
- Operators may rely on wide CSVs manually → ship a small `scripts/show_cohort_metadata.py` that prints manifest details on demand.
- Parallel development on cohort tooling → announce the rollout plan and guard the slim CSV write behind a temporary config toggle to prevent conflicts.

# Phase 2 Plan – Cohort Asset Cutover (Incremental)

## Goal
Migrate `cohort_membership` and related helpers to deterministic generation IDs in a staged rollout, using the helper APIs introduced in Phase 1.

## Steps

### Step 1 – Draft Stage (Feature Flag On)
1. Extend `selected_combo_mappings`/draft asset logic to compute draft signatures and deterministic IDs when `DD_DETERMINISTIC_GEN_IDS` is true.
2. Keep essay/evaluation stages on legacy IDs for now; proxy helpers resolve draft IDs back to legacy values when the flag is off.
3. Add unit tests for draft deterministic IDs, including replication handling.

### Step 2 – Essay Stage
1. Build essay signatures using deterministic draft IDs and essay templates.
2. Update essay membership writing to use deterministic IDs (flag-controlled).
3. Adjust supporting logic (parent lookups, tests) to handle mixed cohorts (some deterministic, some legacy).

### Step 3 – Evaluation Stage
1. Derive evaluation signatures from deterministic essay IDs, evaluation templates, and models.
2. Update evaluation membership output accordingly, with feature flag fallback.
3. Update tests and scripts relying on evaluation IDs.

### Step 4 – Cohort Asset Simplification
1. Drop reuse-specific signature lookups inside `cohort_membership`; always compute deterministic IDs from task parameters.
2. Seed metadata for any newly materialized deterministic IDs and rely on Dagster's partition skipping for reruns.
3. Trim membership metadata/metrics to focus on row counts and evaluation fill-up diagnostics.

### Step 5 – Test & Fixture Updates
1. Regenerate test fixtures (`tests/data_pipeline_test/gens/**`) and update unit/integration tests to assert deterministic IDs when the flag is enabled.
2. Run end-to-end suites under both modes (flag off/on) to confirm behavior matches legacy expectations.

### Step 6 – Rollout
1. Enable the flag in staging; materialize sample cohorts, confirm deterministic IDs, and Dagster skipping.
2. Coordinate with operators on rollout timing; once validated, remove the flag and switch all environments to deterministic IDs.

## Risks & Mitigations
- Handle missing metadata via explicit validation errors early in the pipeline.
- Monitor performance; if signature lookups become a bottleneck, prioritize the Phase 3 manifest/cache plan.
- Keep regression fixtures in sync by regenerating them through cohort materializations instead of manual edits.

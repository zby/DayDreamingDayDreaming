# Action Plan: Cohort Membership as a Scoping Layer

## Objectives
- Treat `data/2_tasks` + membership assets as the live definition of "generations included in the current cohort run".
- Keep `data/gens/**/metadata.json.cohort_id` as the immutable origin cohort that first produced a generation, with a future rename to `origin_cohort_id`.
- Remove non-key columns from `cohort_membership.csv`, shifting template/combo/model metadata into a reusable manifest backed by gens metadata.
- Support selective reruns (`skip-existing`) by reintroducing prior generations into the active cohort list without mutating their origin metadata.

## Guiding Principles
- **Single Source of Truth:** Metadata files own per-generation lineage; membership files own cohort scope.
- **Compatibility First:** Maintain a transition layer that reads both wide and slim schemas until all cohorts are regenerated.
- **Fail Fast:** Surface missing metadata or manifest mismatches at asset materialization time.
- **Unified Consumption:** Dagster assets and external scripts resolve cohort scope and metadata through the same helper utilities.

### Current Helper Inventory (Jan 2026)
- `build_curated_entries`, `build_from_drafts` – curated cohort assembly (module-level, unit tested)
- `expand_cartesian_drafts`, `expand_cartesian_essays` – Cartesian cohort expansion via injected row builders
- `build_evaluations_for_essay` – evaluation reuse + allocation helper returning `MembershipRow`s
- `MembershipRow` dataclass – canonical row representation with `to_dict()` bridge for legacy code paths

## Phase Plan

### Phase 1 – Manifest & Dual-Read Infrastructure
- [ ] Build a `StageManifest` helper under `src/daydreaming_dagster/data_layer/` that maps `gen_id -> template_id/combo_id/llm_model_id/parent_gen_id/replicate/origin_cohort_id`.
- [ ] Populate the manifest during `cohort_membership` materialization (derive from existing DataFrame columns for now) and route curated/evaluation-only metadata reconstruction through the manifest builder instead of the membership rows.
- [ ] Refactor `_seed_generation_metadata` to consume manifest entries directly, deleting the per-row `_normalize_*` plumbing and `template_modes` scaffolding; keep a short-lived feature flag to fall back to the legacy DataFrame until historical cohorts migrate.
- [ ] Add validations to error when required manifest fields are absent for any stage/gen pair.
- [ ] Write unit/integration tests covering manifest creation, dual-read, metadata seeding, and the manifest-first code path.

### Phase 2 – Cohort Scope Service & Skip-Existing Flow
- [ ] Introduce a `CohortScope` service that loads the manifest + membership table and exposes:
  - `active_stage_gen_ids(cohort_id)` – current cohort scope from `cohort_membership`.
  - `origin_metadata(gen_id)` – immutable metadata pulled from gens files/manifest.
- [ ] Teach the service to union in reused generations when `skip-existing` is enabled:
  - Read the target stage (e.g., evaluations) from task definitions.
  - Fetch existing gens with matching prompts/templates from metadata, verify they belong to allowed origin cohorts, and add them to the active scope without copying metadata.
  - Mark reused gens so downstream assets can short-circuit materializations (`DagsterAssetCheck` or `skipped` outputs).
- [ ] Document operational flow: a cohort run may contain gens from other origin cohorts; metadata remains unchanged; membership lists carry those gens for scheduling only.
- [ ] Ensure `MembershipService.stage_gen_ids` delegates to `CohortScope.active_stage_gen_ids` for a single entry point.
- [ ] Collapse `register_cohort_partitions` onto the `CohortScope` API so it accepts stage-specific gen-id lists instead of re-filtering the full membership DataFrame.

### Phase 3 – Slim Membership Artifact
- [ ] After manifest + scope service are in place, emit `cohort_membership.csv` with only `stage`, `gen_id`, `cohort_id`, and persist the richer metadata exclusively through the manifest output.
- [ ] Provide a compatibility loader that accepts both wide and slim CSVs until all historical cohorts are re-materialized.
- [ ] Shrink `tests/helpers/membership.write_membership_csv` to the minimal column set and update integration/unit fixtures to source metadata through manifest-aware helpers.
- [ ] Update `utils/membership_lookup` to hydrate metadata from the manifest (single CSV pass for scope only) once slim membership is the default.
- [x] Deprecate scripts that edited membership metadata directly (former `scripts/backfill_draft_combo_ids.py` removed); rely on validation utilities instead.

### Phase 4 – Consumer Alignment
- [ ] Update Dagster assets (`aggregated_scores`, pivots, etc.) and external scripts to import the new scope helper, ensuring both pipelines agree on active cohort membership.
- [ ] Add regression tests/smoke checks verifying `generation_scores_pivot` contains only active cohort rows even when skip-existing reuses prior evaluations.
- [ ] Publish operator documentation describing how to include existing generations in a cohort via configuration instead of manual CSV edits.

### Phase 5 – Metadata Rename & Cleanup
- [ ] Gate the metadata key rename (`cohort_id -> origin_cohort_id`) behind feature toggles and compatibility readers.
- [ ] Backfill historical `metadata.json` files to carry both keys during rollout.
- [ ] Once all code reads through the manifest, remove the legacy `cohort_id` alias and delete wide-CSV compatibility paths.

## Skip-Existing Handling Details
- When an operator enables `skip-existing` for a stage:
  - The task selection logic derives the candidate gen_ids from previous cohorts using manifest metadata (matching on template/combo/parent).
  - Those gen_ids are appended to the membership list with the current cohort ID for scheduling, but `origin_cohort_id` in metadata stays untouched.
  - Dagster partitions check the manifest to determine whether the asset output already exists; if so, materialization is short-circuited and downstream consumers read cached outputs.
  - Reporting assets (aggregated scores, pivots) filter by active membership while preserving origin metadata for transparency.

## Risks & Mitigations
- Missing metadata for historical gens → ship a backfill script that rebuilds the manifest from existing `metadata.json` before slimming the CSV.
- Manual workflows expecting wide CSVs → provide `scripts/show_cohort_manifest.py` to display enriched metadata on demand.
- Concurrent changes to cohort tooling → land the manifest + scope service behind feature flags and coordinate rollout with the team.
- Metadata rename disrupting ad-hoc tooling → keep a temporary reader that accepts both `cohort_id` and `origin_cohort_id` keys until all consumers migrate.

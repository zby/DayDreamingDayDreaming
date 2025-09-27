# Analysis: Slimming the Cohort Membership Schema

## Current shape and motivation
- `cohort_membership` currently writes a **wide** CSV with: `stage`, `gen_id`, `cohort_id`, `parent_gen_id`, `combo_id`, `template_id`, `llm_model_id`, and `replicate`.
- The table is used as both (a) the authoritative list of cohort gen_ids *and* (b) a denormalised cache of generation metadata.
- We do not materialize enough rows for denormalisation to matter for performance; the duplication mainly exists so downstream steps can fetch template/model data without re-reading `metadata.json`.
- Goal: treat membership strictly as "which gen_ids belong to this cohort" and move all task metadata back to the canonical gens store (where raw/parsed assets already persist it).

## Consumers of the wide schema

### Dagster assets
- `group_cohorts.cohort_membership` seeds `metadata.json` for every staged gen via `_seed_generation_metadata` using membership columns (`template_id`, `combo_id`, `parent_gen_id`, `llm_model_id`, `replicate`).
- `group_cohorts.register_cohort_partitions` only needs `stage`/`gen_id`.
- Stage assets (prompt/raw/parsed) call `resolve_generation_metadata`, which expects the metadata seeded above to include template/combo/model/parent. They do **not** read the CSV directly, but today the CSV is the source for that metadata.
- `results_processing.aggregated_scores` uses `membership_service.stage_gen_ids` (stage/gen only). The rest of the scoring/pivot assets consume enriched metadata coming from `data/gens/**/*/metadata.json`.
- `MembershipService.require_row` can enforce presence of `template_id` or `llm_model_id` fields; production assets do not call it yet, but tests expect those columns.

### Utilities & scripts
- `utils.membership_lookup.find_membership_row_by_gen` returns the full row and is used by tests and helpers. Callers only need `stage`/`gen_id` today.
- `scripts/backfill_draft_combo_ids.py` reads `combo_id` from membership rows to patch draft metadata.
- `scripts/data_checks/check_cohort_parsed_coverage.py` needs only `stage`/`gen_id`.
- External pivot scripts (`scripts/build_pivot_tables.py`, `scripts/copy_essays_with_drafts.py`) consume aggregated scores that already embed combo/template/model fields from gens metadata and are cohort-agnostic.

### Tests/helpers
- `tests/helpers/membership.write_membership_csv` emits the wide schema.
- Asset unit tests assert on individual columns such as `template_id` and `llm_model_id`.

## Path to a minimal schema (stage, gen_id, cohort_id)

1. **Design a canonical stage manifest**
   - Introduce a small data layer (e.g., `data/cohorts/<id>/stage_manifest.json`) that maps each gen_id to its template/model/combo/parent.
   - Populate it inside `cohort_membership` when we already know the values (replacing the current CSV duplication) or derive it from existing `data/2_tasks/*` tables.
   - `_seed_generation_metadata` should read from this manifest (or task tables) instead of the DataFrame columns, keeping metadata.json writes intact.

2. **Trim the CSV output**
   - After metadata seeding, drop all non-key columns before writing `membership.csv`. The table becomes three columns: `stage`, `gen_id`, `cohort_id`.
   - Keep parent relationships elsewhere (metadata or manifest) because evaluation/essay assets still need deterministic parents.

3. **Refactor lookup helpers**
   - Update `membership_lookup.find_membership_row_by_gen` to return only the key fields and, when callers request template/model info, redirect them to the stage manifest or the gens metadata.
   - Simplify `MembershipService.require_row`; if extra columns are required, have the resource hydrate them from `metadata.json` instead of the CSV.

4. **Update downstream code & tests**
   - Rewrite `tests/helpers/membership.write_membership_csv` and dependent tests to use the slim schema. When tests need template/combo/model they should store metadata fixtures under `data/gens/.../metadata.json` or leverage the new manifest helper.
   - Remove/retool tests that assert the CSV contains rich metadata (replace with checks that metadata files were seeded correctly).

5. **Revisit scripts**
   - `scripts/backfill_draft_combo_ids.py` becomes obsolete once metadata seeding no longer depends on membership values. Either delete it or rework it to validate metadata against the manifest rather than editing from the CSV.
   - `scripts/data_checks/check_cohort_parsed_coverage.py` already works with the key columns and does not need changes.

6. **Align Dagster pivots with scripting path**
   - Confirm `aggregated_scores` stays cohort-scoped by filtering gen_ids but continues to rely on gens metadata for enrichment.
   - Document that both Dagster pivots (`generation_scores_pivot`, `final_results`, etc.) and external scripts read the same enriched scores so we can eventually share a utility layer and drop the remaining divergence.

## Risks & mitigation
- **Metadata availability:** we must guarantee template/combo/model/parent fields are present in `metadata.json` *before* prompt/raw assets run. Moving the source of truth to a manifest means ensuring that manifest is written atomically and surfaces validation errors early.
- **Backcompat with existing cohorts:** old cohorts contain wide CSVs; the migration should either rewrite them or make the lookup layer tolerant of both shapes until regenerated.
- **Operator workflows:** Some users inspect `membership.csv` manually; document the new format and provide a helper script (e.g., `show_cohort_manifest.py`) when they need detailed task metadata.

## Incremental rollout sketch
1. Create the stage manifest helper and migrate `_seed_generation_metadata` to use it while still writing the wide CSV.
2. Update tests/helpers/scripts to read metadata from the manifest or gens store; stop depending on CSV columns.
3. Switch membership writing to the slim schema (keep a feature flag or backcompat guard for existing cohorts if necessary).
4. Delete deprecated helpers and scripts once all consumers have moved off the wide CSV.

This keeps cohort membership focused on “which gen_ids are in scope” while consolidating task metadata in the gens store—the same pattern already used by cross-experiment scripts. With that separation, unifying Dagster pivots and external tooling becomes much simpler.


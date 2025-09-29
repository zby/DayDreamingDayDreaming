# CohortScope Manifest Follow-up

## Context
- `CohortScope` now serves as the single entry point for resolving cohort stage IDs and per-generation metadata, sourcing details from gens metadata files.
- The membership CSV is slim (stage/gen only); all rich metadata is derived directly from `data/gens/**/metadata.json`.

## Deferred Optimization: Manifest Cache
1. **Manifest Emission**
   - Update `cohort_membership` to emit `cohorts/<cohort_id>/manifest.json` containing the metadata currently read from gens store (template IDs, parent IDs, combo, origin cohort ID, replicate, mode).
   - Store a version marker to allow future schema evolution.

2. **CohortScope Integration**
   - Teach `CohortScope` to load the manifest when present and fall back to on-demand metadata reads.
   - Add a CLI or maintenance asset to regenerate manifests for historical cohorts.

3. **Consumers**
   - Switch heavy consumers (e.g., reporting aggregation, scripts) to rely on `CohortScope` exclusively, avoiding ad-hoc metadata loads.
   - Expose helper methods for skip-existing workflows (e.g., `reuse_candidates(stage)`), centralizing the logic for selective reruns.

4. **Validation & Metrics**
   - Add quick validation (checksum or schema check) when loading manifests to detect drift.
   - Optionally emit debug counters (number of gens per stage, reused gens) for observability.

## Open Questions
- Manifest schema versioning and upgrade path.
- Whether to include derived fields (e.g., resolved essay/draft parents) or keep manifest minimal.

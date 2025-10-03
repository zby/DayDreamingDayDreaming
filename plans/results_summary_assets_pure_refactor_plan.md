# Plan: Extract Pure Transformations for Results Summary Assets

## Objectives
- Decouple heavy pandas transformations from Dagster asset wrappers so logic can be unit-tested without IO or Dagster contexts.
- Documents reporting assets were removed; focus shifts solely to results-summary refactor while keeping reporting cleanup out of scope.
- Reduce test scaffolding by letting unit tests exercise pure functions with DataFrames or in-memory stubs.
- Preserve existing asset contracts (asset names, IO managers, metadata behavior) while slimming wrappers.

## Refactor Strategy
1. **First Option: Minimal Parameter Pure Helpers**
   - Create a new module (e.g. `daydreaming_dagster/results_summary/transformations.py`) exporting pure helpers for:
     - `compute_generation_scores_pivot` (takes the aggregated scores DataFrame, iterable of allowed evaluation templates/models, and an optional generation→path mapping).
     - `compute_final_results` (operates on a filtered scores DataFrame plus simple thresholds).
     - `compute_evaluation_model_template_pivot` (requires only the scores frame and optional coverage hints).
     - `filter_perfect_score_rows` (filters the scores frame and annotates perfect rows with a notes column).
   - Asset wrappers stay responsible for Dagster context, spec loading, and assembling those lightweight params. No new cohort context object unless this approach proves insufficient.
   - If we later discover repeated param bundles or complex interactions, we can evolve toward a small dataclass/typed dict, but treat that as a fallback path.

2. **Slim Dagster Asset Wrappers**
   - In `assets/results_summary.py`, have each asset wrapper focus on:
     - Validating context/partition and fetching cohort configuration (spec allowlists, Paths map) once.
     - Calling the appropriate pure helper with plain inputs (DataFrames, iterables, mappings).
     - Emitting Dagster metadata and returning the helper result.
   - Ensure `_require_cohort_partition` and spec loading logic remain in the wrapper layer; do not move Dagster logging into pure helpers.

3. **Update Unit Tests**
   - Rewrite `assets/tests/test_results_summary_unit.py` to import the new pure transformation functions and exercise them with in-memory DataFrames plus lightweight allowlist/config fixtures (no Dagster `build_asset_context`).
   - Adjust `test_parsed_scores_unit.py` only if helper signatures change (e.g. new helper args) to keep testing the pure `cohort_aggregated_scores_impl` behavior.

4. **Backfill Integration Coverage**
   - Add/keep one integration path that materializes the results-summary asset group end-to-end using existing fixtures, ensuring wrappers still wire correctly after extraction.

5. **Documentation & Cleanup**
   - Update module docstrings/comments to point to the new transformation layer.
   - If behavior diverges (e.g. error handling adjustments), document in `REFactor_NOTES.md` per contract.

## Files Expected to Change
- `src/daydreaming_dagster/assets/results_summary.py`
- New modules under `src/daydreaming_dagster/results_summary/` for pure helpers.
- Unit tests:
  - `src/daydreaming_dagster/assets/tests/test_results_summary_unit.py`
  - `src/daydreaming_dagster/assets/tests/test_parsed_scores_unit.py` (signature adjustments only if needed)
- Potential integration tests under `tests/` if new coverage is added.

## Testing Plan
- Unit: `.venv/bin/pytest src/daydreaming_dagster/assets/tests/test_results_summary_unit.py`
- Integration smoke: `.venv/bin/pytest tests/test_results_summary_pipeline.py` (or nearest existing cohort pipeline test) after ensuring fixtures align.

## Risks & Mitigations
- **Spec loading assumptions**: keep wrapper responsibility for reading cohort specs; pure helpers should not touch disk. Verify with targeted tests.
- **Metadata regressions**: mirror current metadata additions in wrappers to avoid breaking Dagster expectations.
- **IO manager paths**: ensure CSV IO manager still receives identical data frames post-refactor; cover via integration smoke test.

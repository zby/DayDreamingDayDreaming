# Plan: Retire `constants.py` in favor of `Paths`

## Inventory (2025-02-14)
- `src/daydreaming_dagster/constants.py` currently re-exports `STAGES` from `types`, the stage aliases (`DRAFT`, `ESSAY`, `EVALUATION`), and assembles `GEN_FILES` from the filename literals in `data_layer.paths`.
- Internal imports:
  - `src/daydreaming_dagster/resources/gens_prompt_io_manager.py` imports `STAGES`.
  - `src/daydreaming_dagster/assets/documents_reporting.py` imports `STAGES`.
- `GEN_FILES` has **no in-repo usages**; risk is limited to external consumers importing `daydreaming_dagster.constants.GEN_FILES`.

## Design direction
1. Replace internal `constants` imports with direct `types.STAGES` (and drop stage aliases in favor of literals where needed).
2. Introduce a single source for generation artifact filenames via `data_layer.paths` (e.g., `GEN_ARTIFACT_FILENAMES` tuple or a helper on `Paths`) so call sites use `Paths` exclusively.
3. Remove `constants.py` once call sites have migrated, accepting that any out-of-repo importers must update to the new access pattern immediately.

## Implementation steps
1. Update call sites to rely on `Paths` helpers and `types.STAGES`, adjusting tests/docs as needed.
2. Add the `GEN_ARTIFACT_FILENAMES` (or equivalent) helper to `data_layer.paths`.
3. Delete `constants.py` and ensure package `__all__` or init files remain consistent.
4. Run targeted tests to confirm behavior and document the breaking change in `REFactor_NOTES.md`.

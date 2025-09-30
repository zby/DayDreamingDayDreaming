# Batch D — Domain Models & Types Review

## Highlights
- `ContentCombination.from_concepts` generates combo IDs with Python’s built-in `hash`, which is deliberately randomized per interpreter session. This makes combo IDs non-deterministic across runs, conflicting with downstream expectations (e.g., CSV manifests, tests that assume stability). Use the existing `generate_combo_id` helper instead.
- Stage utilities scatter identical helpers (e.g., `_count_non_empty_lines`) across `stage_inputs.py` and `stage_parsed.py`, and re-implement template resolution already offered by `Paths`. Consolidating into a shared module would cut duplication and clarify lifecycles.
- `Stage` literal/type is defined (`types.py`) but many call sites accept raw `str` and normalise manually. Adopting the alias (plus validators) would tighten contracts and reduce downstream `str(stage)` code.

## Models (`src/daydreaming_dagster/models/`)
- **`content_combination.py`**
  - `from_concepts` computes combo IDs via `hash(tuple(concept_ids))` (`content_combination.py:27-44`). Because Python randomises `hash` per process, IDs change between runs. Replace with `generate_combo_id` for stability, and add a regression test mirroring `assets/tests/test_utilities_unit.py`.
  - Metadata is an untyped `dict[str, str]`. Consider a small dataclass or TypedDict capturing `strategy`, `level`, etc., so downstream code documents expectations.
  - `from_concepts_multi` / `_get_available_levels` emit complex metadata structures but no caller consumes them. Verify usage before keeping—these may belong in experimental helpers rather than the core model.

- **`concept.py`**
  - Dataclass is minimal; no validation on description levels. If schema grows, introduce constants or enums for supported levels to prevent silent typos.

## Types (`src/daydreaming_dagster/types.py`)
- `Stage` Literal + `STAGES` tuple exist, yet numerous modules (cohort assets, unified stage code, IO managers) still take `str` parameters. Gradually refactor signatures to accept `Stage` and add `typing.cast` at Dagster boundaries, reducing repeated `str(stage)` conversions and invalid stage checks scattered throughout the code.

## Unified Stage Pipeline (`src/daydreaming_dagster/unified/`)
- `stage_inputs.py` and `stage_parsed.py` both define `_count_non_empty_lines`; extract to `stage_core` (or a small helper module) to avoid drift.
- `_stage_input_asset` and `_stage_parsed_asset` duplicate validation for missing parents, combo lookups, etc. Evaluate whether validations can live in `GenerationMetadata` or a shared validator to cut nested `DDError` scaffolding.
- `_templates_root` in `stage_core.py` re-implements `Paths.templates_root`. Align on `Paths` to consolidate environment overrides and reduce direct `Path("data")` usage.
- `StageSettings` lookups pull dict values with raw strings; once Stage typing is adopted, adjust `ExperimentConfig.stage_config` to use `Stage` keys (TypedDict or `dict[Stage, StageSettings]`).

## Tests (`src/daydreaming_dagster/models/tests`, `src/daydreaming_dagster/unified/tests`)
- Model tests cover the fallback chain and multi-level combos but do not assert ID determinism across runs. After switching to `generate_combo_id`, add a regression ensuring stable IDs.
- `assets/tests/test_utilities_unit.py` already validates `generate_combo_id`; once `from_concepts` reuses it, we can drop redundant checks or convert to shared fixtures.
- Unified stage tests exercise success cases but only partially cover failure contexts (e.g., `_stage_input_asset`’s combo lookup errors). Adding targeted tests will help refactor validations into shared helpers.

## Recommendations
1. Swap `hash`-based ID generation for `generate_combo_id` and document the contract in the model docstring.
2. Centralize shared stage utilities (min-line counts, template roots) under `stage_core` or a dedicated helper module.
3. Incrementally migrate stage-related functions to accept the `Stage` literal, relying on `types.Stage` throughout to enforce valid values.
4. Introduce lightweight typed structures (dataclasses/TypedDicts) for combo metadata to clarify expected keys.
5. Expand unit coverage around combo ID determinism and unified stage error paths to support subsequent refactors.

## Bold (Contract-Breaking) Ideas
- Promote `Stage` to an `Enum` and emit enum values in persisted metadata; would require migration for existing CSVs/JSON but simplifies validation.
- Replace free-form combo metadata with versioned schema objects persisted alongside combos, enabling forward-compatible evolution of concept combination strategies.

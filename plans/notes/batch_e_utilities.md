# Batch E — Utilities & Constants Review

## Highlights
- Shared helpers are fragmented: `generation.py` mirrors functionality already provided by `GensDataLayer`, and unified stage modules each copy private utilities (e.g., `_count_non_empty_lines`). Consolidation would reduce maintenance surface.
- Deterministic IDs rely on multiple layers (`utils.ids`, `assets/group_cohorts.py`, `utils.cohort_scope`). Consider centralising signature logic in a single module with documented contracts.
- Error handling is consistent (`DDError`), but some utilities still raise generic exceptions or return `None` silently; tighten these to enforce structured errors throughout.

## Utilities
- **ID helpers (`src/daydreaming_dagster/utils/ids.py`)**
  - Public functions accept raw `str` stage values; switch to `Stage` Literal to align with other modules and avoid typo guards. Expose a validation helper so assets don’t duplicate stage checks.
  - `_hash_bytes` is private but `reserve_gen_id` exposes `length` parameter; confirm callers always use 16. If so, simplify signature to reinforce deterministic IDs.
  - `signature_from_metadata` expects `template_id` keys that differ per stage (`essay_template`, `evaluation_template`); document contract or normalise before returning to minimise branching downstream.

- **Generation utilities (`src/daydreaming_dagster/utils/generation.py`)**
  - Provides file writers (`write_gen_raw`, etc.) that duplicate `GensDataLayer` methods. Assess if callers (e.g., tests) can use data-layer directly to shrink API surface. If kept, consider routing through `Paths` for consistent path resolution.
  - `load_generation` returns a dict with optional keys; evaluate whether a dataclass would better capture schema and leverage deterministic ID helpers.

- **Combo IDs (`src/daydreaming_dagster/utils/combo_ids.py`)**
  - Imports `generate_combo_id` lazily inside `get_or_create_combo_id`. After fixing the model helper to be deterministic, keep this dependency but consider caching the import (or documenting intentionally dynamic import for boot speed).
  - CSV operations use Pandas for every call; for frequent lookups, a lightweight cache or memoization could prevent repeated reads.

- **Cohort scope (`src/daydreaming_dagster/utils/cohort_scope.py`)**
  - Caches membership and metadata per cohort/gen ID; consider exposing `Paths` integration rather than manual path concatenation for clarity.
  - Stage comparisons use `.lower()` on strings; adopt `Stage` literal to enforce allowed values and remove case conversion.

- **Evaluation parsers (`src/daydreaming_dagster/utils/eval_response_parser.py`)**
  - Raises `DDError` consistently—good reference for other utilities. Evaluate if strategies belong in a registry to support custom parsing without editing this module.

## Constants (`src/daydreaming_dagster/constants.py`)
- `GEN_FILES` mirrors filenames already defined in `Paths`. Clarify whether constants serve external callers; if not, reference `Paths` directly to avoid drift.
- Stage aliases (`DRAFT`, `ESSAY`, `EVALUATION`) duplicate `types.STAGES`. If unused externally, consider inlining where needed.

## Tests (`src/daydreaming_dagster/utils/tests/`)
- Test coverage exists for ID helpers, cohort scope, generation writers, evaluation parsers, etc. Ensure new refactors keep tests colocated (e.g., when merging generation utilities into `GensDataLayer`).
- Add regression tests for stage literal enforcement once utilities adopt `Stage` types.

## Recommendations
1. Merge duplicated file I/O helpers into `GensDataLayer` (or route through it) to reduce parallel APIs writing raw/parsed/prompt metadata.
2. Centralise shared util functions (line counting, ID signatures) in a single module to eliminate copy-paste across unified stages.
3. Tighten stage typing in utilities by leveraging `types.Stage` and `STAGES`; remove manual `.lower()` conversions.
4. Clarify constants’ purpose; if `GEN_FILES` is unused, drop it or document intended consumers.
5. Expand unit tests for deterministic ID behaviour and stage validation when updating utilities.

## Bold (Contract-Breaking) Ideas
- Introduce a formal schema module (TypedDict/dataclasses) for generation metadata/shared structures, replacing free-form dicts across utilities and assets.
- Deprecate `utils.generation` in favour of a consolidated data-layer interface, even if it requires test adjustments.

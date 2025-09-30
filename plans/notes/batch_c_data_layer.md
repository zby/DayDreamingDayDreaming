# Batch C — Data Layer Foundations Review

## Overview
- Scope: `data_layer/paths.py`, `data_layer/gens_data_layer.py`, and call sites in `unified/`, assets using gens store helpers, plus `tests/data_layer/test_gens_data_layer.py`.
- Key theme: helpers exist but consumers frequently bypass them, rebuilding `Path("data")/...` strings and reimplementing template resolution. Simplification should focus on making `Paths`/`GensDataLayer` the single source of truth and tightening stage validation.

## Module Findings

- **Paths** (`src/daydreaming_dagster/data_layer/paths.py:1`)
  - Stage arguments are raw strings with no validation; downstream code repeatedly guards against typo stages. Consider accepting `Stage` enum/`Literal` so invalid stages fail fast.
  - `templates_root()` re-reads `GEN_TEMPLATES_ROOT` each call, while `unified/stage_core._templates_root` re-implements the same logic (`src/daydreaming_dagster/unified/stage_core.py:33`). Provide a shared resolver in `Paths` (or expose `Paths.templates_root` in utilities) and delete duplicate helper.
  - `input_path()` is an alias for `prompt_path()` (`paths.py:38-48`). Decide on a single canonical name to avoid confusion.
  - Consumers still construct literals like `Path("data") / "gens"` (e.g., `definitions.py:126`, `group_cohorts.py:596`). Supplying lightweight wrappers (`Paths.ensure_dir(stage, gen_id)`) or exposing `Paths` via resource injection would let orchestration modules drop manual concatenation.
  - `from_context()` insists on `context.resources.data_root` but Definitions currently configures that resource as a bare string. Document expectation (Path vs. str) or coerce to `Path` internally to avoid surprises.

- **GensDataLayer** (`src/daydreaming_dagster/data_layer/gens_data_layer.py:1`)
  - Read/write helpers share almost identical logic (build path, ensure dir, write text/JSON). Factor out `_write_text(kind, target_func)` / `_read_json(kind)` to collapse duplication and centralize error ctx formatting.
  - `reserve_generation()` simply mkdirs but Stage modules call it before every write (`unified/stage_inputs.py:26`, `stage_raw.py:29`, etc.). Consider exposing higher-level operations (e.g., `layer.ensure_generation(stage, gen_id)`) so callers don’t need to juggle `create=True`.
  - `resolve_generation_metadata()` trims and lowercases fields but leaves `template_id` empty strings valid. Clarify contract or raise a `DDError` when `template_id` is missing (`gens_data_layer.py:252`), aligning with stage inputs.
  - Current API doesn’t surface typed metadata objects for raw/parsed JSON; downstream code loads files directly (`utils/evaluation_scores.py:16`). Explore returning dataclasses for `RawMetadata`/`ParsedMetadata` to standardize schema access.

- **Tests** (`tests/data_layer/test_gens_data_layer.py:1`)
  - Coverage ensures basic read/write but not stage validation or error contexts for raw/parsed metadata. Add tests for `_write_*` helpers once refactored, and for `Paths.from_context` failure cases.

## Dependency Mapping
- Assets (`assets/group_draft.py:36`, `group_essay.py:36`, `group_evaluation.py:38`) and unified pipeline (`unified/stage_inputs.py:26`, `stage_raw.py:118`, `stage_parsed.py:132`) instantiate `GensDataLayer.from_root(context.resources.data_root)` on every call. Consider promoting a shared `gens_data_layer` resource so consumers don’t recreate it.
- `Paths` usage clusters: orchestration (`definitions.py`), cohort builder (`group_cohorts.py:550`), utils (`utils/evaluation_scores.py:16`), documents checks (`checks/documents_checks.py:46`), and unified stages (`stage_core.py:313`, `stage_core.py:452`). Many of these still duplicate literal joins; highlight candidates for migration once `Paths` exposes richer helpers.
- Template resolution currently straddles `Paths` and bespoke helpers (`unified/stage_core.py:39`). Consolidating on `Paths.template_file(...)` would remove environment branching logic from stage code.

## Recommendations
1. **Tighten stage typing**: update `Paths`/`GensDataLayer` signatures to accept `Stage` enums, adding validation and avoiding repeated string normalization. Adjust call sites incrementally (start with unified modules) and extend tests.
2. **Extract shared read/write primitives**: factor JSON/text I/O in `GensDataLayer` into reusable helpers with consistent `DDError.ctx`. Drives down duplication and clarifies error handling expectations.
3. **Expose `Paths`/`GensDataLayer` via resources**: define Dagster resources that hand out pre-bound helpers (`paths`, `gens_data_layer`) so assets stop hardcoding `Path("data")`. This also simplifies future data-root overrides.
4. **Retire duplicate template logic**: replace `_templates_root` and other `Path("data") / "1_raw"` references with `Paths` utilities, ensuring env overrides apply everywhere.
5. **Expand unit coverage**: add colocated tests for `Paths.from_context` (positive/negative), stage validation, and new helper abstractions. Reuse tmp paths so suites stay fast (<1s target).

## Bold / Contract-Breaking Ideas
- Rename `Paths.input_path()` → `prompt_path()` (with compatibility shim) to eliminate aliasing confusion; warn outside consumers via `BACKCOMPAT` tag.
- Persist a canonical metadata schema (dataclass / pydantic) for gens-store JSON, potentially restructuring files (e.g., splitting evaluation metadata). Would require updating downstream parsers.
- Allow configurable data roots per run via resource config rather than hardcoding `"data"` in Definitions; external callers must pass overrides.

## Testing Follow-up
- Design focused fixtures for GEN templates root override and stage validation to support future refactors.
- Ensure `tests/data_layer/test_gens_data_layer.py` exercises new helpers and assertion on `DDError.code`/`ctx` rather than string messages.

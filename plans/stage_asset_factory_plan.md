# Stage Asset Helper Plan

## Objective
Collapse the duplicated prompt/raw/parsed Dagster assets for draft, essay, and evaluation stages by introducing shared helper builders while preserving existing contracts and structured error handling.

## Pre-Implementation Checklist
- [x] Capture current asset metadata for each stage (group, io manager keys, required resources, dependencies) from:
  - `src/daydreaming_dagster/assets/group_draft.py:27`
  - `src/daydreaming_dagster/assets/group_essay.py:27`
  - `src/daydreaming_dagster/assets/group_evaluation.py:28`
  Record in the table below for quick reference before touching code.
- [x] Document evaluation-specific skip rules (`parsed_exists` short-circuit) and confirm no analogous logic exists for draft/essay.
- [x] Re-run baseline unit tests to revalidate the starting point: `.venv/bin/pytest src/daydreaming_dagster/assets/tests/test_essay_generation_tasks.py src/daydreaming_dagster/unified/tests -q`.
- [x] Note current metadata assertions in tests (`test_stage_inputs`, `test_stage_raw`, `test_stage_parsed`) so new expectations stay aligned.
- [x] Ensure `radon cc` scores for the three stage modules are recorded (already captured in `reports/radon_cc.txt`) to compare after refactor.
- [x] Snapshot existing import surface: grep for `from daydreaming_dagster.assets.group_* import` to anticipate downstream adjustments.

### Asset Metadata Snapshot (fill before implementation)
| Stage | Dagster group        | Partitions def                | Prompt IO manager           | Response IO manager        | Required resources                                          | Extra deps                     |
|-------|----------------------|------------------------------|-----------------------------|-----------------------------|--------------------------------------------------------------|--------------------------------|
| draft | `generation_draft`   | `draft_gens_partitions`       | `draft_prompt_io_manager`    | `in_memory_io_manager`       | `{"data_root", "experiment_config"}` (prompt); `{"data_root", "experiment_config", "openrouter_client"}` (raw); `{"data_root", "experiment_config"}` (parsed) | `deps={AssetKey('draft_raw')}` on parsed |
| essay | `generation_essays`  | `essay_gens_partitions`       | `essay_prompt_io_manager`    | `in_memory_io_manager`       | `{"data_root", "experiment_config"}` (prompt); `{"data_root", "experiment_config", "openrouter_client"}` (raw); `{"data_root", "experiment_config"}` (parsed) | `deps={AssetKey('essay_raw')}` on parsed |
| evaluation | `evaluation`        | `evaluation_gens_partitions` | `evaluation_prompt_io_manager` | `in_memory_io_manager`       | `{"data_root", "experiment_config"}` (prompt); `{"data_root", "experiment_config", "openrouter_client"}` (raw); `{"data_root", "experiment_config"}` (parsed) | Prompt depends on `EVALUATION_TEMPLATES_KEY`; parsed `deps={AssetKey('evaluation_raw')}` |

## Current Pain Points
- Stage modules reimplement near-identical asset bodies with stage-specific constants.
- Skip/resume semantics (e.g., "parsed exists" guard in evaluation assets) drift between stages.
- IO manager keys, partition definitions, and resource requirements are manually duplicated, increasing risk when introducing new stages or adjusting configuration.

## Simplification Strategy
1. **Introduce Helper Builders**
   - Create `build_prompt_asset`, `build_raw_asset`, and `build_parsed_asset` functions inside a new module (e.g., `src/daydreaming_dagster/assets/stage_asset_helpers.py`).
   - Each helper accepts explicit parameters (stage name, Dagster group, partitions definition, IO manager key(s), required resource keys, dependency list) plus simple booleans or callbacks (`needs_content_combinations`, `skip_if_parsed_exists`).
   - Helpers return decorated Dagster asset callables using `asset_with_boundary`, `functools.wraps`, and the unified `_stage_*` helpers.

2. **Refactor Asset Modules**
   - Replace implementations in `group_draft.py`, `group_essay.py`, `group_evaluation.py` with assignments using the new helper builders (one call per asset).
   - Maintain module-level docstrings, `__all__`, and any comments describing stage behavior.
   - Provide thin compatibility wrappers if external imports depend on module-level callables.

3. **Error Boundary Alignment**
   - Ensure factory applies `asset_with_boundary` so `DDError` handling remains consistent.
   - Centralize skip/logging via hooks; remove in-module logging that duplicates `_error_boundary` responsibilities.

4. **Testing Updates**
   - Parameterize stage-focused tests (e.g., `test_essay_generation_tasks`, `unified/tests/test_stage_*`) over a simple list of stage names to assert consistent metadata and outputs.
   - Add a small helper-specific unit test verifying each builder sets Dagster metadata (group, resource keys, partitions, IO manager keys) as expected.
   - Confirm evaluation skip behavior via explicit test coverage (expect reuse of parsed artifacts when present).

5. **Migration Safety Checks**
   - Run targeted pytest suites (`.venv/bin/pytest src/daydreaming_dagster/assets/tests -k "stage" -q` and `src/daydreaming_dagster/unified/tests -q`).
   - Compare pre/post `radon cc` entries for stage modules to ensure complexity decrease.
   - (Optional) If Dagster CLI available offline, run `uv run dagster asset list -f src/daydreaming_dagster/definitions.py` before and after to confirm identical asset keys.

## Risks & Mitigations
- **Dagster Decorator Nuances**: Use `functools.wraps` and explicit `name=` to avoid asset name collisions from reused callables.
- **Skip Logic Divergence**: Capture existing evaluation-only skip behavior in helper arguments before deleting the legacy branch; add targeted tests that cover skip + metadata.
- **Test Coverage Gaps**: Parameterizing tests reduces bespoke fixtures; ensure assertions on metadata (`function`, `stage`, `gen_id`, lengths) remain stage-specific where necessary.

## Deliverables
- Shared stage asset helper module providing prompt/raw/parsed builders.
- Simplified stage asset modules delegating to the factory.
- Updated tests and any new fixtures/utility functions required for parameterization.
- Summary note in `REFactor_NOTES.md` if incidental logging or behavior is intentionally removed.

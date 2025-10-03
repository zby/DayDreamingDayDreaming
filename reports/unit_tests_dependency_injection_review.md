# Unit Test Structure Review (Dependency Injection Opportunities)

Date: 2025-03-18
Reviewer: ChatGPT (Codex CLI)

Goal: Identify colocated unit tests that would become simpler or faster if the
production code exposed narrower seams, purer helpers, or dedicated test
fixtures.
Coverage: Unit tests under `src/daydreaming_dagster/**/tests/`.

---

## Summary Table

| Test module(s) | Current pain points | Suggested structural changes |
|----------------|--------------------|------------------------------|
| `models/tests/test_content_combination.py`, `assets/tests/test_utilities_unit.py`, `assets/tests/test_membership_row_unit.py` | Already exercise small pure helpers; light setup. | Keep as-is; ensure models continue to supply pure factories so tests stay simple. |
| `spec_dsl/tests/test_loader.py`, `spec_dsl/tests/test_compiler.py`, `spec_dsl/tests/test_errors.py` | Loader/compiler tests write JSON/YAML files just to reach parsing paths. | Let loader accept file-like objects or raw dict payloads; expose `compile_design_from_dict(...)` so tests can skip disk IO. |
| `resources/tests/test_in_memory_io_manager.py` | Tests build Dagster contexts manually to reach IO managers. Tight coupling to Dagster internals. | Expose production helper `build_generation_context(stage, gen_id, data_root)` returning the minimal context interface; reuse in tests to avoid direct `build_output_context` usage. |
| `checks/tests/test_documents_checks.py` | Uses `SimpleNamespace` to fake Dagster context and touches private `_files_exist_check_impl`. | Promote `_files_exist_check_impl` to public helper accepting `(paths, stage, gen_id)` so tests can call a pure function without fabricating Dagster context objects. |
| `data_layer/tests/test_gens_data_layer_ids.py`, `utils/tests/test_ids.py` | Real filesystem writes for every ID reservation / metadata read. | Introduce `InMemoryGensStore` or let `GensDataLayer` accept an injected storage backend; tests could use in-memory dicts while production keeps filesystem backend. |
| `cohorts/tests/test_spec_planner.py` | Relies on fixture directories under `tests/fixtures/spec_dsl`; bespoke catalog dict duplicated. | Ship a `SpecFixtureCatalog` helper (in production) that returns minimal catalogs + spec dataclasses so tests don’t depend on external fixture trees. |
| `assets/tests/test_cohort_id_asset.py`, `assets/tests/test_cohort_membership.py`, `assets/tests/test_cohort_builders_unit.py`, `assets/tests/test_essay_generation_tasks.py` | Each test constructs raw CSVs, cohort specs, and membership CSVs from scratch; lots of duplication and disk churn. | Provide a reusable `CohortDataRootBuilder` in production that seeds raw tables, spec bundles, and gens directories; expose builders for membership manifests so tests focus on behaviour instead of scaffolding. |
| `assets/tests/test_results_summary_unit.py`, `assets/tests/test_parsed_scores_unit.py`, `assets/tests/test_documents_reporting.py` | Assets read/write cohort specs, run Dagster materializations, and require `build_asset_context`; tests effectively run mini integration flows. | Split assets into pure functions (`results_summary_from_frames(...)`, `render_documents_reports(...)`) and keep Dagster asset wrappers thin. Tests could call pure functions with in-memory DataFrames, avoiding Dagster instances and file IO. |
| `assets/tests/test_stage_assets_unit.py`, `assets/tests/test_partitions_wiring.py`, `assets/tests/test_error_boundary.py` | Verify metadata/partitions constants but rely on Dagster asset defs or contexts. | Expose stage metadata via dataclasses (`StageAssetSpec`) so assertions compare plain objects without referencing Dagster internals. |
| `unified/tests/test_stage_services_unit.py`, `unified/tests/test_stage_inputs.py`, `unified/tests/test_stage_raw.py`, `unified/tests/test_stage_parsed.py` | Heavy use of on-disk gens directories, manual metadata JSON, bespoke context mocks. | Extract a `StageIOFacade` interface (filesystem + in-memory implementations). Stage services would depend on the facade; tests inject the in-memory fake and assert against captured writes. |
| `utils/tests/test_evaluation_scores.py`, `utils/tests/test_evaluation_processing.py`, `utils/tests/test_cohort_scope.py`, `utils/tests/test_eval_response_parser.py` | Aggregator tests hit the filesystem via `Paths` helpers; repeated metadata seeding. | Allow score aggregators to accept injected `load_generation`/metadata callables; ship small metadata factories so tests stop writing JSON. |
| `assets/tests/test_documents_reporting.py`, `resources/tests/test_in_memory_io_manager.py`, `assets/tests/test_stage_assets_unit.py` | Several tests depend on Dagster `build_asset_context` or `materialize`, increasing runtime and coupling. | Offer a production test harness (e.g., `asset_test_context(data_root, partition_key)`) and expose report calculations as pure helpers to reduce Dagster runtime usage. |
| `config/tests/test_paths_unit.py`, `tests/test_definitions_stage_registry.py` | Light smoke coverage but assert on internal resource types. | Expose public accessors (e.g., `definitions.describe()`) returning serializable configs to avoid peeking at private attributes. |

---

## Detailed Observations

### Spec DSL tests (`spec_dsl/tests/...`)
- File-backed specs make simple validation tests noisy. Supporting direct dict
  inputs in the loader/compiler would eliminate repeated temp-file handling.
- Consider shipping a minimal `ExperimentSpecFactory` with canned axis/rule
  examples; both production code and tests could reuse it.

### Cohort-oriented assets (`assets/tests/test_cohort_*`, `test_essay_generation_tasks.py`)
- Four separate modules reimplement the same `_write_spec` and raw CSV setup.
  Tests spend ~80% of their lines on scaffolding rather than assertions.
- A production `CohortFixtures` utility that emits a fully wired temporary data
  root (raw tables, spec bundle, gens skeleton) would remove this duplication
  and keep specs consistent.

### Results summary & reporting assets (`assets/tests/test_results_summary_unit.py`, `test_parsed_scores_unit.py`, `test_documents_reporting.py`)
- Logic under test munges DataFrames, yet tests must build Dagster contexts and
  on-disk CSVs. Splitting transformations into pure functions (with thin asset
  wrappers) would let unit tests invoke the pure layer with pandas frames
  directly. Dagster materialization paths can fall to integration tests.

### Unified stage services (`unified/tests/test_stage_*`)
- Stage helpers repeatedly reseed gens directories and metadata JSON.
- Introduce a `StageIOFacade` protocol plus in-memory implementation for tests.
  Stage services would depend on the facade instead of concrete filesystem
  layout, and tests could assert against captured writes without disk IO.

### Score aggregation utilities (`utils/tests/test_evaluation_scores.py`, etc.)
- Aggregator already allows injecting `load_generation_fn`; extend this to cover
  metadata lookups so tests can keep everything in-memory.
- Provide small factory functions (e.g., `make_eval_metadata(...)`) to avoid
  duplicating JSON payloads across tests.

### Resource/Dagster context-heavy tests (`resources/tests/test_in_memory_io_manager.py`, `assets/tests/test_stage_assets_unit.py`)
- Context creation via Dagster helpers is repeated and brittle. Publishing a
  thin context-builder in production (used by tests and CLI) would stabilize
  tests and centralize Dagster version drift handling.

### Stable smoke tests (`models/tests/test_content_combination.py`, `assets/tests/test_utilities_unit.py`, `utils/tests/test_eval_response_parser.py`)
- Already exercise pure logic with minimal setup; keep them unchanged while
  ensuring the corresponding production modules stay dependency-light.

---

## Prioritized Recommendations

1. **Ship a `CohortDataRootBuilder`** that seeds specs, raw tables, and gens
   directories; refactor cohort asset tests to use it.
2. **Expose pure transformation helpers** for results-summary and documents
   assets so unit tests no longer reach Dagster materializations.
3. **Introduce a storage facade for `GensDataLayer`** (filesystem + in-memory
   implementations). Adopt it in stage services and data-layer tests.
4. **Add loader/compiler entry points that accept dict payloads** in the spec DSL
   so tests can bypass temporary files.
5. Long-term: **publish a unified Dagster context factory** for assets and
   resource tests to minimize direct coupling to `build_asset_context`.

---

## Notes
- Once cohort outputs are fully partition-scoped, revisit unit tests that still
  touch raw catalogs to ensure they rely on cohort allowlists instead.
- Any new helper factories should live in production modules (not tests) so they
  benefit integration tests and scripts as well.


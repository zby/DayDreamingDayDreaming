**Goal**
- Remove the intermediate “raw_data” compute assets (`concepts`, `llm_models`, `link_templates`, `essay_templates`, `evaluation_templates`).
- Make downstream consumers read raw CSVs/files directly and apply filtering (e.g., `active`) within the consumer assets.
- Preserve pull-based staleness: changes to raw CSVs should still mark downstream stale and auto-materialize without human intervention.

**Why**
- Simplify the graph by modeling the raw layer as immutable external sources only.
- Reduce redundant materializations and keep logic close to where it matters (consumers).
- Avoid duplicating raw transforms when downstream can cheaply re-read raw data.

**Current State (relevant)**
- CSVs are modeled as explicit SourceAssets (5 keys): `concepts_metadata_csv`, `llm_models_csv`, `link_templates_csv`, `essay_templates_csv`, `evaluation_templates_csv` under the `raw_sources` group.
- Intermediate compute assets load/filter raw files and are currently upstream of core and task assets.
- A schedule scans the CSVs and materializes the `raw_data` group when changes are detected.

**Target State**
- No `raw_data` compute assets.
- Consumers read raw files directly via small helpers and perform filtering (e.g., `active`) locally.
- Consumers declare non-material inputs as dependencies on SourceAsset keys (non_argument_deps) or use observations-only pull.
- The schedule switches from “materialize raw group” to “emit SourceAssetObservations for changed CSVs”. Auto-materialize then pulls downstream.

**Design Options**
- A) Observation-only pull (recommended)
  - Keep CSVs as SourceAssets. Replace the current schedule with an observation job that emits `SourceAssetObservation` per changed CSV (with `data_version` = fingerprint). Enable AutoMaterialize on consumers; the daemon materializes only stale consumers.
  - Pros: No need to materialize raw compute assets; clean pull semantics; focused runs.
  - Cons: Requires adding the small observation job and flipping the schedule.

- B) Direct read + declared deps (alternate)
  - Remove raw compute assets. Modify consumers to read files but declare `non_argument_deps={...CSV AssetKeys...}` so staleness is tracked on the SourceAssets. Keep current schedule but target a job that directly materializes consumers (selection), or rely on an AssetReconciliationSensor/auto-materialize.
  - Pros: Simple; works even without observations.
  - Cons: You lose the richer metadata trail of observations on sources.

Recommendation: Option A. It’s cleaner operationally and leverages Dagster’s source observation model for “pull”.

**Refactor Steps**
1) Introduce read helpers (pure functions, testable)
   - New module: `daydreaming_dagster/utils/raw_readers.py` with functions:
     - `read_concepts(data_root: Path, filter_active: bool) -> List[Concept]` (uses concepts_metadata.csv + description files).
     - `read_llm_models(data_root: Path) -> pd.DataFrame`.
     - `read_link_templates(data_root: Path, filter_active: bool) -> pd.DataFrame` (csv + join text content on demand).
     - `read_essay_templates(data_root: Path, filter_active: bool) -> pd.DataFrame`.
     - `read_evaluation_templates(data_root: Path) -> pd.DataFrame`.
   - These functions do not create Dagster assets; they just read from disk.

2) Update consumer assets to inline filtering and reading
   - `content_combinations`:
     - Remove `concepts` input parameter; call `read_concepts(data_root, filter_active=True)` inside.
     - Declare `non_argument_deps={AssetKey('raw_source', 'concepts', 'concepts_metadata_csv')}` so staleness tracks the CSV. (Descriptions assumed immutable.)
   - `link_generation_tasks`:
     - Remove inputs `llm_models` and `link_templates`; call the readers internally. Apply `active` filtering on templates here.
     - Declare `non_argument_deps={... llm_models_csv, link_templates_csv}`.
   - `essay_generation_tasks`:
     - Remove inputs `essay_templates`; call the reader and filter active.
     - Declare `non_argument_deps={... essay_templates_csv}`.
   - `evaluation_tasks` and any evaluation consumers:
     - Remove `evaluation_templates` input; call the reader.
     - Declare `non_argument_deps={... evaluation_templates_csv}`.
   - Any other asset that used the raw compute assets must be updated to read via the helpers and declare the appropriate non_argument_deps.

3) Delete intermediate raw compute assets
   - Remove `concepts`, `llm_models`, `link_templates`, `essay_templates`, `evaluation_templates` from `assets/raw_data.py`.
   - Keep only the five CSV SourceAssets (with group `raw_source_assets` for clarity).
   - Update `definitions.py` to drop removed assets from `assets=[...]`.

4) Flip the schedule to observations (Option A)
   - Replace `raw_reload_job` with a small job/op `observe_raw_sources` that takes the list of changed CSVs and emits a `SourceAssetObservation` per file with a `data_version` = fingerprint and useful metadata (size, mtime).
   - Update the schedule’s `execution_fn` to return a `RunRequest` targeting `observe_raw_sources` and pass changed CSVs via run config.
  - Ensure downstream consumers have `AutomationCondition` (already present) so the daemon pulls them when the observed versions change.

5) Selections and groups
   - Remove any references to `group:raw_data` in docs/commands.
   - If convenient, group readers/consumers under existing groups (`task_definitions`, etc.).
   - Keep CSV SourceAssets under `raw_source_assets` for UI clarity.

6) Documentation
   - Update README/docs to explain that raw CSVs are external sources; consumers read files directly and filter.
   - Note assumptions: CSVs change; template/description text files are effectively append-only/immutable; new versions appear as new IDs/rows.

7) Backward-compat and migration window
   - None. This refactor is intentionally breaking: remove intermediate assets outright and update all consumers/tests accordingly. If something is missed, allow failures and fix forward.

**Testing Plan**
- Unit tests
  - `tests/utils/test_raw_readers.py` for each reader: use temp `data_root`, assert parsing, filtering, content join, and failure messages.
  - Consumer assets unit-level tests: verify they work without raw inputs and still produce the same schemas/metadata.

- Integration tests
  - Import `Definitions`; ensure graph loads without raw compute assets.
  - Observation path: modify one CSV, run the schedule’s `execution_fn` → it returns a RunRequest; execute the observation job; assert that only impacted consumers are stale (can assert on materialization requests in an auto-materialize dry run or by evaluating a reconciliation plan in tests).
  - End-to-end: run `content_combinations` pipeline selection; confirm it reads concepts via the reader and applies `active` filtering.

- Regression tests
  - Replace or adapt tests that imported/depended on old raw compute assets (e.g., `test_raw_data_simple.py`): move those checks to reader tests.
  - Keep existing higher-level tests (generation/evaluation) intact; update any fixtures to not rely on raw assets.

**Rollout Steps (breaking, no backcompat)**
- Commit 1 (breaking refactor):
  - Add `raw_readers.py`. Refactor all consumer assets to read/filter directly and declare `non_argument_deps` on CSV SourceAssets.
  - Delete the five raw compute assets immediately and remove them from `definitions.py` and any selections/tests.
  - Fix failing imports/tests until green.
- Commit 2 (schedule flip to observations):
  - Add observation job/op and change the schedule to emit observations for changed CSVs instead of materializing any raw assets.
  - Confirm auto-materialize runs consumers as expected.
- Commit 3 (docs):
  - Update README/docs to match the new model (no intermediate raw assets).

**Risks & Mitigations**
- Loss of raw caching: Consumers will re-read CSVs on each run. Mitigate by keeping readers efficient and data small (true here), or add light in-process memoization if needed.
- Hidden I/O in consumers: Use clear logging and output metadata to surface counts, paths, and filter stats.
- Missing staleness if text files change: Document the assumption; rely on CSV row additions (-v1, -v2 IDs) to reflect changes.

**Success Criteria**
- No `raw_data` compute assets remain; graph still loads cleanly.
- On CSV change, schedule emits observations; auto-materialize runs only affected consumers.
- All tests pass; new reader tests cover parsing and filtering; generation/evaluation flows unmodified from a user perspective.

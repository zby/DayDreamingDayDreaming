**Objective**
- Refactor the raw layer so the external files under `data/1_raw/**` are represented as Dagster `SourceAsset`s. Keep current “loader” logic as intermediate compute assets that apply filtering/aggregation and produce exactly the same outputs (thus no downstream changes).
- This enables observation-driven staleness: later, a schedule will emit `SourceAssetObservation`s with per-file fingerprints so only impacted downstream becomes stale and auto-materializes.

**Scope**
- In-scope: Introduce per-file (or per-subset) SourceAssets; rewire existing raw compute assets to depend on them; preserve asset keys/IO types; register in `definitions.py`.
- Out-of-scope (handled by a separate plan): Observation/schedule wiring (Option B). This plan sets the stage by making all raw inputs SourceAssets.

**Current Raw Assets (from `assets/raw_data.py`)**
- `concepts`: reads `1_raw/concepts/concepts_metadata.csv` and description files from `1_raw/concepts/descriptions-{sentence,paragraph,article}/*.txt` and filters by `active`.
- `llm_models`: reads `1_raw/llm_models.csv`.
- `link_templates`: reads `1_raw/link_templates.csv` and `1_raw/generation_templates/links/*.txt` with optional `active` filtering.
- `essay_templates`: reads `1_raw/essay_templates.csv` and `1_raw/generation_templates/essay/*.txt` with optional `active` filtering.
- `evaluation_templates`: reads `1_raw/evaluation_templates.csv` and `1_raw/evaluation_templates/*.txt`.

All of these are currently software-defined `@asset`s that directly read files. They also already have `AutomationCondition.eager()` on the compute assets.

**Granularity Strategy**
- Concepts: Use ONLY the metadata CSV as the SourceAsset (no per-concept description SourceAssets). We assume concept definitions are append-only; new concepts are added as new rows/IDs (e.g., with -v1, -v2), and existing concept description files do not change in-place.
- Templates/Eval: Use ONLY the CSVs as SourceAssets (no per-template `.txt` SourceAssets). We assume template text files do not change in-place; updates occur by adding new template rows/IDs in the CSVs.
- Other raw inputs: `llm_models.csv` as a single SourceAsset.

**Asset Key Conventions**
- Namespace under a clear, stable prefix, e.g. `AssetKey(["raw_source", <relative_path_components>])`.
  - Example: `raw_source/concepts_metadata.csv`.
  - Example: `raw_source/concepts/descriptions-sentence/<file>.txt`.
- Maintain current compute asset keys for compatibility: `concepts`, `llm_models`, `link_templates`, `essay_templates`, `evaluation_templates` remain unchanged.

**Intermediate Compute Assets (unchanged outputs)**
- Keep the existing compute assets but explicitly declare dependencies on the new SourceAssets via `non_argument_deps` (lineage only). The body continues to read from `data_root` paths and apply current filtering and validation.
  - `concepts` depends on: ONLY the `concepts_metadata.csv` SourceAsset. It continues to read description files from disk; changes to existing descriptions will NOT mark it stale (by design/assumption). Adding a new concept implies the metadata CSV changes, which will mark it stale.
  - `llm_models` depends on: `llm_models.csv` SourceAsset.
  - `link_templates` depends on: `link_templates.csv` SourceAsset + `generation_templates/links/*.txt` SourceAssets.
  - `essay_templates` depends on: `essay_templates.csv` SourceAsset + `generation_templates/essay/*.txt` SourceAssets.
  - `evaluation_templates` depends on: `evaluation_templates.csv` SourceAsset + `evaluation_templates/*.txt` SourceAssets.
- Rationale: Declared dependencies allow observations on those SourceAssets to mark these compute assets stale, while code remains simple and unchanged for now.

- **Handling “active” filtering**
- Leave the active filter in the compute assets (unchanged behavior). SourceAssets represent raw bytes presence/version only; policy decisions (active/inactive) stay in compute. For concepts, since only the metadata CSV is observed, toggling an existing row’s fields will mark stale (CSV changed), while editing description text alone will not.
- Optional refinement later: introduce tiny directory/CSV-level “sentinel” compute assets to encapsulate filtering logic, but not needed now.

**Implementation Steps**
1) Inventory files and define key mapping
   - Create a small helper in `daydreaming_dagster/utils/raw_source_keys.py`:
     - `relative_path_to_source_key(rel_path: str) -> AssetKey` per the `raw_source/` convention.
     - Helpers to enumerate expected CSV/template/description files from `data_root`.
   - This helper is also used later by the observation schedule.

2) Add SourceAssets
   - New module `daydreaming_dagster/assets/raw_sources.py` that constructs `SourceAsset` objects for each discovered file at import time.
   - For directories that may grow: build SourceAssets dynamically based on current filesystem contents at import. Accept that adding a new file requires a Dagster process restart to register the new SourceAsset; the observation schedule will still detect changes in-between.
   - Attach minimal metadata (e.g., `path`) and put them in a group like `raw_sources` for clarity.

3) Rewire compute assets to declare dependencies
   - In `assets/raw_data.py`, update decorators to include `ins` mapping to the corresponding SourceAsset keys. Body stays the same.
   - Keep function names, return types, metadata, and group name (`raw_data`) unchanged to avoid downstream changes.

4) Register in `definitions.py`
   - Import `raw_sources` list and pass via `Definitions(..., assets=[...], source_assets=[...])`.
   - Ensure no asset key collisions; verify groupings (`raw_sources` vs `raw_data`).

5) Backward compatibility and docs
   - Verify asset keys for compute assets remain identical (avoid breaking downstream selection strings/tests).
   - Add short docs in `docs/` about raw SourceAssets and observation-driven staleness (linked from plan B).

**Testing Plan**

- Unit tests (colocated next to utils/assets as appropriate):
  - `test_raw_source_keys.py`: path→AssetKey mapping is deterministic and reversible enough for debugging; enumerate expected files under a temp `data_root`.
- `test_raw_sources_registry.py`: given a temp directory with a few files, the module builds the expected set of SourceAssets with correct keys and metadata. Assert that no SourceAssets are created for concept description files; only `concepts_metadata.csv` is present for concepts.
  - `test_raw_data_dependencies.py`: ensure compute assets declare `ins` that include the relevant SourceAsset keys (can inspect `assets_def` to assert upstream keys).

- Snapshot/behavior tests for compute assets (unchanged outputs):
  - For each compute asset, run it against a temp `data_root` with sample files and confirm outputs are identical to pre-refactor expectations (e.g., concept count, template content columns). These can live alongside existing tests or in `tests/`.

- Integration tests (in `tests/`):
  - Boot `Definitions` with a temp `data_root` and `DAGSTER_HOME`.
  - Materialize `raw_data` assets; assert success and correct metadata.
  - Emit a manual `SourceAssetObservation` for `concepts/concepts_metadata.csv` and assert `concepts` becomes stale; observe that changing only a description file does not toggle staleness (documented limitation by design).
  - Ensure downstream selections still work (e.g., `AssetSelection.groups("raw_data")`).

**Rollout & Staging**
- Commit 1 (logic):
  - Add utils + SourceAssets module and register them.
  - Rewire compute decorators to declare `ins` dependencies.
  - Ensure all tests pass locally.
- Commit 2 (style):
  - Run Black/Ruff and commit only formatting changes.
- Post-merge: restart `dagster dev` (daemon on) to load the new SourceAssets. No data migration required.

**Risks & Mitigations**
- Many SourceAssets (per-file): small dataset keeps this manageable. If it grows noisy, collapse some paths to directory-level SourceAssets without changing compute keys.
- Dynamic file discovery at import: adding new files requires a process restart to register new SourceAssets; the observation schedule will still notice new files for observation events after restart.
- Asset key drift: strictly adhere to the `raw_source/...` convention and pin compute asset keys unchanged.

**Next Plan (separate)**
- After this refactor lands, proceed with the Option B observation plan assuming: “All raw inputs are modeled as per-file SourceAssets.” That plan wires the schedule that emits per-file observations and enables auto-materialize for downstream pull.

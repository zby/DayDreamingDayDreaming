# Batch A — Orchestration Shell Review

**Scope**: `definitions.py`, `config/`, `jobs/`, `schedules/`, `checks/`
**Context**: Baseline metrics captured in `plans/notes/metrics_baseline.md`. Tests to rerun after changes: `.venv/bin/pytest src/daydreaming_dagster -k "(definitions or raw_schedule or documents_checks)"` plus integration smoke `pytest tests/test_import_definitions.py`.

## `src/daydreaming_dagster/definitions.py`
- **Manual asset registry** (`definitions.py:80-119`): Long asset list is easy to drift when new assets land. Consider switching to `load_assets_from_modules([group_draft, ...])` or small helper that groups per stage. Keeps orchestration thin and aligned with module boundaries. Risk: medium (asset selection order may change); add targeted smoke test asserting key asset keys present.
- **Duplicate imports + unused symbols** (`definitions.py:31-53`, `definitions.py:68`): `Stage` never used; `group_cohorts` imported twice (for `cohort_membership` and again for `cohort_id`). Inline into single import block or import module once and reference attributes to cut noise. Risk: low; unit tests untouched.
- **Resource dict repetition** (`definitions.py:126-160`): Multiple IO managers share the same base path construction (`Path("data")`). A tiny helper (e.g., `data_root = Path("data")`) or using `Paths.from_context` during definition would shrink duplication and reduce chance of typos if directories move. Ensure `config/tests/test_paths_unit.py` still passes.
- **Mixed types for `data_root` resource** (`definitions.py:131`): currently a plain string while IO managers get `Path`. Unify on `Path` (or `Paths`) so downstream helpers receive consistent types. Should be contract-safe but check for callers expecting a string (grep for `resource_key="data_root"`).
- **Executor wiring** (`definitions.py:73-78`): Env-based branch is fine but could move to helper in `utils` so other entry points (CLI tooling) reuse logic. Optional clean-up.

## `src/daydreaming_dagster/config/paths.py`
- File is a deprecated re-export shim. No in-repo usages (`rg "config.paths" src` returns none). Candidate for removal once external consumers confirmed. Mark with `BACKCOMPAT:` tag or schedule deletion to avoid lingering indirection. Tests: ensure any integration referencing old path (if any) adapt first.

## `src/daydreaming_dagster/jobs/`
- Package exists but is empty; consider deleting directory or adding README explaining absence to avoid false-positive expectations when scanning repo. Low risk.

## `src/daydreaming_dagster/schedules/raw_schedule.py`
- **Hard-coded paths** (`raw_schedule.py:20-25`, `raw_schedule.py:65`): Raw CSV locations sprinkled as literals; leverage `Paths` or a single `RAW_ROOT = Path("data") / "1_raw"` to simplify adjustments and support alternative data roots in tests. Need a unit exercising override (mock env) before refactor.
- **Helper sprawl** (`raw_schedule.py:36-61`): `_cron`, `_dagster_home`, `_build_state`, `_changed_files` are minimal wrappers but add vertical noise. Move fingerprinting helpers into `utils/raw_state.py` (already defines `FingerprintState`) so schedule file focuses on Dagster mechanics. Existing unit `src/daydreaming_dagster/schedules/tests` (none) → should add new tests around `_changed_files` before moving.
- **State persistence side effects** (`raw_schedule.py:67-79`): Write happens before run kicks off; consider storing in Dagster run tags or deferring write until downstream asset success. That is a deeper change (contract-breaking for retries); log as bold idea for later.
- **Tag truncation** (`raw_schedule.py:75`): Only first five filenames included; maybe supply counts instead to avoid long strings. Cosmetic improvement.

## `src/daydreaming_dagster/checks/documents_checks.py`
- **Unused helper + heavy dependency** (`documents_checks.py:25-38`): `_resolve_doc_id` never called, yet keeps `pandas` import alive. Removing both simplifies the module and avoids unnecessary dependency at runtime. Risk: low; add unit ensuring metadata unaffected.
- **Context probing logic** (`documents_checks.py:21-22`): Pulls partition key via `getattr`. Dagster exposes `context.partition_key`. Inline to reduce reflection, but confirm compatibility with asset check context API. Needs new unit test.
- **Dynamic global mutation** (`documents_checks.py:66-68`): Loop modifies `globals()` to register checks. Instead, return explicit names in dictionary and reference in `Definitions` to reduce magic. Would require touching `definitions.py` asset check list. Tests: update `assets/tests/test_documents_reporting.py` or add new ones for checks.
- **Metadata surfacing** (`documents_checks.py:49`): Provide parsed file existence and optionally actual path status (boolean) to aid triage. Could add more structured metadata (counts) if logs show confusion.

## Test & Coverage Follow-ups
- Add a slim unit suite for `raw_schedule` helpers to enable safe refactors (validate fingerprint diffing and env overrides).
- Introduce tests around `documents_checks` to lock behavior before removing `_resolve_doc_id` and pandas dependency.
- Consider an orchestration smoke test asserting expected assets/checks/schedules are registered (guard against accidental drops when automating registration).

## Confusing Constructs to Catalog
- Manual `Definitions` wiring vs. module-driven registration; easy to miss new assets.
- Dynamic `globals()` mutation in checks; surprising for new contributors.
- Empty `jobs/` package suggests missing functionality.

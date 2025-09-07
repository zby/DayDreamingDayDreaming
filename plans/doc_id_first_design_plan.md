# Plan: Doc ID–First Lineage and Resolution

Status: draft
Owner: daydreaming_dagster
Related: plans/remove_sqlite_index_plan.md (pointer files)

## Summary

Replace “latest-by-task” linking with explicit, pinned doc IDs across stages. Essays must reference a concrete parent draft `doc_id`; evaluations must reference a concrete target `doc_id`. We will treat doc IDs as required (no optionality), which improves determinism, reproducibility, and debuggability, and removes any DB reliance.

## Goals

- Deterministic lineage: essay → draft doc_id; evaluation → target doc_id.
- Enforce pinned lineage: reject tasks missing required doc IDs.
- Minimal surface for runtime lookups: prefer a filesystem helper such as `get_row_by_doc_id(stage, doc_id)`; keep “latest-by-task” pointers for ops/legacy only.
- Update scripts/UX to make it easy to curate doc_id–pinned flows.

## Non‑Goals (this iteration)

- CSV audit log of all docs (deferred; see remove_sqlite_index_plan).
- Maintaining “latest-by-task” for production paths (it will remain as an ops tool only).

## Former dependencies on latest‑by‑task (now removed)

- Essays previously resolved drafts via `get_latest_by_task('draft', draft_task_id)`; now use `parent_doc_id` + filesystem.
- Evaluations previously resolved targets via `get_latest_by_task(stage, document_id)`; now use `parent_doc_id` + filesystem.
- DB‑row asset checks removed; filesystem existence checks remain.

## Proposed Design

1) Task table schema changes
- essay_generation_tasks.csv
  - Add required `parent_doc_id` (string). Assets resolve the draft by this doc_id.
- evaluation_tasks.csv
  - Use required `parent_doc_id` (string) to point to the parent essay document to evaluate.
  - `document_id` (task id) is deprecated for lookup; keep only for legacy references during migration and remove from code paths.

2) Asset behavior (require doc_id)
- essay_prompt / essay_response
  - Read draft text by `stage='draft'` + `parent_doc_id` (direct path).
  - If `parent_doc_id` missing → fail fast with a clear error (no fallback).
  - Metadata: include `parent_doc_id` in essay metadata.json (keep consistent).
- evaluation_prompt / evaluation_response
  - Resolve by `stage='essay'` + `parent_doc_id` (direct path).
  - If `parent_doc_id` missing → fail fast with a clear error (no fallback).
  - Metadata: include `parent_doc_id` = parent essay doc_id.

3) Filesystem helper alignment
- Minimal helper API (pure, path‑based):
  - `get_row_by_doc_id(stage, doc_id)` → derive `docs_root/<stage>/<doc_id>`, load `metadata.json`, and return a row dict with derived paths (`raw.txt`, `parsed.txt`, `prompt.txt`).
- Document.write_files()
  - Ensure `metadata.json` contains `task_id`, `parent_doc_id` (required for essays/evaluations), `template_id`, `model_id`, `run_id`, `attempt`, `created_at`.
  - No runtime dependency on pointer files; any “latest” browsing remains an ops‑only concern.

 

4) Scripts & CLI UX
- scripts/register_partitions_for_generations.py (explicit changes)
  - Inputs accepted
    - Essays: accept draft doc IDs to pin lineage via any of:
      - `--essay-parent-doc-ids <path>` CSV/TSV with a `parent_doc_id` column
      - `--essay-parent-list <comma-separated>` with entries like `doc:<doc_id>`
    - Evaluations: accept essay doc IDs (targets) via any of:
      - `--evaluation-parent-doc-ids <path>` CSV/TSV with a `parent_doc_id` column
      - `--evaluation-parent-list <comma-separated>` with entries like `doc:<doc_id>`
  - Columns written
    - `data/2_tasks/essay_generation_tasks.csv`:
      - Required: `essay_task_id`, `parent_doc_id`, plus existing template/model fields
    - `data/2_tasks/evaluation_tasks.csv`:
      - Required: `evaluation_task_id`, `parent_doc_id`, plus template/model fields
      - `document_id` kept only for legacy; do not use for resolution
  - Behavior changes
    - Build evaluation rows from provided `parent_doc_id` (essay doc), not from `essay_task_id`.
    - Validate that every output row has a non-empty `parent_doc_id`; fail fast otherwise.
  - Backward compatibility (optional)
    - Optional flags to read legacy inputs and emit a clear error suggesting `--evaluation-parent-doc-ids`.
- scripts/print_latest_doc.py / list_partitions_latest.py
  - Keep as ops tools; document that production flows should pin doc_id via task CSVs.

5) Checks and reporting
- Asset checks: validate by path only when a parent/target doc_id is specified (required now).
- Cross‑experiment appenders
  - Capture the produced `doc_id` from asset metadata and use it directly in appended CSVs (avoid re‑resolving latest).

## Migration / Rollout

Phase A — Enforce required IDs (Completed)
- parent_doc_id required in essay/evaluation task tables; assets and docs updated.

Phase B — Asset changes + tests
- Update assets to hard‑require parent_doc_id (remove latest‑by‑task paths):
  - `daydreaming_dagster/assets/group_generation_essays.py`
  - `daydreaming_dagster/assets/group_evaluation.py`
- Update unit tests to pass parent_doc_id and assert clear failures when missing.
- Run tests (unit first, then integration):
  - `.venv/bin/pytest daydreaming_dagster/ -q`
  - `.venv/bin/pytest tests/ -q`
- Gate: all tests pass locally and in CI.

Unit test updates (doc‑id expectations and fail‑fast)
- `daydreaming_dagster/assets/test_core_tasks_schema.py`
  - Flip evaluation task schema to require `parent_doc_id` (keep `document_id` only if still emitted for legacy views; not used for resolution).
  - Build `evaluation_task_id` using the essay doc id (now `parent_doc_id`).
- `daydreaming_dagster/assets/test_selection_evaluation.py`
  - Construct rows with `parent_doc_id` instead of `document_id` when building evaluation tasks from selected docs.
  - Update assertion to compare `set(evaluation_tasks_df["parent_doc_id"])` to `set(selected_docs)`.
- `daydreaming_dagster/assets/test_results_processing.py`
  - In parsed scores enrichment test, rename denormalized column `document_id` to `parent_doc_id` and adjust joins/expected columns accordingly.
  - Update results_processing asset expectations to carry `parent_doc_id` through; paths still derive from task IDs.
- `tests/test_essay_response_copy_mode.py`
  - Replace task‑based draft resolution with `parent_doc_id` in the input row; remove `documents_index` stub.
  - Add a new test asserting fail‑fast when `parent_doc_id` is missing for essay response (clear error message).
- Any tests mocking `get_latest_by_task` or depending on `documents_index` should instead mock the path‑based loader (`get_row_by_doc_id`).

Phase C — Script support + examples
- Extend `register_partitions_for_generations.py` to accept doc IDs and populate task CSVs.
- Add examples in docs/guides/operating_guide.md for curated, doc_id‑pinned runs.

Phase D — Documentation and ops tooling only
- In guides and CI examples, use doc_id‑pinned flows exclusively.
- Keep “latest-by-task” pointers available only for ops scripts and debugging; not used in asset paths.

Phase E — Optional cleanup
- Reduce code paths that rely on “latest” in core assets once adoption is high. Keep pointers and scripts for ops.

Phase F — Index removal + filesystem helpers
- Remove latest‑by‑task usage from:
  - `daydreaming_dagster/checks/documents_checks.py`
  - `daydreaming_dagster/assets/cross_experiment.py`
  - `daydreaming_dagster/assets/documents_reporting.py`
- Replace `DocumentsIndexResource` with a minimal `FilesystemDocumentHelper` (pure, path‑based).
- Replace `idx.get_by_doc_id_and_stage` calls with the filesystem helper.
- Delete SQLite index code and tests (or keep a thin shim temporarily):
  - `daydreaming_dagster/utils/documents_index.py`
  - `daydreaming_dagster/resources/documents_index.py`
  - `daydreaming_dagster/utils/test_documents_index.py`
- Provide an ops‑only script for browsing latest by task if needed.
- Run unit/integration tests and update any remaining call sites.

## Risks & Mitigations

- Incomplete curation: users might omit the doc_id. Mitigation: fail early with actionable error and provide helper scripts to fetch valid doc_ids.
- Stage ambiguity for evaluation targets: Out of scope; evaluations target essays only.
- Legacy data: tasks produced before this change won’t have doc IDs. Mitigation: allow pinning later via a helper that reads documents index/pointers and writes the pinned IDs back to CSVs.

## Success Criteria

- Essays and evaluations accept and prefer pinned doc IDs; outputs include lineage in metadata.json.
- Cross‑experiment appenders record doc_id directly from upstream asset metadata.
- Guides demonstrate doc_id‑pinned workflows; “latest” is documented as fallback only.
- All tests pass; determinism improves in re‑runs and backfills.

## Implementation Tasks (Engineering Checklist)

1) Task tables
- Enforce required `parent_doc_id` in essay and evaluation task rows (schema + runtime validation).
- Wire through utils/raw_readers and assets/group_task_definitions.

2) Assets
- generation_essays: require parent_doc_id in `_load_phase1_text`/`essay_response`; no fallback.
- group_evaluation: require parent_doc_id (essay doc) in prompt/response; no fallback.

3) Filesystem helper
- Provide path‑based `get_row_by_doc_id`.
- Do not use any “latest” lookup in assets; keep such capability in ops scripts only.

4) Scripts
- register_partitions_for_generations: accept doc IDs, write new columns.
- Optional: helper to backfill parent/target doc_id into task CSVs from pointers.

5) Docs & Checks
- Update operating guide with doc_id pinning examples.
- Make checks warn when running without pinned lineage in non‑dev runs.

## Follow‑Up Plan: Scripts + Pivots (Doc‑ID First)

Goal
- Ensure cross‑experiment scripts and pivots operate on parent_doc_id (the canonical generation doc id) rather than task ids, so results aggregate deterministically across versions.

Targets
- scripts/parse_all_scores.py
- scripts/build_pivot_tables.py
- scripts/build_evaluation_results_table.py
- scripts/rebuild_results.sh (invocation order/paths)
- (audit) scripts/copy_essays_with_drafts.py and scripts/find_legacy_evals.py for task‑id assumptions

Tasks
1) parse_all_scores (switch to docs/ store)
- Parse directly from the new store under `data/docs/evaluation/**/`:
  - Discover eval docs by iterating `data/docs/evaluation/<doc_id>/parsed.txt` (or `raw.txt` if needed).
  - Load `metadata.json` in the same directory for fields: `parent_doc_id`, `task_id` (evaluation_task_id), `evaluation_template`, `model_id`, `created_at`.
- Emit `doc_id` (evaluation document id) as the primary key in the CSV.
- Do not rely on `evaluation_task_id` filename patterns; treat tasks CSV as optional context only.
- Include `parent_doc_id` from metadata.json; pivots will use it as the stable generation key.
- Keep existing score and parser fields; add `doc_dir`, `used_response_path`, and `created_at` for traceability.

2) build_pivot_tables (pivot by parent_doc_id)
- Require and group by `parent_doc_id` (generation doc, stable across attempts) for primary pivots.
- Keep `doc_id` (evaluation doc) available for evaluation‑centric breakdowns as a secondary key.
- Do not infer from task ids; rely on parsed_scores parent_doc_id sourced from metadata.json.

3) build_evaluation_results_table (include doc_id and enrich via metadata)
- Ensure the table includes `doc_id` (evaluation doc) and optionally `parent_doc_id` by loading `metadata.json` for each `doc_id`.
- Avoid dependence on `evaluation_tasks.csv`; treat tasks as optional context.

4) Selection and copy helpers (audit and align)
- scripts/copy_essays_with_drafts.py: prefer `parent_doc_id` for locating essays; keep legacy paths for historical runs with a clear warning.
- scripts/find_legacy_evals.py: update messaging to recommend pinning and highlight missing `parent_doc_id` cases.

5) Docs and examples
- Update examples in docs/guides/operating_guide.md and selection_and_cube.md to show pivots keyed by `parent_doc_id`.
- Refresh `rebuild_results.sh` to use the new scripts/flags and ensure the order produces parsed_scores with `parent_doc_id`.

Validation
- Unit scope: add small tests for `parse_all_scores.py` (CLI function refactored for import) to verify `parent_doc_id` extraction and legacy mapping.
- Integration scope: run `rebuild_results.sh` on a fixture data_root; check that pivot tables group by `parent_doc_id` and match counts with legacy mode off.

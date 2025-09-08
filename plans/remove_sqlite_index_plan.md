# Plan: Remove SQLite Index (Filesystem + Metadata Only)

Status: ready
Owner: daydreaming_dagster
Scope: Remove `SQLiteDocumentsIndex` and all DB reads/writes from runtime paths. Use a minimal, path-based helper over `metadata.json` in a flat docs layout. Keep optional ops-only scripts if desired.

## Summary

This plan executes after plans/doc_id_first_design_plan.md. Assets and scripts now prefer pinned doc IDs (`parent_doc_id`) and resolve parents/targets by filesystem path. We still have SQLite usage in several places; this plan removes it and replaces any remaining reads with a small path‑based helper that loads `metadata.json` from the flat layout `docs_root/<stage>/<doc_id>/` (no `logical_key_id` directory level).

Current code state (snapshot)
- Flat layout in code: `utils/ids.py::doc_dir` returns `docs/<stage>/<doc_id>`.
- Partitions script updated: `scripts/register_partitions_for_generations.py` requires an essay document id column (doc‑id first) and writes `evaluation_tasks.csv` with `parent_doc_id`.
- DB still referenced in runtime code and tests:
  - Assets: `assets/group_generation_essays.py` and `assets/group_evaluation.py` import `documents_index` and call DB lookups/inserts.
  - Checks: `checks/documents_checks.py` includes DB‑based asset checks.
  - Utils/Resource: `utils/documents_index.py`, `resources/documents_index.py` present.
  - Tests: `utils/test_documents_index.py`, `tests/test_pipeline_integration.py` wire `DocumentsIndexResource`.
  - Ops scripts: several `scripts/backfill/*` and `scripts/print_latest_doc.py` use the DB.
  - Docs: `docs/llm_documents_index_guide.md` references DB enablement/backfill.

Goal: Eliminate the DB dependency, keep IDs and behavior stable, and make artifacts fully portable. Pointer files for “latest” are optional and remain ops‑only.

Current progress
- Document helper is implemented and used by draft/essay/evaluation assets to write `raw.txt`, `parsed.txt`, optional `prompt.txt`, and `metadata.json` consistently.
- Assets still insert rows into SQLite; reads for phase linking/reporting still use the DB. This plan removes that dependency.
 - Verified: all essay and evaluation records already include `parent_doc_id`; no additional audit/check needed in this plan.

Deferred
- Global CSV audit log is deferred; we can add an append‑only journal later if needed.

## What SQLite Gives Us Today (to replace)

- Retrieval helpers (to be replaced):
  - Path‑based: `get_by_doc_id_and_stage(doc_id, stage)` and read of `raw/parsed/prompt` → replace with filesystem helper.
  - Latest‑by‑task: `get_latest_by_task(stage, task_id)` → no longer used by runtime after doc_id‑first; keep only for ops scripts if desired.
- Append‑only “attempts” per logical key with created_at ordering
- Lightweight reporting (iterate rows)

## Proposed Filesystem Design

1) Document layout (flat)
- `docs_root/<stage>/<doc_id>/`
  - `raw.txt`, `parsed.txt`, optional `prompt.txt`, `metadata.json`

Lineage model and field semantics
- `doc_id` (primary identifier):
  - Uniquely identifies a single concrete document artifact written under `docs/<stage>/<doc_id>/`.
  - Assigned per attempt (derived from logical_key_id + run/attempt); immutable once written.
  - Appears as the first token of `evaluation_task_id` in the doc‑id‑first scheme.
- `parent_doc_id` (lineage pointer):
  - Drafts: `parent_doc_id` is empty/None (no upstream document).
  - Essays: `parent_doc_id` equals the `doc_id` of the draft from which the essay was generated.
  - Evaluations: `parent_doc_id` equals the `doc_id` of the essay being evaluated.
  - Encodes a strict parent→child relation used for deterministic reproduction and auditing; never points to task ids.
  - Present in tasks (essays/evaluations), in asset metadata, and in `metadata.json` for outputs.
Validation rules to enforce in assets/scripts
- `parent_doc_id` must be non‑empty for essays/evaluations and must exist on disk under the expected stage:
  - Essays: `docs/draft/<parent_doc_id>/...` must exist.
  - Evaluations: `docs/essay/<parent_doc_id>/...` must exist.
- Fail fast with clear errors when missing/invalid.

2) metadata.json (extend)
- Ensure it includes at least:
  - `task_id` (draft_task_id | essay_task_id | evaluation_task_id)
  - `template_id`, `model_id`
  - `parent_doc_id` (essays/evaluations)
  - `run_id`, `attempt` (attempt is also encoded in `doc_id`)
  - `created_at` (ISO timestamp; write once on create)

3) Pointer files (ops‑only, optional)
- If needed for ops browsing of “latest”, maintain:
  - Per logical key `_latest.json` under `docs_root/<stage>/<logical_key_id>/`.
  - Optional by‑task buckets under `docs_root/<stage>/_by_task/...`.
- Runtime assets will not depend on these pointers.

4) Filesystem helper API (runtime)
- Minimal helper (pure functions) to replace DB reads in runtime paths using the flat layout:
  - `get_row_by_doc_id(stage, doc_id) -> dict` resolves `docs_root/<stage>/<doc_id>`, loads `metadata.json`, and returns a dict with at least: `doc_id`, `stage`, `doc_dir`, and metadata fields (`task_id`, `parent_doc_id`, `template_id`, `model_id`, `created_at`).
  - `read_raw(row)`, `read_parsed(row)`, `read_prompt(row)` derive from `row["doc_dir"]`.
  - Optional ops helper: `find_latest_doc_id_by_task_id(stage, task_id)` by scanning `docs_root/<stage>/*/metadata.json` (ordered by `created_at`). Runtime assets must not rely on this.

5) Writes (Document helper)
- Ensure `metadata.json` contains required fields and `created_at`.
- Do not require pointer updates for runtime; provide standalone scripts to rebuild optional pointers if we choose to maintain them for ops.

## Migration and Rollout Plan

Phase A — Prerequisite (Completed)
- Doc‑id first: Adopt `parent_doc_id` across tasks, assets, and scripts (minor docs pending update).
- Partitions: `scripts/register_partitions_for_generations.py` updated to require essay doc ids and write `evaluation_tasks.csv` with `parent_doc_id`.

Phase B — Replace runtime reads with filesystem helper (assets first)
- Add `utils/filesystem_rows.py` with the helper API above.
- Refactor assets to remove DB reads/writes and drop the `documents_index` resource:
  - `assets/group_generation_essays.py`: load parent draft via `get_row_by_doc_id('draft', parent_doc_id)`; remove `get_latest_by_task` and DB inserts; `required_resource_keys` should not include `documents_index`.
  - `assets/group_evaluation.py`: load target essay via `get_row_by_doc_id('essay', parent_doc_id)`; remove DB reads/inserts; drop `documents_index` from `required_resource_keys`.
- Keep `utils/document.Document` for filesystem writes only (no `DocumentRow` dependency in runtime).
- Update/adjust unit tests for these assets to assert fail‑fast when `parent_doc_id` missing and success when present.

Phase C — Remove SQLite code and resource
- Delete `utils/documents_index.py` and `resources/documents_index.py`.
- Remove DB-based checks from `checks/documents_checks.py` (keep filesystem checks only) and stop registering them.
- Update tests: remove/port `utils/test_documents_index.py` and any integration wiring of `DocumentsIndexResource` in `tests/test_pipeline_integration.py`.
- Ensure `definitions.py` has no references to `documents_index` (already true), and assets no longer require it.

Phase D — Optional ops tooling
- Provide filesystem-based ops scripts if needed; deprecate or rewrite DB‑dependent backfills:
  - Keep or port: `scripts/print_latest_doc.py` (FS scan), `scripts/list_partitions_latest.py`.
  - Mark DB backfills in `scripts/backfill/*` as legacy; optionally port small, high‑value ones to scan `metadata.json` instead of SQLite.
- Update/retire `docs/llm_documents_index_guide.md` to a short “Filesystem Docs” guide.

## Risks & Mitigations

- Performance on large trees: path‑based reads are O(1); reporting that scans trees can be batched and cached. Pointers are optional if faster ops views are needed.
- Backward compatibility: doc_id‑first removes “latest” coupling from runtime paths; ops scripts can retain optional latest pointers.
- Data integrity: `created_at`, `doc_id`, `logical_key_id`, and content hashes remain in metadata and files.

## Detailed Tasks

1) Filesystem helper
- Implement `get_row_by_doc_id(stage, doc_id)` and `read_raw/parsed/prompt(row)`.

2) Replace usages
- Update checks, cross‑experiment, and reporting to call the filesystem helper.

3) Clean up SQLite
- Remove `documents_index` resource, SQLite module, DB checks, and associated tests. Update imports and docs.

4) Optional ops scripts
  - Add/rewrite pointer maintenance and “latest” browsing tools under `scripts/`.

5) Partitions script (already updated)
- `scripts/register_partitions_for_generations.py` is updated to require the essay document id in the curated input and to write evaluation `parent_doc_id`. No further changes needed here.

## Success Criteria

- All unit/integration tests pass with SQLite fully removed from runtime.
- No behavioral change in asset outputs, file layout, or IDs; lineage remains doc‑id first.
- Docs/guides reflect filesystem‑only runtime; DB docs archived or clearly marked legacy.

## Validation Plan
- Unit: run `.venv/bin/pytest daydreaming_dagster/ -q` after refactors; update tests that referenced DB.
- Integration: run `.venv/bin/pytest tests/ -q` and a targeted materialization of one draft/essay/evaluation partition to confirm end‑to‑end reads from filesystem.
- Scripts: run `./scripts/rebuild_results.sh` and ensure outputs are produced; run `scripts/parse_all_scores.py` and `scripts/build_pivot_tables.py` and confirm pivots group by `parent_doc_id`.

## Open Questions
  
Out‑of‑scope for this iteration
- Append‑only CSV index (global audit log) — postponed.

- Do we need a global report of all docs across stages? If yes, implement a simple `walk_docs()` that yields rows using `metadata.json`.
- Should we encode `task_id` into directory names? Keep directories stable; if needed, use optional pointer JSON for ops clarity.

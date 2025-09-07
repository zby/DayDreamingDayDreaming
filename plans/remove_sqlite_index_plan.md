# Plan: Replace SQLite Documents Index with Filesystem + Metadata

Status: draft
Owner: daydreaming_dagster
Scope: Remove `SQLiteDocumentsIndex` entirely. After doc_id-first rollout, replace remaining reads with a minimal path-based helper over `metadata.json`. Pointer files are optional and ops-only.

## Summary

This plan executes after plans/doc_id_first_design_plan.md is completed. Assets now require pinned doc IDs (`parent_doc_id`) and resolve parents/targets directly by filesystem path. We currently still dual‑write to a SQLite table (`documents`) and use it in a few non‑asset paths (checks/reporting/ops). Since all linking data exists on disk, we will remove SQLite and refactor remaining reads to a small path‑based helper that loads `metadata.json` from the flat layout `docs_root/<stage>/<doc_id>/` (note: no logical_key_id directory level in the path).

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

2) metadata.json (extend)
- Ensure it includes at least:
  - `task_id` (draft_task_id | essay_task_id | evaluation_task_id)
  - `template_id`, `model_id`
  - `parent_doc_id` (for essays/evaluations)
  - `run_id`, `attempt` (we already encode attempt in doc_id; keep explicit for portability)
  - `created_at` (ISO timestamp; write once on create)

3) Pointer files (ops‑only, optional)
- If needed for ops browsing of “latest”, maintain:
  - Per logical key `_latest.json` under `docs_root/<stage>/<logical_key_id>/`.
  - Optional by‑task buckets under `docs_root/<stage>/_by_task/...`.
- Runtime assets will not depend on these pointers.

4) Filesystem helper API (runtime)
- Minimal helper (pure functions) to replace SQLite reads using the flat layout:
  - `get_row_by_doc_id(stage, doc_id)` → resolve `docs_root/<stage>/<doc_id>`, load `metadata.json`, and return a dict with at least: `doc_id`, `stage`, `doc_dir`, and selected metadata fields (`task_id`, `parent_doc_id`, `template_id`, `model_id`, `created_at`).
  - `read_raw/parsed/prompt(row)` → derive from `row["doc_dir"]`.
  - Optional ops helpers: `find_latest_doc_id_by_task_id(stage, task_id)` by scanning `metadata.json` (ordered by `created_at`).

5) Writes (Document helper)
- Ensure `metadata.json` contains required fields and `created_at`.
- Do not require pointer updates for runtime; provide standalone scripts to rebuild optional pointers if we choose to maintain them for ops.

## Migration and Rollout Plan

Phase A — Prerequisite (Completed)
- plans/doc_id_first_design_plan.md enforced: parent_doc_id required; assets resolve by doc_id; verified coverage.

Phase B — Replace runtime reads with filesystem helper
- Introduce minimal helper (e.g., `utils/filesystem_rows.py:get_row_by_doc_id`).
- Update non‑asset call sites to remove SQLite usage:
  - `daydreaming_dagster/checks/documents_checks.py`
  - `daydreaming_dagster/assets/cross_experiment.py`
  - `daydreaming_dagster/assets/documents_reporting.py`
- Update unit tests accordingly and run `.venv/bin/pytest daydreaming_dagster/ -q` and `.venv/bin/pytest tests/ -q`.

Phase C — Remove SQLite code and resource
- Delete `daydreaming_dagster/utils/documents_index.py` and `daydreaming_dagster/resources/documents_index.py`.
- Remove tests that exercised SQLite index (`utils/test_documents_index.py`) or port them to filesystem helper semantics.
- Update `definitions.py` and any configs to drop `documents_index` resource.

Phase D — Optional ops tooling
- Provide scripts for pointer maintenance and browsing if useful:
  - `scripts/rebuild_pointers.py` (from metadata.json)
  - `scripts/verify_pointers.py` (compare to a scan)
  - `scripts/print_latest_doc.py` (ops convenience)
- Make clear these are not used by runtime assets.

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
- Remove `documents_index` resource, SQLite module, and tests. Update imports and docs.

4) Optional ops scripts
  - Add/rewrite pointer maintenance and “latest” browsing tools under `scripts/`.

5) Partitions script (already updated)
- scripts/register_partitions_for_generations.py has been updated to require the essay document ID in the curated input and to write evaluation `parent_doc_id` accordingly. No further changes needed here.

## Success Criteria

- All unit/integration tests pass with SQLite fully removed.
- No behavioral change in asset outputs, file layout, or IDs.
- Docs and guides updated; no references to DB or index resources in runtime paths.

## Open Questions
  
Out‑of‑scope for this iteration
- Append‑only CSV index (global audit log) — postponed.

- Do we need a global report of all docs across stages? If yes, implement a simple `walk_docs()` that yields rows using `metadata.json`.
- Should we encode `task_id` into directory names? Keep directories stable; if needed, use optional pointer JSON for ops clarity.

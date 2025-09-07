# Plan: Replace SQLite Documents Index with Filesystem + Metadata

Status: draft
Owner: daydreaming_dagster
Scope: Replace `SQLiteDocumentsIndex` with filesystem-based lookup using `metadata.json` and small pointer files.

## Summary

We currently dual-write generation/evaluation artifacts to `data/docs/<stage>/<logical_key_id>/<doc_id>/` and a SQLite table (`documents`). Reads for phase-to-phase linking and reports use the SQLite index. Given that all data needed for linking exists on disk already, we can remove SQLite by introducing a tiny filesystem index (pointer files) and expanding `metadata.json` to make lookups reliable and fast.

Goal: Keep the same user‑visible behavior and IDs, simplify infra (no DB), reduce coupling, and make artifacts fully portable.

Current progress
- Document helper is implemented and used by draft/essay/evaluation assets to write `raw.txt`, `parsed.txt`, optional `prompt.txt`, and `metadata.json` consistently.
- Assets still insert rows into SQLite; reads for phase linking/reporting still use the DB. This plan removes that dependency.

Deferred
- Global CSV audit log is deferred; we can add an append‑only journal later if needed.

## What SQLite Gives Us Today (to replace)

- Retrieval helpers:
  - `get_latest_by_task(stage, task_id)`
  - `get_by_doc_id_and_stage(doc_id, stage)`
  - `read_raw/parsed/prompt` path resolution
- Append‑only “attempts” per logical key with created_at ordering
- Lightweight reporting (iterate rows)

## Proposed Filesystem Design

1) Document layout (unchanged)
- `docs_root/<stage>/<logical_key_id>/<doc_id>/`
  - `raw.txt`, `parsed.txt`, optional `prompt.txt`, `metadata.json`

2) metadata.json (extend)
- Ensure it includes at least:
  - `task_id` (draft_task_id | essay_task_id | evaluation_task_id)
  - `template_id`, `model_id`
  - `parent_doc_id` (for essays/evaluations)
  - `run_id`, `attempt` (we already encode attempt in doc_id; keep explicit for portability)
  - `created_at` (ISO timestamp; write once on create)

3) Pointer files (fast lookups, atomic updates)
- Latest by logical key (per directory, optional):
  - File: `docs_root/<stage>/<logical_key_id>/_latest.json`
  - Content: `{ "doc_id": ..., "created_at": ..., "parser": null, ... }`
- Latest by task id (bucketed to avoid huge directories):
  - Dir: `docs_root/<stage>/_by_task/<hh>/<task_key>.json`
    - `<hh>` = first 2 hex of SHA256(task_id), `task_key` = full SHA256(task_id)
    - Content: `{ "task_id": ..., "doc_id": ..., "logical_key_id": ..., "created_at": ... }`
  - Rationale: O(1) lookups without scanning; low contention because each task_id updates its own file.

 - 4) Read API (drop‑in replacement)
 - New `FSIndex` class with the minimal surface used by runtime assets:
  - `get_latest_by_task(stage, task_id)` → read bucketed JSON; fallback to scan when missing
  - `get_by_doc_id_and_stage(doc_id, stage)` → direct path resolution: `docs_root/<stage>/<doc_id>` (no scan)
  - `read_raw/parsed/prompt(row)` → derive from `doc_dir` (same as today)
  - Note: `get_latest_by_logical` can be added later for ops tooling; not required by assets.
- Scans are only on fallback and can be cached per run.

5) Writes (Document helper)
- Extend `Document.write_files()` to also update the two pointer files atomically:
  - Compute `task_id` and `logical_key_id` from its metadata/fields
  - Write JSON to `_latest.json` and bucketed task file via temp‑file + replace
  - If pointers are missing or corrupted, they’ll be rebuilt by a standalone script (below)

## Migration and Rollout Plan

Phase 0 — Inventory & Flag
- Add `DD_DOCS_FS_INDEX=1` to switch reads to FSIndex while keeping SQLite writes on (belt‑and‑suspenders).
- Sites to update reads:
  - generation_essays: `_load_phase1_text_by_draft_task`, `_load_phase1_text_by_parent_doc`
  - evaluation: `evaluation_prompt`
  - documents_reporting: `documents_latest_report`, `documents_consistency_report`
  - asset checks: `*_db_row_present_check`

Phase 1 — Implement FSIndex (read‑only)
- Add `utils/fs_index.py` implementing the API above (no writers yet).
- Add small unit tests for lookups with synthetic docs.
- Resource: add `DocumentsFSIndexResource` (same `.get_index()` semantics) next to existing resource.

Phase 2 — Dual‑write pointers
- Extend `utils/document.Document.write_files()` to update pointer files.
- Backfill script: `scripts/rebuild_fs_index.py` to rebuild pointers from `metadata.json` when needed.
- Keep SQLite writes during this phase; reads controlled by flag.

Phase 3 — Flip reads to FSIndex
- Toggle default: `DocumentsIndexResource.get_index()` returns FSIndex when `DD_DOCS_FS_INDEX=1` (or make FS default, SQLite behind flag).
- Update checks/reports to use FSIndex.
- CI: run essay/draft/eval unit tests with FS enabled.

Phase 4 — Remove SQLite
- Delete `utils/documents_index.py` and `resources/documents_index.py` SQLite paths.
- Update docs (guides + architecture) to remove DB mentions.
- Clean tests depending on DB.

## Risks & Mitigations

- Performance on large trees: mitigated by pointer files; scans only on cold/missing pointers.
- Concurrency on pointer updates: per‑logical `_latest.json` and per‑task bucketed JSON minimize contention; atomic replace guarantees consistency. Worst case: transient missing pointers → fallback scan.
- Backward compatibility: keep SQLite reads behind flag during transition; scripts to rebuild FS pointers.
- Data integrity: `created_at`, `doc_id`, `logical_key_id`, and content hashes remain in metadata and files; pointers are derivable.

## Detailed Tasks

1) Add FSIndex
- File: `daydreaming_dagster/utils/fs_index.py`
- Methods: `get_latest_by_task`, `get_by_doc_id_and_stage`, `read_raw/parsed/prompt`
- Simple dataclass Row type mirroring `dict` structure used today (doc_id, stage, doc_dir, etc.)

2) Extend Document helper
- Add `created_at` (ISO) and `attempt` to `metadata.json` on write.
- Add `update_pointers(docs_root, stage, logical_key_id, task_id, doc_id)` called inside `write_files()`.

3) Resource layer
- New: `DocumentsFSIndexResource(docs_root=...)` with `.get_index()` returning `FSIndex`.
- Transition: `DocumentsIndexResource` can delegate to FSIndex by default; env var flips behavior.

4) Replace reads site‑by‑site
- generation_essays: use `documents_index.get_index()` → FSIndex transparently.
- evaluation_prompt: same change; keep failure messages the same.
- documents_reporting + checks: port to FSIndex.

5) Backfill & Maintenance
- Script: `scripts/rebuild_fs_index.py` to regenerate all pointer files from `metadata.json` (walk directories, choose latest by `created_at` then lexical doc_id).
- Script: `scripts/verify_fs_index.py` to compare pointers vs a fresh scan and report drift.

6) Deletion
- Remove SQLite code and dependencies after a full cycle on FS in CI and a local dry run.

## Success Criteria

- All unit/integration tests pass with `DD_DOCS_FS_INDEX=1` and with SQLite removed.
- Latency of phase‑to‑phase lookups comparable to current (pointers present).
- No behavioral change in asset outputs, file layout, or IDs.
- Docs and guides updated; no references to DB in runtime paths.

## Open Questions
  
Out‑of‑scope for this iteration
- Append‑only CSV index (global audit log) — postponed until after FSIndex cutover proves stable.

- Do we need a global report of all docs across stages? If yes, implement a simple `walk_docs()` in FSIndex that yields rows using metadata.json.
- Should we encode `task_id` into directory names to avoid maintaining by‑task pointers? We keep directories stable and rely on pointer JSON for clarity and atomicity.

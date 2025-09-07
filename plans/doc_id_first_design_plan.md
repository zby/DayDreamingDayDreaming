# Plan: Doc ID–First Lineage and Resolution

Status: draft
Owner: daydreaming_dagster
Related: plans/remove_sqlite_index_plan.md (FSIndex + pointers)

## Summary

Replace “latest-by-task” linking with explicit, pinned doc IDs across stages. Essays should reference a concrete parent draft `doc_id`; evaluations should reference a concrete target `doc_id`. Keep a legacy fallback to “latest” only when a doc ID is not provided. This improves determinism, reproducibility, and debuggability, and simplifies the future FSIndex (no DB).

## Goals

- Deterministic lineage: essay → draft doc_id; evaluation → target doc_id.
- Backward-compatible: if a doc_id is not provided, use latest-by-task as a temporary fallback.
- Minimal surface for runtime lookups: prefer `get_by_doc_id_and_stage()` (path-based), keep “latest-by-task” pointers for ops/legacy only.
- Update scripts/UX to make it easy to curate doc_id–pinned flows.

## Non‑Goals (this iteration)

- CSV audit log of all docs (deferred; see remove_sqlite_index_plan).
- Removal of the “latest-by-task” fallback (we’ll de-emphasize it, not delete it immediately).

## Current dependencies on latest‑by‑task

- Essays: `_load_phase1_text_by_draft_task`, `essay_response` → parent draft row via `get_latest_by_task('draft', draft_task_id)`.
- Evaluations: `evaluation_prompt`, `evaluation_response` → target doc via `get_latest_by_task(stage, document_id)`.
- Checks and cross‑experiment appenders rely on “row present for this partition”.

## Proposed Design

1) Task table schema changes
- essay_generation_tasks.csv
  - Add optional `parent_doc_id` (string). When present, assets resolve the draft by doc_id.
- evaluation_tasks.csv
  - Reuse `parent_doc_id` (string) to point to the parent essay document to evaluate. Keep existing `document_id` (task id) only for legacy fallback during migration.

2) Asset behavior (prefer doc_id; fallback to latest)
- essay_prompt / essay_response
  - If `parent_doc_id` present → read draft text by `stage='draft'` + `doc_id` (direct path).
  - Else → legacy fallback: resolve via latest‑by‑task using `draft_task_id`.
  - Metadata: include `parent_doc_id` in essay metadata.json (keep consistent).
- evaluation_prompt / evaluation_response
  - If `parent_doc_id` present → resolve by `stage='essay'` + `doc_id` (direct path). Optionally allow `source_stage` to override when evaluating drafts directly.
  - Else → legacy fallback: resolve via latest‑by‑task using `document_id` (task id).
  - Metadata: include `parent_doc_id` = parent essay doc_id.

3) FSIndex + Document helper alignment
- FSIndex (from remove_sqlite_index_plan):
  - `get_by_doc_id_and_stage(doc_id, stage)` → resolve `docs_root/<stage>/<doc_id>` directly.
  - `get_latest_by_task(stage, task_id)` → read bucketed pointer JSON (fallback/ops only).
- Document.write_files()
  - Ensure `metadata.json` contains `task_id`, `parent_doc_id` (if any), `template_id`, `model_id`, `run_id`, `attempt`, `created_at`.
  - Maintain pointer updates (latest‑by‑task and optional per‑logical `_latest.json`).

4) Scripts & CLI UX
- scripts/register_partitions_for_generations.py
  - Accept doc IDs in curated inputs:
    - For essays: `parent_doc_id` column or `parent:` prefix in an input list to pin parent.
    - For evaluations: `parent_doc_id` column or `doc:` prefix to pin the parent essay doc.
  - Populate `parent_doc_id` in `data/2_tasks/*.csv` when provided.
- scripts/print_latest_doc.py / list_partitions_latest.py
  - Keep as ops tools; document that production flows should pin doc_id via task CSVs.

5) Checks and reporting
- Asset checks: where we validate “row present in index”, allow either path:
  - If a doc_id is referenced (parent/target), check by path (`docs_root/<stage>/<doc_id>`).
  - Else, check via latest‑by‑task pointer (transition period only).
- Cross‑experiment appenders
  - Capture the produced `doc_id` from asset metadata and use it directly in appended CSVs (avoid re‑resolving latest).

## Migration / Rollout

Phase A — Add fields + no‑op behavior changes
- Add `parent_doc_id` to essay and evaluation task tables; wire through utils/raw_readers and assets/group_task_definitions.
- Update assets to prefer doc_id when present; otherwise keep existing behavior.
- Update docs: recommend pinning doc IDs for reproducible runs.

Phase B — Script support + examples
- Extend `register_partitions_for_generations.py` to accept doc IDs and populate task CSVs.
- Add examples in docs/guides/operating_guide.md for curated, doc_id‑pinned runs.

Phase C — Prefer pinned by default (policy)
- In guides and CI examples, use doc_id‑pinned flows; treat “latest-by-task” as a fallback only.
- Update checks to warn (not fail) when running without pinned parent/target in non‑interactive environments.

Phase D — Optional cleanup
- Reduce code paths that rely on “latest” in core assets once adoption is high. Keep pointers and scripts for ops.

## Risks & Mitigations

- Incomplete curation: users might omit the doc_id. Mitigation: maintain fallback + clear metadata indicating fallback mode.
- Stage ambiguity for evaluation targets: Mitigation: include an explicit `source_stage` in evaluation_tasks when needed (defaults to essay when both exist).
- Legacy data: tasks produced before this change won’t have doc IDs. Mitigation: allow pinning later via a helper that reads documents index/pointers and writes the pinned IDs back to CSVs.

## Success Criteria

- Essays and evaluations accept and prefer pinned doc IDs; outputs include lineage in metadata.json.
- Cross‑experiment appenders record doc_id directly from upstream asset metadata.
- Guides demonstrate doc_id‑pinned workflows; “latest” is documented as fallback only.
- All tests pass; determinism improves in re‑runs and backfills.

## Implementation Tasks (Engineering Checklist)

1) Task tables
- Add optional columns: `parent_doc_id` (essay), `target_doc_id` (evaluation).
- Wire through utils/raw_readers and assets/group_task_definitions.

2) Assets
- generation_essays: prefer parent_doc_id in `_load_phase1_text`/`essay_response`; fallback to pointer by draft_task_id.
- group_evaluation: prefer parent_doc_id (essay doc) in prompt/response; fallback to latest‑by‑task.

3) FSIndex
- Provide path‑based `get_by_doc_id_and_stage`.
- Keep pointer‑based `get_latest_by_task` for fallback and ops.

4) Scripts
- register_partitions_for_generations: accept doc IDs, write new columns.
- Optional: helper to backfill parent/target doc_id into task CSVs from pointers.

5) Docs & Checks
- Update operating guide with doc_id pinning examples.
- Make checks warn when running without pinned lineage in non‑dev runs.

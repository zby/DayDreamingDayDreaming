# Plan: Doc IDs in Task Tables (doc-id–first execution)

Status: draft
Owner: daydreaming_dagster
Related: plans/doc_id_first_design_plan.md, plans/remove_sqlite_index_plan.md

## Summary

Move all generation/evaluation task tables under `data/2_tasks/` to a doc‑id–first model by adding an explicit `doc_id` column that is used at execution time to read/write documents in the new docs store (`data/docs/<stage>/<doc_id>`). This removes ambiguity from task‑based file paths, eliminates the need for VersionedTextIOManager for runtime lookups, and enables deterministic re‑runs and cross‑run reuse with zero index/DB dependence.

Key idea: each task row carries the exact document ID to read/write. For upstream linkage, we keep `parent_doc_id` columns where appropriate (essays → draft; evaluations → essay).

## Scope

- Draft generation: `data/2_tasks/draft_generation_tasks.csv` gets a `doc_id` column (draft doc to be created).
- Essay generation: `data/2_tasks/essay_generation_tasks.csv` gets a `doc_id` column (essay doc to be created). Continues to require `parent_doc_id` (draft doc).
- Evaluation: `data/2_tasks/evaluation_tasks.csv` gets a `doc_id` column (evaluation doc to be created). Continues to require `parent_doc_id` (essay doc).

## Doc ID Allocation Strategy

We cannot hash the future response text when creating tasks. We will pre‑allocate doc IDs deterministically from task metadata plus uniqueness salt. No logical_key_id will be computed or persisted; only `doc_id` is used for addressing.

- Function: `reserve_doc_id(stage, task_id, run_id=None, salt=None)`
  - Inputs: `stage`, canonical `task_id` string for the row; optional `run_id` (e.g., YYYYMMDDHHMMSS or Dagster run id); optional `salt` (cryptographic random 64‑bit or UUID4).
  - Output: a 16‑char base36 id (same length as existing) computed deterministically from the inputs (thin wrapper around existing id helpers). Do not compute or store any `logical_key_id`.
- Store this reserved `doc_id` in the task row before execution. Execution must use this ID to write under `data/docs/<stage>/<doc_id>`.
- Collisions are practically negligible with 96‑bit entropy; detection: fail if directory already exists with a different lineage.

## CSV Schema Changes

- `draft_generation_tasks.csv` (add):
  - `doc_id` (string; required)
- `essay_generation_tasks.csv` (add):
  - `doc_id` (string; required)
  - `parent_doc_id` (string; required) — draft doc id to load phase‑1 text.
- `evaluation_tasks.csv` (add):
  - `doc_id` (string; required)
  - `parent_doc_id` (string; required) — essay doc id to evaluate.

All other existing fields remain; `document_id` is legacy and should be ignored by runtime assets after migration.

## Asset Runtime Changes

- Task definitions (when materialized from the UI)
  - `draft_generation_tasks`, `essay_generation_tasks`, and `evaluation_tasks` assets will include a `doc_id` column in their CSV outputs by calling `reserve_doc_id(stage, task_id, run_id, salt)` for each row.
  - This is non‑breaking in Phase 0: columns are added but not yet required by downstream runtime assets.
  - At cutover (Phase 1), runtime assets will read the `doc_id` value and use it to write to `data/docs/<stage>/<doc_id>`.

- Draft pipeline
  - `draft_prompt`:
    - Accept `doc_id` from tasks.
    - Render prompt from template and pass to `draft_response` in memory (no runtime re-reads).
  - `draft_response`:
    - Consume the prompt argument passed in memory (no re‑read from IO). Write the document to `data/docs/draft/<doc_id>` via `Document.write_files()` including `prompt.txt`. Document paths depend only on `<stage>/<doc_id>` (no logical keys).

- Essay pipeline
  - `essay_prompt`/`essay_response`: prefer the provided `doc_id` (essay) and `parent_doc_id` (draft) from tasks. Write to `data/docs/essay/<doc_id>`.

- Evaluation pipeline
  - `evaluation_prompt`/`evaluation_response`: prefer the provided `doc_id` (evaluation) and `parent_doc_id` (essay) from tasks. Write to `data/docs/evaluation/<doc_id>`.

- IO Managers
  - Remove VersionedTextIOManager from generation/evaluation assets; use in‑memory passing for prompts; rely on docs store for persistence.

## Script Updates

- `scripts/register_partitions_for_generations.py`
  - When curating tasks, allocate and write `doc_id` for each row using the reserve function.
  - Materialize `content_combinations` automatically (already added).

- `scripts/select_top_prior_art.py`
  - When selecting existing top generations, set `doc_id = parent_doc_id` in `essay_generation_tasks.csv` (we are curating essays that already exist in docs). Populate legacy IDs for back‑compat.

- `scripts/parse_all_scores.py`
  - Already reads from docs store; ensure it emits `doc_id` and `parent_doc_id` consistently.

## Migration Plan (Single Cutover; no dual‑path)

Principle: prepare all non‑breaking changes behind the scenes, verify with dry‑runs, and switch once. Avoid mixed modes in runtime assets.

Phase 0 — Prep (non‑breaking)
- Add `reserve_doc_id(stage, task_id, run_id=None, salt=None)` in `utils/ids.py` (base36 16 chars). Do not introduce any `logical_key_id` concept.
- Extend task CSV schemas to include `doc_id` (draft/essay/eval) and `source_doc_id` (draft optional), but do not change asset behavior yet.
- Update scripts only (no asset changes):
  - `register_partitions_for_generations.py`: allocate and write `doc_id` for new rows; optionally set `source_doc_id`.
  - `select_top_prior_art.py`: write `doc_id = parent_doc_id` for curated essays.
  - `parse_all_scores.py`: ensure it emits `doc_id` and `parent_doc_id`.
- Backfill helpers (non‑breaking):
  - Backfill `template_id`, `model_id`, `combo_id`, and `parent_doc_id` in `metadata.json` (done).
  - Skip backfilling `doc_id` into existing task CSVs; we will regenerate the task CSVs instead (existing tasks are not important). Keep current assets ignoring `doc_id` for now.
- Validators (non‑breaking):
  - Add a dry‑run script that checks task CSV `doc_id` vs docs store (conflicts, duplicates, missing parents).

Phase 1 — Cutover (atomic switch)
- Change runtime assets to require and use `doc_id`/`parent_doc_id` exclusively:
  - `draft_prompt`/`draft_response`: require `doc_id`; read prompt via `source_doc_id` when provided; otherwise render; pass prompt in memory.
  - `essay_*`, `evaluation_*`: require `doc_id` and `parent_doc_id`.
- Remove VersionedTextIOManager from generation/evaluation assets in `definitions.py`.
- All reads/writes go to `data/docs/<stage>/<doc_id>`; no task‑named files.

Phase 2 — Cleanup
- Remove legacy task‑based filename loading, fallbacks, and any code referencing `document_id` for runtime resolution.
- Remove any remaining references or parameters related to `logical_key_id`.
- Update docs/guides/examples to reflect `doc_id` execution.
- Optional: add doc‑replay partitions (assets keyed by `doc_id`) for precise re‑runs.

## Validation

- Unit: add tests for `reserve_doc_id` stability and length; tests for assets reading by `source_doc_id` and writing to the expected doc_dir for provided `doc_id`.
- Integration: curate a small set of tasks with `doc_id` and materialize draft→essay→evaluation to confirm outputs land in the expected `data/docs/<stage>/<doc_id>`.
- Scripts: run `register_partitions_for_generations.py` (with `--materialize-combos`) to confirm CSVs include `doc_id` and optional `source_doc_id`.

## Notes & Considerations

- ID collisions: practically negligible; detect by checking if `data/docs/<stage>/<doc_id>` already exists with mismatched `task_id` or `parent_doc_id` and fail with a clear message.
- Human readability: `doc_id` is opaque; for humans, keep legacy task IDs in CSVs and logs for ergonomics during the transition.
- Long‑term: consider doc‑replay partitions (assets keyed by `doc_id`) for precise re‑runs that bypass tasks entirely.

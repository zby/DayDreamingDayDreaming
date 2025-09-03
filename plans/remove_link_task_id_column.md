# Plan: Remove `link_task_id` column and variables (finalize draft-only terminology)

Goal: Eliminate `link_task_id` (and primary `link_template`) from active code paths and tables, standardize on `draft_task_id` and `draft_template`, while maintaining a short compatibility window for reading legacy artifacts and CSVs.

## Principles
- Source of truth: `draft_task_id = {combo_id}_{draft_template}_{generation_model_id}`.
- Essay IDs: `essay_task_id = {draft_task_id}_{essay_template}`.
- Backward-compat only where needed to read legacy files; do not emit `link_*` in new outputs.

## Scope
- CSV schemas under `data/2_tasks/` and cross-experiment outputs under `data/7_cross_experiment/`.
- Asset code and scripts referencing `link_task_id`/`link_template`.
- Docs/tests updated to draft-only naming.

## Step-by-step

1) Schemas and builders
- `essay_generation_tasks` (asset):
  - Remove `link_template` column from output; keep only `draft_template`.
  - Rename local variables to `draft_task_id` in code for clarity.
- `document_index` (asset):
  - Drop `link_template` column; include only `draft_template` (None for legacy one-phase essays).
  - Retain `file_path`, `source_dir`, `source_asset` for provenance.
- `evaluation_tasks` (asset):
  - Stop emitting `link_template` column; include `draft_template` instead.
  - Ensure downstream consumers (evaluation assets, results) do not reference `link_*`.

2) Cross-experiment tracking
- `evaluation_results_append` (asset):
  - Remove `link_template` from appended rows/metadata; include `draft_template` only.
- Generation tables (auto-append assets and rebuild scripts):
  - Write `draft_generation_results.csv` and `essay_generation_results.csv` unchanged (already draft-centric).

3) Scripts
- `build_evaluation_results_table.py`:
  - Ensure canonical columns include `draft_template` and not `link_template`.
  - When joining older CSVs, map legacy `link_template` → `draft_template` in-memory; do not write `link_template`.
- `build_generation_results_table.py`:
  - Already writes draft tables; no `link_*` columns.
- `copy_essays_with_links.py`:
  - Prefer renaming to `copy_essays_with_drafts.py` (or keep name but switch CLI text and metadata keys to `draft_template`).

4) Asset code
- `two_phase_generation.py`:
  - Replace lookups of `task_row["link_task_id"]` with `task_row["draft_task_id"]`.
  - Parser/copy modes: read parser from `draft_templates.csv`; variable names aligned (`draft_template`).
- `llm_evaluation.py`:
  - No changes needed (uses `file_path`), but remove any docstrings mentioning link IDs.

5) Tests
- Update fixtures and schema tests to assert absence of `link_*` in produced DataFrames.
- Update any parsing helpers that expect `link_*` to prefer `draft_*`.

6) Docs
- Remove remaining references to `link_task_id`/`link_template` outside legacy notes.
- Clarify that `links_responses/` is read as a legacy fallback only.

7) Migration / Compatibility
- One-time rebuild:
  - Re-materialize `group:task_definitions` to regenerate CSVs without `link_*`.
  - Rebuild cross-experiment tables via `./scripts/rebuild_results.sh` to ensure new columns are present.
- Provide a small migration script (optional) that:
  - Reads `essay_generation_tasks.csv` and drops `link_template` if present.
  - Converts any `link_task_id` usage in older CSVs to `draft_task_id`.

## Acceptance criteria
- No code paths emit or require `link_task_id` / `link_template`.
- `essay_generation_tasks.csv` columns: `essay_task_id, draft_task_id, combo_id, draft_template, essay_template, generation_model, generation_model_name`.
- `document_index` and `evaluation_tasks` include `draft_template`, not `link_template`.
- Cross-experiment `evaluation_results.csv` includes `draft_template` only.
- Tests and docs reflect draft-only naming; legacy is mentioned only in fallbacks.

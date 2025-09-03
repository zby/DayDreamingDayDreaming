# Remove “links” phase (final cleanup) and standardize on “draft”

Goal: Eliminate the “links” phase from active code paths and schemas while preserving historical data by reading legacy files from `data/3_generation/links_responses/` as a fallback. Standardize all phase‑1 semantics on “draft”. No `links_response` asset or IO manager remains.

## Scope
- Code: two_phase_generation, llm_evaluation, core document plumbing, raw readers/loader, cross‑experiment metadata, and scripts where applicable.
- Data: write only to `draft_*` dirs/columns; continue to read legacy `links_responses/` files for history.
- Tests/Docs: align wording and expectations to “draft”.

## Changes At A Glance
- Drop any dependency on `links_response_io_manager` (use draft IO + file fallbacks).
- Keep a read‑only fallback to `data/3_generation/links_responses/` for historical files.
- Ensure tasks/tables use `draft_*` columns; stop emitting `link_*` except where needed for legacy provenance fields.
- Update comments/docstrings to “draft”.

## Step‑By‑Step

1) two_phase_generation.py (minimal refactor, no behavior change)
- Remove unused imports.
- Add `_get_essay_generator_mode(data_root, template_id)` helper.
- Add `_load_phase1_text(context, link_task_id) -> (text, source_label)` with fallback order:
  1. `draft_response_io_manager.load_input(MockLoadContext(link_task_id))`
  2. direct file at `<data_root>/3_generation/draft_responses/{link_task_id}.txt`
  3. direct file at `<data_root>/3_generation/links_responses/{link_task_id}.txt`
- Use this helper in `_essay_prompt_impl` and parser/copy paths of `_essay_response_impl`.
- Fix `fk_relationship` metadata to reference `{link_task_id}` on RHS.
- Update comments from “links” to “draft”.

2) llm_evaluation.py (docs only)
- Update docstrings: remove references to `links_response_io_manager`; clarify that it reads document content directly from `file_path` provided by tasks.

3) core.py (follow‑ups)
- Continue writing “draft” rows; when enriching, prefer `draft_template` field (stop using `link_template` naming except for legacy provenance in outputs).
- Keep legacy read from `links_responses` when a matching `draft_responses` file is missing.

4) Utilities
- raw_readers: keep `read_draft_templates()` as source of truth; preserve legacy shim only if still referenced.
- template_loader: accept `phase="draft"` (allow `"links"` alias with deprecation log until removed in the final pass).

5) Cross‑experiment assets/scripts
- Ensure tracking assets emit `draft_*` fields; provenance can include `source_dir` = `draft_responses` or `links_responses`.
- Scripts: prefer `draft_responses` and `draft_generation_tasks.csv`; read `links_responses` as fallback only.

6) Tests & Docs
- Update tests to assert “draft” semantics; do not rely on `links_response` asset.
- Update README/guides to use “draft” for Phase‑1; note legacy read‑fallback briefly.

7) Final removal after one stable cycle
- Remove `"links"` alias in template loader and any `read_link_templates` shim.
- Remove all `link_*` column names from active code paths and CSVs.
- Remove any remaining mentions from docs and tests (excluding historical changelogs).

---

Acceptance Criteria
- Phase‑1 outputs only under `data/3_generation/draft_*`.
- No active code requires `links_response` assets/IO.
- Historical files under `links_responses/` are still readable via fallback.
- Cross‑experiment tracking and evaluation continue to work.

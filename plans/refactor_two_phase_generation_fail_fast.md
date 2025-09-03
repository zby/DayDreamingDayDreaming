# two_phase_generation: fail‑fast refactor (remove legacy fallbacks)

Goal: simplify Phase‑1/Phase‑2 generation assets to a single, strict “draft → essay” path, remove legacy "links" fallbacks, normalize naming to draft_*, and fail quickly with clear errors (no silent shims). We’ll fix templates/config/tests in follow‑ups.

Scope
- File: `daydreaming_dagster/assets/two_phase_generation.py`
- Affects: `draft_prompt`, `draft_response`, `essay_prompt`, `essay_response`, helpers.
- Out of scope (follow‑ups): template files, parsers module rename, cross‑repo link→draft cleanup beyond this file.

Principles
- Fail fast. If required inputs/metadata are missing, raise Failure with actionable metadata.
- Do not migrate or edit git‑tracked templates or any files under `data/1_raw/` in this refactor. Historical templates remain as‑is; assets will support them by emitting both variables (see 3).
- Prefer draft‑only semantics in code; remove legacy fallbacks and auto‑derivation.
- Keep behavior explicit and predictable.

Planned Changes
1) Remove legacy I/O fallbacks
   - `_load_phase1_text(context, draft_task_id)`:
     - Remove check for `links_responses/`.
     - Only attempt: `draft_response_io_manager.load_input(MockLoadContext(draft_task_id))`, then direct file `data/3_generation/draft_responses/{draft_task_id}.txt`.
     - If not found, raise Failure (no silent pass). Include `draft_task_id`, expected path(s) in metadata.

2) Require canonical IDs and columns
   - Essay assets must use `draft_task_id` exclusively; do not consult `link_task_id`.
   - If `draft_task_id` missing in `essay_generation_tasks` row, raise Failure. Do not derive from `essay_task_id`.

3) Naming cleanup (minimal compatibility shim)
   - Replace “links” terms with “draft” across code paths.
   - Jinja variables in `essay_prompt`: emit both `draft_block` (preferred) and `links_block` (legacy) pointing to the same text to avoid migrating historical templates.
   - Parser import: use `get_draft_parser` (we will introduce/alias this in `utils/link_parsers.py` in a separate change).
   - Metadata keys: prefer `source_draft_task_id` (keep `source_link_task_id` during deprecation window if needed).

4) Generator modes (strict handling)
   - `essay_prompt`:
     - Keep returning a placeholder string for parser/copy (as now), but include `generator_mode` in metadata.
   - `essay_response`:
     - parser mode: read draft; look up parser from `draft_templates.csv` (`parser` column).
       - If parser missing/unknown → Failure (no fallback to raw text).
       - If parser returns empty/invalid → Failure.
     - copy mode: return draft text verbatim; if empty/whitespace → Failure.
     - llm mode: call LLM with explicit `essay_generation_max_tokens` from config.
   - Unknown `generator` values → Failure.

5) Config normalization (no legacy keys)
   - Expect `ExperimentConfig` to provide:
     - `draft_generation_max_tokens` (rename from `link_generation_max_tokens`).
     - `essay_generation_max_tokens` (new).
     - `min_draft_lines` (default 3).
   - Enforce `min_draft_lines` in `draft_response`; if fewer lines → Failure.

6) Consistency & minor cleanup
   - Use a single global Jinja `Environment` for both prompts.
   - Rename local vars for clarity: `draft_templates_df` (not `link_templates_df`).
   - Normalize text reads (CRLF → LF) with a simple replace.

7) Error metadata (actionable, consistent)
   - Always include: function, `draft_task_id`/`essay_task_id`, `draft_template`, `essay_template`, model information when relevant, and expected file paths.

Migration Notes
- Templates: not migrated. Assets will render both `draft_block` (preferred) and `links_block` (legacy) so historical templates continue to work.
- Parsers: add `get_draft_parser` alias (or rename) in `utils/link_parsers.py` and update references.
- Config: update `ExperimentConfig` keys as above; remove legacy names where used.
- Tests/scripts: update to draft‑only semantics; remove uses of `link_task_id`, `links_responses`, and `links_block`.

Generated Tables Migration (inline)
- Objective: migrate generated CSVs to include canonical `draft_*` columns without touching templates or raw inputs.
- Targets (apply if present):
  - `data/5_parsing/parsed_scores.csv`
  - `data/7_cross_experiment/parsed_scores.csv`
  - `data/7_cross_experiment/evaluation_results.csv`
  - `data/6_summary/generation_scores_pivot.csv`
  - `data/6_summary/final_results.csv`
- Rules (non‑destructive):
  - If `draft_task_id` missing and `link_task_id` exists → add `draft_task_id = link_task_id`.
  - If `draft_template` missing and `link_template` exists → add `draft_template = link_template`.
- Rollout:
  1) Run migration script `scripts/migrate_generated_tables_to_draft_schema.py` (dry‑run, then apply).
  2) Rebuild pivots/tables if needed: `scripts/rebuild_results.sh`.
  3) Switch assets to fail‑fast draft‑only semantics (this plan).

Execution Plan (PR sequence)
1) Asset refactor: apply fail‑fast, draft‑only changes in `two_phase_generation.py`; emit both `draft_block` and `links_block`; prefer `source_draft_task_id` metadata.
2) Config update: add `draft_generation_max_tokens`, `essay_generation_max_tokens`, `min_draft_lines` to `ExperimentConfig`; migrate call sites.
3) Parsers: add `get_draft_parser` alias in `utils/link_parsers.py` (no template edits in `data/1_raw`).
4) Generated tables migration: run `python scripts/migrate_generated_tables_to_draft_schema.py` (then `./scripts/rebuild_results.sh`).
5) Unit/integration tests update:
   - Rename references from link_* → draft_* (IDs, templates, metadata) where applicable.
   - Keep compatibility tests that rely on `links_block` working, but prefer new `draft_block` in new tests.
   - Targeted files: `tests/test_essay_response_copy_mode.py`, `tests/test_essay_response_parser_mode.py`, `tests/test_pipeline_integration.py`, and any `links`-named tests in `daydreaming_dagster/assets/`.
6) Test run and verification:
   - Fast pass: `uv run pytest daydreaming_dagster/ -q`.
   - Full suite: `uv run pytest -q` (sets `DAGSTER_HOME=$(pwd)/dagster_home` if needed for integration tests).
   - Fix any failures; avoid adding fallbacks — fix inputs/tests instead.
7) Commit changes (before final sweep):
   - Staging rules: do NOT use `git add -A`. Stage only intended files (assets/, tests/, scripts/, plans/). Avoid staging data artifacts under `data/`.
   - Suggested message: `refactor(two_phase): fail-fast draft-first assets, tests + tables migration`. Follow with a separate style-only commit if needed after running Black/Ruff.
   - Example: `git add daydreaming_dagster/assets/two_phase_generation.py daydreaming_dagster/assets/test_two_phase_generation.py tests/test_essay_response_* scripts/*copy_essays_with_drafts.py scripts/migrate_generated_tables_to_draft_schema.py plans/refactor_two_phase_generation_fail_fast.md` then `git commit -m "refactor(two_phase): fail-fast draft-first assets, tests + tables migration"`.
8) Sweep remaining modules/scripts for residual `links` naming and remove legacy reads.

Risks
- Breaks existing templates/tests until follow‑ups land. Intentional per fail‑fast directive.
- Requires coordinated config update; surface clear Failures to guide fixes.

Rollback
- Reintroduce legacy shims behind a feature flag if needed (not planned).

# Plan: Unify Template CSV Schemas and Model Columns

## High‑Level Goals
- Unify template CSV schemas (required vs optional columns per file) without breaking existing data.
- Unify task model naming: prefer `model_id` consistently; derive `model_name` at runtime via `llm_models.csv`.

## Summary Decisions
- Keep `generator` and `parser` as separate fields (different concerns):
  - `generator` = how text is produced (`llm` vs `copy`).
  - `parser` = how text is interpreted (e.g., `in_last_line`, `complex`, or draft parsers).
- Unify column names and validation across CSVs; provide backcompat reads in code.

## Target CSV Schemas
- `draft_templates.csv`
  - Required: `template_id`, `active`
  - Optional: `parser` (defaults to identity)
  - Optional: `generator` (defaults to `llm`)
- `essay_templates.csv`
  - Required: `template_id`, `active`, `generator` in {`llm`, `copy`}
  - `parser`: not used today (ignore if present)
- `evaluation_templates.csv`
  - Required: `template_id`, `active`, `parser` in {`in_last_line`, `complex`}
  - Optional: `generator` (defaults to `llm`)

## Unified Model Naming in Tasks (Bonus)
- Prefer `model_id` consistently in task CSVs for all stages.
- Derive `model_name` at runtime via `llm_models.csv` mapping.
- Backcompat mapping in code:
  - `model_id = row.get("generation_model") or row.get("evaluation_model") or row.get("model_id")`
  - `model_name = row.get("generation_model_name") or row.get("evaluation_model_name") or map(model_id)`

## Step‑By‑Step Plan
1) Audit template/model usage
   - Map where draft/essay/evaluation templates and model columns are read/written:
     - Template loaders: `utils/raw_readers.py`, `utils/evaluation_parsing_config.py`
     - Assets: `group_generation_draft.py`, `group_generation_essays.py`, `group_evaluation.py`
     - Task builders: `group_task_definitions.py`, `group_cohorts.py`

2) Add CSV schema validators (utils/raw_readers.py)
   - `draft_templates.csv`: require `template_id`, `active`; allow `parser` (optional); `generator` optional default `llm`.
   - `essay_templates.csv`: require `template_id`, `active`, `generator` (normalize lower; validate {`llm`,`copy`}); ignore `parser` if present.
   - `evaluation_templates.csv`: require `template_id`, `active`, `parser` (normalize lower; validate {`in_last_line`,`complex`}); `generator` optional default `llm`.
   - Error messages: actionable and consistent.

3) Draft stage parser default
   - In `group_generation_draft.py`, continue reading `parser` from `draft_templates.csv` when present; default to identity when absent.
   - Ensure draft `generator` defaults to `llm` if missing (no behavior change).

4) Essay require `generator`
   - In `group_generation_essays.py`, require `generator` column with allowed {`llm`,`copy`} (already enforced).
   - `copy` ⇒ pass‑through (skip LLM; copy draft `parsed.txt` to essay `parsed.txt`).

5) Evaluation require `parser`
   - In `utils/evaluation_parsing_config.py`, keep `parser` required (already implemented) and ensure allowed values set.
   - In `group_evaluation.py`, continue to load parser via `load_parser_map`.

6) Unify task model columns (write side)
   - In `group_task_definitions.py`/`group_cohorts.py`, set canonical `generation_model`/`evaluation_model` to `model_id` and optionally include `*_model_name` from mapping for readability.

7) Backcompat mapping helper (read side)
   - Add `resolve_model(row, models_df) -> (model_id, model_name)` in `utils/dataframe_helpers.py` (or `raw_readers.py`) to centralize legacy/modern column mapping.
   - Update draft/essay/evaluation assets to call `resolve_model` instead of ad‑hoc column checks.

8) Docs and examples
   - Update README and guides to list unified columns per CSV file and the model id/name policy.

9) Tests (targeted)
   - Unit: loaders reject missing required columns/invalid values; accept optional columns; draft parser defaults to identity; essay generator branching; evaluation requires parser.
   - Unit: `resolve_model` returns correct id/name across legacy and unified schemas.
   - Integration: smoke for one partition per stage with minimal fixtures.

## Notes
- We keep the essay legacy alias `links_block` permanently (documented in code) because historical templates reference it.
- No immediate CSV migration required — backcompat reads support legacy column names, while write paths move toward the unified schema.
- Error messages should always include the expected action (which CSV/column to fix).


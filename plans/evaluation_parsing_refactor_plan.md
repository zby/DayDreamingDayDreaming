# Evaluation Parsing Refactor Plan (v2)

This plan aligns evaluation response parsing with the newly added `parser` column in `data/1_raw/evaluation_templates.csv`. We will make parsing strategy explicit and data-driven, reduce ad-hoc logic, and keep compatibility during migration.

## Objectives
- Single source of truth for per-template parsing strategy (`parser` column).
- Prefer template-provided strategy; fall back to legacy detection only when necessary.
- Minimize code duplication and fragile heuristics.
- Provide clear validation, schema guarantees, and tests.

## Current State (Summary)
- `evaluation_tasks` does not include a parser column; parsing strategy is inferred in `utils/evaluation_processing.detect_parsing_strategy` via a hardcoded legacy set.
- `utils/eval_response_parser` implements strategies: `complex` and `in_last_line`.
- `parse_evaluation_files` chooses strategy indirectly; `parse_evaluation_file_from_filename` infers template name from filename.

## Target State
- `evaluation_tasks` includes a `parser` column sourced from `evaluation_templates.csv`.
- `parse_evaluation_files` reads the strategy directly from the task row when present.
- `detect_parsing_strategy` remains only as a temporary fallback and is marked deprecated.

## Data Model Changes
- evaluation_templates.csv: new column `parser` with allowed values: `complex`, `in_last_line`.
- evaluation_tasks (asset output): propagates a nullable `parser` column (string).

## API/Behavior Changes
- `parse_evaluation_response(response_text, evaluation_template, parser=None)`
  - If `parser` is a known value → use it.
  - Else → call `detect_parsing_strategy(evaluation_template)` and log a warning when falling back.
- `parse_evaluation_files(..., parse_function=None, context=None)`
  - For each task row, pass `parser=row.get('parser')` to the parse path.
- Cross-experiment functions remain metadata-light; they continue to infer template and use fallback detection unless a templates DataFrame is provided (future optional enhancement).

## Code Changes (by module)

1) daydreaming_dagster/assets/groups/group_task_definitions.py
- evaluation_tasks:
  - When constructing tasks, join evaluation templates with `template_id` and include `parser` in the output schema.
  - Optional (Phase 2): validate non-empty `parser` for active templates; otherwise, attach a metadata warning or raise Failure when strict.

2) daydreaming_dagster/utils/evaluation_processing.py
- parse_evaluation_response(response_text, evaluation_template, parser=None):
  - Implement parser-preferred behavior; fallback to detect_parsing_strategy with a one-line deprecation note.
- parse_evaluation_files(evaluation_tasks, base_path, parse_function=None, context=None):
  - Retrieve `row.get('parser')`, pass to parse path. Include `used_parser` in result dict for auditability (optional metadata field).
- detect_parsing_strategy(evaluation_template):
  - Mark as deprecated in docstring; keep legacy set for now.
- parse_evaluation_file_from_filename(filename, base_path):
  - No schema changes; continue using detection (no metadata join). Consider an overload that accepts `templates_df` later.

3) daydreaming_dagster/utils/eval_response_parser.py
- No behavioral changes. Optionally remove unused `_extract_last_non_empty_line` (dead code cleanup).

## Validation & Tests

New/updated unit tests:
- utils/test_evaluation_processing.py
  - parser=complex: response with `Total Score: 7/10` → score 7.0.
  - parser=in_last_line: last line `**SCORE: 8**` → score 8.0.
  - missing parser + legacy template: falls back to complex and succeeds.
  - invalid parser value: falls back (Phase 1) with a log warning; assert parse still succeeds.
- assets/test_core_tasks_schema.py
  - Allow/expect `parser` column in `evaluation_tasks` (ensure schema test includes it or treats it as allowed/optional).

Execution order for validation:
- `uv run pytest daydreaming_dagster/utils/test_evaluation_processing.py -q`
- `uv run pytest daydreaming_dagster/assets/test_core_tasks_schema.py::test_evaluation_tasks_id_and_columns -q`
- `uv run pytest -q`

## Rollout Plan
- Phase 1 (Soft)
  - Implement parser propagation and preferred usage.
  - Log a warning when falling back to detection (missing/invalid parser).
- Phase 2 (Strict)
  - Enforce non-empty, valid `parser` for active templates in `evaluation_tasks` (raise Failure with clear remediation).
- Phase 3 (Cleanup)
  - Remove legacy detection lists and deprecate/remove `detect_parsing_strategy`.

## Risks & Mitigations
- CSV contains unknown `parser` value:
  - Phase 1: warn + fallback to detection.
  - Phase 2+: validate and fail fast with actionable error.
- Downstream code/tests expect old schema:
  - Update schema assertions and fixtures to include/allow `parser`.
- Cross-experiment without metadata:
  - Keep current behavior; optionally add an overload to accept templates DataFrame later.

## Acceptance Criteria
- `evaluation_tasks` includes `parser` for active templates.
- `parse_evaluation_files` prefers `parser` from tasks and successfully parses both strategies across tests.
- Fallback path is exercised and logs a warning when `parser` missing/invalid.
- Full test suite passes.

## Tasks Checklist
1. Update `evaluation_tasks` to include `parser` from evaluation templates.
2. Update `parse_evaluation_response` to accept/use `parser`.
3. Pass `parser` through `parse_evaluation_files` and add optional `used_parser` to outputs.
4. Add/adjust unit tests (processing + schema).
5. Optional cleanup: remove unused helper in eval_response_parser.
6. Phase 2 switch: add validation on `parser` presence/validity (behind a flag or once data is ready).

## Future Enhancements
- Add `parser_params` (JSON) to templates for fine-grained control (e.g., lines to scan, pattern toggles).
- Provide a cross-experiment API that accepts `templates_df` and uses parser metadata.
- Emit Dagster metadata per run indicating `used_parser` and parsing outcomes.


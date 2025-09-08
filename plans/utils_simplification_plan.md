# Utils Simplification and Deprecation Plan

Date: 2025-09-08

Scope: daydreaming_dagster/utils

Goals
- Keep only current, well-scoped helpers that the assets actively use.
- Remove legacy task-id path scanning and prefer the docs store (data/docs/<stage>/<doc_id>).
- Improve failure diagnostics where helpful, without expanding backcompat surface.

Status by Module (summary)
- ids.py — current; used for doc-id–first flow. Keep.
- versioned_files.py — current; used for RAW/versioned artifacts. Keep.
- csv_reading.py — current; unit-tested; optional adoption in raw_readers. Keep.
- raw_readers.py — current; used by task_definitions and essay generator mode; consider csv_reading adoption. Keep.
- document.py — current; writes docs store artifacts. Keep.
- filesystem_rows.py — current; loads docs store by doc_id. Keep.
- document_locator.py — BACKCOMPAT(PATHS); task-id lookup under data/3_generation/*.
  - TODO-REMOVE-BY: 2025-01-01 — delete after callers switch to doc-id/docs store.
- draft_parsers.py — current; registry for draft extraction. Keep.
- template_loader.py — current; phase-aware. Keep.
- eval_response_parser.py — current; strategies complex/in_last_line; covered by tests. Keep.
- evaluation_parsing_config.py — current; strict template→parser mapping. Keep.
- evaluation_processing.py — current; parses results and cross-experiment helpers. Keep.
- selected_combos.py — current; subset validation; used in task_definitions. Keep.
- combo_ids.py — current; append-only mapping; light cleanup applied. Keep.
- dataframe_helpers.py — current; ID validation + row selection Failures. Keep.
- file_fingerprint.py, raw_state.py — current; used by schedules. Keep.
- raw_write.py — current; thin wrapper over versioned_files. Keep.

Changes Applied (safe)
- utils/raw_readers.py: removed unused read_csv_with_context import (no behavior change).
- utils/combo_ids.py: removed unused hashlib import.
- utils/document_locator.py: added TODO-REMOVE-BY 2025-01-01 note to enforce deprecation.
- Validation: .venv/bin/pytest -q daydreaming_dagster/utils → 39 passed.

Proposed Simplifications (next steps)
1) Remove document_locator usage (deprecate task-id scanning)
   - Call sites: assets/cross_experiment.py (draft/essay appenders), assets/group_task_definitions.py (document_index helper, minor logging).
   - Replace with: doc-id/doc store resolution or task-table derived file paths.
   - Acceptance: no assets import document_locator; tests green; cross-experiment rows continue to resolve files.
   - Timeline: before 2025-01-01.

2) Adopt csv_reading.read_csv_with_context in raw_readers (optional behavior change)
   - Benefit: clearer Dagster Failures with contextual lines on parse errors.
   - Scope: draft_templates.csv, essay_templates.csv, evaluation_templates.csv, llm_models.csv.
   - Acceptance: unit/integration tests pass; error messages in failures include context block.

3) Add docs store metadata helper (optional)
   - New util: filesystem_rows.read_metadata(row) → dict for metadata.json if present.
   - Benefit: consistent access pattern where assets need metadata.
   - Acceptance: covered by a small unit test; no behavioral change for existing assets unless adopted.

Validation Plan
- After each change, run the narrowest relevant tests:
  - document_locator removal: run assets tests touching cross_experiment and task_definitions; spot-check paths.
  - csv_reading adoption: run tests that read templates/models; add/adjust unit tests for error paths.
  - metadata helper: add a unit test under utils.

Backcompat & Policy Notes
- Do not expand legacy path scanning; prefer fail-fast and explicit configuration.
- Keep fallbacks narrow and tagged (BACKCOMPAT|FALLBACK) with TODO-REMOVE-BY dates.


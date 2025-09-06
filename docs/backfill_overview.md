# LLM Documents Backfill — High‑Level Overview

This document describes, at a high level, what the backfill does and the rules it applies to reconstruct a consistent documents index (SQLite + per‑document directories) from historical artifacts on disk.

## Purpose
- Import historical generation/evaluation artifacts from the filesystem into a simple, queryable index.
- Normalize identifiers and directory layout so that downstream assets can resolve inputs strictly via the index (DB‑first), without bespoke filesystem scans.
- Preserve legacy provenance for diagnostics and one‑time migration audits.

## Inputs (read‑only)
- Authoritative cross‑experiment tables under `data/7_cross_experiment/`:
  - `draft_generation_results.csv` and `link_generation_results.csv` (Phase‑1 drafts, current and legacy)
  - `essay_generation_results.csv` (Phase‑2 essays)
  - `generation_results.csv` (combined view, optional)
  - `evaluation_results.csv` (evaluation runs)
  - `parsed_scores.csv`, `evaluation_scores_by_template_model.csv` (scoring/analysis; optional during backfill)
- Response content under `data/3_generation/` (used only to read text via the `response_file` column from the CSVs):
  - `draft_responses/`, `links_responses/`, `links_prompts/`, `essay_responses/`, `evaluation_responses/`
- Optional task CSVs under `data/2_tasks/` for additional context.

## Outputs (write)
- A row in SQLite `documents` per attempt:
  - `doc_id`, `logical_key_id`, `stage`, `task_id`, `parent_doc_id`, `template_id`, `model_id`, `run_id`, `doc_dir`, sizes, small metadata.
- A per‑document directory in `data/docs/{stage}/{logical_key_id}/{doc_id}/` with:
  - `raw.txt`, `parsed.txt`, optional `prompt.txt`, and `metadata.json`.

## Key Concepts
- Task identifiers come directly from the cross‑experiment CSVs:
  - Drafts/links: use `draft_task_id` or `link_task_id` exactly as `task_id`.
  - Essays: use `essay_task_id` exactly as `task_id`.
  - Evaluations: use `evaluation_task_id` exactly as `task_id`.
- Do not reconstruct `task_id` from other columns (e.g., `combo_id`, template, model).
- No filename parsing: document details must come from CSV columns; treat filenames/stems as opaque.

## Invariants
- Backfill is idempotent by `doc_id` (computed from a stable tuple); reruns with the same `run_id` skip existing rows.
- It never mutates rows after insert; you can always rebuild from a clean slate by dropping `data/docs/` and the SQLite file.
- It does not update pipeline code or modify canonical runtime behavior.

## Processing Order (why it matters)
1) Drafts (current Phase‑1)
2) Legacy links (treated as drafts)
3) Essays (Phase‑2)
4) Evaluations

Legacy drafts must be present in the DB when essays are processed so parentage can be resolved deterministically.

## Draft Ingestion
- For each CSV row in `data/7_cross_experiment/draft_generation_results.csv` and `data/7_cross_experiment/link_generation_results.csv`:
  - Use the row’s `draft_task_id` or `link_task_id` as the document `task_id`.
  - Load response content using `response_file` (path is relative to `data/3_generation/`).
  - Write `doc_dir` with `raw.txt`, `parsed.txt` (same as raw when no parser), optional `prompt.txt` (for links via `links_prompts/` when available), and `metadata.json`.
  - Insert a `documents` row with `stage='draft'`, `task_id` from the CSV, and the CSV‑provided metadata (model, template, timestamps, sizes).
  - If the response file referenced by `response_file` is missing or unreadable, add the row to the bad‑generations list (see below) with reason `missing_response_file` and skip creating a document.

- For each CSV row in `data/7_cross_experiment/essay_generation_results.csv`:
  - Use the row’s `essay_task_id` as the document `task_id`.
  - Resolve `parent_doc_id` (upstream draft) via CSV joins, not filename heuristics:
    - Primary join: match a draft row (from draft/link CSVs) on `combo_id` and `generation_model` with `generation_status='success'` and `generation_timestamp` ≤ essay’s timestamp; choose the latest such draft.
    - If multiple drafts tie on timestamp, prefer rows from `draft_generation_results.csv` over `link_generation_results.csv`.
    - If no parent draft can be resolved from the CSVs, add the essay row to the bad‑generations list with reason `unresolved_parent` and skip it in this pass.
  - Compute `logical_key_id` consistently:
    - When parent found: `H("essay", parent_doc_id, essay_template_id, generation_model)` (matches runtime asset logic).
    - Otherwise: stable fallback `H("essay", combo_id, essay_template_id, generation_model)`.
  - Insert a row with `stage='essay'`, `task_id` from the CSV, and CSV‑provided metadata.

## Evaluation Ingestion
- For each CSV row in `data/7_cross_experiment/evaluation_results.csv`:
  - Use the row’s `evaluation_task_id` as the document `task_id`.
  - Prefer explicit linkage from the CSV:
    - If `essay_task_id` is present, resolve to that essay via its `task_id`.
    - Else if `generation_task_id` is present, resolve to the draft via its `task_id`.
  - If neither linkage can be resolved from the CSVs, add the row to the bad‑generations list with reason `unresolved_target` and skip it in this pass.
  - Compute `logical_key_id = H("evaluation", parent_doc_id, evaluation_template, evaluation_model)`.

## Bad‑Generations List (second pass)
- Any row that cannot be ingested without filename parsing or has missing inputs is appended to `data/7_cross_experiment/bad_generations.csv` with:
  - `source_csv` (draft/link/essay/evaluation), a stable `row_index` or `task_id`, and `reason` (`missing_response_file`, `unresolved_parent`, `unresolved_target`, etc.).
  - Include key columns from the source row to aid repair (`combo_id`, template id, model, timestamps, `response_file`).
- The first pass never attempts to infer details from filenames; the second pass can apply targeted repair heuristics or manual fixes based on this list.



## Idempotency & Safety
- `doc_id` is stable per (`logical_key_id`, `run_id`, source CSV row key), so reruns with the same `run_id` won’t duplicate rows.
- Backfill writes only new rows/directories; it doesn’t mutate or delete.
- To rebuild, remove `data/docs/` and the SQLite file, then rerun.

## Reporting & Verification
- After backfill, materialize:
  - `documents_latest_report` (latest OK rows by stage)
  - `documents_consistency_report` (missing raw/parsed/prompt, dir_exists)
- Backfill emits counters: `draft_ok`, `draft_missing_response`, `essay_parent_resolved`, `essay_unresolved_parent`, `eval_resolved`, `eval_unresolved_target`, plus the count of rows written to `bad_generations.csv`.

## Post‑Backfill Runtime
- Generation assets operate DB‑first; readers do not need filesystem fallbacks.
- Writers can dual‑write during the soak period and later disable legacy file writes entirely.

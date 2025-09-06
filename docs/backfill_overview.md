# LLM Documents Backfill — High‑Level Overview

This document describes, at a high level, what the backfill does and the rules it applies to reconstruct a consistent documents index (SQLite + per‑document directories) from historical artifacts on disk.

## Purpose
- Import historical generation/evaluation artifacts from the filesystem into a simple, queryable index.
- Normalize identifiers and directory layout so that downstream assets can resolve inputs strictly via the index (DB‑first), without bespoke filesystem scans.
- Preserve legacy provenance for diagnostics and one‑time migration audits.

## Inputs (read‑only)
- Files under `data/3_generation/`:
  - `draft_responses/` (Phase‑1 drafts; current two‑phase)
  - `links_responses/` and `links_prompts/` (legacy Phase‑1 “links” as draft‑like)
  - `essay_responses/` (Phase‑2 essays)
  - `evaluation_responses/` (evaluations)
- Optional task CSVs under `data/2_tasks/` for additional context.

## Outputs (write)
- A row in SQLite `documents` per attempt:
  - `doc_id`, `logical_key_id`, `stage`, `task_id`, `legacy_task_id`, `parent_doc_id`, `template_id`, `model_id`, `run_id`, `doc_dir`, sizes, small metadata.
- A per‑document directory in `data/docs/{stage}/{logical_key_id}/{doc_id}/` with:
  - `raw.txt`, `parsed.txt`, optional `prompt.txt`, and `metadata.json`.

## Key Concepts
- Canonical identifiers (current pipeline):
  - Draft task id: `combo_id__draft_template__model`
  - Essay task id: `combo_id__essay_template__model`
  - Evaluation task id: `document_id__evaluation_template__evaluation_model`
- Legacy identifiers (historical filenames):
  - Underscore format: `combo_v1_<hash>_<template>_<model>[...optional...]`
  - We persist this exact stem in `legacy_task_id` for traceability and matching.

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
- For each draft file (both current `draft_responses/` and legacy `links_responses/`):
  - Derive canonical `task_id` when possible:
    - Current triple: `combo__draft_template__model`
    - Legacy underscore: parse `combo_v1_<hash>_<draft_template>_<model>` and rewrite to canonical triple.
  - Always set `legacy_task_id` to the filename stem (exact original id).
  - Write `doc_dir` with `raw.txt`, `parsed.txt` (same as raw when no parser), optional `prompt.txt` (links), and `metadata.json`.
  - Insert a `documents` row with stage=`draft` and canonical `task_id`.

## Essay Ingestion (parent linkage rules)
- For each essay file:
  - Compute canonical `task_id` when possible:
    - Current triple: `combo__essay_template__model`
    - Legacy underscore: parse `combo_v1_<hash>_<essay_template>_<model>` and rewrite to canonical triple.
  - Resolve `parent_doc_id` (the upstream draft) using:
    1) Legacy prefix‑of (underscore): derive legacy draft id by dropping the final essay template token and match against `draft.legacy_task_id`.
    2) Legacy prefix‑of (double‑underscore): if the essay id has ≥4 `__` parts, the first 3 parts form the draft id; match against `draft.task_id`.
    3) Fallback: canonical combo+model search with `GLOB`: `combo__*__model` (underscores literal).
  - Compute `logical_key_id` consistently:
    - When parent found: `H("essay", parent_doc_id, essay_template, model)` (matches runtime asset logic).
    - Otherwise: stable fallback `H("essay", base)`.
  - Insert a row with `stage='essay'`, canonical `task_id`, and `legacy_task_id` set to the filename stem.

## Evaluation Ingestion
- Reconstruct `document_id` from the leading triple in `evaluation_task_id = document_id__evaluation_template__evaluation_model`.
- Resolve `parent_doc_id` by exact match on `document_id` (either draft or essay).
- Use `H("evaluation", parent_or_document_id, evaluation_template, evaluation_model)` for `logical_key_id`.

## Why `legacy_task_id`?
- Disambiguates canonical vs. historical naming and makes legacy prefix‑of matching unambiguous.
- Lets you audit where a row came from and how the canonical id was normalized.
- Enables simple, transparent repairs (e.g., “match essays by legacy prefix equality, then rewrite canonical task ids”).

## Idempotency & Safety
- `doc_id` is stable per (`logical_key_id`, `run_id`, filename), so reruns with the same `run_id` won’t duplicate rows.
- Backfill writes only new rows/directories; it doesn’t mutate or delete.
- To rebuild, remove `data/docs/` and the SQLite file, then rerun.

## Reporting & Verification
- After backfill, materialize:
  - `documents_latest_report` (latest OK rows by stage)
  - `documents_consistency_report` (missing raw/parsed/prompt, dir_exists)
- Backfill prints parent resolution stats for essays: `prefix_hits`, `glob_hits`, `none`, and `alias_inserts` (if aliasing is enabled).

## Post‑Backfill Runtime
- Generation assets operate DB‑first; readers do not need filesystem fallbacks.
- Writers can dual‑write during the soak period and later disable legacy file writes entirely.


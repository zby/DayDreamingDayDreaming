# Plan: Unify Parsing for Drafts and Evaluations (parsed.txt–first)

Status: proposal
Owner: daydreaming_dagster
Related: docs/guides/docs_store.md

## Summary

Unify how the pipeline parses LLM outputs across stages:
- Both drafts (Phase‑1) and evaluations write a normalized parsed.txt under `data/docs/<stage>/<doc_id>/`.
- Evaluations use the configured parser (from `evaluation_templates.csv`) at runtime, just like drafts use `draft_templates.csv`.
- parsed.txt becomes the single source of truth for downstream processing (reports, selection, pivots). Scripts that aggregate results read parsed.txt (fall back to raw.txt when absent).

Benefits:
- Single, predictable read path for downstream tasks and ops scripts.
- Less duplication: one place to encode parsing logic and tests.
- Fewer surprises during re‑runs and cross‑experiment scans.

## Scope

- Runtime assets
  - Evaluation: parse LLM response according to `evaluation_templates.csv:parser`, write parsed.txt next to raw.txt and prompt.txt.
  - Drafts (already parsed): no change in behavior; ensure consistent interface with evaluation parsers.
- Aggregation/analysis scripts
  - `scripts/parse_all_scores.py`: prefer parsed.txt (fall back to raw.txt), then parse using the configured parser. Optionally, if parsed.txt already contains a normalized `SCORE: X` line, use that fast‑path.
- Tests and docs
  - Add unit/integration tests that parsed.txt is written for evaluations and contains a normalized SCORE line.
  - Update docs to state evaluations also produce parsed.txt and downstream tools read it.

## Design

- Parser discovery
  - Drafts: `draft_templates.csv` (column `parser`, optional; identity when missing)
  - Evaluations: `evaluation_templates.csv` (column `parser`, required for strict mode)
  - Config loader: `daydreaming_dagster/utils/evaluation_parsing_config.py` and draft parser registry unify on a simple interface: `parse_fn(text) -> dict` with at least `{score, error}` for evaluations.
- Normalized parsed.txt for evaluations
  - Textual output that always ends with a single, machine‑readable `SCORE: <float>` line (0–10). When the raw format already has it, copy through; otherwise append a trailing normalized line.
  - Keep full reasoning/body text intact (do not only persist the score).
- Error handling
  - If parsing raises, still write raw.txt; optionally skip parsed.txt or write a minimal parsed.txt with an error note; include the error in asset metadata for observability.

## Code Changes (Files/Modules)

- `daydreaming_dagster/assets/group_evaluation.py`
  - After LLM call, parse the response using `require_parser_for_template()` + `parse_llm_response()`.
  - Normalize parsed text to include a definitive `SCORE: X` line.
  - Write `raw.txt`, `parsed.txt`, `prompt.txt`, and `metadata.json` (already implemented for drafts via `Document`).
  - Add clear failure on missing `parent_doc_id` and missing/unknown parser.
  - Feature flag (ExperimentConfig) for soft rollout: `eval_write_parsed_enabled` (default on); when off, current behavior remains (raw‑only).

- `daydreaming_dagster/utils/eval_response_parser.py`
  - Keep both `in_last_line` and `complex` strategies.
  - Ensure `parse_llm_response()` returns `{score: float, error: str|None}` consistently.

- `daydreaming_dagster/utils/draft_parsers.py`
  - No change required; if we want a unified shim, add a tiny adapter so both draft and evaluation parsers have a common call shape.

- `scripts/parse_all_scores.py`
  - Continue to scan `data/docs/evaluation/<doc_id>/`.
  - Prefer `parsed.txt`; if absent, fall back to `raw.txt` (back‑compat) and parse via configured parser.
  - If `parsed.txt` already includes a `SCORE: X` line, use that fast‑path without re‑parsing.
  - Keep robust error reporting per document (doc_id, template, model, created_at, path, error).

- `daydreaming_dagster/checks/documents_checks.py`
  - Checks already validate presence of `parsed.txt` or `raw.txt`. Optionally tighten to prefer parsed.txt for evaluations.

- Docs
  - `docs/guides/docs_store.md`: note evaluations produce parsed.txt with normalized SCORE line.
  - README snippets referencing parsed scores stay valid.

## Migration Plan

- Phase 0 (non‑breaking)
  - Add evaluation parsed.txt writing behind `eval_write_parsed_enabled=False`.
  - Add unit tests for parser mapping and normalized SCORE serialization.
  - Keep `parse_all_scores` unchanged.

- Phase 1 (cutover)
  - Enable `eval_write_parsed_enabled=True` by default.
  - Simplify `parse_all_scores`: prefer parsed.txt; re‑parse raw only when parsed.txt missing.
  - Update asset checks to prefer parsed.txt presence for evaluations.

- Phase 2 (cleanup)
  - Remove temporary flags and fallbacks where appropriate once data is fully migrated.
  - Trim any dead code in scripts that assumed evaluation parsing was only out‑of‑band.

## Testing

- Unit tests
  - `utils/test_eval_response_parser.py`: already covers parsing strategies; add tests for normalized SCORE line generation when missing.
  - Add a lightweight test to ensure `evaluation_response` writes parsed.txt and includes SCORE line.

- Integration tests
  - Pipeline integration: materialize one evaluation; assert `data/docs/evaluation/<doc_id>/parsed.txt` exists and contains a valid SCORE line; run `parse_all_scores.py` and confirm the record uses parsed.txt path and extracts the expected score.

## Backward Compatibility

- `parse_all_scores.py` remains tolerant: if parsed.txt isn’t present (older docs), it falls back to raw.txt and parses on the fly.
- No changes to task CSV schemas or partition keys.

## Risks & Mitigations

- Parser mismatch between runtime and aggregator: mitigated by using the same `evaluation_templates.csv` parser map in both places.
- Partial rollouts: mitigate with a feature flag and fallback to aggregator parsing of raw.

## Done Criteria

- Evaluation docs consistently contain parsed.txt with normalized `SCORE: X`.
- Aggregation consumes parsed.txt (fast‑path) and only parses raw as a fallback.
- Tests cover both normalized and already‑SCORE‑present cases.
- Docs updated; no DB dependencies.


# Plan: Unify Parsing for Drafts and Evaluations (parsed.txt–first)

Status: final draft (ready)
Owner: daydreaming_dagster
Related: docs/guides/docs_store.md

## Summary

Pipeline behavior (target):
- Drafts and evaluations write parsed.txt under `data/docs/<stage>/<doc_id>/`.
- Evaluations parse responses using the configured parser from `evaluation_templates.csv` (strict mapping) at runtime, mirroring drafts using `draft_templates.csv`.
- parsed.txt is the first‑class and required source for downstream processing. Aggregators read parsed.txt only (no raw.txt fallback) after migration.

Benefits:
- Single, predictable read path for downstream tasks and ops scripts.
- Less duplication: one place to encode parsing logic and tests.
- Fewer surprises during re‑runs and cross‑experiment scans.

## Scope (post‑migration)

- Runtime assets
  - Evaluation: `daydreaming_dagster/assets/group_evaluation.py` parses the LLM response using `require_parser_for_template()` + `parse_llm_response()`, and writes raw.txt, parsed.txt, prompt.txt, and metadata.json via `Document`.
  - Drafts: `daydreaming_dagster/assets/group_generation_draft.py` already parses per `draft_templates.csv:parser` (identity when missing) and writes parsed.txt.
- Aggregation/analysis scripts
- `scripts/aggregate_scores.py`: read parsed.txt only. If missing, treat as error. No raw.txt fallback once migration completes.
- Tests and docs
  - Unit tests cover evaluation parsing strategies
  - Docs reflect docs‑store outputs; evaluation parsed.txt now standard.

## Design

- Parser discovery
  - Drafts: `draft_templates.csv` (column `parser`, optional; identity when missing)
  - Evaluations: `evaluation_templates.csv` (column `parser`, strict; loaded via `utils/evaluation_parsing_config.load_parser_map`)
  - Parsers: drafts use `utils/draft_parsers.get_draft_parser`; evaluations use `utils/eval_response_parser.parse_llm_response` with strategies `complex` or `in_last_line`.
- Normalized parsed.txt for evaluations
  - Migration backfill writes number‑only parsed.txt (just the float score on a single line) for legacy docs.
  - Aggregator accepts either a number‑only file or parsed text containing a trailing `SCORE: <float>` line — no raw fallback post‑migration.
- Error handling
  - On parsing exceptions, raw.txt is still written; parsed.txt remains the normalized text (without injected score line). Errors surface via downstream parsing and metadata.

## Code Map (target)

- `daydreaming_dagster/assets/group_evaluation.py`
  - Parses and writes numeric‑only parsed.txt (just the score on one line).
  - Validates presence of `parent_doc_id` and uses `Document` for filesystem writes.

- `daydreaming_dagster/utils/eval_response_parser.py`
  - Provides `parse_llm_response()` with `complex` and `in_last_line` strategies; returns `{score, error}`.

- `daydreaming_dagster/utils/draft_parsers.py`
  - Registry for draft‑phase parsers; identity when unspecified.

- `scripts/aggregate_scores.py`
  - Require parsed.txt; drop raw.txt fallback after backfill.
  - Fast‑path numeric‑only parsed.txt; otherwise extract score from a trailing `SCORE: <float>` line.
  - Do not consult parser maps in the aggregator (runtime already produced parsed.txt).
  - Error row (with metadata) when parsed.txt is missing/unreadable; continue.

- `daydreaming_dagster/checks/documents_checks.py`
  - Asset checks ensure either parsed.txt or raw.txt exists for draft/essay/evaluation docs.

- Docs
  - Guides and README note docs‑store outputs; evaluation parsed.txt present by default.

## Migration Plan

- Backfill: run `scripts/backfill/backfill_evaluation_parsed_texts.py` to create parsed.txt for all existing evaluation docs. The backfill writes number‑only parsed.txt.
- Update aggregator: modify `scripts/aggregate_scores.py` to require parsed.txt and remove raw fallback; read numeric‑only directly, else extract from a `SCORE:` line.
- Verify: run the aggregator over the corpus and ensure zero missing‑parsed cases.
- Update documentation

### Implementation Tasks (explicit)
- Update `scripts/aggregate_scores.py` as above; ensure stable schema and clear error reporting when parsed.txt is missing/unparsable.
- Keep runtime assets unchanged (already writing parsed.txt for drafts/evaluations).

## Testing

- Unit tests
  - `utils/test_eval_response_parser.py`: strategies covered;
  - Asset tests exercise `evaluation_response` writing parsed.txt.

- Integration tests
  - Pipeline integration asserts parsed.txt existence and aggregator behavior on parsed/raw.

## Backward Compatibility

- After migration, no raw fallback remains in aggregation. Legacy docs must be backfilled.
- No changes to task CSV schemas or partition keys.

## Risks & Mitigations

- Parser mismatch between runtime and aggregator: mitigated by the shared `evaluation_templates.csv` parser map used by both.
- Missing parsed.txt: migration script ensures coverage; aggregator will error otherwise.

## Done Criteria

- Evaluation docs contain parsed.txt with a number or a trailing `SCORE:` line.
- Aggregation reads parsed.txt only; no raw fallback remains.
- Docs align with current behavior; no DB dependencies.

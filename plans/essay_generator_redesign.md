# Essay Generator Metadata Redesign

## Background
- Current behavior: `essay_templates.csv` has a `generator` column controlling mode: `llm` (default), `parser`, or `copy`.
- In parser mode, code may look up the parser name from `draft_templates.csv` (`parser` column), creating two cross‑file dependencies.
- Draft templates also have a `parser` (legacy for link/draft parsing), which can conflict conceptually with essay parser selection.

## Problem
- Ambiguity: Parser selection for the essay phase is not sourced from the essay template itself.
- Conflicts: A draft template’s `parser` may not be appropriate for all essay templates that consume its drafts.
- Coupling: Essay behavior depends on draft template CSV, making reasoning and data updates error‑prone.

## Goals
- Single source of truth for essay generator behavior.
- Eliminate conflicts between draft and essay CSVs.
- Keep migration simple with clear deprecation and validation.

## Proposed Design
1) Essay templates own their generator behavior entirely.
   - `essay_templates.csv` columns:
     - `template_id` (key)
     - `generator` ∈ {`llm` (default/blank), `parser`, `copy`}
     - `parser` (string, required iff `generator == 'parser'`)
     - existing metadata (name, description, active, …)
2) Draft templates’ `parser` remains only for draft‑phase internals (if still needed) and is not read by the essay phase.
   - Optionally rename to `draft_phase_parser` in the future to reduce confusion (separate follow‑up).
3) Validation/guards:
   - If `generator == 'parser'` and `parser` is empty in `essay_templates.csv` → hard Failure with clear resolution.
   - If essay code detects a parser value in `draft_templates.csv` for the referenced draft template, emit a deprecation WARNING (metadata) but do not use it.
4) No change to `copy` or `llm` modes.

## Data Changes
- Update `data/1_raw/essay_templates.csv` to include a `parser` column.
- For each essay template with `generator=parser`, set `parser` to a registered name (e.g., `essay_idea_last`, `essay_block`).
- No required edits to `draft_templates.csv` for this redesign; its `parser` will be ignored by the essay phase.

## Code Changes (targeted)
- `assets/groups/group_generation_essays.py`:
  - Replace parser lookup: read from `essay_templates.csv` `parser` column only.
  - Remove fallback to `draft_templates.csv`.
  - On missing parser for `generator=parser`, raise `Failure` with metadata: `essay_template`, `expected_column='parser'`, `resolution` hint.
  - When a draft template has a parser column set, add metadata warning (`deprecated_parser_source='draft_templates.csv'`) to nudge data cleanup.
- `utils/raw_readers.py`:
  - No structural change required; just ensure reading `essay_templates.csv` preserves the `parser` column.
- `utils/link_parsers.py`:
  - No change; registry remains the source of truth for parser function names.

## Validation & Tooling
- Add an optional “templates sanity check” asset that validates:
  - For every essay template with `generator=parser`, a non‑empty `parser` is present and registered.
  - Emit warnings if any referenced draft template also defines a `parser` (deprecated source for essay phase).
- Include metadata counts and quick resolution tips.

## Migration Plan
1) Add `parser` column to `essay_templates.csv` (commit data update).
2) Populate parser values for `generator=parser` rows.
3) Deploy code change that reads only from the essay CSV.
4) Run the sanity check asset and fix any flagged issues.
5) Announce deprecation of using `draft_templates.csv.parser` for essay phase; schedule removal later.

## Tests
- Update/extend tests in `tests/`:
  - Parser mode: succeeds when `essay_templates.csv` has `parser`, independent of draft template.
  - Missing parser: raises `Failure` with clear message.
  - Copy and LLM modes remain unchanged.
  - Optional test that metadata includes a deprecation warning when the draft template also has a parser defined.

## Rollout & Risks
- Rollout can be staged per above; minimal risk as behavior becomes more explicit.
- Risk: stale data if `parser` not populated in essay CSV → addressed by hard failure plus clear guidance.
- Future cleanup: consider renaming draft’s `parser` to `draft_phase_parser` to make semantics explicit.


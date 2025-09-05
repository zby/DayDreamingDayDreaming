# Draft-Coupled Parsing (Parser-First at Phase 1)

## Rationale
- The parser semantics belong to the draft template: it defines how to extract the essay-ready fragment from the raw LLM draft.
- Make `draft_response` return the already-parsed fragment that downstream essay generation consumes.
- Persist the raw LLM output separately for audit/debugging.

## Goals
- Single, clear ownership: parsing configured by `draft_templates.csv` only.
- `draft_response` output is the parsed fragment (essay-ready).
- Raw LLM draft stored under a separate location with versioning.
- Minimize surface changes to essay phase; keep two-phase architecture intact.

## Data Model
- `data/1_raw/draft_templates.csv`:
  - Keep column `parser` (required for templates that need parsing; optional if identity).
- Parsed output (current behavior, kept):
  - `data/3_generation/draft_responses/<draft_task_id>[_vN].txt` → parsed fragment (asset output via IO manager).
- Raw output (new):
  - `data/3_generation/draft_responses_raw/<draft_task_id>[_vN].txt` → raw LLM text (side-write, not the asset output).
  - Versioning strategy identical to parsed (monotonic suffix `_vN`).

## CSV Changes
- `data/1_raw/essay_templates.csv`:
  - Keep `generator` ∈ {`llm`, `copy`}.

## Code Changes
- `assets/groups/group_generation_draft.py`:
  - After calling the LLM, immediately persist RAW response to `draft_responses_raw/` with next version number.
  - Look up parser name from `draft_templates.csv` for the `draft_template` used by the task.
    - If `parser` is empty/absent: use identity (no-op) parser.
    - Otherwise, resolve function via `get_draft_parser(name)`; raise Dagster `Failure` if unknown.
  - Apply parser to RAW response → `parsed_text`.
  - Validate `parsed_text` non-empty; raise `Failure` if invalid.
  - Return `parsed_text` (this remains the asset’s materialized output).
  - Add metadata: `parser`, `raw_chars`, `parsed_chars`, `raw_path`, `version_used`.
- `utils/link_parsers.py`:
  - No change; remains the registry (`essay_idea_last`, `essay_block`, etc.).
- `utils/raw_readers.py`:
  - No change required; ensure `draft_templates.csv` loader preserves `parser` column.

## Essay Phase Implications
- `assets/groups/group_generation_essays.py`:
  - Default mode remains `llm` using the parsed draft as input.
  - Keep `generator=copy` as a first-class mode:
    - Behavior: return the parsed draft fragment (Phase‑1 output) unchanged (no LLM).
    - Metadata: include `mode=copy`, `source_draft_task_id`, `chars`, `lines`.
  - Deprecate `generator=parser` (redundant after parser-first). If encountered, warn and treat as `llm` (or hard-fail via a validation asset policy).

## Config & Tuning
- `experiment_config`:
  - Optional flags:
    - `save_raw_draft_enabled: bool = True` (toggle raw side-write).
    - `raw_draft_dir_override: Optional[str]` (default: `data/3_generation/draft_responses_raw`).
- IO Managers:
  - Keep existing IO manager behavior for parsed outputs; raw path is direct file write.

## Validation / Tests
- Unit tests:
  - Given a draft template with `parser=essay_idea_last`, `draft_response` returns the extracted idea and writes raw to `draft_responses_raw/`.
- Unknown parser → `Failure` with clear resolution.
- Identity parser behavior when `parser` empty.
- Integration tests:
  - Essay `generator=copy` returns the parsed fragment.
  - Essay `generator=llm` consumes parsed draft as before.
  - Backward compatibility: encountering `generator=parser` emits a warning and defaults to `llm` (or fails validation if policy prefers).

## Migration Plan
1) Ensure `draft_templates.csv` has correct `parser` for each draft template (set empty for identity).
2) Deploy code to write raw drafts and return parsed fragments.
3) Optionally add a cleanup job to migrate any essay-level `parser/copy` usages.
4) Communicate deprecation schedule for essay-level parsing.

## Risks & Mitigations
- Risk: Disk usage increase due to raw saves → allow opt-out via config.
- Risk: Some essay templates rely on unparsed drafts → identity parser path preserves behavior.
- Clear errors with metadata and path hints to speed up fixes.

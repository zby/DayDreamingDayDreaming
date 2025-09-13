# Plan: Unify *_prompt_asset Implementations

Purpose: Replace three stage-specific prompt asset entrypoints with a single parametrized function, mirroring `response_asset`, while keeping behavior and contracts predictable and testable.

## Goals

- Single `prompt_asset(context, stage, *, content_combinations=None)` entrypoint.
- Uniform membership resolution, mode detection, rendering, and metadata.
- Early copy-mode short-circuit for all stages using template `generator`.
- Keep tests green; only make explicit, documented contract changes.

## Current State (Summary)

- `essay_prompt_asset(context)`
  - require_membership_row(stage=essay, cols=[template_id, parent_gen_id])
  - resolve_generator_mode(kind=essay)
  - if mode=copy → return sentinel (already implemented)
  - load_parent draft parsed text; render essay template with values: `{draft_block, links_block}`; emit metadata

- `evaluation_prompt_asset(context)`
  - require_membership_row(stage=evaluation, cols=[template_id, parent_gen_id])
  - resolve_generator_mode(kind=evaluation); if copy → sentinel
  - load_parent essay parsed text; render evaluation template with values: `{response}`; emit metadata

- `draft_prompt_asset(context, content_combinations)`
  - require_membership_row(stage=draft)
  - resolve_generator_mode(kind=draft); if copy → sentinel
  - find combination by `combo_id` from `content_combinations`
  - render draft template with values: `{concepts}`; emit metadata

Common building blocks used:
- require_membership_row, resolve_generator_mode, render_template, load_generation_parsed_text (for essay/evaluation only).

## Proposed Unified API

```
def prompt_asset(context, stage: Literal["draft","essay","evaluation"], *, content_combinations: Optional[list]=None) -> str
```

- Resolve membership once with stage-specific required columns:
  - essay → [template_id, parent_gen_id]
  - evaluation → [template_id, parent_gen_id]
  - draft → [template_id] (+ `content_combinations` required at call site)
- Resolve mode once via `resolve_generator_mode(kind=stage, ...)`.
- Copy-mode short-circuit (all stages):
  - Return sentinel string: "COPY_MODE: no prompt needed" and emit metadata with `mode=copy`, `template_id`, and `parent_gen_id` when available.
- Template values provider:
  - draft → `{concepts: content_combination.contents}` (requires lookup by combo_id)
  - essay → `{draft_block: parent_text, links_block: parent_text}`
  - evaluation → `{response: parent_text}`
- Render via `render_template(stage, template_id, values)`; no file I/O here.
- Emit unified metadata (see below) and return prompt text.

## Unified Metadata (Prompt Assets)

- Always include:
  - `function`: f"{stage}_prompt"
  - `gen_id`
  - `template_id` (rename existing `essay_template`/`template_used` to `template_id`)
  - `mode`: "llm" or "copy"
- When parent stage exists:
  - `parent_gen_id`
  - `source_used` (e.g., "draft_gens_parent" or "essay_gens")
- Content lengths (keep for readability in UI):
  - `document_content_length` for parent_text (essay/evaluation)
  - `prompt_length` (chars) for rendered prompt

BACKCOMPAT: Retain existing keys in the short term (e.g., `template_used`, `essay_template`) but add `template_id` and mark the older keys for removal with TODO-REMOVE-BY tags in code comments once consumers migrate.

## Helper Unifications (No deeper recursion)

- Membership requirements map (shared dict):
  - `{ "essay": ["template_id","parent_gen_id"], "evaluation": ["template_id","parent_gen_id"], "draft": ["template_id"] }`
- Parent text loader (thin wrapper):
  - `load_parent_parsed_text(context, stage, gen_id)` already exists; we can use it to get `(parent_gen_id, parent_text)` for essay/evaluation.
- Metadata builder (new small util in assets/_helpers.py):
  - `build_prompt_metadata(stage, gen_id, template_id, *, parent_gen_id=None, mode, extras=None)`
  - Minimizes duplication and keeps Dagster UI consistent.

## Reasonable Contract Changes

- Remove min-lines enforcement from prompt assets (already done for essay); keep validations in response stage where raw output exists.
- Standardize return type for prompt assets to: string prompt or sentinel (never writes files; IO managers own persistence).
- Standardize metadata key `template_id` across stages (keep old keys in the short term with BACKCOMPAT tags).
- Consistent error metadata `function=f"{stage}_prompt"` + f-stringed details.

## Migration Steps

- Add `prompt_asset` + `build_prompt_metadata`.
- Delegate wrappers in assets/group_* to unified `prompt_asset`.
- Update metadata emission, keep BACKCOMPAT keys.
- Align tests and docs.

## Out of Scope (for now)

- Deeper refactors of content combination storage or discovery.
- Removing all BACKCOMPAT keys immediately.
- Changing IO manager behavior for prompts.


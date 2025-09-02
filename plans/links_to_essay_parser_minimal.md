# Links→Essay Parser via Essay Phase (Minimal Change)

Purpose

- Turn outputs from the new links template (`deliberate-rolling-thread-v1`) into essay documents without calling an essay LLM.
- Keep changes minimal: use metadata to select a parser, produce essays into the existing `essay_responses/`, and let current evaluation read them.

Summary

- Add a lightweight essay template entry (`parsed-from-links-v1`) to `data/1_raw/essay_templates.csv` so the existing `essay_generation_tasks` produces partitions for this mode.
- Reuse the existing essay phase: modify `essay_response` to support a parser mode that reads the links response, extracts the final idea, and writes an essay file to `essay_responses/` for evaluation.
- Do not alter evaluation logic; it will pick up the essay files via `document_index`/`evaluation_tasks` as usual.

Schema Changes (Minimal)

- `data/1_raw/essay_templates.csv`
  - New column: `generator` (string; values: `llm` | `parser`; default `llm` if missing)
  - Add a row: `parsed-from-links-v1,Parsed From Links (Final Idea),Parse final <essay-idea> from links output; no LLM,true,parser`
  - Optionally set other essay templates to `active=false` to route all work through the parser.

Parser: `essay_idea_last`

- Input: links response authored with `deliberate-rolling-thread-v1` (tags on their own lines).
- Contract guaranteed by template:
  - Multiple `<essay-idea stage="i">` blocks; one per step, opening/closing tags on their own lines.
  - A `<thinking>` block per step, which the parser ignores.
 - Selection:
   - Choose parser by link template name; current mapping: `deliberate-rolling-thread-v1` → `essay_idea_last`.
- Algorithm:
  - Regex: find all blocks matching `<essay-idea(?:\s+[^>]*)?>\n([\s\S]*?)\n</essay-idea>` (DOTALL, multiline).
  - Extract `stage` attribute when present: `stage="(\d+)"`.
  - Choose the block with the highest numeric stage; if attributes missing, pick the last in order.
  - Return inner content verbatim (no trimming beyond removing the trailing newline from the capture group boundary).
- Failure handling:
  - If zero matches: fail the partition with a helpful message and example tags.
  - If malformed tags (e.g., nested or multiple opens without close): fail with guidance to re‑generate.

Essay Phase Behavior (Parser Mode)

- Asset: reuse `generation_essays/essay_response` with a conditional branch.
- Branching rule per partition:
  1) Lookup `essay_generation_tasks` row; inspect `essay_template` and its `generator` field.
  2) If `generator == 'parser'` (e.g., `parsed-from-links-v1`):
     - Load `links_response` for the FK `link_task_id` via `links_response_io_manager`.
     - Select parser by `link_template` value (mapping in code; supports `deliberate-rolling-thread-v1` → `essay_idea_last`).
     - Apply the parser; on missing/unsupported mapping, fail with guidance.
     - Write parsed text to `essay_response_io_manager` under the same `essay_task_id`.
     - Emit metadata: `mode=parser`, `parser=name`, `source_link_task_id`, `chars`, `lines`.
  3) Else (`generator == 'llm'` or column missing): keep current behavior (LLM generation).
  4) Optional: in `essay_prompt`, if `generator == 'parser'`, return a minimal placeholder or skip heavy work.

Evaluation Wiring (unchanged)

- `evaluation_tasks` already includes essay documents from `essay_responses/`; the parsed outputs will appear under stage `essay2p` and be evaluated normally.
- No changes to `evaluation_prompt`/`evaluation_response` needed.

Minimal Code Touch Points

- Data: add `generator` column in essay templates; add `parsed-from-links-v1` row.
- essay_response: add branch for `generator == 'parser'` with parse/write path; select parser by `link_template` mapping.
- essay_prompt: optionally return a trivial string for parser mode to avoid wasted rendering.
- Definitions: no new assets; keep registrations unchanged.

Runbook

1) Data
   - Add `parsed-from-links-v1` to `data/1_raw/essay_templates.csv` with `generator=parser` and set it active; set other essay templates inactive for this experiment.
2) Code
   - Modify `essay_response` to support parser mode as above; optionally simplify `essay_prompt` for parser mode.
3) Execute
   - Materialize `group:task_definitions`.
   - Materialize `group:generation_links` to produce links responses.
   - Materialize `group:generation_essays` (the existing essay assets; parser mode will activate automatically for the parsed template).
   - Materialize evaluation assets as usual.
4) Validate
   - Inspect 3–5 parsed essays; confirm they equal the last `<essay-idea>` block.
   - Confirm `evaluation_tasks` includes these essays and evaluation completes.

Future Extensions

- Add parser unit tests (fixtures with well‑formed, missing, and malformed tags).
- Generalize the parser registry to more templates and extraction modes.
- Optionally emit a JSON sidecar with all `<essay-idea>` stages for analysis.
- Add a guard in `evaluation_tasks` to skip docs with non‑existent files to reduce noise.
 
Testing Plan

- Unit tests: `tests/test_essay_idea_last_parser.py`
  - Picks highest stage when multiple `<essay-idea stage="…">` blocks exist.
  - Falls back to the last occurrence when stage attributes are absent.
  - Preserves inner newlines verbatim inside the selected block.
  - Raises a clear error when no `<essay-idea>` blocks are present.
- Asset behavior tests: `tests/test_essay_response_parser_mode.py`
  - In parser mode (generator=parser), loads links_response, selects the correct parser by link_template, writes the parsed essay to `essay_responses/<essay_task_id>.txt`, and emits informative metadata.
  - Fails with a descriptive error when link_template has no parser mapping.
  - Both files are initially `pytest.skip`-guarded until implementation is added.

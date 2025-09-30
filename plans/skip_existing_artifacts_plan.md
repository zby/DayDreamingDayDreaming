# Skip Existing Artifacts Implementation Plan

## Goal
Introduce stage-level guards that reuse previously materialized artifacts by default. If `raw.txt` (or the stage-specific output) already exists, the Dagster asset returns the stored data instead of invoking the LLM or parser again. A `force` flag will allow explicit regeneration when needed. This unifies the current ad-hoc skip behaviour and lets us retire the complex `mode:` directives in `data/2_tasks/selected_essays.txt`.

## Scope & Principles
- Apply the skip rule to every stage:
  - **Raw phase**: skip when `raw.txt` exists (and optionally warn if `raw_metadata.json` is missing).
  - **Parsed phase**: skip when `parsed.txt` exists.
  - **Input stage** (if any future stage writes inputs) can follow the same pattern.
- Replication counts remain the mechanism for producing additional variants; increasing the replicate index (e.g. drafting replicate 2) bypasses the skip because the file won’t exist.
- Provide an explicit `force` override (per-stage setting or run config) that ensures regeneration when required.
- Emit metadata indicating whether a stage reused an existing artifact.

## Task Breakdown

1. **Data Layer Enhancements**
   - Add small helpers in `GensDataLayer` (or `Paths`) to test for existing raw/parsed files.
   - Optionally expose readers that return both text and metadata for reuse.

2. **Stage Guard Implementation**
   - Update `_stage_raw_asset` to:
     - Check `raw.txt` before generating.
     - When skipping, read the file, emit metadata (with `reused: true` flag), and return.
     - Honour a `force` boolean from stage settings/run config.
   - Apply the same pattern to the parsed stage helper (`_stage_parsed_asset`) using `parsed.txt`.
   - Ensure downstream artifacts (metadata writes) are no-ops when skipping, apart from optional warnings if metadata files are absent.

3. **Configuration & Overrides**
   - Extend `StageSettings` / experiment config schema with an optional `force` flag per stage.
   - Allow forcing via dagster run config (e.g. `run_config['ops']['evaluation_raw']['config']['force'] = true`).
   - Document default behaviour (reuse) and override semantics.

4. **Metadata & Logging**
   - Add a `reused` boolean in Dagster output metadata to surface skip decisions.
   - Log a warning if `raw_metadata.json` is missing when `raw.txt` is reused (informational only).

5. **Cohort Simplification**
   - With skip guards in place, review curated mode handling in `group_cohorts.py`:
     - Remove `mode:` directives that exist solely to avoid re-running evaluations.
     - Keep replication allocator unchanged (it still ensures new replicates find an unused index).
   - Update docs describing `selected_essays.txt` to emphasise replication counts over mode flags.

6. **Testing**
   - Unit tests for `_stage_raw_asset` and `_stage_parsed_asset` verifying skip behaviour (create a temp gens directory with existing files, ensure the LLM/parse functions are not called).
   - Integration tests (Dagster) for running the same partition twice: first run generates, second run reuses without hitting the LLM client mock.
   - Tests for `force=true` to ensure regeneration occurs and metadata marks `reused: false`.

7. **Documentation & Cleanup**
   - Update operator docs explaining the new default reuse behaviour.
   - Note that replication counts control additional variants; instruct users to bump replicate numbers for deliberate refreshes.
   - Remove obsolete documentation around `mode: evaluation-only` and `# skip-existing-evaluations` once behaviour is unified.

## Resolved Design Decisions

### Skip when metadata is missing?
**Decision: Yes, skip with a warning.**

- The primary artifact is the text file (`raw.txt`, `parsed.txt`) - that's what downstream stages consume
- Metadata is supplementary (timestamps, model info, tokens used)
- Forcing regeneration just because metadata is missing wastes LLM calls/time
- Warning provides visibility without blocking workflow
- Aligns with "simpler code" principle - one file determines skip behavior
- **Exception:** If future work makes metadata semantically required (e.g., prompt version validation), revisit this decision

### Extend guard to parsed metadata and other derivatives?
**Decision: Not initially - handle on-demand.**

- YAGNI principle: add guards when you have a concrete use case
- Current pain point is raw/parsed text regeneration (the expensive operations)
- Other derivatives are typically fast to regenerate or don't exist yet
- Keeps first pass focused and testable
- **Guideline for future:** If adding a guard for derivative X, follow the same contract: check file existence, emit `reused: true/false` metadata, honor `force` flag

## Deliverables
- Updated stage helpers with skip logic and `force` support.
- Simplified curated cohort logic and documentation.
- Tests covering reuse and forced regeneration.

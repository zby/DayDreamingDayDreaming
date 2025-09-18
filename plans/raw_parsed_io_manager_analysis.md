# Analysis: Adding IO Managers for Raw and Parsed Assets

## Context
- Prompt assets already rely on `GensPromptIOManager` (now backed by `GensDataLayer`).
- Raw/parsed assets currently write via `GensDataLayer` inside the asset bodies so they can persist `raw.txt`, `raw_metadata.json`, and `parsed.txt`, `parsed_metadata.json` immediately—even when validation fails.
- Early writes are important: truncation/min-lines errors must leave `raw.txt` and `raw_metadata.json` on disk for debugging.

## Proposal under review
- Introduce IO managers for `raw` and `parsed` assets (draft/essay/evaluation) that delegate persistence to `GensDataLayer`.
- When generation fails validation, capture the data in a dedicated log file (e.g., `raw_failures.log`) rather than relying on early writes.

## Requirements to preserve/expose
1. **Early persistence for failure analysis**
   - Need a mechanism equivalent to current pre-validation writes.
   - IO manager approach must either:
     - Receive “partial” results prior to raising, or
     - Embrace a side-channel (log file) to store truncated outputs.
2. **Metadata fidelity**
   - `raw_metadata.json` and `parsed_metadata.json` must include fields produced today (finish reason, truncated flag, parser name, run_id, etc.).
3. **Deterministic layout**
   - IO managers must resolve paths through `GensDataLayer` to avoid duplicating path logic.
4. **Failure semantics**
   - Asset should still raise on truncation/min-lines violations; IO manager has to persist artifacts even when asset returns None (or fails).

## Feasibility considerations
- Dagster IO managers only execute after an asset returns successfully; they cannot run on exceptions. Without asset-level side effects we’d lose raw artifacts on failure.
- To work around this, the asset could:
  - Persist via IO manager-compatible object (e.g., `RawGenerationResult` dataclass) when success, **and**
  - Write a failure log (or call `GensDataLayer`) directly before raising for truncation.
  - Net result: still need direct data-layer write path in the asset for error scenarios.
- With both IO manager + manual failure write, complexity increases without deleting much code.

## Migration risks
- Deleting current `data_layer.write_raw/parsed` calls would remove diagnostic artifacts unless the failure logging is bulletproof.
- Introducing nine new IO managers (draft/essay/eval × raw/parsed) duplicates configuration unless we invest in a factory (similar to prompt IO manager). Still need to propagate new `io_manager_key` usage across assets and tests.
- Tests assume early-write behaviour (see `tests/integration/test_33_draft_min_lines_failure.py` etc.); migrating to IO managers requires rewriting or stubbing Dagster context to ensure logs appear.

## Alternative approach
- Keep assets writing through `GensDataLayer`, but encapsulate shared logic in helper functions to avoid duplication.
- If logs are desired, add optional logging within `stage_raw_asset` on failure paths without altering success path.

## Recommendation
- Adding IO managers for raw/parsed does not materially simplify code because we still need asset-level side effects for failure cases.
- Risk of losing diagnostic artifacts outweighs the benefit unless Dagster introduces post-failure IO hooks.
- Recommend **not** implementing raw/parsed IO managers at this time. Instead:
  - Refactor shared data-layer writes into helper utilities if readability is a concern.
  - Add a failure log (e.g., append to `data/gens/<stage>/<gen_id>/raw_failure.log`) inside existing try/except blocks without altering IO boundaries.

## Outstanding questions
- Would we accept delayed persistence (no files when failure occurs) if logs were sufficient? (Current operators depend on raw files for manual debugging.)
- Are there performance bottlenecks with the current approach that IO managers would solve? (None identified.)

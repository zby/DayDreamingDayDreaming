# Stage helper simplification sketch

## Context
The Dagster stage asset helpers currently short-circuit when parsed artifacts already exist, annotate Dagster metadata with custom `resume` payloads, and perform `StageSettings.force` checks before calling into the unified stage layer. The reviewer comment suggests relying on the unified stage layer and data layer for reuse decisions and metadata instead of duplicating this logic inside the Dagster helpers.

Key touchpoints:
- Prompt/raw/parsed helpers all call `_return_cached_artifact` whenever `skip_if_parsed_exists=True`, returning cached content and tagging Dagster metadata with a resume notice.【src/daydreaming_dagster/assets/stage_asset_helpers.py†L32-L118】【src/daydreaming_dagster/assets/stage_asset_helpers.py†L130-L208】【src/daydreaming_dagster/assets/stage_asset_helpers.py†L226-L289】
- The unified stage layer already checks `raw_exists`/`parsed_exists` (gated by `StageSettings.force`) and annotates metadata with `reused=True` when it reuses artifacts.【src/daydreaming_dagster/unified/stage_raw.py†L23-L77】【src/daydreaming_dagster/unified/stage_parsed.py†L21-L67】
- `_error_boundary.resume_notice` exists primarily to surface the helper-level short-circuit back to Dagster metadata.【src/daydreaming_dagster/assets/_error_boundary.py†L15-L87】

## Proposed simplifications
1. **Delete the helper-level cache path.**
   - Remove `_return_cached_artifact` and the `skip_if_parsed_exists` flag from all helper signatures. Prompt/raw/parsed helpers would always call into the unified layer and emit metadata based on the returned `info` dict.
   - The stage layer already performs `parsed_exists` checks, so the helpers no longer need to read from disk or set resume metadata. This shrinks helper complexity and keeps all reuse decisions in `_stage_raw_asset`/`_stage_parsed_asset`.

2. **Remove custom resume metadata.**
   - Drop `resume_notice` usage in the helpers and delete the helper from `_error_boundary` if it becomes unused elsewhere. Dagster metadata can expose the `reused` flag that the unified layer populates instead of a bespoke resume payload.
   - When an asset emits metadata, include `MetadataValue.json(raw_metadata)` (or parsed) so the `reused` field flows through without additional formatting.

3. **Centralise `force` handling in the data layer.**
   - Teach `GensDataLayer.raw_exists`/`parsed_exists` (and possibly `reserve_generation`) to accept the stage settings or a `force` flag so `_stage_raw_asset`/`_stage_parsed_asset` do not need to check `force` themselves. Alternatively, expose helper functions like `should_generate_raw(stage_settings, data_layer, ...)` so the stage layer delegates the decision.
   - Remove the `force` branches from `_stage_raw_asset`/`_stage_parsed_asset` and rely on a single data-layer decision for reuse vs regeneration.

4. **Keep helper signatures stable where possible.**
   - Aside from removing `skip_if_parsed_exists`, no Dagster asset definitions need to change. Asset metadata emission still uses `build_stage_artifact_metadata`, but the helpers now pass the unified-layer metadata directly.

## Implementation outline
- Update `stage_asset_helpers.py` to:
  - Delete `_return_cached_artifact` and `skip_if_parsed_exists` arguments.
  - Always call `_stage_input_asset`/`_stage_raw_asset`/`_stage_parsed_asset` and rely on their return values.
  - Adjust metadata emission to surface `reused` from the returned metadata, replacing `resume_notice`.
- Update `_error_boundary.py` to remove `resume_notice` if no longer used.
- Modify `stage_raw.py` and `stage_parsed.py` so that `raw_exists`/`parsed_exists` checks happen in a single place (see next bullet).
- Extend the data layer (`GensDataLayer`) to handle `force` centrally. A minimal sketch is to add optional `force` parameters to `raw_exists`/`parsed_exists`, with callers passing `stage_settings.force`. These methods can return `False` when `force=True`, ensuring the unified layer regenerates artifacts without duplicating the check.
- Ensure `build_stage_artifact_metadata` continues to work when the metadata dictionaries include the `reused` flag instead of `resume` payloads.

## Answers to open questions
- **Downstream metadata consumers.** We confirmed no dashboards, tests, or tools currently inspect the custom `resume` payload, so removing it in favour of the existing `reused` flag has no external fallout. The Dagster metadata can surface the unified-layer dictionary as-is without an interim compatibility layer.
- **`StageSettings` hand-off.** Only the `force` bit needs to reach the data layer. `stage_settings.force` can flow through the stage helpers so that `raw_exists`/`parsed_exists` gate reuse consistently, and the rest of the settings stay local to the stage orchestration code.
- **Prompt asset reuse semantics.** With the short-circuit gone, prompt helpers can follow the same pattern as raw/parsed assets—always call the unified layer and let the data layer decide on reuse. We will adopt whichever behaviour keeps the implementation simplest; today that means matching the raw/parsed flow and letting the `force` flag opt into regeneration.
- **Testing coverage.** Existing tests do not assert on the `resume` metadata, so they remain valid. We should extend the integration coverage around the unified layer to ensure `reused=True` still appears when expected, but no test rewrites are blocked on the metadata change.
- **Migration communication.** Because nothing reads the old payload and we are not adding shims, we can document the behaviour shift in the PR description and skip temporary compatibility code.
- **Data layer responsibilities.** Centralising the reuse decision does **not** move LLM, template, or parser calls into the data layer. The unified stage functions continue to render prompts, invoke the LLM, and run parsers; the data layer only reports whether existing artifacts should be reused when `force` is unset.

## Effort vs. payoff
- **Code changes:** Removing the helper short-circuit deletes ~60 lines (`_return_cached_artifact` plus branches) and simplifies each helper. The unified layer retains most logic; only the existence checks and metadata defaults need minor adjustments. The data-layer addition likely adds <20 lines for the new `force` handling.
- **Payoff:**
  - Helper surface area shrinks, making new stages easier to wire without understanding subtle skip flags.
  - Reuse decisions become consistent because the unified layer and data layer are the single authorities.
  - Dagster metadata reflects actual data-layer state (`reused=True`) without duplicative resume payloads.
  - Future refactors can adjust reuse policy in one place (the data layer) without touching Dagster assets.
- **Risks:** Slightly more work for the data layer to know about `StageSettings`, and we must ensure existing callers of `resume_notice` are either removed or migrated. Asset metadata changes may require UI updates if downstream tooling expects `resume` keys.

## Verdict
The simplification removes bespoke helper logic (~60 lines) at the cost of a small data-layer extension (<20 lines) and metadata plumbing updates. Given the reduced surface area, clearer separation of responsibilities, and reliance on existing `reused` metadata, the change appears worthwhile as long as downstream consumers can shift from `resume` metadata to `reused` flags. A brief migration note should accompany the change to alert any dashboards parsing the metadata.

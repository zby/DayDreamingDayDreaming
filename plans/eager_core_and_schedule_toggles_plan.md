**Purpose**
- Capture a small, optional enhancement set we may adopt later to streamline “pull”-style updates without automating any LLM-costing assets.
- The changes are currently not applied (we reverted them), but this plan explains what they are for and how to enable them if/when needed.

**Context**
- We want pull semantics: when downstream is materialized, any stale upstream is rebuilt. We do not want to auto-run assets that make LLM calls. Core/task assets can be automated safely.
- Our schedule already detects CSV changes and rematerializes task_definitions; CSVs are modeled as AssetSpec sources and core assets declare deps to inherit staleness.

**Proposed Enhancements (Deferred)**
- Core/task assets eager automation:
  - Add `automation_condition=AutomationCondition.eager()` to these assets only:
    - `content_combinations`, `content_combinations_csv`
    - `link_generation_tasks`, `essay_generation_tasks`, `evaluation_tasks`
  - Do not add automation to any LLM assets (`links_prompt`, `links_response`, `essay_prompt`, `essay_response`, `evaluation_prompt`, `evaluation_response`). These stay manual.
  - Result: when CSVs change (or are otherwise stale), the daemon may auto-materialize the core/task layer; LLM assets still require explicit runs. If users kick off downstream runs, Dagster will pull in stale core/task upstream as needed.

- Schedule/ops safety toggles via environment:
  - `RAW_SCHEDULE_ENABLED` (default: true): gate the registration of the schedule in `Definitions`. Useful for CI or ad-hoc runs without the daemon.
  - `RAW_SCAN_COOLDOWN_SEC` (default: 0): ignore files modified within N seconds to avoid scanning half-written files; applied in the schedule’s scan step.
  - `RAW_STATE_DIR` (default: `${DAGSTER_HOME}/state`): optional override for where we persist the fingerprint JSON.
  - Optional improvement: pending→last state promotion (write “pending” on tick; promote to “last” after successful run) to avoid skipping retries if a run fails. Keep current “write last immediately” if simplicity is preferred.

**Non‑Goals**
- No automatic materialization of LLM assets.
- No source observations or reconciliation sensors right now (we rely on schedule + eager core for pull semantics).
- No partition or per-file observations beyond CSV level.

**Effects**
- “Pull” works as intended: materializing downstream will rebuild stale core/task upstream; LLM steps remain manual and under user control.
- The daemon can keep the task CSVs fresh after CSV edits, reducing manual steps without incurring API costs.

**Implementation Sketch**
- Core assets: add `automation_condition=AutomationCondition.eager()` to the five task_definitions assets listed above.
- Schedule: wrap registration behind `RAW_SCHEDULE_ENABLED`; add cooldown filter in the scan; allow `RAW_STATE_DIR` override in the state util; keep current CSV-only scanning and selection (materialize `group:task_definitions`).

**Testing**
- Verify that changing a CSV triggers the schedule and updates only task_definitions artifacts.
- Confirm that LLM assets do not auto-run; downstream runs pull in stale core/task assets when needed.
- Toggle env vars to ensure enabling/disabling the schedule and cooldown work as expected.

**Rollout**
- Keep behind env toggles and defer merging until we feel the extra polish is helpful (e.g., to reduce manual clicks during development).
- Document the toggles (`RAW_SCHEDULE_ENABLED`, `RAW_SCAN_COOLDOWN_SEC`, `RAW_STATE_DIR`) and the scope (core only; LLM manual).


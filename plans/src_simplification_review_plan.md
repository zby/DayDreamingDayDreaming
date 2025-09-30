# src Simplification Review Plan

Goal: produce a prioritized list of refactor recommendations that simplify `src/daydreaming_dagster` without altering published contracts. Record stretch ideas that require breaking APIs/data layouts when they unlock substantial simplification. Maintain a running catalog of confusing constructs (e.g., mixing OO state with `Callable` fields) and store notes in `plans/notes/confusing_constructs.md` to guide readability fixes.

## 0. Scope & Constraints
- Include every module under `src/daydreaming_dagster/`; skip `scripts/` for now.
- Preserve guarantees from `tools/refactor_contract.yaml`, error codes/types, and file schemas.
- Review outcome: recommendations + supporting observations, not code changes.

## 1. Preparation Pass
- Read `AGENTS.md`, `tools/refactor_contract.yaml`, and module-level READMEs to confirm expectations.
- Baseline metrics: `radon cc`, `sloccount`, and current pytest status (unit focus). Capture numbers for later comparison.
  - Commands: `uv run radon cc src/daydreaming_dagster -s --total-average`, `uv run sloccount src/daydreaming_dagster`, `.venv/bin/pytest src/daydreaming_dagster -q` (or `uv run pytest src/daydreaming_dagster -q`).
- Inventory existing colocated tests inside each module tree; note any missing coverage hotspots.

## 2. Review Batches (load-limited)
Process each batch independently; document findings before moving on to keep context snapshots small.

### Batch A — Orchestration Shell
Targets: `definitions.py`, `config/`, `jobs/`, `schedules/`, `checks/`.
Focus points:
- Entry-point wiring clarity; ensure assets/resources registration is minimal.
- Detect duplicated selection logic, unused schedules, or config indirection.
- Note opportunities to collapse configuration layers or rename ambiguous identifiers.
Deliverable: list of simplification candidates (per file) + test impacts.

### Batch B — Assets & Resources Boundary
Targets: `assets/`, `resources/`.
Focus points:
- Asset/resource signatures vs. Dagster IO manager expectations.
- Repeated try/except ladders; confirm only boundary layers format errors.
- Confirm any error handling that touches `src/daydreaming_dagster/utils/errors.py` stays aligned with the `DDError` contract (codes + minimal ctx).
- Opportunities to inject dependencies instead of global lookups.
Deliverable: recommendations for asset/resource consolidation and clearer dependency flow; flag bold refactors (contract-breaking) separately with migration thoughts. Log any confusing constructs encountered (e.g., callable-typed fields in stateful objects). Review colocated tests alongside code to identify opportunities to simplify or rebalance coverage.

### Batch C — Data Layer Foundations
Targets: `data_layer/` (paths, readers, writers, adapters).
Focus points:
- Path helpers vs. direct string concatenations; enforce `Paths` usage.
- Serialization/deserialization: ensure structured errors, remove incidental logging.
- Highlight IO primitives that should be pure or memoized.
Deliverable: prioritized list for data layer cleanup and validation strategy; note any high-impact schema rewrites even if they would break consumers. Capture confusing patterns such as hybrid functional/object APIs. Assess paired tests for duplication or complexity and note simplification ideas.

### Batch D — Domain Models & Types
Targets: `models/`, `types.py`, `unified/`.
Focus points:
- Validate dataclass/TypedDict usage; merge redundant schema definitions.
- Check alignment between models and downstream validators/parsers.
- Flag complicated transformations that could be simplified or split.
Deliverable: model refactor ideas and notes on schema interactions; capture disruptive schema/API simplifications with clear risk callouts. Document confusing constructs (e.g., Callable-typed dataclass fields) for later cleanup. Evaluate colocated tests for readability and maintenance burdens.

### Batch E — Utilities & Constants
Targets: `utils/`, `constants.py`.
Focus points:
- Identify utilities that should move closer to callers or be deleted.
- Ensure error helpers follow the structured error pattern.
- Double-check helpers interacting with `DDError` maintain consistent codes/types and avoid incidental message formatting.
- Look for reusable pieces that can replace ad-hoc logic uncovered in earlier batches.
Deliverable: shortlist of utility consolidations and deprecations; include bold removals that would require contract updates. Record confusing helper patterns that impede comprehension. Include test simplification recommendations where helpers have redundant coverage.

## 3. Cross-Cutting Analysis
- Map error-code usage across batches; confirm no incidental message coupling remains.
- Aggregate complexity hotspots (functions/classes) for potential extraction or deletion.
- Note testing gaps that affect multiple batches and propose targeted unit/integration tests.
- Identify logging boundaries and recommend a single formatting layer where missing.

## 4. Synthesize Recommendations
- Merge batch findings into a single ordered list (highest leverage first).
- For each recommendation: include summary, impacted files, risk rating, and test needs.
- Separate sections for contract-safe recommendations vs. bold breakages (with justification and migration outline).
- Aggregate the confusing-construct catalog into a dedicated appendix with suggested remediation strategies.
- Record any `BACKCOMPAT` requirements or open questions for stakeholders.

## 5. Handoff Package
- Produce final review report (markdown) summarizing recommendations + metrics deltas.
- Store supporting notes per batch (optional) in `plans/notes/` for traceability.
- Call out quick wins vs. deep refactors so implementation can be parallelized.
- Document any intentional drops in incidental behavior in `REFactor_NOTES.md`, linked from the report when relevant.

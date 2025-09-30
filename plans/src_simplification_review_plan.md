# src Simplification Review Plan

Goal: produce a prioritized list of refactor recommendations that simplify `src/daydreaming_dagster` without altering published contracts. Record stretch ideas that require breaking APIs/data layouts when they unlock substantial simplification.

## 0. Scope & Constraints
- Include every module under `src/daydreaming_dagster/`; skip `scripts/` for now.
- Preserve guarantees from `tools/refactor_contract.yaml`, error codes/types, and file schemas.
- Review outcome: recommendations + supporting observations, not code changes.

## 1. Preparation Pass
- Read `AGENTS.md`, `tools/refactor_contract.yaml`, and module-level READMEs to confirm expectations.
- Baseline metrics (store outputs under `reports/`):
  - `uv run ruff check src` (lint snapshot)
  - `uv run radon cc -s -o SCORE src/daydreaming_dagster > reports/radon_cc.txt`
  - `uv run cloc src/daydreaming_dagster > reports/cloc_src.txt`
  - `.venv/bin/pytest src/daydreaming_dagster -q --durations=15 > reports/pytest_unit.txt`
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
- Opportunities to inject dependencies instead of global lookups.
Deliverable: recommendations for asset/resource consolidation and clearer dependency flow; flag bold refactors (contract-breaking) separately with migration thoughts.

### Batch C — Data Layer Foundations
Targets: `data_layer/` (paths, readers, writers, adapters).
Focus points:
- Path helpers vs. direct string concatenations; enforce `Paths` usage.
- Serialization/deserialization: ensure structured errors, remove incidental logging.
- Highlight IO primitives that should be pure or memoized.
Deliverable: prioritized list for data layer cleanup and validation strategy; note any high-impact schema rewrites even if they would break consumers.

### Batch D — Domain Models & Types
Targets: `models/`, `types.py`, `unified/`.
Focus points:
- Validate dataclass/TypedDict usage; merge redundant schema definitions.
- Check alignment between models and downstream validators/parsers.
- Flag complicated transformations that could be simplified or split.
Deliverable: model refactor ideas and notes on schema interactions; capture disruptive schema/API simplifications with clear risk callouts.

### Batch E — Utilities & Constants
Targets: `utils/`, `constants.py`.
Focus points:
- Identify utilities that should move closer to callers or be deleted.
- Ensure error helpers follow the structured error pattern.
- Look for reusable pieces that can replace ad-hoc logic uncovered in earlier batches.
Deliverable: shortlist of utility consolidations and deprecations; include bold removals that would require contract updates.

## 3. Cross-Cutting Analysis
- Map error-code usage across batches; confirm no incidental message coupling remains.
- Aggregate complexity hotspots (functions/classes) for potential extraction or deletion.
- Note testing gaps that affect multiple batches and propose targeted unit/integration tests.
- Identify logging boundaries and recommend a single formatting layer where missing.
- Highlight deterministic-ID touchpoints to ensure new recommendations stay inside the simplification contract.

## 4. Synthesize Recommendations
- Merge batch findings into a single ordered list (highest leverage first).
- For each recommendation: include summary, impacted files, risk rating, and test needs.
- Separate sections for contract-safe recommendations vs. bold breakages (with justification and migration outline).
- Record any `BACKCOMPAT` requirements or open questions for stakeholders.
- Link back to supporting metrics (`reports/`) and existing plans (e.g., `cohort_membership_refactor_plan.md`).

## 5. Handoff Package
- Produce final review report (markdown) summarizing recommendations + metrics deltas.
- Store supporting notes per batch (optional) in `plans/notes/` for traceability.
- Call out quick wins vs. deep refactors so implementation can be parallelized.

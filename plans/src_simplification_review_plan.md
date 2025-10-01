# src Simplification Review Plan

Goal: produce a prioritized list of refactor recommendations that simplify `src/daydreaming_dagster` without altering published contracts. Record stretch ideas that require breaking APIs/data layouts when they unlock substantial simplification.

## 0. Scope & Constraints
- Cover every module under `src/daydreaming_dagster/` (assets, data_layer, unified, utils, etc.); defer `scripts/` unless a recommendation needs confirmation of downstream usage.
- Preserve guarantees from `tools/refactor_contract.yaml`, error codes/types, and file schemas produced by the Dagster assets (e.g., cohort membership CSVs, normalized evaluation scores CSVs).
- Review outcome: prioritized recommendations + supporting observations, not code edits.

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
Targets: `assets/`, `resources/`, shared asset helpers (`assets/stage_asset_helpers.py`, `assets/_error_boundary.py`).
Focus points:
- Asset/resource signatures vs. Dagster IO manager expectations.
- Repeated try/except ladders; confirm only boundary layers format errors.
- Opportunities to inject dependencies instead of global lookups; confirm skip/resume metadata is centralized via `resume_notice`.
Deliverable: recommendations for asset/resource consolidation and clearer dependency flow; flag bold refactors (contract-breaking) separately with migration thoughts.

### Batch C — Data Layer Foundations
Targets: `data_layer/` (paths, gens data layer, evaluation_scores, parsers).
Focus points:
- Path helpers vs. direct string concatenations; enforce `Paths` usage.
- Serialization/deserialization: ensure structured errors, remove incidental logging; validate new normalized evaluation score tables and any other cohort outputs.
- Highlight IO primitives that should be pure or memoized; check for duplication with `GensDataLayer` now that legacy utils are gone.
Deliverable: prioritized list for data layer cleanup and validation strategy; note any high-impact schema rewrites (e.g., alternate normalization formats) even if they would break consumers.

### Batch D — Domain Models & Unified Stage Helpers
Targets: `models/`, `types.py`, `unified/` (stage input/raw/parsed, parser registry).
Focus points:
- Validate dataclass/TypedDict usage; merge redundant schema definitions.
- Check alignment between models and downstream validators/parsers.
- Flag complicated transformations that could be simplified or split.
Deliverable: model refactor ideas and notes on schema interactions; capture disruptive schema/API simplifications with clear risk callouts.

### Batch E — Utilities & Cross-Cutting Helpers
Targets: `utils/` (evaluation_scores, evaluation_processing, ids, errors, raw readers).
Focus points:
- Identify utilities that should move closer to callers or be deleted (e.g., legacy shims now covered by data_layer).
- Ensure error helpers follow the structured error pattern.
- Look for reusable pieces that can replace ad-hoc logic uncovered in earlier batches; confirm evaluation ranking still separates aggregation from presentation.
Deliverable: shortlist of utility consolidations and deprecations; include bold removals that would require contract updates.

## 3. Cross-Cutting Analysis
- Map error-code usage across batches; confirm no incidental message coupling remains and that `Err` codes surface through the new data-layer helpers.
- Aggregate complexity hotspots (functions/classes) for potential extraction or deletion; include updated Radon snapshots (e.g., `seed_cohort_metadata`, `_prepare_curated_entries`).
- Note testing gaps that affect multiple batches and propose targeted unit/integration tests (e.g., normalized evaluation scores round trip).
- Identify logging/resume boundaries and recommend a single formatting layer where missing.
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

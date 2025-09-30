# src Simplification Review Plan

## Purpose
- Produce a prioritized list of refactor recommendations that simplify `src/daydreaming_dagster` while preserving published contracts, file schemas, and error codes/types.
- Capture "stretch" ideas that would require contract updates when they unlock meaningful simplification so they can be evaluated separately.
- Maintain a running catalog of confusing constructs (e.g., stateful objects with `Callable` fields) to steer future readability improvements.

## Scope & Guardrails
- Examine every module under `src/daydreaming_dagster/`; exclude `scripts/` unless a dependency chain demands it.
- Honor the constraints in `tools/refactor_contract.yaml`; do not propose changes that alter error codes/types or on-disk data shapes without flagging them as contract-breaking.
- Outcomes are recommendations, baseline metrics, and supporting notes—no code edits.

## Working Rhythm
1. Stay within the testing loop: read colocated unit tests first, keep potential regressions in mind, and log missing coverage candidates.
2. Treat each review batch as an independent pass and snapshot notes immediately before switching areas.
3. Prefer deleting incidental behavior over preserving it; focus on structured errors, dependency injection, and minimal boundaries.

## Preparation Pass
- Read `AGENTS.md`, `CLAUDE.md`, `tools/refactor_contract.yaml`, and any module-level READMEs.
- Capture baseline metrics (store raw outputs in `plans/notes/metrics_baseline.md`):
  - `uv run radon cc src/daydreaming_dagster -s --total-average`
  - `uv run sloccount src/daydreaming_dagster`
  - `.venv/bin/pytest src/daydreaming_dagster -q` (narrower `-k` when needed)
- Inventory colocated unit tests for each subpackage; note high-risk gaps.

## Review Batches
Process in order; log findings per batch in `plans/notes/` to keep scope bounded.

### Batch A — Orchestration Shell
**Targets:** `definitions.py`, `config/`, `jobs/`, `schedules/`, `checks/`  
**Focus:** simplify entry wiring, trim redundant selection logic, remove unused schedules/config hops, clarify naming.  
**Deliverable:** per-file simplification candidates with expected test impact; store notes in `plans/notes/batch_a_orchestration.md`.

### Batch B — Assets & Resources Boundary
**Targets:** `assets/`, `resources/`  
**Focus:** align signatures with Dagster expectations, collapse try/except ladders to a single boundary, ensure `DDError` usage stays structured, highlight dependency-injection opportunities.  
**Deliverable:** consolidation map + bold (contract-breaking) suggestions; `plans/notes/batch_b_assets_resources.md`.

### Batch C — Data Layer Foundations
**Targets:** `data_layer/` (paths, readers, writers, adapters)  
**Focus:** enforce `Paths` helpers, tighten serialization/deserialization, flag impure IO primitives, document hybrid functional/object patterns.  
**Deliverable:** prioritized cleanup list and validation strategy; `plans/notes/batch_c_data_layer.md`.

### Batch D — Domain Models & Types
**Targets:** `models/`, `types.py`, `unified/`  
**Focus:** merge redundant dataclasses/TypedDicts, align schemas with validators, isolate complex transformations, record callable-typed fields for follow-up.  
**Deliverable:** model refactor ideas + schema risks; `plans/notes/batch_d_models_types.md`.

### Batch E — Utilities & Constants
**Targets:** `utils/`, `constants.py`  
**Focus:** identify helpers to inline or delete, confirm error helpers stick to structured codes, surface reusable primitives, locate redundant tests.  
**Deliverable:** utilities consolidation/deprecation shortlist; `plans/notes/batch_e_utilities.md`.

## Cross-Cutting Analysis
- Map `Err`/`DDError` usage and locate places still relying on message text.
- Summarize complexity hotspots from Radon and batch notes; suggest extractions or deletions.
- Identify testing debt that spans batches (e.g., missing parser coverage).
- Review logging boundaries; recommend a single formatting surface when multiple layers emit logs.
- Document in `plans/notes/cross_cutting_analysis.md` and append confusing constructs to `plans/notes/confusing_constructs.md`.

## Synthesis & Prioritization
- Merge batch insights into an ordered recommendation list (highest leverage first) with: summary, impacted files, risk level, required tests.
- Split into contract-safe vs. contract-breaking tracks; include migration or compatibility notes for the latter.
- Highlight quick wins (can land in one PR) versus deep refactors (require dedicated spikes).
- Consolidate into `plans/simplification_review_report.md` and link supporting notes.

## Handoff Package
- Deliver `plans/simplification_review_report.md` plus all batch notes.
- Flag any `BACKCOMPAT:` or `TEMPORARY:` scaffolding that must accompany future implementation.
- If incidental behavior must be dropped, log rationale in `REFactor_NOTES.md`.
- Provide next-step checklist: metrics to rerun after refactors, targeted tests to add, and owners for each recommendation.

## Deliverables Checklist
- [ ] Baseline metrics snapshot (`plans/notes/metrics_baseline.md`)
- [ ] Batch notes A–E (`plans/notes/batch_*.md`)
- [ ] Cross-cutting analysis + confusing constructs catalog
- [ ] Prioritized recommendation list (`plans/simplification_review_report.md`)
- [ ] Updates to `REFactor_NOTES.md` for any dropped incidental behavior

Stay disciplined on scope; if new complexities emerge mid-review, record them in notes and treat as follow-up rather than expanding this pass.

# Multi-Stage Generation Plan (Final-Only Evaluation)

Scope

- Introduce a staged generation flow that attaches one concept per step.
- Keep the pipeline document-centric and compatible with current v1 assets.
- Evaluate only the final outcome (no intermediate stage evaluations by default).

Goals

- Incremental synthesis: integrate one concept at a time into a living draft.
- Deterministic, controllable scheduling with resume/skip and bounded budgets.
- Traceability: stage-aware IDs, schema, and artifacts that align with the current repo layout.
- Final-only scoring: per-stage evals are optional and disabled by default.

Flow Overview

- Stage 0 (seed): create a seed outline/synopsis from `combo_id` context.
- Stage i (concept attach): for each concept in sequence:
  - Links scratchpad (v2): quick internal/prior-art search focused on concept_i.
  - Integration pass: revise the draft to integrate concept_i using the scratchpad.
  - Optional compression: reduce redundancy and maintain coherence as the doc grows.
- Stage F (final): polish for style/consistency; export final artifact for evaluation.

Concept Ordering

- Default: deterministic order (e.g., sorted `concept_id`).
- Optional: rank by per-concept signals (prior-art/novelty) when available.
- Cap: `max_stages_per_doc` to bound cost (e.g., top-K concepts).

IDs and Schema

- Generation task ID (staged):
  - `{document_id}__s{stage:02}__{concept_id}__{template}__{model}`
  - Columns: `document_id`, `combo_id`, `stage_index`, `concept_id`,
    `generation_template`, `generation_model`, `generation_response_path`,
    `source_asset`, `source_dir`, `created_at_ns`.
- Evaluation task ID (final-only):
  - For final stage only: `{document_id}__s{stage:02}__{template}__{model}`
  - Columns: `document_id`, `stage_index`, `evaluation_template`, `evaluation_model`,
    `evaluation_response_path`.
- Validation helpers: extend `is_valid_*_task_id` to accept optional `__sNN__` segment; tolerate v1 IDs (no stage) by mapping them to `final`.

Artifacts and Paths

- Drafts (per stage):
  - Essays: `data/3_generation/essay_responses/{document_id}__s01__{concept_id}__{template}__{model}.md`
  - Links: `data/3_generation/links_responses/{document_id}__s01__{concept_id}__{link_template}__{model}.txt`
- Scratchpads (internal):
  - `data/4_scratch/{document_id}/s01_{concept_id}/links.jsonl`
  - `data/4_scratch/{document_id}/s01_{concept_id}/integrate.jsonl`
- Final export:
  - `data/5_outputs/{document_id}/final.md`

Templates (v2)

- Links v2: quick internal/prior-art search scratchpad tailored to concept_i.
- Integrate concept: revise the current draft to integrate concept_i; output only the revised draft.
- Compression/refocus (optional): preserve key claims, remove redundancy, retain narrative thread.
- Final polish: style and consistency pass.

Scheduling and Control

- Plan expansion: `combo_id` → ordered `(document_id, stage_index, concept_id)` tasks.
- Config knobs: `max_stages_per_doc`, `models_by_stage`, `templates_by_stage`, `evaluate_per_stage` (default false), `repair_on_fail` (default off).
- Resume/skip: if a stage artifact exists and passes schema, skip; enforce dependency on previous stage existence.

Evaluation Strategy (Final-Only)

- Only evaluate the final stage documents by default.
- Optional per-stage checks (disabled by default): concept integration presence/faithfulness, local coherence.
- Pivots: include `stage_index`; default final-only views filter `stage == final` (or max stage per `document_id`).

Curation and Counts

- Seed selection: use `data/7_cross_experiment/parsed_scores.csv` to pick top-N seeds.
- Stage expansion: for each curated document, plan up to `max_stages_per_doc` stage tasks.
- Write curated generation task CSVs (per stage):
  - Essays: `data/2_tasks/essay_generation_tasks.csv`
  - Links: `data/2_tasks/link_generation_tasks.csv`
- Counts control: curated tasks × models × templates; evaluation tasks generated only for final stage.

Dataflow Integration

- Document index: add `concept_ids` (ordered) and derived `stage_plan` metadata.
- Generation tasks: created per stage from the plan; write stage-aware task IDs.
- Evaluation tasks: created only for final stage; no legacy scans.
- Parsed scores: pass through `stage_index`, `concept_id`, and `generation_response_path` where available.
- Pivots: stage in index; provide final-only convenience views.

Migration and Compatibility

- v1 files without stages are treated as `final` (or `s00`).
- Parsers accept both ID formats; writers prefer v2 staged format.
- Keep existing v1 templates; add v2 templates for staged flow.

Open Questions

- Use alphabetical vs. signal-based concept ordering by default?
- Evaluate every final doc or sample (e.g., per model/template) to reduce costs?
- Add cross-concept contradiction checks at final stage?

Next Steps

1) Define stage-aware ID patterns and CSV schemas (no code changes yet).
2) Specify staged planner that expands combos into per-stage tasks.
3) Draft v2 templates (links/integrate/compress/final-polish) with clear IO contracts.
4) Document config knobs and count formulas; add final-only evaluation guidance.
5) Plan parser/pivot updates to include `stage_index` while keeping final-only defaults.


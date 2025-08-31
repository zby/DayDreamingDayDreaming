# Plan: Evaluate Drafts (links_response) and Drop Essay Stage (for now)

## Context
- Phase 1 output (`links_response`) already produces solid essay drafts.
- The current evaluation fans out from `essay_task_id` and consumes `essay_response`.
- We want a simpler pipeline: evaluate drafts directly and stop using the essay stage for now.

## Goals
- Make evaluation fan out from `link_task_id` (drafts) instead of `essay_task_id` (refined).
- Keep the DAG shape simple and stable; avoid renaming assets in this change.
- Preserve backward compatibility where feasible, but accept that evaluation partition keys/paths will change.

## Scope (MVP)
- Switch evaluation tasks and prompts to read `links_response` as the evaluation source.
- Remove essay stage from the main flow (keep code in repo; no rename yet).
- Update results parsing to reflect link-based evaluation.
- Update tests/fixtures accordingly.

## Proposed Architecture
- Source of truth for evaluation: `links_response` (drafts).
- Partitioning: evaluation partitions derive from `link_task_id`.
- IDs: `evaluation_task_id = f"{link_task_id}_{eval_template_id}_{eval_model_id}"`.
- Templates: no change (evaluation templates still render against a `response` string).

## Asset Changes
1) `evaluation_tasks` (in `assets/core.py`)
   - Input: `link_generation_tasks` instead of `essay_generation_tasks`.
   - Schema: replace `essay_task_id` with `link_task_id`.
   - ID format: `evaluation_task_id = f"{link_task_id}_{eval_template_id}_{eval_model_id}"`.
   - Dynamic partitions: register from the new `evaluation_task_id` list.

2) `evaluation_prompt` (in `assets/llm_evaluation.py`)
   - Remove dependency on `essay_response_io_manager`.
   - Add/use `links_response_io_manager` to load drafts.
   - For a given `evaluation_task_id`, parse or lookup `link_task_id` from `evaluation_tasks` row.
   - Load draft with `MockLoadContext(link_task_id)` and render evaluation template with `response=draft_content`.
   - Output metadata: `source_stage=draft`, `source_partition=link_task_id`, `fk=eval:{id} -> link:{id}`.

3) `evaluation_response` (in `assets/llm_evaluation.py`)
   - No logic change; still calls LLM on the prompt. Keep dependencies consistent with the updated prompt.

## Schema/Partition Changes
- `evaluation_tasks.csv` columns: `[evaluation_task_id, link_task_id, evaluation_template, evaluation_model, evaluation_model_name]`.
- Partition keys for evaluations change (now derived from `link_task_id`).
- File paths for evaluation responses change accordingly under `data/4_evaluation/evaluation_responses/`.

## Downstream Adjustments
1) `parsed_scores` (in `assets/results_processing.py`)
   - Merge with `evaluation_tasks[['evaluation_task_id','link_task_id','evaluation_template','evaluation_model']]`.
   - Join with `link_generation_tasks` (not `essay_generation_tasks`) to enrich with `combo_id`, `link_template`, `generation_model`, `generation_model_name`.
   - Keep a stable output schema; map `generation_template` to the link template if needed or drop if misleading.

2) Summary/Analysis utilities
   - Where variance/aggregation previously keyed by `essay_task_id`, update to key by `link_task_id`.
   - Keep helpers that parse IDs in tests updated to extract `link_task_id` from `evaluation_task_id`.

## Testing Plan
- Unit: parameter-free tests for `evaluation_prompt` reading drafts via mocked `links_response_io_manager`.
- Update fixtures:
  - `tests/data_pipeline_test/2_tasks/evaluation_tasks.csv` → link-based schema.
  - Any tests referencing `essay_task_id` from evaluation IDs updated to `link_task_id`.
- Integration: re-run results parsing over evaluation responses produced from link partitions.

## Rollout Steps
1) Update `evaluation_tasks` to link-based and re-register dynamic partitions.
2) Rewire `evaluation_prompt` to load from `links_response_io_manager`.
3) Adjust `parsed_scores` joins to use `link_generation_tasks`.
4) Update tests/fixtures; run `uv run pytest` for unit and integration tests.
5) Materialize: 
   - `uv run dagster asset materialize --select "group:generation_links"`
   - `uv run dagster asset materialize --select "group:evaluation"`
6) Rename "link" to "essay_draft" (follow-up PR):
   - Assets: `links_prompt` → `essay_draft_prompt`, `links_response` → `essay_draft_response`.
   - IO managers: `links_prompt_io_manager` → `essay_draft_prompt_io_manager` (base: `data/3_generation/essay_draft_prompts/`), `links_response_io_manager` → `essay_draft_response_io_manager` (base: `data/3_generation/essay_draft_responses/`).
   - Partitions (optional): `link_tasks_partitions` → `essay_draft_tasks_partitions`.
   - Groups (optional): `generation_links` → `generation_drafts`.
   - Update references across assets, tests, and docs.

## Risks / Mitigations
- Breaking change in evaluation partition keys/paths: communicate in PR; old evaluation artifacts won’t be reused.
- Downstream tools expecting `essay_task_id`: migrate to `link_task_id`; provide a compatibility helper if needed (not in MVP).

## Future Work (Post-MVP)
- Consider re-introducing a refined essay stage as optional (separate branch of the graph).
- If needed, add dual-mode evaluation that can fan out from both drafts and refined via separate partitions.

## Acceptance Criteria
- Evaluation prompts/responses generated using drafts only.
- `evaluation_tasks` and `evaluation_prompt` no longer reference `essay_task_id`/`essay_response`.
- Tests pass after fixture updates.
- Results parsing produces enriched rows keyed by `link_task_id` with correct file paths.

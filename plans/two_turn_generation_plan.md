# Plan: Two‑Turn Generations (Link → LLM, Essay‑like → Copy)

Status: ready
Owner: daydreaming_dagster

Objective
- Run remaining draft templates in two turns to simplify validation and isolate failure modes:
  1) Turn 1 (link‑style → LLM essay): Activate link‑style draft templates and a single LLM essay template (`synthesis-theme-narrative-v19`). Generate drafts and essays end‑to‑end. Disable copy essays.
  2) Turn 2 (essay‑like → copy): Deactivate link‑style drafts, activate essay‑like drafts, switch essay template to copy mode (`parsed-from-links-v1`) to finalize drafts without LLM.

Guiding principles
- Doc‑id‑first: All task CSVs must include `doc_id`; essays/evaluations must carry `parent_doc_id` pointing at the upstream doc.
- Fail‑fast: Scripts/assets should error on missing IDs or files instead of auto‑fixing data.
- No DB: Use filesystem docs store under `data/docs/<stage>/<doc_id>/`.

Turn 1 — Link‑style drafts → LLM essay v19
- Draft templates to activate (CSV: `data/1_raw/draft_templates.csv`):
  - creative-synthesis-v9, creative-synthesis-v10
  - deliberate-rolling-thread-v2, deliberate-rolling-thread-v3
  - rolling-summary-v1, rolling-summary-simplified-v1
  - connective-thread-simplified-v1
  - pair-explorer, pair-explorer-recursive-v5
  - recursive-light, recursive-precise-v2
- Essay template to activate (CSV: `data/1_raw/essay_templates.csv`):
  - synthesis-theme-narrative-v19 (generator=llm)
- Deactivate for this turn:
  - Draft: creative-synthesis-v7
  - Essay: parsed-from-links-v1 (copy)
- Steps
  1) Materialize tasks: `uv run dagster asset materialize --select "group:task_definitions" -f daydreaming_dagster/definitions.py`
  2) Drafts: `uv run dagster asset materialize --select "group:generation_draft" -f daydreaming_dagster/definitions.py`
  3) Essays: `uv run dagster asset materialize --select "group:generation_essays" -f daydreaming_dagster/definitions.py`
  4) Optional evaluations: `uv run dagster asset materialize --select "group:evaluation" -f daydreaming_dagster/definitions.py`

Turn 2 — Essay‑like drafts → copy
- Draft templates to activate:
  - creative-synthesis-v2, creative-synthesis-v6, creative-synthesis-v7
  - gwern_original, problem-solving-v2
  - recursive_construction, recursive-construction
  - systematic-analytical-v2
  - application-implementation-v2, research-discovery-v3
- Essay template to activate:
  - parsed-from-links-v1 (generator=copy)
- Deactivate for this turn:
  - synthesis-theme-narrative-v19 (LLM)
- Steps: same sequence as Turn 1 (tasks → drafts → essays → optional evals).

Notes & caveats
- creative-synthesis-v8: referenced by existing docs, but draft template file appears missing locally. Keep it inactive or add a template file before use.
- Using curated runs: you can also build `data/2_tasks/essay_generation_tasks.csv` directly (with `parent_doc_id` and `doc_id`) and run via scripts/register_partitions_for_generations.py for reproducible batches.
- Keep evaluations focused on essay outputs; single‑phase copy runs will pass through draft text unchanged.

Validation checklist
- All task CSVs include `doc_id`; essays/evals include non‑empty `parent_doc_id`.
- Docs written to `data/docs/<stage>/<doc_id>/` with `metadata.json` and `parsed.txt` present.
- For Turn 1, essay prompts/responses are produced via LLM; for Turn 2, essays copy draft content without LLM.


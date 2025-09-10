# Plan: Two‑Turn Generations under a Single Cohort (Link → LLM, Essay‑like → Copy)

Status: ready
Owner: daydreaming_dagster

Objective
- Run the two turns of generation under ONE explicit cohort for reproducibility and easier analysis.
  1) Turn 1 (link‑style → LLM essay): Activate link‑style draft templates and a single LLM essay template (`synthesis-theme-narrative-v19`). Generate drafts and essays end‑to‑end. Disable copy essays.
  2) Turn 2 (essay‑like → copy): Deactivate link‑style drafts, activate essay‑like drafts, switch essay template to copy mode (`parsed-from-links-v1`) to finalize drafts without LLM.

Guiding principles
- Gen‑id‑first using gens store: generations live under `data/gens/<stage>/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}`.
- Fail‑fast: assets error on missing data rather than silently fixing it.
- No DB: filesystem + CSVs only.

Pinning one cohort across turns
- Export a single cohort ID before both turns so dynamic partitions and membership accumulate under the same cohort:
  - `export DD_COHORT=cohort_two_turn_$(date +%Y%m%d)`
  - Alternatively pass via run config: `ops: { cohort_id: { config: { override: "cohort_two_turn_custom" } } }`
- With `DD_COHORT` set, assets will use this explicit ID and write/update `data/cohorts/$DD_COHORT/manifest.json` and `membership.csv` as you run turns.

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
  1) Materialize cohort + membership + tasks:
     `uv run dagster asset materialize --select "cohort_id,cohort_membership,group:task_definitions" -f daydreaming_dagster/definitions.py`
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
- Steps (same cohort ID still exported)
  1) Materialize cohort_membership + tasks (recomputes membership under same cohort, registers new partitions add‑only):
     `uv run dagster asset materialize --select "cohort_membership,group:task_definitions" -f daydreaming_dagster/definitions.py`
  2) Drafts: `uv run dagster asset materialize --select "group:generation_draft" -f daydreaming_dagster/definitions.py`
  3) Essays: `uv run dagster asset materialize --select "group:generation_essays" -f daydreaming_dagster/definitions.py`
  4) Optional evaluations: `uv run dagster asset materialize --select "group:evaluation" -f daydreaming_dagster/definitions.py`

Notes & caveats
- creative-synthesis-v8: referenced by existing docs, but the draft template file may be missing locally. Keep it inactive or add the template file before use.
- You can also run curated turns by seeding `data/2_tasks/selected_essays.txt` with essay gen_ids; `cohort_membership` will use curated mode and still respect `DD_COHORT`.
- Evaluations target essays; copy‑mode essays simply pass through draft text.

Validation checklist
- Gens files present: `data/gens/<stage>/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}`.
- `data/cohorts/$DD_COHORT/membership.csv` includes both turns’ rows; dynamic partitions registered for all gen_ids.
- Turn 1 essays come from LLM; Turn 2 essays copy draft content (no LLM).

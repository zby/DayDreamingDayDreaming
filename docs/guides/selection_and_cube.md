# Selection And The Current Experiment Cube (2025-09)

This note updates how we think about the “Current Experiment Cube” and describes a selective evaluation mode that avoids materializing the full Cartesian product.

## Background: The Cube

- Today, task builders expand combinations aggressively:
  - Generation: `combos × draft_templates × generation_models` (→ drafts; legacy `link_templates`), then `drafts × essay_templates` (→ essays)
  - Evaluation: `documents × evaluation_templates × evaluation_models`
- The “current cube” is defined by active rows in CSVs under `data/1_raw`.

## Problem: Targeted Evaluation

We often want to evaluate only a subset of documents (e.g., prior-art “winners”) with a specific evaluation template such as `novelty.txt`, without expanding the full Cartesian product or toggling active rows that would affect unrelated runs.

## Unified Document Axis Recap

The document-centric evaluation architecture decouples selection (which documents) from analysis (how we pivot). Each `evaluation_tasks` row carries the document `file_path`, so evaluators operate uniformly across drafts, two-phase essays, and legacy one-phase files.

## External Script Approach (No Code Changes, No Legacy Scan)

Use a lightweight script that produces a curated set of documents for evaluation, while Dagster continues to “fan out” as usual from discovered documents:

- Input: policy-specific selector (e.g., winners from `parsed_scores` for prior-art v1/v2; or an arbitrary list later).
- Ingestion surface: write standard generation task CSVs and place/symlink chosen documents under the canonical folders (`draft_responses/` for drafts; legacy `links_responses/` supported, `essay_responses/` for essays). Avoid `generation_responses/` — it is not scanned by evaluation.
  - For two-phase essays: source from `essay_responses/{essay_task_id}.txt`; derive `{combo_id, essay_template, model_id}` from metadata if needed.
  - For drafts-as-one-phase: source from `draft_responses/{draft_task_id}.txt` (legacy fallback: `links_responses/{link_task_id}.txt`); set `essay_template = draft_template` for identity in your CSV row.
  

Operator flow:
- Prepare winners via script (write curated CSVs + symlink/copy files under canonical folders; optionally write partition list).
- Register tasks once: materialize `evaluation_tasks` to discover the curated files via the standard generation task tables.
- Run only desired partitions: loop over partition keys and materialize `evaluation_prompt,evaluation_response`.
- Parse: re-materialize `parsed_scores` to ingest the new results.

Pros:
- Zero pipeline code changes; reversible and easy to automate.
- Works for cross-experiment winners (just place the raw text locally first).
- Keeps the cube stable; selection is operational and explicit.

Trade-offs:
- Requires a small amount of glue logic in the script to derive IDs and ensure files are present in canonical folders.

## Curated Selection (Split Scripts, Non‑Cube)

To make targeted runs simpler and editable, the selection flow is split into two scripts:

1) Find top‑N and write an editable list
- `scripts/find_top_prior_art.py` reads `data/7_cross_experiment/parsed_scores.csv`, filters prior‑art templates, and writes the best `document_id`s to a simple text file you can tweak by hand.
- Usage:
  - `uv run python scripts/find_top_prior_art.py --top-n 25`
  - Outputs: `data/2_tasks/selected_generations.txt` (one id per line)
  - Optional CSV: `--csv-output data/2_tasks/selected_generations.csv`

2) Register curated tasks and partitions (drafts, essays, evaluations)
- `scripts/register_partitions_for_generations.py` takes the list (txt or CSV) and:
  - Merges curated rows into `data/2_tasks/draft_generation_tasks.csv` and `data/2_tasks/essay_generation_tasks.csv` (de‑duped by task id).
  - Registers dynamic partitions (add‑only by default; can reset first) for:
    - `draft_tasks` (Phase‑1)
    - `essay_tasks` (Phase‑2)
    - `evaluation_tasks` for all active evaluation templates × evaluation‑capable models
- Usage examples:
  - `export DAGSTER_HOME=$(pwd)/dagster_home`
  - `uv run python scripts/register_partitions_for_generations.py --input data/2_tasks/selected_generations.txt`
  - Flags:
    - `--no-write-drafts` to only curate essays
    - `--no-register` to only update CSVs (skip partition registration)
    - `--no-reset-partitions` for additive partition registration; default is reset (non‑cube curated set)
    - `--no-clean-2-tasks` to avoid clearing `data/2_tasks`; by default, the script cleans that directory and writes only curated task CSVs. The selected list (`selected_generations.txt` / `.csv`) is preserved.

3) Run from the Dagster UI (or CLI)
- After registration, refresh the UI and materialize only the selected partitions:
  - Drafts: `draft_prompt,draft_response` for each `draft_task_id`
  - Essays: `essay_prompt,essay_response` for each `essay_task_id`
  - Evaluations: `evaluation_prompt,evaluation_response` for each selected `evaluation_task_id`

Why this works without k_max tweaks
- A curated target might have been built with a different `k_max` than your current experiment. The `content_combinations` asset reads `data/2_tasks/selected_combo_mappings.csv` (written by the register script by filtering `data/combo_mappings.csv`). That injects the exact `concept_id`s and `description_level` for each curated `combo_id`, independent of the current `k_max`.
- Prerequisites:
  - `data/combo_mappings.csv` contains the `combo_id` (created when the combo was first generated)
  - `data/1_raw/concepts_metadata.csv` contains the listed `concept_id`s and the level’s description files exist (e.g., `descriptions-paragraph/`)

Exact recovery vs regeneration
- If a legacy single‑phase file exists (e.g., `data/3_generation/generation_responses/<id>.txt`), you can backfill the two‑phase draft by copying it to `data/3_generation/draft_responses/{draft_task_id}.txt`. The parser/copy essay modes then work without LLM calls.
- If you need to regenerate, run the draft partition with your configured LLM resource after registering the partitions.

## Selective Evaluation: In-Pipeline Options (Future Work)

1) Score-Filtered Selection asset
- Asset `evaluation_selection` reads `parsed_scores` and emits a set of `document_id`s by policy:
  - Filter by templates (e.g., prior-art v1/v2), ranking strategy (top-N/threshold), union/intersection across templates.
- `evaluation_tasks` consumes selection (when present) and expands only the chosen documents.

2) Manifest-Based Selection
- Introduce `data/2_tasks/document_manifest.csv` with normalized columns (`document_id, stage, origin, file_path, combo_id, draft_template (legacy link_template), essay_template, generation_model_id/name, draft_task_id (legacy link_task_id), essay_task_id, source_asset, source_dir`).
- Include manifest rows in `document_index`; optionally disable the legacy directory scan after migration.

3) Targeted Partition Materialization (operational)
- Keep the cube unchanged; run specific partitions by key using the list generated by the script.

## Migration Away from Legacy Scan

- The legacy scan of `generation_responses/` is removed from evaluation. Use the external script to produce standard generation task CSVs and place texts in canonical folders.

## Example: Novelty on Prior-Art Winners (Current Approach)

1. Compute winners from `data/7_cross_experiment/parsed_scores.csv` where `evaluation_template ∈ {gemini-prior-art-eval, gemini-prior-art-eval-v2}` using your policy (top-N/threshold; union).
2. For each winner, ensure its essay exists under `data/3_generation/essay_responses/{essay_task_id}.txt`.
3. Write curated `data/2_tasks/essay_generation_tasks.csv` (and optionally a matching `draft_generation_tasks.csv`).
4. Materialize `evaluation_tasks` once (register partitions), then materialize evaluations for the active templates/models.
5. Re-run `parsed_scores` and downstream pivots.

## Acceptance Criteria for Future Implementation (Optional)

- `evaluation_tasks` supports optional selection input and only expands chosen documents when present.
- `document_manifest` rows appear in `document_index` with full normalized columns.
- Legacy directory scan can be disabled without losing the ability to evaluate arbitrary historical docs.

---

## Preferred Workflow: External Script Writes Standard Generation Tasks (No New Assets)

In many cases the cleanest solution is to let an external script generate the standard generation task CSVs that our pipeline already expects. Evaluation then fans out strictly from generation tasks (no legacy directory scans, no special manifest asset).

### What the Script Produces

- `data/2_tasks/draft_generation_tasks.csv`
  - Columns: `draft_task_id, combo_id, draft_template, generation_model, generation_model_name`
  - For each row, a file must exist at: `data/3_generation/draft_responses/{draft_task_id}.txt` (or legacy `links_responses/{link_task_id}.txt`)

- `data/2_tasks/essay_generation_tasks.csv`
  - Columns: `essay_task_id, link_task_id, combo_id, link_template, essay_template, generation_model, generation_model_name`
  - For each row, a file must exist at: `data/3_generation/essay_responses/{essay_task_id}.txt`

Notes:
- Use the same naming the pipeline already uses:
  - `draft_task_id = {combo_id}_{draft_template}_{generation_model_id}`
  - `essay_task_id = {link_task_id}_{essay_template}` (i.e., `{combo_id}_{link_template}_{generation_model_id}_{essay_template}`)
- `generation_model` must be an ID present in `data/1_raw/llm_models.csv`; `generation_model_name` should be the provider/model string (e.g., `deepseek/deepseek-r1:free`).
- Do not place anything into `data/3_generation/generation_responses/` for this workflow; keep legacy empty.

Curated counts and drafts:
- `evaluation_tasks` builds from both `draft_generation_tasks` (drafts) and `essay_generation_tasks` (essays). If `data/2_tasks/draft_generation_tasks.csv` still contains many rows (from the cube), those drafts will be included in the document set and inflate counts.
- To get exact doc counts, either:
  - Write an empty `draft_generation_tasks.csv` (exclude drafts entirely), or
  - Derive a curated `draft_generation_tasks.csv` from your curated essays to include only matching draft tasks.
    - Quick helper (one-liner):
      ```bash
      uv run python - <<'PY'
      import pandas as pd
      edf = pd.read_csv('data/2_tasks/essay_generation_tasks.csv')
      cols=['link_task_id','combo_id','link_template','generation_model','generation_model_name']
      df = pd.DataFrame(edf[cols].drop_duplicates())
      # Rename to draft_* for the curated draft task table
      df = df.rename(columns={'link_task_id':'draft_task_id','link_template':'draft_template'})
      df.to_csv('data/2_tasks/draft_generation_tasks.csv', index=False)
      print('Wrote curated draft_generation_tasks.csv')
      PY
      ```

### How the Script Selects Winners

1) Read `data/5_parsing/parsed_scores.csv` (or any other source)
- Filter to `evaluation_template ∈ {gemini-prior-art-eval, gemini-prior-art-eval-v2}`
- Choose your policy: top-N per doc, threshold (e.g., ≥9.0), union/intersection

2) Resolve each winner to its source text and fields
- Drafts-as-one-phase: compute `draft_task_id`; source file at `draft_responses/{draft_task_id}.txt` (legacy: `links_responses/{link_task_id}.txt`)
- Two-phase essays: compute `essay_task_id`; source file at `essay_responses/{essay_task_id}.txt`
- If the text lives outside this repo, copy or symlink it into the canonical folder above.

3) Write curated CSVs
- Write minimal CSVs containing only the chosen rows (do not regenerate the full cube).

### Runbook (No Code Changes)

1. Prepare curated CSVs and files via the script:
- `data/2_tasks/draft_generation_tasks.csv` (optional)
- `data/2_tasks/essay_generation_tasks.csv` (optional)
- Ensure the corresponding text files exist under `draft_responses/` (legacy: `links_responses/`) and/or `essay_responses/`.

2. Register evaluation partitions (from your curated tasks only):
```bash
uv run dagster asset materialize --select evaluation_tasks -f daydreaming_dagster/definitions.py
```

3. Run targeted evaluation (e.g., novelty) only for desired partition keys:
```bash
# For each key: {document_id}__{evaluation_template}__{evaluation_model_id}
uv run dagster asset materialize --select "evaluation_prompt,evaluation_response" \
  --partition "{document_id}__novelty__{evaluation_model_id}" \
  -f daydreaming_dagster/definitions.py
```

4. Parse and analyze:
```bash
uv run dagster asset materialize --select parsed_scores,generation_scores_pivot -f daydreaming_dagster/definitions.py
```

### Exact Task Counts (sanity)
- `total_tasks = documents × active_evaluation_templates × active_evaluation_models`
- To get e.g. 10 documents × 2 templates × 1 model = 20 tasks:
  - Ensure only your 10 docs are present (curated essays; drafts excluded or curated)
  - In `data/1_raw/evaluation_templates.csv`, set only the target templates to `active: true`
  - In `data/1_raw/llm_models.csv`, set only the target evaluator (e.g., `sonnet-4`) to `for_evaluation: true`

### UI Partition Keys vs Source of Truth
- Partition keys are defined by `evaluation_task_id = {document_id}__{evaluation_template}__{evaluation_model_id}` (double underscores).
- If the UI shows stale or mismatched keys, re-materialize `evaluation_tasks` and copy keys directly from `data/2_tasks/evaluation_tasks.csv`.

### Important Gotchas

- Do not rematerialize `group:task_definitions` during this curated run; those assets will regenerate/overwrite the CSVs you wrote. Work only with `evaluation_tasks` and downstream assets.
- Keep `data/3_generation/generation_responses/` empty to avoid registering legacy-derived partitions.
- Ensure `novelty` (or your target template) is present/active in `data/1_raw/evaluation_templates.csv`; you will only materialize those partitions.
- If you need to reduce the cross product size further, temporarily keep only the target evaluation template/model rows active in the raw CSVs before step 2.

# Active Experiment Cube

This note defines the “Active Experiment Cube” (aka current experiment) and how the repo uses `active` columns to select a Cartesian product of inputs to generate and evaluate. It also explains how historical outputs persist on disk and how changing the active cube later can reuse prior outputs while filling in missing pieces.

## Concept

- Active Experiment Cube (AEC): The working set of combinations we intend to materialize now.
- Defined by “active” flags in raw config tables and model capabilities:
  - Concepts: `data/1_raw/concepts_metadata.csv` (column `active`)
  - Draft templates: `data/1_raw/draft_templates.csv` (columns `active`, `parser` for Phase‑1 extraction; parser names are defined in `daydreaming_dagster/utils/draft_parsers.py`)
  - Essay templates: `data/1_raw/essay_templates.csv` (column `active`)
  - Evaluation templates: `data/1_raw/evaluation_templates.csv` (column `active`)
  - LLM models: `data/1_raw/llm_models.csv` (columns `for_generation`, `for_evaluation`)
  - Experiment parameters: `ExperimentConfig` (e.g., `k_max`, description_level)

The “cube” name reflects that the AEC is a Cartesian product across dimensions (axes). We build different cubes for generation and evaluation, but they share the same principles.

## Axes (High‑Level) and Examples

The cube is a Cartesian product across active subsets of several axes. Keep axes explicit and minimal; vary one or two at a time.

### Common Axes
- Concepts: filtered by `active`, combined into `combo_id` with `k_max` and `description_level`.
- Per‑stage templates: each stage is its own axis (e.g., `draft_templates`, `essay_templates`, `stageN_templates`), each with its own `active` flags.
- Models: two separate axes — Generation Models and Evaluation Models. We store both in a single CSV with boolean flags (`for_generation`, `for_evaluation`), but they participate in different products.
- Evaluation templates: scoring prompts with `active` flags.

Note: The evaluation target (which stage’s outputs are evaluated) is not a cube axis — it’s a pipeline selection choice (see the unified plan’s Hybrid Document Axis) that determines which documents are included when building evaluation tasks.

#### Why two model axes in one CSV?
- Single inventory: One canonical list of model identifiers centralizes metadata (provider, context window, pricing, aliases), avoiding duplication.
- Orthogonal roles: `for_generation` and `for_evaluation` are orthogonal booleans; a model can be active for one, both, or neither. Logically, this yields two axes: the set of active generation models and the set of active evaluation models.
- Different products: Generation cubes use only the Generation Models axis; Evaluation cubes use only the Evaluation Models axis. Overlap is allowed but not required.
- Operational simplicity: Operators flip flags in one place; downstream builders pick the appropriate subset per axis.

Only a subset on each axis is marked active for the current experiment; the cube is the Cartesian product of those subsets.

### Example: Generation Cube (current design as an example)
- Axes (example): `content_combinations × draft_templates × generation_models` → draft tasks; then `draft_tasks × essay_templates` → essay tasks.
- Example outputs:
- Draft gens: `data/gens/draft/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}`
  - RAW (optional side-write): `data/3_generation/draft_responses_raw/{gen_id}_vN.txt` (useful when Phase‑1 parsing fails)
  - Essay gens: `data/gens/essay/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}`
- Legacy example: one‑phase essays under `data/3_generation/generation_responses/{combo_id}_{essay_template}_{model_id}.txt`.

### Example: Evaluation Cube (document‑centric)
- Axes (example): `documents_to_evaluate × evaluation_templates × evaluation_models`.
  - `documents_to_evaluate` is a filtered set of existing documents discovered from tasks and/or filesystem.
- Outputs (examples): gens under `data/gens/evaluation/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}`.
  - Note: In some experiments, draft outputs may be treated as “effective one‑phase” documents for evaluation convenience. This is a pipeline selection choice; provenance (e.g., original stage, source path) should still be retained in the data.

## Switching Generation Modes: Caveats

Switching between evaluating drafts (Phase 1) and essays (one‑ or two‑phase) is not automatic and typically requires rewiring parts of the evaluation pipeline (e.g., which document type is targeted and how prompts load files). The Active Experiment Cube helps with statistics and comparability (you can pivot across dimensions), but it does not by itself flip evaluation modes.

Recommended patterns:
- Choose a single document stage for an experiment and keep other dimensions minimal; evaluate that stage.
- If changing modes (e.g., drafts → essays), treat it as a new experiment selection; rebuild tasks to target the new stage.
- Keep historical outputs on disk; later experiments can reuse them if IDs match.

## Persistence and “Completing the Cube”

- The repo never deletes prior outputs automatically; historical artifacts persist under `data/gens/` (canonical) and legacy `data/3_generation`/`data/4_evaluation` (where present).
- When you change the AEC (e.g., add a new essay template or model), the next task build will:
  - Create partitions for the new Cartesian products.
  - Keep partitions for previously generated products (if still addressable by ID).
  - Materialize only missing outputs; existing files are respected (overwrite is off by default for responses).
- This enables incremental runs that “complete the cube” without re‑doing existing work.

## Identity and Stability

- combo_id: stable ID produced by `ComboIDManager` based on concept IDs and `k_max`/description level.
- draft_task_id (legacy link_task_id): `{combo_id}_{draft_template}_{generation_model_id}`
- essay_task_id (two‑phase): `{draft_task_id}_{essay_template}`
- one‑phase essay stem: `{combo_id}_{essay_template}_{generation_model_id}`
- evaluation_task_id (document‑centric pattern): typically `{document_id}__{evaluation_template}__{evaluation_model_id}`; stage is tracked as a column, not embedded in the ID

Stable IDs make it easy to:
- Detect which parts of the cube are already done (files present).
- Join back task rows to summarize results across experiments.
- Compare across modes by tagging results with the evaluated stage (for reporting) and pivoting on templates/models; the evaluated stage is a derived label, not a cube axis.

## Keep the Cube Small (Scope Discipline)

The Cartesian product grows quickly. For reliable iteration and readable stats:
- Limit most dimensions to a single active choice (e.g., one essay template, one generation model) while exploring another dimension.
- Prefer narrow, focused cubes for each experiment; vary only one or two axes at a time.
- Use pivots to compare across runs/axes, not by blowing up the active cube in one go.
- Avoid enabling many templates and models simultaneously unless you specifically need that coverage and have budget/time.

## Operational Flow (Current Experiment)

1) Set the AEC by editing `active` flags and ExperimentConfig.
2) Materialize task definitions (auto in Dagster or via CLI). This registers dynamic partitions for the current cube.
3) Generate drafts and/or essays according to the experiment’s focus.
4) Build evaluation tasks for the chosen document source(s).
5) Evaluate, parse scores, and produce summaries.

CLI tips:
- `uv run dagster asset materialize --select group:task_definitions -f daydreaming_dagster/definitions.py`
- Drafts only: `uv run dagster asset materialize --select group:generation_draft -f daydreaming_dagster/definitions.py`
- Two‑phase essays: `uv run dagster asset materialize --select group:generation_essays -f daydreaming_dagster/definitions.py`
- Evaluate: `uv run dagster asset materialize --select group:evaluation -f daydreaming_dagster/definitions.py`

Optional run tags:
- You can add a run tag like `experiment_id=YYYYMMDD_desc` to group runs in Dagit. This does not affect IDs or file paths but helps tracking.

## Inclusion Policy Examples

- Add a new essay template, keep the rest the same:
  - Mark it `active=true` in `essay_templates.csv`.
  - Build essay tasks; only new `{essay_template}` expansions will be missing and thus materialized.

- Add a new generation model for drafts:
  - Mark `for_generation=true` in `llm_models.csv`.
  - New `draft_task_id` rows are added; only those get generated.

- Evaluate only a subset (e.g., drafts with a specific link template):
  - Keep the AEC broad, then select partitions in Dagit/CLI to materialize a slice.

## Data Hygiene and Overwrite Rules

- Prompts overwrite is allowed (reflect latest templates).
- Responses are stored under `data/gens/<stage>/<gen_id>`. New generations receive fresh `gen_id`s; prompts may overwrite to reflect template changes. Optional RAW side-writes may be versioned for debugging.
- This ensures historical experiments remain intact while allowing iterative prompt/template tweaks.

## Naming: “Active Experiment Cube” vs Alternatives

- Active Experiment Cube (AEC): emphasizes a structured, multi‑axis product.
- Current Experiment: colloquial name often used in conversation.
- Alternatives (less precise): Working Set, Active Slice, Live Grid, Target Matrix.

We’ll use “Active Experiment Cube (AEC)” in docs and “current experiment” informally.

## Future Enhancements (Optional)

- Explicit “document index” asset that lists documents available to evaluate (draft, two‑phase essay, one‑phase essay), governed by simple toggles, so evaluation tasks can be built directly from it.
- Lightweight “completeness” report that shows how much of the current cube is materialized (drafts, essays, evaluations).

# Gens Store (gen-id first)

The pipeline persists all generated artifacts under a simple, portable filesystem layout keyed by `gen_id`.

## Layout

- Root: `data/gens/<stage>/<gen_id>/`
- Files per document:
  - `raw.txt` — full unprocessed model output
  - `parsed.txt` — normalized/parsed text used by downstream stages
  - `prompt.txt` — prompt used to produce the response (when available)
  - `metadata.json` — minimal metadata (see below)

Stages:
- `draft` — Phase 1 drafts
- `essay` — Phase 2 essays (parent: draft gen)
- `evaluation` — Evaluations of essays (parent: essay gen)

## Metadata

Each `metadata.json` should include at least:
- `task_id` — draft/essay/evaluation task id
- `template_id` — generation/evaluation template id
- `model_id` — model id used
- `parent_gen_id` — lineage pointer (essay→draft, evaluation→essay); empty for drafts
- Optional: `function`, `usage`, other provenance details

## Invariants

- Membership (data/cohorts/<cohort_id>/membership.csv) includes a `gen_id` per row and is the source of truth. Assets write to `data/gens/<stage>/<gen_id>` based on that.
- Essays and evaluations require a valid `parent_gen_id` that exists on disk:
  - `draft` parent for essays
  - `essay` parent for evaluations
- Prompts are passed in-process between prompt/response assets; no runtime re-reads.

## Programmatic Access

Prefer the centralized `Paths` helpers for building paths and use `load_generation` to read documents:

```python
from daydreaming_dagster.config.paths import Paths
from daydreaming_dagster.utils.generation import load_generation

paths = Paths.from_str("data")
gens_root = paths.gens_root
gen = load_generation(gens_root, "essay", "abc123xyz")
parsed = gen["parsed_text"]  # or gen["raw_text"] / gen["prompt_text"] / gen["metadata"]

# Or address files directly via Paths (single source of truth):
print(paths.parsed_path("essay", "abc123xyz"))
```

## Troubleshooting

- Missing generation dir:
  - Check that `data/cohorts/<cohort>/membership.csv` lists the `gen_id` for the stage.
  - Ensure upstream parents exist (for essays/evaluations).
- Empty `parsed.txt` but present `raw.txt`:
  - The generator or parser may have failed; inspect `raw.txt` and `metadata.json`.
- Not seeing prompts:
  - Prompt side-writes are best-effort. For drafts and essays generated via LLM, `prompt.txt` is typically present.

## Notes

- Optional RAW side-writes under `data/3_generation/*_raw/` can be enabled in `ExperimentConfig` for debugging.
- Single source of truth for the filesystem layout: `src/daydreaming_dagster/config/paths.py`.

### Replicates

- The pipeline can generate multiple replicates per stage by varying a deterministic salt inside the gen_id (e.g., `rep1..repN`).
- Aggregations (e.g., generation_scores_pivot) average replicates across identical task keys and evaluator axes using mean.
- Grouping keys used for averaging exclude `gen_id` and replicate; they include:
  - `combo_id`, `stage`, `draft_template`, `generation_template`, `generation_model` in the index, and the evaluator axis (`evaluation_template` × `evaluation_llm_model`) as columns.
- Averaging is currently done across cohorts when multiple cohorts are present in the underlying data; typical runs operate within the current cohort.
- The pipeline no longer depends on a SQLite index; the filesystem layout is the source of truth.

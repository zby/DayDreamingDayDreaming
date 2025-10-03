# Gens Store (gen-id first)

The pipeline persists all generated artifacts under a simple, portable filesystem layout keyed by `gen_id`.

## Layout

- Root: `data/gens/<stage>/<gen_id>/`
- Files per document:
  - `prompt.txt` — prompt handed to the generator (if available)
  - `raw.txt` — full unprocessed model output
  - `parsed.txt` — normalized/parsed text used by downstream stages
  - `metadata.json` — canonical main metadata (mode/copy lineage, template, ids)
  - `raw_metadata.json` — metadata captured at raw generation time (LLM info, finish reason, token usage)
  - `parsed_metadata.json` — metadata captured by the parser (parser name, success flags)

Stages:
- `draft` — Phase 1 drafts
- `essay` — Phase 2 essays (parent: draft gen)
- `evaluation` — Evaluations of essays (parent: essay gen)

## Metadata

`metadata.json` (main metadata) includes stage, gen id, mode (`llm`/`copy`), template id, optional lineage (combo/cohort/parent ids), and any legacy fields that could not be classified. `raw_metadata.json` captures the generator run (model id, finish reason, duration, token usage, copy provenance). `parsed_metadata.json` records parser details, success flags, and links back to raw metadata.

## Invariants

- Membership (data/cohorts/<cohort_id>/membership.csv) includes a `gen_id` per row and is the source of truth. Assets write to `data/gens/<stage>/<gen_id>` based on that.
- Essays and evaluations require a valid `parent_gen_id` that exists on disk:
  - `draft` parent for essays
  - `essay` parent for evaluations
- Prompts are passed in-process between prompt/response assets; no runtime re-reads.

## Programmatic Access

Prefer the centralized `Paths` helpers together with the `GensDataLayer` abstraction when accessing documents programmatically:

```python
from pathlib import Path
from daydreaming_dagster.data_layer.paths import Paths
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer

paths = Paths.from_str("data")
data_layer = GensDataLayer.from_root(paths.data_root)

metadata = data_layer.read_main_metadata("essay", "abc123xyz")
parsed = data_layer.read_parsed("essay", "abc123xyz")

# Paths remains the single source of truth for file locations
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

- Optional RAW side-writes (`raw.txt`) live alongside artifacts in `data/gens/<stage>/<gen_id>/` when enabled via `ExperimentConfig` for debugging.
- Single source of truth for the filesystem layout: `src/daydreaming_dagster/data_layer/paths.py`.

### Replicates

- The pipeline can generate multiple replicates per stage by varying a deterministic salt inside the gen_id (e.g., `rep1..repN`).
- Aggregations (e.g., generation_scores_pivot) average replicates across identical task keys and evaluator axes using mean.
- Grouping keys used for averaging exclude `gen_id` and replicate; they include:
  - `combo_id`, `stage`, `draft_template`, `generation_template`, `generation_model` in the index, and the evaluator axis (`evaluation_template` × `evaluation_llm_model`) as columns.
- Averaging is currently done across cohorts when multiple cohorts are present in the underlying data; typical runs operate within the current cohort.
- The pipeline no longer depends on a SQLite index; the filesystem layout is the source of truth.

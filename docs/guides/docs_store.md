# Gens Store (gen-id first)

The pipeline persists every generation under a simple, portable filesystem layout keyed by `gen_id`. The gens store is the single source of truth for prompts, model outputs, and metadata.

## Layout

- Root: `data/gens/<stage>/<gen_id>/`
- **Phase files per stage:**
  - `prompt.txt` — canonical stage input (rendered template or copied parent text)
  - `raw.txt` — full unprocessed model output captured immediately after generation
  - `parsed.txt` — normalised/parsed text that downstream stages consume
  - `metadata.json` — main metadata seeded by the cohort compiler and enriched by asset runs
  - `raw_metadata.json` — execution details for the raw phase (timing, finish reason, token usage)
  - `parsed_metadata.json` — execution details for the parsed phase (parser name, success flags)

Stages:
- `draft` — first stage that generates brainstorming drafts (combination driven)
- `essay` — second stage that turns drafts into essays (parent: draft `gen_id`)
- `evaluation` — third stage that scores or critiques essays (parent: essay `gen_id`)

## Metadata contracts

`metadata.json` is written before the raw/parsed stages run. Cohort assets seed it with:
- `stage`, `gen_id`, `template_id`, `mode` (`llm` or `copy`)
- lineage (`parent_gen_id`, `combo_id`, `origin_cohort_id`)
- execution hints (`llm_model_id`, `replicate`)

Stage assets augment it with file pointers and run-level metrics (`prompt_chars`, `total_tokens`, `duration_ms`, etc.). Treat this file as authoritative configuration for subsequent runs—the raw/parsed assets refuse to execute if required lineage fields are missing.

`raw_metadata.json` captures the expensive LLM call. When `ExperimentConfig.stage_config[stage].force` is `False` (the default), the raw helper reuses an existing `raw.txt` without changing the persisted metadata; setting `force=True` triggers a fresh generation and overwrites both the artifact and metadata. Parsed helpers follow the same contract—if `parsed.txt` already exists and the stage is not forced we leave the existing files untouched, otherwise we rewrite both `parsed.txt` and `parsed_metadata.json`.

## Regeneration & invariants

- Membership (`data/cohorts/<cohort_id>/membership.csv`) drives which `gen_id`s exist. If a generation directory is missing, first confirm the cohort membership row.
- Essays and evaluations require their parent generation on disk; assets raise `Err.INVALID_CONFIG` if `parent_gen_id` is absent.
- Whenever `raw.txt` exists, `raw_metadata.json` **must** be present. The raw helper raises `Err.DATA_MISSING` if it detects the file without metadata—delete the directory or rerun with `force=True` to repair. Parsed artifacts follow the same rule: `parsed.txt` and `parsed_metadata.json` travel together, and the helper falls back to regeneration only when either file is missing.
- Prompts are persisted by the stage-input assets. `prompt.txt` is reused when present and rewritten only when the stage is forced or missing input; treat it as the canonical `input_path` referenced in metadata.

## Programmatic access

Use the centralized helpers instead of constructing paths manually:

```python
from daydreaming_dagster.data_layer.paths import Paths
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer

paths = Paths.from_str("data")
layer = GensDataLayer.from_root(paths.data_root)

meta = layer.read_main_metadata("essay", "abc123xyz")
parsed = layer.read_parsed("essay", "abc123xyz")

# prompt.txt and input_path share the same location
print(paths.input_path("essay", "abc123xyz"))
```

## Troubleshooting

- **Missing directory** → confirm the `gen_id` is registered in cohort membership and that parents exist on disk.
- **Artifacts reused unexpectedly** → rerun the stage with `ExperimentConfig.stage_config[stage].force=True` (or trigger the Dagster asset with a `force` override) to regenerate artifacts in place if the existing files appear stale.
- **Parser failures** → inspect `parsed_metadata.json` for the `parser_name` and `error` preview, then open the corresponding `raw.txt`.
- **Prompt mismatch** → compare `prompt.txt` with the rendered inputs recorded in Dagster event metadata. The stage-input asset reuses the existing prompt unless the stage is forced or the file is missing.

## Notes

- `src/daydreaming_dagster/data_layer/paths.py` remains the authoritative definition of filenames and directories.
- The gens store intentionally avoids an external index; text files are the contract.

### Replicates

- Deterministic IDs encode replicate salts (e.g., `d_..._rep1`). Use `replicate` from `metadata.json` to group outputs.
- Aggregations such as `generation_scores_pivot` average replicates across identical task keys while respecting evaluator axes.
- Grouping keys exclude the raw `gen_id`; prefer `combo_id`, `stage`, `generation_template`, `generation_model`, and evaluator identifiers for cohort-level reporting.

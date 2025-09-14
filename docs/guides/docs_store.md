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

Preferred: use `load_generation` to read from the gens store.

Example:
```python
from pathlib import Path
from daydreaming_dagster.utils.generation import load_generation

gens_root = Path("data") / "gens"
gen = load_generation(gens_root, "essay", "abc123xyz")
text = gen["parsed_text"]  # or gen["raw_text"] / gen["prompt_text"] / gen["metadata"]
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
- The pipeline no longer depends on a SQLite index; the filesystem layout is the source of truth.

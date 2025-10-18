# DayDreaming Dagster Pipeline

Dagster-based pipeline exploring whether offline LLMs can reinvent the “Daydreaming Loop” via a structured, three-stage (draft → essay → evaluation) generation workflow. For the full set of project goals and guiding questions, start with [`docs/theory/project_goals.md`](docs/theory/project_goals.md).

## Start Here

DayDreaming treats **cohorts** as the contract between planning and execution. A cohort is a bundle of specs (YAML defined in [`docs/spec_dsl.md`](docs/spec_dsl.md)) that enumerates which prompts, models, and replicas we want to evaluate. The Dagster assets compile that spec into deterministic generation IDs and results on disk. To get oriented on how the pieces fit together:

1. **Learn what a spec contains.** Read the spec DSL guide to see how axes, allowlists, and structured couplings are authored. Most feature work involves extending these specs. [`docs/spec_dsl.md`](docs/spec_dsl.md)
2. **Follow the cohort lifecycle.** The cohorts guide explains how specs become manifests, `membership.csv`, and seeded generation metadata. It is the primary contract the code relies on. [`docs/cohorts.md`](docs/cohorts.md)
3. **Map the implementation.** Once the contract is clear, dive into the architecture overview to see which modules load the cohort bundle, register partitions, and run the unified stage pipeline. [`docs/architecture.md`](docs/architecture.md)
4. **Stay operational.** Keep the operating guide nearby for Dagster commands, partition tips, and maintenance tasks. [`docs/guides/operating_guide.md`](docs/guides/operating_guide.md)

Everything else in this README focuses on getting the environment running; the docs above carry the authoritative details for planning experiments and navigating the code.

## Quick Start

Prereqs: Python 3.9+, [uv](https://docs.astral.sh/uv/), and an OpenRouter API key stored in `.env`.

```bash
# Install dependencies and seed Dagster metadata
uv sync
export DAGSTER_HOME=$(pwd)/dagster_home

# Start Dagster UI (http://localhost:3000)
uv run dagster dev -f src/daydreaming_dagster/definitions.py

# Or materialize assets directly from the CLI
uv run dagster asset materialize --select "group:cohort" -f src/daydreaming_dagster/definitions.py
```

See the operating guide for partitioning examples, essay/draft pipelines, and score aggregation workflows.

## Reproducing the DayDreaming Experiments

No cohorts are pre-materialized in this repository; every run is generated locally from the cohort specs. Two reference cohort definitions ship as part of the source tree:

- `best_novelty_all_evals` &mdash; the original two-stage experiment (draft → essay → evaluation) that recreates Gwern's Daydreaming loop.
- `creative-synthesis-gap-v1` &mdash; a follow-up cohort focused on the same concept bundle with tweaked templates and evaluation rubrics. It uses the identical two-stage pipeline so the results are directly comparable.

While the cohorts themselves are local-only, we ship representative result essays under `data/results/` so you can inspect the intended outputs without re-running the pipeline.

Everything required to generate these cohorts lives under `data/`. To reproduce from scratch:

1. **Sync dependencies and point Dagster at your workspace.**
   ```bash
   uv sync
   export DAGSTER_HOME=$(pwd)/dagster_home
   ```
2. **Register the dynamic partitions for the cohort you want to run.**
   ```bash
   DD_IN_PROCESS=1 uv run dagster asset materialize \
     --select "register_cohort_partitions" \
     --partition "<cohort-id>" \
     -f src/daydreaming_dagster/definitions.py
   ```
   Replace `<cohort-id>` with `best_novelty_all_evals` or `creative-synthesis-gap-v1`.
3. **Materialize the generation stages.** Start with drafts, then essays, then evaluations; each command can be filtered to the cohort via Dagster's asset selection syntax. For example:
   ```bash
   DD_IN_PROCESS=1 uv run dagster asset materialize \
     --select "cohort_id,cohort_membership,content_combinations" \
     -f src/daydreaming_dagster/definitions.py

   DD_IN_PROCESS=1 uv run dagster asset materialize \
     --select "group:generation_draft" \
     -f src/daydreaming_dagster/definitions.py

   DD_IN_PROCESS=1 uv run dagster asset materialize \
     --select "group:generation_essay" \
     -f src/daydreaming_dagster/definitions.py

   DD_IN_PROCESS=1 uv run dagster asset materialize \
     --select "group:generation_evaluation" \
     -f src/daydreaming_dagster/definitions.py
   ```
4. **Aggregate and inspect scores.** The cohort reporting assets write CSV summaries under `data/cohorts/<cohort-id>/reports/` and copy the top ranked essays into `data/results/`.

For single-stage experiments (direct essay generation), skip the `group:generation_draft` materialization and rely on essay templates that read concept combinations directly. The cohort planning docs walk through how to author that variant.

The raw inputs that drive both experiments are versioned under `data/1_raw/`:

- `concepts/` &ndash; sentence, paragraph, and article descriptions for each concept in the synthesis set.
- `templates/draft/`, `templates/essay/`, `templates/evaluation/` &ndash; prompt scaffolding.
- `llm_models.csv` &ndash; canonical identifiers for the OpenRouter endpoints used in each stage.

### Model availability timeline

We intentionally limited runs to models that were publicly accessible before mid-2025. The file `data/openrouter_free_models.csv` captures the release dates of the free OpenRouter variants we targeted. Relevant cutovers:

- **DeepSeek R1 (reasoning)** &mdash; 20 Jan 2025
- **Gemini 2.5 Pro (free endpoint)** &mdash; 25 Mar 2025

Evaluation also uses `sonnet-4`; while Anthropic does not publish a formal cutover in the CSV, the model ID is frozen in `data/1_raw/llm_models.csv` so the exact endpoint is reproducible. When swapping to newer checkpoints, document the release date alongside the cohort for apples-to-apples comparisons.

### Outputs and manual verification

The pipeline copies the highest-scoring essays into `data/results/` for quick inspection. Currently you will find five files:

| Cohort | Essay IDs | Why they matter |
| --- | --- | --- |
| `best_novelty_all_evals` | `e_sq5klak2lljyyom`, `e_1cx6440bb5zj9466`, `e_4nqjjtqtnpxlljlz` | Original two-stage run where older models (DeepSeek R1, Gemini 2.5 Pro) independently rediscovered the Daydreaming mechanism. |
| `creative-synthesis-gap-v1` | `e_2b4k6tvo4kf7ibjh`, `e_3hdt16ed4bh528s0` | Follow-up run that tweaks templates yet still delivers essays with the same mechanism plus fresh supporting detail. |

Use the LLM scores to rank candidates, but confirm the behaviour yourself: each essay should (1) restate the generator–verifier loop and feedback flywheel, (2) add its own insights beyond the concept bullet points, and (3) stay readable despite relying on the pre–Q2 2025 free endpoints listed above.

## Development Cheatsheet

- Tests: `.venv/bin/pytest` (unit tests under `src/daydreaming_dagster/`, integration tests under `tests/`).
- Formatting: `uv run black .`
- Linting: `uv run ruff check`
- Conventions & expectations: [`AGENTS.md`](AGENTS.md)

Storage paths, data layouts, and cohort conventions are documented once in the architecture and cohort guides—refer there instead of duplicating notes here.

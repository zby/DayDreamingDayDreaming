# DayDreaming Dagster Pipeline

Dagster-based pipeline exploring whether offline LLMs can reinvent the “Daydreaming Loop” via structured, two-phase generation and evaluation. For the full set of project goals and guiding questions, start with [`docs/theory/project_goals.md`](docs/theory/project_goals.md).

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

## Development Cheatsheet

- Tests: `.venv/bin/pytest` (unit tests under `src/daydreaming_dagster/`, integration tests under `tests/`).
- Formatting: `uv run black .`
- Linting: `uv run ruff check`
- Conventions & expectations: [`AGENTS.md`](AGENTS.md)

Storage paths, data layouts, and cohort conventions are documented once in the architecture and cohort guides—refer there instead of duplicating notes here.

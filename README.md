# DayDreaming Dagster Pipeline

Dagster-based pipeline exploring whether offline LLMs can reinvent the “Daydreaming Loop” via structured, two-phase generation and evaluation.

## Start Here

If you're joining the project, work through the essentials below before diving into a feature:

1. **Map the problem space.** Read the project goals and roadmap to understand why the pipeline exists and what "Daydreaming" means in practice. [`docs/theory/project_goals.md`](docs/theory/project_goals.md)
2. **Trace the data flow.** Skim the architecture reference for how the Dagster assets compose, where data lands on disk, and which modules own each stage. [`docs/architecture.md`](docs/architecture.md)
3. **Learn the cohort loop.** Review how cohorts are defined, scheduled, and evaluated so you can run or extend experiments without breaking the contract. [`docs/cohorts.md`](docs/cohorts.md)
4. **Author specs confidently.** Keep the spec DSL handy when you need to add prompts, models, or evaluation rules. [`docs/spec_dsl.md`](docs/spec_dsl.md)
5. **Stay operational.** Bookmark the operating guide for routine Dagster commands, troubleshooting tips, and cohort maintenance checklists. [`docs/guides/operating_guide.md`](docs/guides/operating_guide.md)

This README keeps only the minimum needed to get the project running—treat it as the launchpad for those docs.

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

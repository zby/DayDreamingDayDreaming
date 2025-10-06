# DayDreaming Dagster Pipeline

Dagster-based pipeline exploring whether offline LLMs can reinvent the “Daydreaming Loop” via structured, two-phase generation and evaluation.

## Start Here

Most topics now live in the `docs/` tree—jump straight to the detailed guides when you need depth:

- Project overview & roadmap: [`docs/index.md`](docs/index.md)
- Goals & scope: [`docs/project_goals.md`](docs/project_goals.md)
- Architecture reference: [`docs/architecture/architecture.md`](docs/architecture/architecture.md)
- Operating guide & troubleshooting: [`docs/guides/operating_guide.md`](docs/guides/operating_guide.md)
- Cohort workflow: [`docs/cohorts.md`](docs/cohorts.md)

This README keeps only the minimum needed to get the project running.

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

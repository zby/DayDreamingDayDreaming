# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python experiment testing whether pre-June 2025 LLMs can "reinvent" Gwern's Daydreaming Loop concept through combinatorial testing. The system uses Dagster for data pipeline orchestration with asset-based architecture.

## Development Commands

### Installation
```bash
uv sync                    # Install project and dependencies
uv sync --dev             # Install with development dependencies
```

### Running the Pipeline
```bash
# Start Dagster UI (set DAGSTER_HOME first)
export DAGSTER_HOME=$(pwd)/dagster_home
uv run dagster dev -f src/daydreaming_dagster/definitions.py

# Materialize assets
uv run dagster asset materialize --select "group:cohort" -f src/daydreaming_dagster/definitions.py
uv run dagster asset materialize --select "group:generation_draft" --partition <gen_id> -f src/daydreaming_dagster/definitions.py
uv run dagster asset materialize --select "group:generation_essays" --partition <gen_id> -f src/daydreaming_dagster/definitions.py
uv run dagster asset materialize --select "group:evaluation" --partition <gen_id> -f src/daydreaming_dagster/definitions.py
```

### Testing & Code Quality
```bash
# Tests (prefer .venv/bin/pytest over uv run pytest)
.venv/bin/pytest                              # Run all tests
.venv/bin/pytest src/daydreaming_dagster/     # Unit tests only
.venv/bin/pytest tests/                       # Integration tests only
.venv/bin/pytest -k "test_name"              # Run specific test

# Code quality
uv run black .                                # Format code
uv run ruff check                             # Check code style
uv run radon cc src/                          # Cyclomatic complexity
```

Note: prefer `.venv/bin/pytest` instead of `uv run pytest` — uv may attempt to write to a global cache outside the sandboxed filesystem, which can fail under restricted execution.

## Repository Layout

- `src/daydreaming_dagster/`: Main package (`assets/`, `resources/`, `models/`, `utils/`, `definitions.py`)
- `tests/`: Integration tests & fixtures; **unit tests live next to code** in `src/daydreaming_dagster/`
- `data/`: Pipeline inputs/outputs (`1_raw/`, `gens/`, `cohorts/`, `7_cross_experiment/`)
- `scripts/`: Results analysis and maintenance scripts
- `docs/`: Architecture documentation (see `docs/theory/project_goals.md`)

## Key Guidelines

### Development Workflow

- **TDD approach**: Write/adjust tests first; run the **narrowest** checks possible (use `-k` flags, node IDs, single partitions)
- **Unit tests** (colocated with source): No file I/O or network; mock LLMs/IO/resources; target <1s per test
- **Integration tests** (`tests/`): May read from `data/`; must not hit real APIs
- Prefer pure functions & dependency injection
- Validate inputs early with clear error messages

### Error Handling: Codes Over Prose

Use structured errors defined in `src/daydreaming_dagster/utils/errors.py`:

```python
from daydreaming_dagster.utils.errors import DDError, Err

# Deep layer - raise with code and context
raise DDError(Err.MISSING_TEMPLATE, ctx={"template_id": tid})

# Boundary layer - add context, log once, re-raise
try:
    process_template(tid)
except DDError as e:
    e.ctx.update({"cohort": cohort_id})
    logger.error(f"Template processing failed: {e.code.name}", extra=e.ctx)
    raise
```

**Testing errors**: Assert `err.code is Err.X` and check minimal `ctx`, **not** message text.

### Simplicity-First Refactoring

When refactoring, follow the contract in `tools/refactor_contract.yaml`:

**MUST preserve:**
- Public APIs declared stable
- File formats & schemas
- Error types/codes (and exit statuses)

**MAY change:**
- Error/log messages (wording)
- Call graph & helper boundaries
- Private data structures

**SHOULD drop:**
- Passing values across layers just to reproduce legacy error strings
- `try/except` blocks that only add message text
- Ad-hoc logging in hot paths (prefer **one boundary** that logs)

### Code Style & Commits

- Black (88 cols), 4 spaces, UTF-8; Ruff advisory
- Naming: `snake_case` (functions/modules), `PascalCase` (classes)
- **Commit hygiene**: Do **not** mix formatting/lint changes with logic changes
- **Staging**: Never use `git add -A`; stage only intentional paths
- Commit formatting changes separately: `style: format with Black`

### Backcompat & Cleanup Tags

- Remove scaffolding & debug prints after larger changes
- Backcompat is narrow & time-boxed; tag with:
  - `BACKCOMPAT:` rationale + link + removal target
  - `TEMPORARY:` scope + removal trigger
  - `TODO-REMOVE-BY:` date or milestone
- Fallbacks allowed only to stay unblocked: `FALLBACK(DATA|PARSER|OPS): ...`

### Security & Data

- Never commit secrets or real outputs; use `.env`/env vars (e.g., `OPENROUTER_API_KEY`, `DAGSTER_HOME`)
- Set `DAGSTER_HOME` to an absolute path: `export DAGSTER_HOME=$(pwd)/dagster_home`
- Large generated folders under `data/` should be git-ignored unless required for tests/docs
- **Do not edit** `data/1_raw/templates/draft/gwern_original.txt` - it's copied from the essay

### Paths

- Single source of truth: `src/daydreaming_dagster/data_layer/paths.py`
- Use `Paths` helpers over string concatenation

### Plans Policy

- Do **not** commit files under `temporary_plans/` or similar scratch directories

## Project Goals (Summary)

- **Purpose**: Demonstrate DayDreaming-style workflows enabling pre-Jun-2025 offline LLMs to generate novel, falsifiable ideas
- **Strategy**: Existence proof over a finite concept space
- **Success criteria**: All novel elements present with causal justification; technically plausible; ≥7.5/10 average evaluations
- **Scope**: Drafting → essay → evaluation; out-of-scope: ranking heuristics, retrieval/browsing, target-specific hints

## Useful Tools

### ast-grep quick reference
```bash
# Search
uv run sg -e '<pattern>' -g 'src/daydreaming_dagster/**/*.py' -n 3

# Structural search
uv run sg -p "call[name='read_text']" -g 'src/daydreaming_dagster/**/*.py'

# Rewrite (dry-run)
uv run sg -r rule.yml --rewrite --diff -g 'src/daydreaming_dagster/**/*.py'
```

---

**Default posture**: Small steps, fast feedback, simpler code. Preserve the **contract**; let the **prose** go.

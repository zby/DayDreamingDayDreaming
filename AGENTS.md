# Repository Guidelines

## Project Structure & Module Organization
- `daydreaming_dagster/`: Dagster package with `assets/`, `resources/`, `models/`, `utils/`, and `definitions.py` (entry for the asset graph).
- `tests/`: Integration tests and fixtures; unit tests live alongside modules in `daydreaming_dagster/`.
- `data/`: Pipeline inputs/outputs (`1_raw/`, `2_tasks/`, `3_generation/`, `4_evaluation/`, `5_parsing/`, `6_summary/`). Do not commit secrets or proprietary outputs.
- `scripts/`: Support scripts for results tables and maintenance.
- `docs/`: Architecture and development notes; see `docs/project_goals.md` for project goals.

## Build, Test, and Development Commands
- Install: `uv sync` (use `uv sync --dev` for Black/Ruff).
- Dagster UI: `uv run dagster dev -f daydreaming_dagster/definitions.py` (set `DAGSTER_HOME=./dagster_home`).
- Materialize setup assets: `uv run dagster asset materialize --select "group:raw_data,group:task_definitions" -f daydreaming_dagster/definitions.py`.
- Two‑phase generation (split tasks):
  - Links: `uv run dagster asset materialize --select "group:generation_links" --partition <link_task_id> -f daydreaming_dagster/definitions.py`.
  - Essays: `uv run dagster asset materialize --select "group:generation_essays" --partition <essay_task_id> -f daydreaming_dagster/definitions.py`.
- Tests: `uv run pytest` (unit: `uv run pytest daydreaming_dagster/`, integration: `uv run pytest tests/`).
- Format/lint: `uv run black .` and `uv run ruff check`.

## Coding Style & Naming Conventions
- Formatting: Black (line length 88); 4‑space indentation; UTF‑8 files.
- Linting: Ruff (advisory; keep warnings low before PR).
- Naming: `snake_case` functions/modules, `PascalCase` classes, tests as `test_*.py`.
- Type hints encouraged; prefer pure functions and dependency injection for testability.
 - Style-only changes must be separate commits. Do not mix formatting/lint fixes with functional changes. Run Black/Ruff after your functional commit, then commit the style diff as `style: format with Black` (or similar).

## Testing Guidelines
- Framework: Pytest with coverage config in `pyproject.toml` (target sensible coverage; current hard gate is 0).
- Structure: fast unit tests colocated with code; integration tests in `tests/` may read from `data/` but must not hit real APIs (mock LLM calls).
- Conventions: functions `test_*`, files `test_*.py`; use fixtures in `tests/fixtures/` where possible.

## Commit & Pull Request Guidelines
- Commits: imperative mood, concise subject (≤72 chars), include scope when helpful (e.g., "assets: two‑phase generation").
 - Separation policy: if a change includes both logic and formatting, split into two commits (logic first, then style). This keeps review diffs readable and aligns with our LLM workflow.
- PRs: clear description, linked issues, test plan (`pytest` output), and, when relevant, screenshots of Dagster runs or sample artifacts. Note any data/schema changes.

## Security & Configuration Tips
- Secrets: never commit API keys or real outputs; use a local `.env` and environment variables (`OPENROUTER_API_KEY`, `DAGSTER_HOME`).
- Data: large generated folders under `data/` should be git‑ignored unless explicitly needed for tests/docs.

## Agent‑Specific Notes
- Follow TDD: write/adjust tests before implementation; keep unit vs. integration boundaries. See `CLAUDE.md` for deeper conventions and patterns.

## Project Goals (Summary)
- Purpose: Demonstrate that DayDreaming‑style workflows enable pre‑June‑2025 offline LLMs to generate genuinely novel ideas; use DayDreaming LLMs as a falsifiable benchmark while maintaining generality beyond this target.
- Benchmark strategy: Existence goal over a finite concept‑combination space; fixed inputs stay target‑neutral; derived inputs may include benchmark terms if model‑produced; scaffolding should surface other novel syntheses.
- Success criteria: Includes all current novel elements, presents a coherent causal justification, is technically plausible and non‑derivative, and meets evaluation thresholds (e.g., ≥7.5/10 avg) with agreement plus binary gates (reinvention yes/no; all novel elements present).
- Scope: In‑scope idea generation (currently two‑phase links → essay) with evaluation and minimal bookkeeping; out‑of‑scope search/ranking heuristics, retrieval/browsing, or target‑specific hints.
- Design guidance: Templates and concepts must be target‑neutral, parseable, general, stable/versioned; concepts come from a universal pool with traceable metadata distinguishing pool vs. experiment subset.
- Evaluation: Score across axes (mechanistic completeness, structure, justification, novelty, grounding/coherence, coverage of novel elements) and apply binary gates; track inter‑evaluator variance and rubric versions.
- DayDreaming checklist: Multi‑stage process with state; feedback signals; memory/externalization/tool use; selection/filters; termination/roll‑over criteria.

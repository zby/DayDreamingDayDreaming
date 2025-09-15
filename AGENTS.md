# AGENTS.md — Quick Guide

Concise, high-signal rules for working in this repo. Keep changes focused, validated early, and aligned with reproducibility.

## Project Layout
- `src/daydreaming_dagster/`: Dagster package (`assets/`, `resources/`, `models/`, `utils/`, `definitions.py`).
- `tests/`: Integration tests and fixtures; unit tests live next to code in `daydreaming_dagster/`.
- `data/`: Inputs/outputs (`1_raw/`, `2_tasks/`, `gens/`, `5_parsing/`, `6_summary/`). Don’t commit secrets or proprietary outputs.
- `scripts/`: Results tables and maintenance.
- `docs/`: Architecture/notes; see `docs/project_goals.md`.

## Setup & Key Commands
- Install: `uv sync` (dev tools: `uv sync --dev`).
- Use project venv: prefer `.venv/bin/python` and `.venv/bin/pytest` (or `uv run <cmd>`).
- Dagster UI (src layout): `export DAGSTER_HOME=$(pwd)/dagster_home && uv run dagster dev -f src/daydreaming_dagster/definitions.py`.
- Auto-rematerialize on `data/1_raw/**/*` changes: run Dagster with the daemon as above.
- Seed once: `uv run dagster asset materialize --select "group:cohort" -f src/daydreaming_dagster/definitions.py`.
- Two‑phase generation:
  - Drafts: `uv run dagster asset materialize --select "group:generation_draft" --partition <gen_id> -f src/daydreaming_dagster/definitions.py`
  - Essays: `uv run dagster asset materialize --select "group:generation_essays" --partition <gen_id> -f src/daydreaming_dagster/definitions.py`
- Tests (src layout): unit `.venv/bin/pytest src/daydreaming_dagster/`; integration `.venv/bin/pytest tests/`.
- Tip: For ad‑hoc scripts, use `PYTHONPATH=src` or install the package (`pip install -e .`) so `daydreaming_dagster` is importable.

## Development Conventions
- Style: Black (88 cols), 4 spaces, UTF‑8; Ruff advisory.
- Naming: `snake_case` functions/modules; `PascalCase` classes; tests `test_*.py`.
- Types: add hints; prefer pure functions and dependency injection.
- Commit hygiene: do not mix formatting/lint with logic. Make functional change first; then `style: format with Black`.
- Staging: never `git add -A`. Stage only intentional paths (code vs data vs docs).

## Testing
- Unit tests (colocated): no file I/O; no network; mock external deps (LLMs, IO, resources). Target <1s per test.
- Integration tests (`tests/`): may read `data/`; must not hit real APIs; fail if required local data missing.
- Test early, test often:
  - Make one focused change → run narrowest relevant test(s).
  - Prefer fastest checks: specific test node (`-k`/node id), single-partition asset materialize, `ruff`/`black --check` for style-only.
  - Add an explicit “Run tests for X” step to your plan before moving on.

## Approvals & Sandboxing
- Interactive modes: propose exact commands; run once approved.
- Non‑interactive: run the minimal validations proactively.

## Cleanup, Backcompat, and Tags
- After larger changes: remove scaffolding, dead code, debug prints. Update docs/comments affected.
- Backcompat: keep narrow, clearly tagged where declared; plan removal.
  - `BACKCOMPAT:` rationale + link + target removal.
  - `TEMPORARY:` scope + removal trigger.
  - `TODO-REMOVE-BY:` date or milestone.
- Fallbacks: avoid automatic data fixes at runtime. If required to stay unblocked, keep minimal and tagged:
  - `FALLBACK(DATA|PARSER|OPS):` why it exists + removal plan.

Plans directory policy
- Do not commit files under `plans/`. These are working notes and should not be included in functional commits.

## Security & Data
- Never commit secrets/real outputs. Use `.env` and env vars (e.g., `OPENROUTER_API_KEY`, `DAGSTER_HOME`).
- Set `DAGSTER_HOME` to an absolute path, e.g., `export DAGSTER_HOME=$(pwd)/dagster_home`.
- Large generated folders under `data/` should be git‑ignored unless explicitly needed for tests/docs.

## ast‑grep (sg) Quick Reference
- Search snippets: `uv run sg -e '<pattern>' -g 'src/daydreaming_dagster/**/*.py' -n 3`.
- Structural search: `uv run sg -p "call[name='read_text']" -g 'src/daydreaming_dagster/**/*.py'`.
- Rewrite (dry‑run): `uv run sg -r rule.yml --rewrite --diff -g 'src/daydreaming_dagster/**/*.py'`.
- Tips: prefer `-e` for quick greps; use `-p`/rules for precise refactors; always scope with `-g`.

## Design Principles
- Dependency injection for testability; avoid creating external services internally.
- Fail early; validate inputs/invariants with clear errors over silent tolerance.

Storage conventions
- Single source of truth for storage and path rules: `src/daydreaming_dagster/config/paths.py`. Prefer using `Paths` helpers over string concatenation in code and tests.

## Project Goals (Summary)
- Purpose: show DayDreaming‑style workflows enable pre‑Jun‑2025 offline LLMs to produce genuinely novel ideas; serves as a falsifiable benchmark while staying target‑neutral.
- Strategy: existence goal over finite concept‑combination space; fixed inputs remain neutral; derived inputs may include benchmark terms if model‑produced.
- Success: all novel elements present; coherent causal justification; technically plausible; ≥7.5/10 average evaluations; agreement + binary gates (reinvention yes/no; all novel elements present).
- Scope: in‑scope generation (drafts → essay), evaluation, minimal bookkeeping; out‑of‑scope ranking heuristics, retrieval/browsing, target‑specific hints.

## Agent Notes
- Follow TDD: write/adjust tests first; keep unit vs integration boundaries.
- Prefer explicit plans; use small iterations; validate after each step.
- For expensive paths, use dry‑runs, single partitions, or filtered tests.

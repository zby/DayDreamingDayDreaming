# Repository Guidelines

## Project Structure & Module Organization
- `daydreaming_dagster/`: Dagster package with `assets/`, `resources/`, `models/`, `utils/`, and `definitions.py` (entry for the asset graph).
- `tests/`: Integration tests and fixtures; unit tests live alongside modules in `daydreaming_dagster/`.
- `data/`: Pipeline inputs/outputs (`1_raw/`, `2_tasks/`, `3_generation/`, `4_evaluation/`, `5_parsing/`, `6_summary/`). Do not commit secrets or proprietary outputs.
- `scripts/`: Support scripts for results tables and maintenance.
- `docs/`: Architecture and development notes; see `docs/project_goals.md` for project goals.

## Build, Test, and Development Commands
- Install: `uv sync` (use `uv sync --dev` for Black/Ruff).
- Dagster UI: `uv run dagster dev -f daydreaming_dagster/definitions.py` (set `DAGSTER_HOME=$(pwd)/dagster_home`).
- Auto-update upstream assets: start Dagster with the daemon (`export DAGSTER_HOME=$(pwd)/dagster_home && uv run dagster dev -f daydreaming_dagster/definitions.py`). Raw loaders and task definitions auto-rematerialize when `data/1_raw/**/*` changes.
- Optional seed (once): `uv run dagster asset materialize --select "group:task_definitions" -f daydreaming_dagster/definitions.py`.
- Two‑phase generation (split tasks):
  - Drafts: `uv run dagster asset materialize --select "group:generation_draft" --partition <draft_task_id> -f daydreaming_dagster/definitions.py` (legacy: `group:generation_links` with `link_task_id`).
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

### Unit vs. Integration Tests
- Unit tests:
  - Colocated with modules under `daydreaming_dagster/`.
  - Must not access files under `data/` or make network calls; mock external deps (LLMs, file I/O, Dagster resources).
  - Aim for very fast execution (<1s per test) and narrow scope.
- Integration tests:
  - Live under `tests/` only; exercise multiple components together.
  - Can read from `data/` when validating data‑dependent behavior.
  - Must not make real API calls (use stubs/mocks); fail if required local data is missing.

### Test Early, Test Often (Plan–Execute–Validate)
- Always test immediately when it is possible to test. Do not batch multiple code changes before running tests.
- Keep iterations small: make one focused change, run the narrowest relevant test(s), then proceed.
- Prefer the fastest checks first:
  - Unit scope: `uv run pytest daydreaming_dagster/utils/test_foo.py::TestFoo::test_bar` or `-k "bar and TestFoo"`.
  - Asset scope: materialize only the changed asset/partition with Dagster CLI, not the whole graph.
  - Lint/format: `ruff`/`black --check` for style-only edits.
- Define validation steps in your plan: after each implementation step, add a concrete “Run tests for X” step and execute it before moving on.
- Use dry-runs and subsets for expensive paths: prefer `--dry-run`, single-partition materializations, or filtered test sets over full runs.
- Approvals/sandboxing awareness:
  - Interactive modes: propose the exact test command and run it as soon as approved.
  - Non-interactive modes (e.g., never/on-failure): run the minimal tests proactively to validate your change.
- Examples of “possible to test” right away:
  - Edited a pure function or parser with existing unit tests → run those tests immediately.
  - Modified one asset → materialize that asset for one representative partition.
  - Adjusted a script argument → run the script with a tiny fixture input.
  - Docs-only edits → no test run required; optionally run link or linter checks if configured.

## Commit & Pull Request Guidelines
- Commits: imperative mood, concise subject (≤72 chars), include scope when helpful (e.g., "assets: two‑phase generation").
 - Separation policy: if a change includes both logic and formatting, split into two commits (logic first, then style). This keeps review diffs readable and aligns with our LLM workflow.
- PRs: clear description, linked issues, test plan (`pytest` output), and, when relevant, screenshots of Dagster runs or sample artifacts. Note any data/schema changes.

### Staging Rules (Important)
- Do not use `git add -A`. This can accidentally stage unrelated files, large artifacts, or local cruft. Instead, explicitly stage only the intended paths (e.g., `git add daydreaming_dagster/assets/core.py` or `git add data/1_raw/...`).
- Separate commits by scope:
  - Data-only changes: stage files under `data/` explicitly.
  - Code-only changes: stage only source files and tests.
  - Docs-only changes: stage only `docs/` or specific markdown files.
- Never mix functional changes and formatting in the same commit; format in a follow-up commit.

## Security & Configuration Tips
- Secrets: never commit API keys or real outputs; use a local `.env` and environment variables (`OPENROUTER_API_KEY`, `DAGSTER_HOME`).
  - Set `DAGSTER_HOME` as an absolute path, e.g., `export DAGSTER_HOME=$(pwd)/dagster_home`.
- Data: large generated folders under `data/` should be git‑ignored unless explicitly needed for tests/docs.

## Agent‑Specific Notes
- Follow TDD: write/adjust tests before implementation; keep unit vs. integration boundaries. See `CLAUDE.md` for deeper conventions and patterns.

### Plans (Do Not Commit by Default)
- Do not commit files under `plans/` unless explicitly requested. Plans are working notes and should typically stay local.
- If a plan is useful to persist, ask for approval first and commit it in a docs-only change.
- Prefer the `update_plan` tool for live task plans and progress tracking instead of writing/committing plan files.

### Using ast-grep (sg) for Code Search/Edits
- Install tools: `uv sync --dev` (sg binary is provided by the `ast-grep-cli` dev dependency). Run with `uv run sg ...`.
- Quick snippet search (example‑driven):
  - `uv run sg -e 'def evaluation_tasks(' -l` — list files defining `evaluation_tasks`.
  - `uv run sg -e 'document_index(' -n 3` — show 3 lines of context for matches.
  - Add a glob to limit scope: `-g 'daydreaming_dagster/**/*.py'`.
- Structural search (pattern DSL):
  - `uv run sg -p 'call[name="read_text"]' -g 'daydreaming_dagster/**/*.py'` — find calls named `read_text`.
  - `uv run sg -p 'function_definition[name="evaluation_prompt"]'` — find a function by name.
- JSON output for tooling: `uv run sg -e 'evaluation_prompt(' --json`.
- Safe rewriting via rules (YAML):
  1) Create `rule.yml`:
     ```yaml
     rule:
       id: rename-func
       language: python
       pattern: |
         def $NAME($PARGS):
           $BODY
       constraints:
         NAME: { regex: "^old_name$" }
       fix: |
         def new_name($PARGS):
           $BODY
     ```
  2) Dry‑run to preview: `uv run sg -r rule.yml --rewrite --diff -g 'daydreaming_dagster/**/*.py'`
  3) Apply once reviewed: `uv run sg -r rule.yml --rewrite -g 'daydreaming_dagster/**/*.py'`
- Tips:
- Prefer snippet `-e` for quick greps; switch to `-p`/rules for precise structure or refactors.
- Always use `-g` to scope searches to the repo path you intend.
- For broad text replacements, keep sg as a guardrail (structural match) and review diffs before committing.

## Design Principles
- Dependency injection for testability: pass external services (LLM clients, IO managers, configs) into functions/classes rather than creating them internally, so units are easy to mock and verify in isolation.
- Fail early rather than over‑defensive code: validate inputs and invariants up front and raise clear errors with actionable metadata, instead of silently tolerating bad state.

## Project Goals (Summary)
- Purpose: Demonstrate that DayDreaming‑style workflows enable pre‑June‑2025 offline LLMs to generate genuinely novel ideas; use DayDreaming LLMs as a falsifiable benchmark while maintaining generality beyond this target.
- Benchmark strategy: Existence goal over a finite concept‑combination space; fixed inputs stay target‑neutral; derived inputs may include benchmark terms if model‑produced; scaffolding should surface other novel syntheses.
- Success criteria: Includes all current novel elements, presents a coherent causal justification, is technically plausible and non‑derivative, and meets evaluation thresholds (e.g., ≥7.5/10 avg) with agreement plus binary gates (reinvention yes/no; all novel elements present).
- Scope: In‑scope idea generation (currently two‑phase drafts → essay) with evaluation and minimal bookkeeping; out‑of‑scope search/ranking heuristics, retrieval/browsing, or target‑specific hints.
- Design guidance: Templates and concepts must be target‑neutral, parseable, general, stable/versioned; concepts come from a universal pool with traceable metadata distinguishing pool vs. experiment subset.
- Evaluation: Score across axes (mechanistic completeness, structure, justification, novelty, grounding/coherence, coverage of novel elements) and apply binary gates; track inter‑evaluator variance and rubric versions.
- DayDreaming checklist: Multi‑stage process with state; feedback signals; memory/externalization/tool use; selection/filters; termination/roll‑over criteria.

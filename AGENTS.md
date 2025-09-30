# AGENTS.md — Quick Guide (Simplicity‑First)

**Goal:** Keep changes small, test‑driven, and reproducible. **Preserve contracts; drop incidental behavior.**

---

## 1) Repo Basics

**Layout**

* `src/daydreaming_dagster/`: Dagster package (`assets/`, `resources/`, `models/`, `utils/`, `definitions.py`)
* `tests/`: integration tests & fixtures; **unit tests live next to code** in `src/daydreaming_dagster/`
* `data/`: inputs/outputs (`1_raw/`, `2_tasks/`, `gens/`, `5_parsing/`, `6_summary/`) — **never commit secrets or proprietary outputs**
* `scripts/`: results and maintenance
* `docs/`: architecture/notes; see `docs/project_goals.md`

**Setup & Key Commands**

* Install deps: `uv sync` (dev tools: `uv sync --dev`)
* Use venv: prefer `.venv/bin/python`, `.venv/bin/pytest` (or `uv run <cmd>`)
* Dagster UI (src layout):
  `export DAGSTER_HOME=$(pwd)/dagster_home && uv run dagster dev -f src/daydreaming_dagster/definitions.py`
* Seed once:
  `uv run dagster asset materialize --select "group:cohort" -f src/daydreaming_dagster/definitions.py`
* Two‑phase generation:

  * Drafts: `uv run dagster asset materialize --select "group:generation_draft" --partition <gen_id> -f src/.../definitions.py`
  * Essays: `uv run dagster asset materialize --select "group:generation_essays" --partition <gen_id> -f src/.../definitions.py`
  * Evaluations: `uv run dagster asset materialize --select "group:evaluation" --partition <gen_id> -f src/.../definitions.py`
* Tests: unit `.venv/bin/pytest src/daydreaming_dagster/`; integration `.venv/bin/pytest tests/`
* Tip for ad‑hoc scripts: `PYTHONPATH=src` or `pip install -e .`

---

## 2) Day‑to‑Day Dev Loop

* **TDD**: write/adjust tests first; run the **narrowest** checks (node ids, `-k`, single partition)
* **Unit tests** (colocated): no file I/O or network; mock LLMs/IO/resources; target <1s/test
* **Integration tests** (`tests/`): may read `data/`; must not hit real APIs
* Prefer pure functions & dependency injection; validate inputs early with clear errors

---

## 3) Simplicity‑First Refactor Contract

Keep a one‑page contract at `tools/refactor_contract.yaml`.

**MUST preserve**

* Public APIs declared stable
* File formats & schemas
* Error **types/codes** (and exit statuses)

**MAY change**

* Error/log **messages** (wording)
* Call graph & helper boundaries
* Private data structures

**SHOULD drop**

* Passing values across layers just to reproduce legacy error strings
* `try/except` blocks that only add message text
* Ad‑hoc logging in hot paths (prefer **one boundary** that logs)

**Success**

* Tests pass; complexity trends down (see CI gates)
* No reliance on string‑matching error messages

---

## 4) Error Handling: Codes over Prose

Deep layers raise structured errors; boundaries format/log.

```python
# src/daydreaming_dagster/utils/errors.py
from enum import Enum, auto

class Err(Enum):
    MISSING_TEMPLATE = auto()
    INVALID_MEMBERSHIP = auto()
    UNKNOWN = auto()

class DDError(Exception):
    def __init__(self, code: Err, *, ctx: dict | None = None, cause: Exception | None = None):
        self.code, self.ctx = code, (ctx or {})
        self.__cause__ = cause
        super().__init__(code.name)
```

* **Deep layer**: `raise DDError(Err.MISSING_TEMPLATE, ctx={"template_id": tid})`
* **Boundary**: add cheap context (e.g., `cohort`, `gen_id`), log once, re‑raise
* Tests assert `err.code is Err.X` (and minimal `ctx`), **not** message text
* If temporary prose is needed, add a tiny mapper `legacy_message(e)` tagged `BACKCOMPAT:` with a removal date

---

## 5) CI Gates: Complexity Budget

Automate: `radon cc` (cyclomatic), `cloc` (LOC), `ruff`/`black --check`.

**PR fails if** complexity ↑ without stronger tests or an explicit rationale.
**Guidelines**: aim for −15% LOC / −20% branches on hot paths over time; avoid nested exception ladders.

---

## 6) LLM‑Assisted Refactors (Scaffolding)

**System preamble (paste verbatim)**

```
You are refactoring for simplicity-first. Preserve only the contract in tools/refactor_contract.yaml.
Eliminate incidental equivalence. Prefer deleting code to preserving low-value behaviors.
Never introduce new data plumbing solely to keep old error strings.
```

**Task trailer**

```
IMPORTANT:
- Keep error types/codes stable; messages/logs may change.
- Move exception handling to a single boundary; deep functions raise DDError(code, ctx).
- Collapse try/except ladders that only decorate messages.
- Add a short REFactor_NOTES.md summarizing dropped incidental behavior.
```

---

## 7) Reviewer Checklist (paste in PR)

* [ ] Any data passed only to reproduce legacy error prose? **Remove it.**
* [ ] Error **codes/types** unchanged; message text may differ
* [ ] Deep layers: `DDError(code, ctx)` only (no string formatting)
* [ ] One boundary formats/logs user‑facing text
* [ ] Tests assert codes/types (and minimal ctx), not message strings
* [ ] Complexity budget met (or justified with tests)
* [ ] Cleanups done (dead code, scaffolding, debug prints); `REFactor_NOTES.md` added if deviating

---

## 8) Conventions & Operational Rules

**Style & structure**

* Black (88 cols), 4 spaces, UTF‑8; Ruff advisory
* Naming: `snake_case` (fns/modules), `PascalCase` (classes), tests `test_*.py`
* Commit hygiene: do **not** mix formatting/lint with logic; make the change, then `style: format with Black`
* Staging: never `git add -A`; stage only intentional paths

**Approvals & sandboxing**

* Interactive modes: propose exact commands and run once approved
* Non‑interactive: proactively run minimal validations

**Cleanup, backcompat, tags**

* Remove scaffolding & debug prints after larger changes
* Backcompat is narrow & time‑boxed; tag with:

  * `BACKCOMPAT:` rationale + link + removal target
  * `TEMPORARY:` scope + removal trigger
  * `TODO-REMOVE-BY:` date or milestone
* Fallbacks allowed only to stay unblocked; keep minimal and tag `FALLBACK(DATA|PARSER|OPS): ...`
* **Plans policy:** do **not** commit files under `temporary_plans/`

**Security & data**

* Never commit secrets or real outputs; use `.env`/env vars (e.g., `OPENROUTER_API_KEY`, `DAGSTER_HOME`)
* Set `DAGSTER_HOME` to an absolute path (e.g., `export DAGSTER_HOME=$(pwd)/dagster_home`)
* Large generated folders under `data/` should be git-ignored unless required for tests/docs

**Paths**

* Single source of truth: `src/daydreaming_dagster/data_layer/paths.py` — use `Paths` helpers over string concat

**ast‑grep quick ref**

* Search: `uv run sg -e '<pattern>' -g 'src/daydreaming_dagster/**/*.py' -n 3`
* Structural: `uv run sg -p "call[name='read_text']" -g 'src/daydreaming_dagster/**/*.py'`
* Rewrite (dry-run): `uv run sg -r rule.yml --rewrite --diff -g 'src/daydreaming_dagster/**/*.py'`

---

## 9) Project Goals (short)

* **Purpose:** demonstrate DayDreaming‑style workflows enabling pre‑Jun‑2025 offline LLMs to generate novel, falsifiable ideas
* **Strategy:** existence proof over a finite concept space; fixed inputs remain neutral; derived inputs may include benchmark terms if model-produced
* **Success:** all novel elements present with causal justification; technically plausible; ≥7.5/10 average evaluations; binary gates (reinvention yes/no; all novel elements present)
* **Scope:** drafting → essay → evaluation; minimal bookkeeping; out-of-scope: ranking heuristics, retrieval/browsing, target-specific hints

---

**Default posture:** small steps, fast feedback, simpler code. Preserve the **contract**; let the **prose** go.

# AGENTS.md — Simplicity-First Field Guide

This document complements the top-level `README.md`. Treat the README as the source for setup, commands, and environment details; the guidance below captures the expectations that frequently trip up automation agents.

---

## 1. Key References
- `README.md` — environment setup, Dagster usage, test commands.
- `docs/spec_dsl.md` — cohort spec vocabulary and simplified YAML schema.
- `docs/cohorts.md` — end-to-end cohort workflow using specs and curated selections.
- `tools/refactor_contract.yaml` — the non-negotiable refactor contract referenced throughout this guide.

Keep these open when working; this file intentionally avoids duplicating the same information.

---

> Cohort workflow details (spec format, naming requirements, migration scripts) live in `README.md` and `docs/spec_dsl.md`. Keep those references handy when editing catalogs or specs.

---

## 2. Daily Development Loop
- Practice TDD: update or add tests first, then code. Run the narrowest test you can (`pytest -k`, single asset partition, etc.).
- Before sending a PR, run the full suite once via `.venv/bin/pytest`. The full run completes quickly and catches wiring errors CI will flag anyway.
- Always invoke the test runner via the project environment: use `.venv/bin/pytest` (not the system `pytest`).
- Unit tests live next to the code under `src/daydreaming_dagster/`; they must avoid network and heavy file I/O. Integration tests go under `tests/` and may use the sample `data/` tree but never real APIs.
- Lean on dependency injection: pass resources, clients, and paths explicitly so tests can stub them.
- After touching specs or catalog CSVs, re-materialize the relevant cohort assets locally to ensure the pipeline still wires together.

---

## 3. Coding Standards & Error Handling
- Style: black (88 cols), 4 spaces, UTF-8; Ruff for lint hints. Keep changes minimal—avoid drive-by formatting in logic commits.
- Use descriptive snake_case names for functions/modules and PascalCase for classes. Ensure new identifiers align with spec/catalog naming expectations.
- Error handling: deep layers raise `DDError(code, ctx)`. Boundaries log/format once. Tests assert error codes and minimal context, not message strings.
- Data layer code should fail fast when encountering missing catalog entries or mismatched IDs. Prefer explicit assertions over silent fallbacks.
- Maintain test determinism: no reliance on string parsing of error prose or unordered dictionaries.

---

## 4. Refactor Workflow & LLM Scaffolding
- The refactor contract at `tools/refactor_contract.yaml` is mandatory—read it before large changes.
- When using assistants, include this system preamble and trailer:
  - **Preamble:**
    ```
    You are refactoring for simplicity-first. Preserve only the contract in tools/refactor_contract.yaml.
    Eliminate incidental equivalence. Prefer deleting code to preserving low-value behaviors.
    Never introduce new data plumbing solely to keep old error strings.
    ```
  - **Task trailer:**
    ```
    IMPORTANT:
    - Keep error types/codes stable; messages/logs may change.
    - Move exception handling to a single boundary; deep functions raise DDError(code, ctx).
    - Collapse try/except ladders that only decorate messages.
    - Add a short REFactor_NOTES.md summarizing dropped incidental behavior.
    ```
- Reviewer checklist (paste into PRs):
  1. Any data passed only to reproduce legacy error prose? Remove it.
  2. Error codes/types unchanged; message text may differ.
  3. Deep layers only raise `DDError(code, ctx)`—no ad-hoc formatting.
  4. Exactly one boundary logs/formats user-facing text.
  5. Tests assert codes/types (and minimal ctx), not message strings.
  6. Complexity budget met (or justified with stronger tests).
  7. Cleanups done (dead code, scaffolding, debug prints); add `REFactor_NOTES.md` when behavior intentionally diverges.

---

## 5. Operational Guardrails
- **Commits:** do not mix logic with formatting. Stage intentional files only (no `git add -A`).
- **Security & data:** never commit secrets or real outputs. Use `.env`/env vars for keys. Large generated folders under `data/` should remain ignored unless required for reproducible tests.
- **Approvals:** in interactive modes, propose exact shell commands before execution. In non-interactive modes, run the smallest checks necessary.
- **Backcompat tags:**
  - `BACKCOMPAT:` short-term compatibility shim; include removal target.
  - `TEMPORARY:` note scope and removal trigger.
  - `TODO-REMOVE-BY:` use dates/milestones for cleanup reminders.
- **Plans:** do not commit files under `temporary_plans/`. Longer-term plans belong in `plans/` and should be deleted once executed.

Stay small, stay reproducible, and keep the spec as the contract. EOF

# LLM Documents Index: Enablement & Validation Guide

This guide summarizes how to enable the documents index (SQLite + per‑doc directories), validate it with small materializations, and monitor with reporting/checks.

## Prerequisites
- Dagster installed and project venv active
- Local data present under `data/`
- For live generation: `OPENROUTER_API_KEY` exported

## Enablement Flags (shell session)
- export DAGSTER_HOME="$(pwd)/dagster_home"
- export DD_IN_PROCESS=1                 # avoid OS semaphores in CI/sandboxes
- export DD_DOCS_INDEX_ENABLED=1         # turn on DB‑first reads + dual‑write
- export DD_DOCS_PROMPT_COPY_ENABLED=1   # prompt snapshot in doc_dir/prompt.txt
- (optional) export OPENROUTER_API_KEY=...

## Backfill Existing Artifacts (optional)
- Dry‑run preview:
  - `.venv/bin/python scripts/backfill_documents_index.py --dry-run`
- Execute backfill:
  - `.venv/bin/python scripts/backfill_documents_index.py --run-id backfill-$(date +%Y%m%d)`
- Inspect a few recent rows:
  - `sqlite3 data/db/documents.sqlite 'select stage,task_id,doc_dir,created_at from documents order by created_at desc limit 10;'`

## Seed & Validate Tasks
- Seed task definitions once:
  - `uv run dagster asset materialize --select "group:task_definitions" -f daydreaming_dagster/definitions.py`

## Materialize Small Subset (3‑minute budget per step)
- Draft (example partition):
  - `DD_IN_PROCESS=1 uv run dagster asset materialize --select draft_response \\
     --partition "combo_v1_1f0b4806fc4a_rolling-summary-simplified-v1_gemini_2.5_flash,combo_v1_1f0b4806fc4a,rolling-summary-simplified-v1,gemini_2.5_flash,google/gemini-2.5-flash" \\
     -f daydreaming_dagster/definitions.py`
- Essay (matching `<essay_task_id>`):
  - `DD_IN_PROCESS=1 uv run dagster asset materialize --select essay_response --partition "<essay_task_id>" -f daydreaming_dagster/definitions.py`
- Evaluation (novelty template, sonned or configured model):
  - `DD_IN_PROCESS=1 uv run dagster asset materialize --select evaluation_response --partition "<evaluation_task_id>" -f daydreaming_dagster/definitions.py`

## Reporting & Checks
- Reporting assets:
  - `DD_IN_PROCESS=1 uv run dagster asset materialize --select documents_latest_report,documents_consistency_report -f daydreaming_dagster/definitions.py`
  - Outputs: `data/7_reporting/documents_latest_report.csv`, `data/7_reporting/documents_consistency_report.csv`
- Asset checks (visible in Dagster UI):
  - File existence (FS or DB) for draft/essay/eval
  - DB row present for draft/essay/eval (non‑blocking when index disabled/unavailable)

## CLI Utilities
- List recent rows:
  - `scripts/list_partitions_latest.py --stage draft --head 5`
- Print latest row for a task:
  - `scripts/print_latest_doc.py task draft <draft_task_id>`

## Cutover Checklist
1) Run with `DD_DOCS_INDEX_ENABLED=1` for a soak period; keep legacy writes on.
2) Ensure checks are green and reports show consistent doc_dirs.
3) Flip `DD_DOCS_LEGACY_WRITE_ENABLED=0` to stop response legacy writes.
4) After a deprecation window, remove filesystem fallbacks in readers.

## Troubleshooting
- PermissionError (multiprocess semaphores) in CI/sandbox:
  - Set `DD_IN_PROCESS=1` for `dagster ... asset materialize` commands.
- No DB rows or empty reports:
  - Confirm `DD_DOCS_INDEX_ENABLED=1` and re‑run.
  - Verify `data/db/documents.sqlite` exists; re‑run backfill if needed.
- Missing prompt.txt in doc_dir:
  - Ensure `DD_DOCS_PROMPT_COPY_ENABLED=1`.


# LLM Documents Storage Index (Draft)

Purpose: Replace long, info‑stuffed filenames with a table‑backed index and short IDs. Keep large text/artifacts on disk in a per‑document directory.

Key Decisions (Updated)
- New ID per version: Each new generation attempt gets a new `doc_id` (no `_vN` filename suffixes).
- Directory per document: The table stores `doc_dir` (path to a directory), which contains raw/parsed artifacts and any large metadata.
- Files remain for human inspection, but filenames become short and stable.

Dependencies
- Runtime: Python stdlib `sqlite3` (no extra pip dependency). Verify availability with: `python -c "import sqlite3; print(sqlite3.sqlite_version)"`.
- Storage: new untracked folders `data/docs/` (artifacts) and `data/db/` (SQLite file). Ensure `.gitignore` excludes these (data/ subtree is already untracked by convention).

Single Table (documents)
- `doc_id` (PK): Short unique ID per produced document (draft/essay/evaluation). New attempt → new `doc_id`.
- `logical_key_id`: Stable hash for grouping attempts of the same logical product (e.g., same stage + template + model + source). Useful for dedup/rollups.
- `stage`: Enum {`draft`, `essay`, `evaluation`}.
- `task_id`: Original pipeline ID (`draft_task_id` | `essay_task_id` | `evaluation_task_id`).
- `parent_doc_id`: Parent document (essay → draft; evaluation → target doc).
- `template_id`: Template used (draft/essay/evaluation).
- `model_id`: LLM model used.
- `run_id`: Dagster run or operator‑supplied experiment label.
 - `prompt_path`: Filesystem path to the prompt file (for quick copy/paste reruns).
- `parser`: Parser name used (if applicable).
- `status`: `ok` | `truncated` | `parse_error` | `gen_error` | `skipped`.
- `usage_prompt_tokens`, `usage_completion_tokens`, `usage_max_tokens`: Token metrics.
- `created_at`: Timestamp.
 - `doc_dir`: Filesystem path to the document directory (relative to data root).
- `raw_chars`, `parsed_chars`: Sizes (optional early on).
- `meta_small`: JSON blob for small metadata (finish_reason, provider, etc.).
- `lineage_prev_doc_id`: Optional link if this doc supersedes a prior attempt.

ID Strategy
- `logical_key_id`: Deterministic base62/base36 of a stable tuple.
  - draft: hash(`draft`, combo_id, draft_template, model_id)
  - essay: hash(`essay`, draft_doc_id, essay_template, model_id)
  - evaluation: hash(`evaluation`, target_doc_id, evaluation_template, model_id)
- `doc_id`: Unique per attempt. Compose from (`logical_key_id`, `run_id`, `attempt_n` or timestamp) → base62/base36 12–16 chars.
  - Guarantees a new ID for each attempt without needing `_vN` in filenames.

Filesystem Layout (directory per doc)
- Unify under a single root for simplicity, grouped by stage:
  - Drafts: `data/docs/draft/<doc_id>/`
  - Essays: `data/docs/essay/<doc_id>/`
  - Evaluations: `data/docs/evaluation/<doc_id>/`
- Within each directory:
  - `raw.txt` — raw LLM response (always saved if enabled)
  - `parsed.txt` — parsed/normalized text (when applicable)
  - `metadata.json` — larger metadata not suitable for the table (usage objects, provider details, traces)
  - `extras/` — optional additional artifacts (attachments, traces, intermediate parser outputs)

Atomic Writes
- Create `doc_dir`, write files to `*.tmp`, `fsync`, then `os.replace` to final names (`raw.txt`, `parsed.txt`, `metadata.json`).
- Insert the DB row after files are durable. On partial failures, leave the directory with `meta_small.backfill=true` or a `status=gen_error` marker for traceability.

Dagster Integration (Write Path)
- After generation, create `<stage>/<doc_id>/` under `data/docs/`.
- Save artifacts: `raw.txt` (and `parsed.txt` if applicable), `metadata.json` (big fields).
- Prompts remain in existing prompt dirs (e.g., `3_generation/draft_prompts/`, `3_generation/essay_prompts/`, `4_evaluation/evaluation_prompts/`). Store `prompt_path` in the table for easy retrieval and manual reruns.
- Append a row to `documents` with `doc_id`, `logical_key_id`, core keys, sizes/hashes (if cheap), `doc_dir`, `prompt_path`, and status.

Write Path (direct adoption)
- Writers will create per-doc directories and insert into SQLite without a feature toggle. Any failure in directory/DB write should fail the asset.
- Legacy outputs (`draft_responses`, `..._raw`) remain written as today for operational continuity, but readers will move to the index.

Dagster Integration (Read Path)
- For downstream steps, resolve via the `documents` table: compute `logical_key_id` from task keys and select the latest `ok` row; no filesystem fallback. Fail fast if not found.
- To reproduce/debug, load `raw.txt` and `metadata.json` from `doc_dir` referenced by the row.

Backfill/Migration Plan (with validation after each step)
1) Implement `ids.py` helper: `compute_logical_key_id(...)`, `new_doc_id(logical_key_id, run_id, attempt_or_ts)`.
   - Validation:
     - Add unit tests `daydreaming_dagster/utils/test_ids.py`:
       - `test_logical_key_id_deterministic()` and `test_new_doc_id_uniqueness()` using fixed inputs.
     - Run: `uv run pytest -q -k 'test_ids and (logical or doc_id)'`.
2) Create a SQLite database and `documents` table at `data/db/documents.sqlite`.
   - Suggested DDL:
     - `CREATE TABLE documents (\n`
       `  doc_id TEXT PRIMARY KEY,\n`
       `  logical_key_id TEXT,\n`
       `  stage TEXT,\n`
       `  task_id TEXT,\n`
       `  parent_doc_id TEXT,\n`
       `  template_id TEXT,\n`
       `  model_id TEXT,\n`
       `  run_id TEXT,\n`
       `  prompt_path TEXT,\n`
       `  parser TEXT,\n`
       `  status TEXT,\n`
       `  usage_prompt_tokens INTEGER,\n`
       `  usage_completion_tokens INTEGER,\n`
       `  usage_max_tokens INTEGER,\n`
       `  created_at TEXT,\n`
       `  doc_dir TEXT,\n`
       `  raw_chars INTEGER,\n`
       `  parsed_chars INTEGER,\n`
       `  meta_small TEXT,\n`
       `  lineage_prev_doc_id TEXT\n`
       `);`
     - Useful indexes: `CREATE INDEX idx_documents_logical ON documents(logical_key_id, created_at);` and `CREATE INDEX idx_documents_stage ON documents(stage, created_at);`
   - Validation:
     - `python - <<'PY'\nimport sqlite3, pathlib; p=pathlib.Path('data/db/documents.sqlite');\ncon=sqlite3.connect(p);\nassert con.execute("select count(*) from sqlite_master where type='table' and name='documents'").fetchone()[0]==1;\nprint('documents table OK')\nPY`
3) Backfill script (`scripts/backfill_documents_index.py`):
   - Scans legacy locations:
     - Draft: `data/3_generation/draft_responses_raw/` (`<draft_task_id>_vN.txt`), `data/3_generation/draft_responses/` (`<draft_task_id>.txt`)
       and legacy `data/3_generation/links_responses/` (treated as draft parsed-only)
     - Essay: `data/3_generation/essay_responses_raw/` (`<essay_task_id>_vN.txt`), `data/3_generation/essay_responses/` (`<essay_task_id>.txt`)
     - Evaluation: `data/4_evaluation/evaluation_responses_raw/` (`<evaluation_task_id>_vN.txt`), `data/4_evaluation/evaluation_responses/` (`<evaluation_task_id>.txt`)
   - For each task_id:
     - Create one attempt per RAW version (when present). Attach `parsed.txt` only to the latest RAW to avoid duplication.
     - If only parsed exists (no RAW), create a single attempt with `parsed.txt` only.
   - For each attempt:
     - Compute `logical_key_id = hash36(stage|task_id)` and a fresh `doc_id = hash36(logical|run_id|uniqueness)`.
     - Create `data/docs/<stage>/<doc_id>/` and place `raw.txt`/`parsed.txt` accordingly (+ `metadata.json`).
     - Insert a row into SQLite (`documents`) with `doc_id`, `logical_key_id`, `stage`, `task_id`, `doc_dir`, sizes, status, and a small JSON metadata (`meta_small`) noting backfill provenance and source paths.
   - Validation:
     - Dry run: `python scripts/backfill_documents_index.py --dry-run` and verify counts printed per stage are non-negative.
     - Real run on a tiny fixture dir (or current repo data):
       - `python scripts/backfill_documents_index.py --stage draft`
       - Verify: `sqlite3 data/db/documents.sqlite 'select stage,count(*) from documents group by stage;'`
       - Spot-check: Ensure a sample `data/docs/draft/<doc_id>/raw.txt` exists and matches source size.
   - Options:
     - `--data-root`: default `data/`
     - `--db`: default `<data-root>/db/documents.sqlite`
     - `--stage`: `all|draft|essay|evaluation`
     - `--run-id`: default `backfill-YYYYMMDD`
     - `--link`: hard-link instead of copy when possible
     - `--dry-run`: report without writing
   - Leaves legacy files in place (non-destructive).

4) Reader updates:
   - Update helpers (e.g., `_load_phase1_text`) to resolve strictly via the `documents` table (latest `ok` by `logical_key_id`). If missing, fail fast with a clear error.
   - Add a simple `documents_index.py` util with: `open_db`, `get_latest_by_task(stage, task_id)`, `insert_document(...)`.
   - Validation:
     - Unit test: mock DB with a single row; `get_latest_by_task` returns expected `doc_dir` and fails when absent.
     - Integration: adjust `tests/test_pipeline_integration.py` to assert readers use the index (e.g., using a temporary DB and file), and fail when index row missing.

5) Writer updates:
   - In `draft_response`, `essay_response`, `evaluation_response` assets: compute `logical_key_id`, create `doc_dir`, write raw/parsed/metadata, store `prompt_path`, and insert a row. Fail if any step fails.
   - Keep the current `save_versioned_raw_text` path intact (no `_vN` in new per-doc directories).
   - Validation:
     - Asset-level tests: materialize one representative partition for each stage with a mock LLM client; assert doc_dir exists and DB row inserted with correct `stage`, `task_id`, and `prompt_path`.
     - CLI check: `sqlite3 data/db/documents.sqlite 'select count(*) from documents where stage="draft";'` > 0 after draft asset run.

6) Docs/Guides:
   - Document the new index, `doc_id`/`logical_key_id`, and how to retrieve artifacts via the table.
   - Clarify prompts remain in prompt dirs and are referenced via `prompt_path`.

7) Testing Plan:
   - Unit: `ids.py` (hash determinism), `documents_index.py` (insert/select), atomic writer util (temp → replace).
   - Integration: per-step validations above; minimal partitions via Dagster CLI; backfill dry-run and real run on tiny fixture set.
4) Switch writers to the new directory pattern and table appends (behind a feature flag initially).
5) Update readers to prefer table resolution; keep legacy fallback temporarily.

Storage Option
- Use SQLite for the index: `data/db/documents.sqlite` with WAL mode for robustness (`PRAGMA journal_mode=WAL`). Single-writer, many-readers fits Dagster’s pattern.
- Keep artifacts on disk in `doc_dir` as designed (raw.txt, parsed.txt, metadata.json, extras/).

Queries Enabled
- Group by `logical_key_id` to find latest successful attempt.
- Filter by `run_id` to slice an experiment’s outputs.
- Aggregate token usage by `model_id`/`template_id`.
- Trace lineage from essay to its draft via `parent_doc_id`.

Open Items
- Standardize `parsed.txt` extension per stage (e.g., `.md` for essays) if beneficial.
- Define retention policy/timeline for legacy files after readers switch fully to the index.

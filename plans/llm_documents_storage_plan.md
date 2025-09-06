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
- Partitions: Each generation/evaluation asset already uses dynamic partitions keyed by task IDs (`context.partition_key`). We will:
  - Drafts: compute `logical_key_id = H("draft", draft_task_id, draft_template, model_id)` where `draft_task_id = context.partition_key` resolved from `draft_generation_tasks`.
  - Essays: derive the parent draft’s `logical_key_id` from `draft_task_id` in `essay_generation_tasks`; fetch the latest `ok` draft row to get `parent_doc_id`; then compute essay `logical_key_id = H("essay", parent_doc_id, essay_template, model_id)`.
  - Evaluations: from `evaluation_tasks` row, determine target document (essay or draft). If it’s an essay, resolve latest essay `doc_id` first; compute `logical_key_id = H("evaluation", target_doc_id, evaluation_template, model_id)`.
- Directory per attempt: After generation, create `<stage>/<doc_id>/` under `data/docs/`.
- Files saved: `raw.txt` (always if enabled), `parsed.txt` (if applicable), `metadata.json` (usage, provider details), optional `extras/`.
- Prompts: Keep writing prompts via existing prompt IO managers (versioned), and store `prompt_path` in `documents` for traceability.
- DB write: Insert into `documents` with `doc_id`, `logical_key_id`, `stage`, `task_id` (= partition key), `parent_doc_id` (essay/evaluation), `template_id`, `model_id`, `run_id` (`context.run_id`), `prompt_path`, `parser`, `status`, token usage, `doc_dir`, sizes, `meta_small`.
- Output metadata: Each asset attaches Dagster metadata with `doc_id`, `logical_key_id`, `doc_dir`, `prompt_path`, `status`, and token counts for quick inspection in the UI.

Write Path (direct adoption)
- Writers will create per-doc directories and insert into SQLite without a feature toggle. Any failure in directory/DB write should fail the asset.
- Legacy outputs (`draft_responses`, `..._raw`) remain written as today for operational continuity, but readers will move to the index.

Dagster Integration (Read Path)
- Reader contract: Consumers compute the appropriate `logical_key_id` and query the `documents` table for the latest `ok` row. No direct filesystem search; fail fast if absent.
- Phase‑2 prompt building: `essay_prompt` uses `draft_task_id` to compute the draft `logical_key_id` and fetches the latest `ok` draft row; it then reads `raw.txt` (or `parsed.txt` if parser mode) from the resolved `doc_dir`.
- Evaluation prompt: resolves the target document the same way (based on `evaluation_tasks` row), then reads the text via `doc_dir` rather than using `file_path`.
- Debugging: Use the row’s `doc_dir` to open `raw.txt` and `metadata.json` directly for reproduction.

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
   - Partition awareness: helpers accept `context` to read `context.partition_key` and task rows, ensuring deterministic `logical_key_id` per partition.
   - Add a simple `documents_index.py` util with: `open_db`, `get_latest_by_task(stage, task_id)`, `get_latest_by_logical(logical_key_id)`, `insert_document(...)`.
   - Validation:
     - Unit test: mock DB with a single row; `get_latest_by_task` returns expected `doc_dir` and fails when absent.
     - Integration: adjust `tests/test_pipeline_integration.py` to assert readers use the index (e.g., using a temporary DB and file), and fail when index row missing.

5) Writer updates:
   - In `draft_response`, `essay_response`, `evaluation_response` assets: compute `logical_key_id` (partition‑aware), derive `parent_doc_id` where applicable, create `doc_dir`, write raw/parsed/metadata, store `prompt_path`, and insert a row. Fail if any step fails.
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

Dagster Details (Partitions, IO Managers, Asset Changes)
- Partitions: existing dynamic partitions map 1:1 to `task_id`s. We preserve current `partitions_def` on assets and compute IDs using `context.partition_key` and the corresponding task row. Re‑materializing a partition creates a new `doc_id` in the same `logical_key_id` group; readers selecting “latest ok” will pick it up automatically.
- Run identity: populate `run_id` from `context.run_id`; allow optional override via a resource/RunConfig key (e.g., `experiment_label`) that we record in `run_id` or an additional `experiment_id` column.
- IO managers (compat): keep current VersionedTextIOManager bindings for prompts and responses. Assets continue to return `str` so existing IO managers persist legacy files for human inspection and for interim compatibility.
- New resource: add a `DocumentsIndex` ConfigurableResource with `db_path` (e.g., `data/db/documents.sqlite`) and `docs_root` (e.g., `data/docs`). Expose methods: `compute_logical_key_id(...)`, `new_doc_id(...)`, `insert_document(...)`, `get_latest_by_task(...)`, `get_latest_by_logical(...)`.
- Asset write changes:
  - `draft_response`: compute `logical_key_id` from (`draft_task_id`, `draft_template`, `model_id`), allocate `doc_id`, write per‑doc dir, insert row, attach metadata.
  - `essay_response`: resolve latest `draft` `doc_id` by computing the draft’s `logical_key_id` from `draft_task_id`; compute essay `logical_key_id` using the parent `doc_id`; allocate `doc_id`, write dir, insert row with `parent_doc_id`.
  - `evaluation_response`: resolve target doc (essay or draft) using `evaluation_tasks` row; compute eval `logical_key_id` with target `doc_id`; allocate `doc_id`, write dir, insert row with `parent_doc_id` = target `doc_id`.
- Asset read changes:
  - `_load_phase1_text` and any file readers switch to DB resolution: compute `logical_key_id` from the relevant task row and fetch latest `ok`; read `raw.txt`/`parsed.txt` from `doc_dir`.
  - During migration, keep a guarded filesystem fallback (warn + mark deprecated) behind a feature flag; default to DB‑only after migration step 4.
- Indexing and performance: add `CREATE INDEX idx_documents_logical ON documents(logical_key_id, created_at);` and optionally `idx_documents_task ON documents(stage, task_id, created_at)` to accelerate partition‑scoped lookups.
- Error handling: if the DB insert fails, mark the Dagster run as failed; if file writes partially succeed, leave `status=gen_error` with `meta_small.backfill=true` to aid cleanup.

Minimal Wiring in definitions.py
- Add `documents_index` resource configuration (db/docs paths) and inject into writer/reader assets via `required_resource_keys`.
- Keep existing IO managers as‑is; no change to `partitions_def` wiring.
- Attach small, consistent output metadata across assets: `doc_id`, `logical_key_id`, `parent_doc_id` (if any), `doc_dir`, `prompt_path`, `status`, token counts.

Feature Flags & Migration Controls
- `DD_DOCS_INDEX_ENABLED` (bool): when false, writers/readers use legacy filesystem only. Default true for writers after step 5; keep a temporary reader fallback until step 4 completes.
- `DD_DOCS_LEGACY_WRITE_ENABLED` (bool): keep legacy VersionedTextIOManager writes for responses. Default true initially; switch to false after confidence, so DB + doc_dir are the single source of truth.
- `DD_DOCS_PROMPT_COPY_ENABLED` (bool): copy the prompt text into `doc_dir/prompt.txt` in addition to IO‑manager prompt file. Default true to ensure reproducibility regardless of prompt version filename discovery.

Schema (DDL) & Indexes
```sql
CREATE TABLE IF NOT EXISTS documents (
  doc_id TEXT PRIMARY KEY,
  logical_key_id TEXT NOT NULL,
  stage TEXT NOT NULL,                 -- draft | essay | evaluation
  task_id TEXT NOT NULL,               -- partition key used by the asset
  parent_doc_id TEXT,                  -- essay->draft, eval->target
  template_id TEXT,
  model_id TEXT,
  run_id TEXT,
  prompt_path TEXT,
  parser TEXT,
  status TEXT NOT NULL,                -- ok | truncated | parse_error | gen_error | skipped
  usage_prompt_tokens INTEGER,
  usage_completion_tokens INTEGER,
  usage_max_tokens INTEGER,
  created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  doc_dir TEXT NOT NULL,
  raw_chars INTEGER,
  parsed_chars INTEGER,
  meta_small TEXT,
  lineage_prev_doc_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_documents_logical ON documents(logical_key_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_documents_stage ON documents(stage, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_documents_task ON documents(stage, task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_documents_parent ON documents(parent_doc_id, created_at DESC);
PRAGMA journal_mode=WAL;
```

Prompt Persistence & Path Capture
- Unknown prompt version filename at asset time: IOManager determines the `_vN` suffix after return. To ensure reproducibility:
  - Always write `prompt.txt` into `doc_dir` alongside `raw.txt`/`parsed.txt` (enabled via `DD_DOCS_PROMPT_COPY_ENABLED`).
  - Best‑effort: after the asset returns, we cannot intercept the IOManager path; therefore, we record the prompt path we used to render (template path + IDs) in `meta_small`, and optionally discover the versioned path on next run when reading.

Idempotency, Retries, and Ordering
- `doc_id` uniqueness: enforce `PRIMARY KEY(doc_id)` with `doc_id = new_doc_id(logical_key_id, run_id, attempt_or_ts)`; collisions are practically impossible if timestamp or monotonic attempt is included.
- Write order in asset: create `doc_dir` → write files (atomic replace) → insert DB row → return text to IOManager.
  - If IOManager write fails, Dagster marks the run failed; DB row exists with `status=ok`. This is acceptable during migration; once `DD_DOCS_LEGACY_WRITE_ENABLED=false`, the IOManager write for responses is skipped to avoid this failure mode.
- Re‑materialization: running the same partition creates a new row with the same `logical_key_id` and a fresh `doc_id`. “Latest ok” selection updates consumers automatically.

Failure & Status Mapping
- `gen_error`: exception from LLM client or write path; insert row only if `doc_dir` exists with partials; otherwise skip row.
- `truncated`: `info.truncated=true` or `finish_reason=length` → set status and raise Failure to prevent downstream use.
- `parse_error`: parser exceptions when producing `parsed.txt`; keep `raw.txt` and mark status.
- `ok`: successful write and validation; record token usage if available.

Performance & Concurrency
- SQLite write pattern: single INSERT per doc, optional small UPDATE for sizes after file write. Keep each write in its own transaction.
- WAL mode enabled for resilience; a single writer (per process) is fine for Dagster’s typical concurrency.
- Index‑only reads for `get_latest_by_logical` and `get_latest_by_task` using `(logical_key_id, created_at)` and `(stage, task_id, created_at)`.

Resource API Sketch
- `DocumentsIndex` (ConfigurableResource):
  - `db_path: str`, `docs_root: str`.
  - `compute_logical_key_id(stage, identifiers: dict) -> str`.
  - `new_doc_id(logical_key_id: str, run_id: str, attempt_or_ts: str|int) -> str`.
  - `insert_document(row: DocumentRow) -> None`.
  - `get_latest_by_task(stage: str, task_id: str, status: set={"ok"}) -> Row|None`.
  - `get_latest_by_logical(logical_key_id: str, status: set={"ok"}) -> Row|None`.
  - `resolve_doc_dir(row) -> Path` and helpers for reading `raw.txt`, `parsed.txt`, `prompt.txt`.

Backfill & Migration Details
- Backfill script scans legacy directories and creates rows:
  - Drafts: for each `{draft_task_id}[_vN].txt`, compute `logical_key_id = H("draft", draft_task_id, draft_template, model_id)` using the tasks tables to recover template/model.
  - Essays: link to draft by matching `essay_generation_tasks` (has `draft_task_id`); set `parent_doc_id` to the latest matching draft.
  - Evaluations: can be optionally backfilled later; priority is generation artifacts.
- Validation reports: counts by stage, number of linked parents, orphaned files, and rows with missing `doc_dir`.

Testing & Validation Additions
- Unit: `ids.py` determinism and `DocumentsIndex` insert/select behavior; prompt copy logic.
- Asset‑level: one partition per stage exercising OK, truncated, parse_error paths; assert DB status and files present.
- Integration: Dagster materialization of a tiny subset with `DD_DOCS_INDEX_ENABLED=true` and legacy writes on; then toggle off legacy writes and re‑run to confirm DB‑only path works.

Rollout Plan
- Phase A (writers on): enable DB writes + per‑doc dirs for writers while readers still support filesystem fallback; verify indexes populate.
- Phase B (readers switch): flip readers to DB‑only; keep legacy writes until confidence is high.
- Phase C (legacy off): disable legacy response writes; keep prompt IOManager (for UI visibility) but rely on `prompt.txt` inside `doc_dir` for reproducibility.

Open Items
- Standardize `parsed.txt` extension per stage (e.g., `.md` for essays) if beneficial.
- Define retention policy/timeline for legacy files after readers switch fully to the index.

# LLM Documents Storage Index (Draft)

Purpose: Replace long, info‑stuffed filenames with a table‑backed index and short IDs. Keep large text/artifacts on disk in a per‑document directory.

Key Decisions (Updated)
- New ID per version: Each new generation attempt gets a new `doc_id` (no `_vN` filename suffixes).
- Directory per document: The table stores `doc_dir` (path to a directory), which contains raw/parsed artifacts and any large metadata.
- Files remain for human inspection, but filenames become short and stable.

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
- `prompt_hash`: Hash of the exact prompt text rendered (optional but useful).
- `parser`: Parser name used (if applicable).
- `status`: `ok` | `truncated` | `parse_error` | `gen_error` | `skipped`.
- `usage_prompt_tokens`, `usage_completion_tokens`, `usage_max_tokens`: Token metrics.
- `created_at`: Timestamp.
- `doc_dir`: Filesystem path to the document directory (relative to data root).
- `raw_sha256`, `parsed_sha256`: Integrity hashes (optional early on).
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

Dagster Integration (Write Path)
- After generation, create `<stage>/<doc_id>/` under `data/docs/`.
- Save artifacts: `raw.txt` (and `parsed.txt` if applicable), `metadata.json` (big fields).
- Prompts remain in existing prompt dirs (e.g., `3_generation/draft_prompts/`, `3_generation/essay_prompts/`, `4_evaluation/evaluation_prompts/`). Store `prompt_path` in the table for easy retrieval and manual reruns.
- Append a row to `documents` with `doc_id`, `logical_key_id`, core keys, sizes/hashes (if cheap), `doc_dir`, `prompt_path`, and status.

Dagster Integration (Read Path)
- For downstream steps, resolve via `doc_id` from the table (preferred) or derive from task keys → `logical_key_id` → latest doc by `created_at`/status.
- To reproduce/debug, load `raw.txt` and `metadata.json` from `doc_dir`.

Backfill/Migration Plan
1) Implement `ids.py` helper: `compute_logical_key_id(...)`, `new_doc_id(logical_key_id, run_id, attempt_or_ts)`.
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
       `  prompt_hash TEXT,\n`
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
   - Options:
     - `--data-root`: default `data/`
     - `--db`: default `<data-root>/db/documents.sqlite`
     - `--stage`: `all|draft|essay|evaluation`
     - `--run-id`: default `backfill-YYYYMMDD`
     - `--link`: hard-link instead of copy when possible
     - `--dry-run`: report without writing
   - Leaves legacy files in place (non-destructive).
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
- Decide whether to compute/hard‑require SHA256 at write time or defer to a maintenance job.
- Standardize `parsed.txt` extension per stage (e.g., `.md` for essays) if beneficial.
- Define retention policy for legacy files after migration completes.

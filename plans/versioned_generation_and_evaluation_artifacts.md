# Plan: Versioned generations and evaluations (non‑destructive reruns)

## Context (current behavior)
- Selection inputs (drafts only)
  - Draft content combinations come exclusively from `data/2_tasks/selected_combo_mappings.csv`.
  - This file is a strict row‑subset of `data/combo_mappings.csv` (identical schema). Any divergence fails fast during `content_combinations` materialization.
  - Essays do not consume combinations directly; they depend on the corresponding draft outputs.
- Two‑phase generation
  - Dynamic partitions: `draft_tasks`, `essay_tasks`, `evaluation_tasks`.
  - Drafts: `data/3_generation/draft_responses/{draft_task_id}.txt`.
  - Essays: `data/3_generation/essay_responses/{essay_task_id}.txt`.
  - Evaluations: `data/4_evaluation/evaluation_responses/{evaluation_task_id}.txt`.
- IO managers
  - `PartitionedTextIOManager` writes one file per partition. Defaults: prompts overwrite; responses controlled by `OVERWRITE_GENERATED_FILES` (off by default to prevent clobbering).
  - If `overwrite=False` and the file exists, a `FileExistsError` is raised (delete or change partition key to re‑run).
- Cross‑experiment appenders (tracking)
  - Append one row per completion (draft/essay/evaluation) into `data/7_cross_experiment/*_results.csv`. Paths point to the single current file (no version field yet).
- File lookup
  - Centralized resolution for reading existing artifacts (e.g., `find_document_path`) so legacy locations remain readable.

## Problem statement
Enable non‑destructive reruns (no overwrites) while keeping reads simple and “latest‑only”. Avoid symlinks or pointer files; rely on filename conventions and scanning.

## Goals
- Preserve existing outputs; each new run produces a new immutable artifact.
- Keep reads simple: upstream assets load the “latest” by default via directory scan.
- Cross‑platform approach with no symlinks or pointer files.
- Defer explicit multi‑version evaluation/comparison to a later phase.

## Proposed design (latest‑by‑scan, no pointers)
### 1) Versioned filenames
- Write responses to versioned files only:
  - Drafts: `draft_responses/{id}_v{N}.txt`
  - Essays: `essay_responses/{id}_v{N}.txt`
  - Evaluations: `evaluation_responses/{id}_v{N}.txt`
- Do not create or update `{id}.txt` pointers.
- Versioning rule: scan for existing `{id}_v*.txt` and pick `1 + max(N)`; start at `v1` if none.

### 2) Reader rule (latest‑by‑scan)
- When loading `{id}`:
  - Prefer the highest `{id}_v{N}.txt` if any exist in the relevant directory.
  - Else, fall back to `{id}.txt` if present (legacy single file).
  - Else, raise a clear error with expected paths.
- Implement centrally and reuse in assets and path resolution helpers.

### 3) IO manager upgrade (prompts and responses)
- Add `VersionedTextIOManager(base_path: Path)`:
  - `handle_output`: write next `{id}_v{N}.txt` atomically (temp → os.replace).
  - `load_input`: apply the reader rule above.
- Wire this for all generation I/O:
  - Prompts: `draft_prompt_io_manager`, `essay_prompt_io_manager`
  - Responses: `draft_response_io_manager`, `essay_response_io_manager`, `evaluation_response_io_manager`

### 4) Cross‑experiment tracking (defer)
- Keep current schemas unchanged for now (no version columns).
- Future extension (optional): add version integers and store concrete `*_vN` paths.

### 5) Selection & UI
- Latest‑only behavior by design: running a partition again appends `_vN` and readers pick it up automatically.
- `data/2_tasks/selected_combo_mappings.csv` continues to define the roster of draft partitions.

### 6) Migration strategy
- Do not rename existing `{id}.txt` files.
- First re‑run for a partition creates `{id}_v1.txt`; later runs create `_v2`, `_v3`, ...
- Readers prefer versioned files immediately; otherwise they read `{id}.txt`.

### 7) Risks / considerations
- Concurrency: two simultaneous writes could race on choosing `N`; typically prevented by Dagster partitions. If needed, add a file lock around scan+write.
- Disk growth: add a housekeeping script later to prune old versions by age/count.

## Execution plan (ready to implement)
1) Helpers
   - Add `versioned_paths.list_versions(dir, id)` and `versioned_paths.find_latest(dir, id)`.
2) IO manager
   - Implement `VersionedTextIOManager` with atomic write and latest‑by‑scan read.
3) Readers
   - Update `_load_phase1_text` direct read path to use latest‑by‑scan.
   - Update `utils/document_locator.find_document_path` to prefer `_vN` variants.
4) Wire up
   - Use `VersionedTextIOManager` for all prompt/response IO managers in `definitions.py`.
5) Remove overwrite flags (prompts and responses)
   - Remove `OVERWRITE_GENERATED_FILES` and per‑manager overwrite settings; prompts and responses always append a new version.
6) Tests
   - Unit: `_v1` then `_v2`, latest selected; legacy `{id}.txt` fallback.
   - Integration‑lite: write twice, assert two files exist, reader picks latest.
7) Docs
   - Update README and guides to explain versioned writes for prompts and responses, latest‑by‑scan reads, and removal of overwrite flags.

## Alternatives considered (deferred)
- Encode version in partition key or suffix: increases parsing and partition churn.
- Subdirectory per id: increases path complexity versus simple suffix.
- Overwrite in place with timestamps: destructive.

---

## Summary
Write `{id}_v{N}.txt` on every rerun. Readers choose the highest `_vN` if present; otherwise they read `{id}.txt`. No symlinks, no pointer files, and no overwrite flag for responses. Docs will be updated accordingly.

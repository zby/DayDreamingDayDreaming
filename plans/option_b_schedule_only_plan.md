**Option B Only: Schedule + Change Detection**

**Objective**
- Automatically detect changes under `data/1_raw/**/*` on a cadence and trigger selective recomputation without human intervention.
- Ensure only the raw assets corresponding to changed files (and their downstream) are affected.

**Assumptions**
- There is one raw asset per source file. This may be:
  - SourceAssets (non-materializable), or
  - Software-defined assets (materializable loaders).
- We run with the Dagster daemon enabled. No upgrade to newer Dagster APIs.
- Small raw dataset; a light per-tick scan is acceptable.

**High-Level Design**
- Schedule evaluates every N minutes (configurable). It scans `data/1_raw/**/*`, computes fingerprints, compares to last successful state, and decides:
  - Concepts: observe ONLY the concepts metadata CSV (`concepts/concepts_metadata.csv`). New concepts are introduced as new rows/IDs; editing description text alone is assumed not to happen. Thus, observations are per-CSV, not per-description file.
  - Templates/Eval: observe ONLY the CSVs (`link_templates.csv`, `essay_templates.csv`, `evaluation_templates.csv`). Template `.txt` files are not observed (assumed immutable for ops purposes).
  - If raw are software-defined assets: launch a run that materializes only the changed raw asset keys; downstream is pulled in the same run or by reconciliation.
- Persist a “last successful” combined fingerprint; use a “pending” fingerprint during an in-flight run to avoid double-triggering on the same change.

**Components**
- Fingerprinter util: walks the raw directory, returns per-file entries `{rel_path, size, mtime, optional_hash}` and a combined fingerprint.
- State store: reads/writes `last` and `pending` fingerprints under `DAGSTER_HOME/state/`.
- File→AssetKey mapping: deterministic mapping from relative path to the raw asset key used in definitions. Special case: description files are not mapped to SourceAssets; only the concepts metadata CSV is.
- Job(s):
  - Observation job (if raw are SourceAssets): an op that accepts a list of changed files and yields `SourceAssetObservation` with `data_version=<per-file-fingerprint>`.
  - Selective materialization job (if raw are software-defined): asset job selecting only changed raw asset keys.
- Schedule: cron (e.g., `*/5 * * * *`) with an `execution_fn` that computes diffs and returns `SkipReason` or `RunRequest` with run_config + tags.
- Optional reconciliation sensor: monitors downstream selection and launches runs when stale due to observations or upstream materializations.

**Data Flow**
- Tick → Scan → Diff (added/modified/removed) → If none: Skip.
- If changes:
  - Write `pending` fingerprint.
  - Emit `RunRequest`:
    - SourceAsset path: run `observe_raw_sources` with config listing changed files and their fingerprints. Ensure that for concepts, only the metadata CSV is considered.
    - Software-defined path: run `raw_reload_job` with only changed asset keys selected.
  - On run success: promote `pending` → `last`. On failure: keep `last` so next tick re-triggers.

**Selectivity**
- Concepts: selectivity is at the metadata CSV level. Adding a concept (new row) triggers recompute; editing description text alone does not. This matches the repository assumption.
- Other SourceAssets: per-file observations for templates/eval keep fine-grained selectivity.
- When many files change, batch into grouped runs (configurable batch size) to balance run overhead vs. granularity.

**Idempotency & Safety**
- Run key: derive from a short hash of the sorted changed file set + per-file fingerprints.
- Stabilization window: ignore files modified in the last 2–5 seconds to avoid half-written files.
- Dedupe: if the same changes persist across ticks, the same run key prevents duplicate launches.

**Configuration Knobs**
- `RAW_SCAN_CRON` (default `*/5 * * * *`).
- `RAW_SCAN_COOLDOWN_SEC` (e.g., 3–5s stabilization window).
- `RAW_SCAN_HASH_MODE` (`mtime_size` vs `content_hash` for confirmation).
- `RAW_STATE_DIR` (default `${DAGSTER_HOME}/state`).
- `RAW_BATCH_SIZE` (optional, for grouping many changed files).
- Feature flag: `RAW_SCHEDULE_ENABLED=true/false` to enable/disable the schedule.

**Code Changes (Yes — we will write code)**
- `daydreaming_dagster/utils/file_fingerprint.py`
  - `scan_raw_tree(base_dir: Path, include_glob: list[str] | None) -> list[FileInfo]` (with fields: rel_path, size, mtime, optional_hash, fingerprint_str)
  - `diff(prev: dict, curr: dict) -> ChangedSet` (added/modified/removed)
  - `combine_fingerprint(entries: Iterable[FileInfo]) -> str`
- `daydreaming_dagster/utils/raw_state.py`
  - Read/write `last_fingerprint.json` and `pending_fingerprint.json` under `DAGSTER_HOME/state/`.
  - Helpers to promote pending→last on success.
- `daydreaming_dagster/utils/raw_mapping.py`
  - `path_to_asset_key(rel_path: str) -> AssetKey` compatible with existing raw asset keys.
  - Optional discovery: build mapping from current definitions.
- `daydreaming_dagster/jobs/observe_raw.py` (only if raw are SourceAssets)
  - Op `emit_observations(changed: list[ChangedEntry]) -> None` yields `SourceAssetObservation` per file with `data_version` and metadata.
  - Job `observe_raw_sources` parameterized by config (list of changed files and fingerprints).
- `daydreaming_dagster/jobs/raw_reload.py` (only if raw are software-defined assets)
  - Define `raw_reload_job = define_asset_job("raw_reload_job", selection=AssetSelection.keys([...]))` built dynamically at schedule time via `RunRequest` and tags.
- `daydreaming_dagster/schedules/raw_schedule.py`
  - `@schedule(..., execution_fn=...)` that calls the shared fingerprinter + diff, decides skip vs run, writes pending, and builds a `RunRequest` with run_key and run_config.
- `daydreaming_dagster/definitions.py`
  - Register the schedule and (optionally) the reconciliation sensor across the downstream selection.

Note: We will keep scanning logic in a reusable util so both the schedule and any future sensor can share it and be unit-tested cleanly.

**Testing Plan**

- Unit tests (colocated under `daydreaming_dagster/utils/`):
  - `test_file_fingerprint.py`
    - Scans a temp directory; verifies entries, relative paths, and deterministic combined fingerprint.
    - `diff` detects added/removed/modified correctly, including mtime-only changes.
    - Hash mode switches (`mtime_size` vs `content_hash`) behave as expected.
  - `test_raw_state.py`
    - Writes/reads last/pending fingerprints in a temp `DAGSTER_HOME`; promotes pending→last on success; survives partial files.
  - `test_raw_mapping.py`
    - Validates path→AssetKey mapping against known example raw keys (use minimal fake definitions or fixtures).

- Schedule tests (unit-style):
  - `test_raw_schedule_execution.py`
    - When fingerprints unchanged → returns `SkipReason`.
    - On change → returns `RunRequest` with expected run_key and tags.
    - Ensures `pending` is written on schedule launch and only promoted on success (simulate success path in a follow-up call).
    - Stabilization window filters too-recent file changes.

- Job/op tests:
  - SourceAssets path: `test_observe_raw.py`
    - Given a list of changed entries, `emit_observations` yields one `SourceAssetObservation` per file with the correct `data_version` and metadata (size, mtime).
  - Software-defined path: `test_raw_reload_job.py`
    - Selection includes only changed asset keys; the job definition matches expected selection.

- Integration tests (in `tests/`):
  - `test_option_b_end_to_end.py`
    - Use a temp directory for `data/1_raw/` and a temp `DAGSTER_HOME`.
    - Seed with one file; run schedule’s `execution_fn` → expect `SkipReason`.
    - Modify/add a file; run `execution_fn` → expect `RunRequest`.
    - Execute the returned run via Dagster test harness:
      - SourceAssets: run `observe_raw_sources` with provided config; verify that an `AssetReconciliationSensor` (evaluated in test) would select the correct downstream assets (assert on requested selection).
      - Software-defined: run `raw_reload_job`; verify only changed raw assets and their downstream are targeted.
    - Confirm state promotion `pending`→`last` after success; second tick skips.

**Edge Cases & Operations**
- File deleted: mark as “removed” and either observe a new version (to invalidate downstream) or materialize a “missing” state if your loaders expect it; document downstream behavior.
- Many files changed: batch by `RAW_BATCH_SIZE` for multiple runs; include batch number tags.
- Rename: treated as removed+added; downstream will recompute accordingly. For concepts, renaming description files without changing the metadata CSV will not trigger recompute (documented constraint).
- Logging: add tags `trigger=raw_schedule`, `raw_fingerprint`, and counts of added/modified/removed.
- Performance: avoid content hashing unless `mtime+size` changed; optionally nightly full-hash job.

**Do We Write Code?**
- Yes. Implement the schedule, fingerprinting/state/mapping utils, and one of the two job paths depending on whether raw are SourceAssets or software-defined.
- New unit tests are required for these utilities and schedule logic. Integration tests add one E2E path; existing tests remain unchanged unless they assume manual triggering only.

**Rollout Steps**
- 1) Land utils with unit tests (`file_fingerprint.py`, `raw_state.py`, `raw_mapping.py`).
- 2) Land schedule and the chosen job path (SourceAssets observation or software-defined selective materialization) with unit tests.
- 3) Wire into `definitions.py` behind `RAW_SCHEDULE_ENABLED`; default off.
- 4) Add integration test `tests/test_option_b_end_to_end.py`.
- 5) Enable in local `.env` and verify with `dagster dev` + daemon.
- 6) Document ops toggles and failure recovery in `docs/` or README.

**Success Criteria**
- Changing one file under `data/1_raw/` within the stabilization window triggers exactly one run on the next tick.
- Only assets derived from that file (and their downstream) are recomputed.
- No duplicate runs for the same change; subsequent ticks skip until the next change.

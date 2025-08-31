**Problem Summary**
- **Goal:** Automatically (no human-in-loop) load/rematerialize the raw asset group whenever files under `data/1_raw/**/*` change, without relying on Observable assets or upgrading Dagster.
- **Context:** Raw assets are sources for later transformations; we don’t need to validate downstream transformations before triggering. We can rely on Dagster daemon for background execution.

**Constraints**
- **Version:** Avoid requiring a new Dagster version (Observable assets off the table).
- **Simplicity:** Prefer minimal, robust code; no external services.
- **Safety:** Avoid duplicate/redundant work; only rematerialize when inputs change.
- **Operational:** Play nicely with `dagster dev` + daemon; run headless.

**Options**
- **A. File-Scanning Sensor → RunRequests**
  - Description: Implement a Dagster `SensorDefinition` that periodically scans `data/1_raw/**/*`, computes a stable fingerprint (mtime + size or content hash), compares against stored state (e.g., a small JSON in `dagster_home` or Dagster sensor cursor), and issues `RunRequest`s to materialize `group:raw` or specific partitions that changed.
  - Pros: Simple, version-stable; fully in-Dagster; easy to reason about; supports idempotency via hashing/cursor.
  - Cons: Requires writing and maintaining a tiny state layer (cursor or JSON). If raw set is large, hashing might be non-trivial (can optimize to mtime+size with periodic full hash validation).

- **B. Frequent Schedule + Change Detection**
  - Description: A Dagster `ScheduleDefinition` runs every N minutes. The job code detects changes (same fingerprint approach) and materializes `group:raw` only when needed.
  - Pros: Very stable; schedules are well-supported; no sensor ticks backlog.
  - Cons: Slightly less reactive than sensors; change detection logic sits in job config or a helper.

- Note: We intentionally exclude a partitioned design because our raw dataset is small; the operational overhead outweighs the benefits.

- **D. External Watchdog Script → CLI Trigger**
  - Description: A lightweight Python watcher (e.g., `watchdog`) monitors `data/1_raw/**/*` and shells out to `uv run dagster asset materialize --select "group:raw"` on change. Optionally run under `systemd`/supervisor.
  - Pros: Very simple; decoupled from Dagster features; no Dagster upgrades.
  - Cons: Extra process and dependency; less integrated observability; risk of retrigger storms without debouncing; testing is a bit more bespoke.

- **E. Git/CI Triggered Materialization**
  - Description: On commits that touch `data/1_raw/**/*`, CI or a pre-commit hook runs the materialization command locally/CI.
  - Pros: Zero runtime complexity; integrates with existing workflows.
  - Cons: Only triggers on commits, not local file drops; less suitable for continuous local development.

**Shortlist and Framing**
- Two leading options: Option A (sensor with cursor) and Option B (schedule + change detection). Option B is the main competitor to A.
- Choose based on preference for reactivity (A) vs. simplicity of scheduling (B). Both avoid Dagster upgrades and integrate with the daemon.
- Keep Option D (external watcher) and Option E (Git/CI) as operational fallbacks when Dagster automation isn’t desired or available.

**Per-File Selectivity (Given: one raw asset per source file)**
- Goal: Only assets corresponding to changed files become stale and/or are rematerialized.
- If raw are `SourceAsset`s per file: On change, emit a `SourceAssetObservation` per changed file with `data_version=<file_fingerprint>` and optional metadata (size, mtime). An `AssetReconciliationSensor` over downstream selection will then materialize only the assets impacted by those specific observed source assets.
- If raw are software-defined assets per file: On change, materialize only the subset of raw assets whose backing files changed (select by asset keys), letting reconciliation (or the same run) pull downstream selectively.
- Mapping file → asset key: Use a deterministic mapping (e.g., `AssetKey(["raw", <relative_path>])` or filename stem). If the repo uses a custom scheme, keep a small manifest helper that maps `relative_path` to `AssetKey` discovered from definitions.
- Grouping: When many files change, either (a) materialize/observe them in one grouped run (simpler) or (b) batch into small groups to balance observability and run overhead.

**Implementation Outline (Option A: Sensor + Cursor)**
- **1. State model:**
  - Use the sensor cursor JSON (via `context.cursor`) to store a map `{relative_path: {size, mtime, hash?}}`.
  - For large files, default to `size+mtime` and only hash after mtime/size changes to confirm.
- **2. Scanner:**
  - Walk `data/1_raw/**/*` (filter to allowed extensions as applicable) and build the current fingerprint map.
  - Compute `changed = added ∪ removed ∪ modified` by comparing to cursor map.
- **3. Triggering strategy:**
  - If `changed` is non-empty and raw are SourceAssets: emit `SourceAssetObservation` events only for the changed files so only their downstream becomes stale. Pair with an `AssetReconciliationSensor` targeting the relevant downstream to auto-run.
  - If `changed` is non-empty and raw are software-defined assets: issue a `RunRequest` targeting only the changed raw asset keys (not the whole group) with a deduped run key.
  - Optionally attach run tags with a summary: number of files changed, first few filenames.
- **4. Cursor update:**
  - Update the cursor to the new fingerprint map only after issuing the run request(s).
- **5. Idempotency & failure handling:**
  - Use run keys per change batch to avoid duplicate runs.
  - On failure, do not update cursor; the sensor will retry (Dagster handles sensor ticks).
- **6. Performance & throttling:**
  - Debounce changes by grouping all changes per tick; optionally add a minimal cool-off window (e.g., ignore events within the last ~3–5 seconds).
  - Limit per-tick scanning time by avoiding full content hashes unless size/mtime drifted.

**Implementation Outline (Option B: Schedule + Change Detection)**
- 1. Define `raw_reload_job = define_asset_job("raw_reload_job", selection=AssetSelection.groups("raw"))`.
- 2. Create a `ScheduleDefinition` (e.g., `*/5 * * * *`; configurable via env var like `RAW_SCAN_CRON`).
- 3. In the schedule `execution_fn`, scan `data/1_raw/**/*`, compute per-file fingerprints and a combined fingerprint. Also keep the diff set of changed files.
- 4. Persist the last successful combined fingerprint under `DAGSTER_HOME` (e.g., `state/raw_fingerprint.json`).
- 5. If unchanged: return `SkipReason("no raw changes")`.
- 6. If raw are SourceAssets: emit `SourceAssetObservation` events only for the changed files, tagging with the per-file fingerprint; let an `AssetReconciliationSensor` pick up and materialize impacted downstream.
- 7. If raw are software-defined assets: return a `RunRequest` that selects only the changed raw asset keys (not the whole group) and uses a deduped run key; downstream can be pulled in the same run or via reconciliation.
- 6. Write the new fingerprint only after a successful run. Use a temporary “pending” file and promote on success to avoid false positives on failure.
- 7. Add a stabilization window (e.g., ignore files modified in the last 2–5s) and log counts scanned for observability.

**Implementation Outline (Option D: External Watchdog)**
- **1. Script:** Small Python script using `watchdog` observes `data/1_raw/**/*`, debounces events for ~2–5s, then calls `uv run dagster asset materialize --select "group:raw" -f daydreaming_dagster/definitions.py`.
- **2. Service:** Optional `systemd` unit to keep the watcher running with restart policy.
- **3. Safety:** Use a PID file or lock to avoid concurrent triggers; log to a rotating file.

**Testing Plan**
- **Unit (cursor logic):**
  - Test diffing logic: added/removed/modified detection from two synthetic scans.
  - Test cursor serialization/deserialization; ensure stability across restarts.
- **Sensor integration:**
  - Use Dagster sensor evaluation tests to simulate files in a temp directory and assert emitted `RunRequest`s and cursor updates.
  - Verify debouncing and run key dedupe.
- **Schedule integration:**
  - Simulate schedule `execution_fn` with different fingerprints and assert `SkipReason` vs `RunRequest`, run key dedupe, and fingerprint promotion on success.
- **Per-file selectivity:**
  - With per-file SourceAssets: assert that only observed assets for changed files receive new `data_version`s and that reconciliation targets only their downstream.
  - With per-file software-defined assets: assert that the selection includes only changed raw asset keys and that downstream is pulled selectively.
- **End-to-end (local):**
  - Run `dagster dev` with daemon. Drop a new file into `data/1_raw/`; confirm a run is launched for `group:raw` and completes.
  - Modify an existing file; confirm a second run is launched.

**Operational Notes**
- **Daemon:** Keep Dagster daemon running (as you already do) so sensors/schedules evaluate.
- **Resource usage:** For very large trees, consider switching from full hashing to `mtime+size` and scheduled full re-hash (e.g., nightly) to avoid CPU spikes.
- **Observability:** Add run tags (e.g., `trigger=raw_sensor` or `trigger=raw_schedule`, plus `raw_changed_count`) to make discovery easy in the UI.
- **Backpressure:** If downstream depends immediately on raw, either let the standard dependency graph pull or add an `AssetReconciliationSensor` to opportunistically materialize downstream when raw updates land.

**Rollout Steps (A or B)**
- A) Sensor path
  1) Add sensor module and wire it in `definitions.py`.
  2) Implement scanner and fingerprint logic (tiny utility in `utils/`).
  3) Start daemon and verify sensor ticks; run local E2E test.
  4) Tune debounce window and selection scope if needed.
  5) Document operational toggles (enable/disable sensor via env var).
- B) Schedule path
  1) Add job + schedule; make cron configurable via env.
  2) Implement fingerprint read/write and `execution_fn` change check.
  3) Start daemon; confirm skips when unchanged and runs when changed.
  4) Add stabilization window and tags; tune cadence.

**Tradeoffs: A vs B**
- **Reactivity:** A triggers promptly based on sensor ticks; B triggers on cron cadence.
- **Simplicity:** B is slightly simpler operationally (no sensor cursors); A keeps everything within Dagster’s sensor model.
- **State handling:** A uses sensor cursor; B uses a tiny state file and run_key dedupe.
- **Compatibility:** Both avoid newer Dagster features and work with the daemon.

**Decision Rubric (A vs B)**
- **Latency needed:** If ≤1–2 min latency matters, pick A. If 5–10 min latency is fine, pick B.
- **Operational simplicity:** Prefer minimal moving parts and easy mental model → B.
- **State preference:** Avoid writing files under `DAGSTER_HOME` → A (cursor). Comfortable with a tiny JSON state file → B.
- **Change pattern:** Burstier/local edits that should group quickly → A (easier debouncing per tick). Rare/periodic updates → B.
- **Run cost:** If raw rematerialization is heavy, B’s predictable cadence can be easier to reason about. If light/fast, either is fine.
- **Observability choice:** Both support tags/logging; no strong differentiator here.
- **Default choice for this repo:** B, if we’re happy with a short cadence (e.g., 5 min) and our raw set remains small. A, if we later want lower latency without external watchers.

**Open Questions**
- Any file types that should be ignored or handled specially?
- Should re-materialization be gated by a checksum change, or is mtime sufficient?

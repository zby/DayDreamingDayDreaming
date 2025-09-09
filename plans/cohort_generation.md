Great—let’s make this lean and shippable. Below is a **scoped, functional plan** to introduce cohorts with a single **CSV membership** that fully specifies task rows per stage and serves as the source of truth for both tasks and partition registration — while keeping the code and rollout simple.

---

## 0) Goals & non‑goals (for this phase)

**Goals (Phase 1):**

* Introduce a **cohort** with:

  * `cohort.yaml` (metadata)
  * `membership.csv` (wide rows containing all task columns per stage) as the single source of truth for tasks and partition registration.
* Provide **two cohort builders**:

  1. Dagster materialization path A: if `data/2_tasks/selected_essays.txt` exists (one essay `gen_id` per line), build an evaluate‑only cohort from those essays.
  2. Dagster materialization path B: otherwise, build a full Cartesian cohort from active axes (content combinations × templates × models).
* Keep artifacts as-is (no copy/symlink in Phase 1).
* Register partitions inside Dagster, directly from membership.csv (no “keys” files or separate registration script).
* Change task assets to consume the cohort: task tables are projected from membership; runtime generation/evaluation assets remain unchanged.

**Non‑goals (defer to later):**

* Symlink/manifest artifact reuse strategies.
* Param snapshots/version pinning.
* Cohort dashboards/summary assets.
* Multi‑cohort orchestration & comparisons.
* New storage formats (e.g., Parquet) or complex indices.

---

## 1) Layout & artifacts

```
data/
  cohorts/
    <cohort_id>/
      cohort.yaml
      membership.csv               # wide rows with task columns per stage
```

**`cohort.yaml` (minimal):**

```yaml
id: 2025-09-09_prior_art_retest
mode: from_essays   # or: cartesian
artifact_reuse: none   # or: copy
notes: "Re-evaluating top essays against current evaluators"
created_at: "2025-09-09T12:34:56Z"
```

**`membership.csv`** — one row per generation containing all data needed to build the respective task tables (single file; union schema). Required columns by stage:

- Common columns (all stages):
  - `stage` in {`draft`,`essay`,`evaluation`}
  - `gen_id`
  - `cohort_id`

- Draft rows (stage=`draft`):
  - `draft_task_id`, `combo_id`, `draft_template`, `generation_model`, `generation_model_name`

- Essay rows (stage=`essay`):
  - `essay_task_id`, `parent_gen_id` (draft gen_id), `draft_task_id`, `combo_id`, `draft_template`, `essay_template`, `generation_model`, `generation_model_name`

- Evaluation rows (stage=`evaluation`):
  - `evaluation_task_id`, `parent_gen_id` (essay gen_id), `evaluation_template`, `evaluation_model`, `evaluation_model_name`
  - Optional: `parser`, `file_path`, `source_dir`, `source_asset`

Notes:
- Stages share one CSV; irrelevant columns for a row may be empty.
- Duplicate rows are tolerated and deduped on read by (`stage`,`gen_id`).

---

## 2) Deterministic IDs & hashing

**Rule:** Use the existing deterministic `reserve_gen_id(stage, task_id, run_id=cohort_id)` helper. No CH‑prefix or new ID scheme. IDs remain base36 and cohort‑scoped via `run_id`.

---

## 3) Cohort materialization via Dagster

Implement cohort creation entirely through Dagster assets and simple CLI invocations.

Add a tiny asset group to materialize membership.csv under `data/cohorts/<cohort_id>/`:

- `cohort_id` (already exists): computes and writes `manifest.json`.
- `cohort_membership` (new asset):
  - Behavior branches depending on filesystem state to select the cohort scope, but always produces full task rows per stage:
    - Path A (curated evaluate‑only): if `data/2_tasks/selected_essays.txt` exists, read essay `gen_id`s (one per line), then:
      - Recover required task fields from gens metadata (for drafts and essays) and active evaluation axes (for evaluations).
      - Compute cohort‑scoped `gen_id`s via `reserve_gen_id(..., run_id=cohort_id)` for any stage that needs them.
      - Emit full draft, essay, and evaluation rows into membership with all required columns for each stage (as defined above). No artifact copying.
    - Path B (full Cartesian): otherwise, derive full draft and essay rows from the active axes (as current task assets do), and evaluation rows from active evaluation axes.
  - Output: write `membership.csv` with a union schema that includes all columns needed by each stage’s task table.
  - The membership asset also registers dynamic partitions (add‑only) for all `gen_id`s by stage.
  - Parent integrity (strict): before writing/registration, validate that:
    - Every essay row’s `parent_gen_id` exists as a draft `gen_id` in the same cohort.
    - Every evaluation row’s `parent_gen_id` exists as an essay `gen_id` in the same cohort.
    - On any violation, fail fast with a clear error that includes counts and a small sample of offending rows. This prevents dangling trees when cohorts are curated/mixed manually.

---

Operate with Dagster commands (no separate cohort CLI):

Commands:
- Build cohort membership (curated if `selected_essays.txt` exists, else Cartesian):
  - `uv run dagster asset materialize --select cohort_membership -f daydreaming_dagster/definitions.py`
- Materialize task tables from cohort (they project from membership):
  - `uv run dagster asset materialize --select "group:task_definitions" -f daydreaming_dagster/definitions.py`

---

## 4) Selection helper: scripts/select_top_prior_art.py (simplified)

Purpose: Produce a curated list of essay `gen_id`s for evaluate‑only cohorts.

Simplified behavior (Phase 1):
- Input: `data/7_cross_experiment/parsed_scores.csv` and configured prior‑art eval templates (as today).
- Output: write `data/2_tasks/selected_essays.txt` containing one essay `gen_id` per line, sorted by score (ties/stability as current or defined explicitly).
- No task CSVs are written by this script in Phase 1; membership is built later by the Dagster asset.

Idempotency and re‑materialization notes:
- If `selected_essays.txt` does not change and the `cohort_id` remains the same, re‑materializing `cohort_membership` produces the same `membership.csv`; dynamic partition registration via Dagster is a no‑op.
- If the list shrinks or changes, the default policy is additive registration (no automatic removal). Operators can clear partitions via Dagster UI/CLI if needed. We can add an optional “reset” toggle on the asset later.
- Evaluations already materialized for a gen_id will not be re‑generated unless explicitly rematerialized; Dagster will show them as up‑to‑date.

Command:
```
uv run python scripts/select_top_prior_art.py --parsed-scores data/7_cross_experiment/parsed_scores.csv
# Produces data/2_tasks/selected_essays.txt
```

Note: `scripts/register_partitions_for_generations.py` is slated for retirement. Partition registration will be handled inside Dagster (UI or CLI) by the `cohort_membership` asset registering dynamic partitions after writing membership.csv.

---

## 5) Asset changes (task assets consume cohort)

Scope: Change the three task-definition assets to take the cohort as input and derive their tables directly from the cohort membership. Generation/evaluation runtime assets remain unchanged.

- Affected assets: `draft_generation_tasks`, `essay_generation_tasks`, `evaluation_tasks` (group `task_definitions`).
- New dependency: add an explicit dependency on `cohort_membership` (and `cohort_id` which already exists).
- Behavior (for each task asset):
  - Read `data/cohorts/<cohort_id>/membership.csv`.
  - Filter rows by `stage` (draft/essay/evaluation) corresponding to the asset.
  - Project the stage-specific columns to match the current task schema exactly (including `gen_id`, `cohort_id`).
  - Dedupe on the task’s logical key if applicable, otherwise dedupe by `gen_id`.
  - Register dynamic partitions using the `gen_id`s from membership (add-only).
  - Emit Dagster metadata with counts and the membership path used.
- Backcompat and ergonomics:
  - If `membership.csv` is missing, fail with a clear message instructing to materialize `cohort_membership` first (preferred), or enable a temporary fallback flag to rebuild from active axes (dev-only).
  - Keep environment override `DD_COHORT` for cohort_id, but prefer the `cohort_id` asset wiring in normal runs.

Runtime assets (groups `generation_draft`, `generation_essays`, `evaluation`):
- No changes in this phase. They continue to read tasks for params and read/write gens under `data/gens/<stage>/<gen_id>`.

### Tasks materialized from cohorts (single path per stage)

Goal: After materializing tasks from a cohort, it must be possible to regenerate drafts, essays, and evaluations. We do not copy or link prior generations; all three stages are regenerated under the new cohort. There is no A/B branching at this stage — the cohort already encodes which IDs belong to each stage.

The cohort materialization (cohort_membership) is responsible for deciding curated vs Cartesian selection and computing the definitive `gen_id` sets per stage for the cohort. Task assets then derive the required task rows from these IDs.

- Draft tasks (from cohort membership)
  - Source of truth: `membership.csv` rows with `stage=draft` containing all required draft task columns and the cohort‑scoped `gen_id`.
  - Output: DataFrame identical to `draft_generation_tasks` by selecting the draft columns from membership; no reconstruction beyond projection/deduplication.
  - Integration: provide `cohort_draft_generation_tasks` (project membership) or add an optional override input to `draft_generation_tasks` to consume the cohort’s draft rows.

- Essay tasks (from cohort membership)
  - Source of truth: `membership.csv` rows with `stage=essay` containing all required essay task columns and the cohort‑scoped `gen_id` and `parent_gen_id`.
  - Output: DataFrame identical to `essay_generation_tasks` by selecting the essay columns from membership; no reconstruction beyond projection/deduplication.
  - Integration: provide `cohort_essay_generation_tasks` (project membership) or add an optional override input to `essay_generation_tasks` to consume the cohort’s essay rows.

- Evaluation tasks (from cohort membership)
  - Source of truth: `membership.csv` rows with `stage=evaluation` containing all required evaluation task columns and the cohort‑scoped `gen_id` and `parent_gen_id`.
  - Output: DataFrame identical to `evaluation_tasks` by selecting the evaluation columns from membership; no reconstruction beyond projection/deduplication.
  - Integration: provide `cohort_evaluation_tasks` (project membership) or an optional override input to `evaluation_tasks` to consume the cohort’s evaluation rows.

Regeneration policy: Because we neither copy nor link prior generations, materializing the generation assets for this cohort will compute fresh drafts, essays, and evaluations using the cohort’s `gen_id`s. Previously existing artifacts from other cohorts are not reused.

---

## 6) Artifact copying

Deferred. Phase 1 does not perform artifact copying or symlinking. Evaluate‑only cohorts assume essays already exist in the gens store. A future helper can provide copy/symlink if needed.

---

## 7) Acceptance criteria

* Asset `cohort_membership` writes `data/cohorts/<cohort_id>/membership.csv` with full task columns per stage and registers corresponding dynamic partitions (add‑only by default).
* If `data/2_tasks/selected_essays.txt` exists, membership encodes full rows for essays and derived drafts/evaluations; otherwise it encodes full Cartesian rows.
* Cohort task assets (or overrides) project stage‑specific columns from membership to produce DataFrames identical to `draft_generation_tasks`, `essay_generation_tasks`, and `evaluation_tasks`.
* After materializing the cohort task assets, it is possible to materialize drafts, then essays, then evaluations for the cohort — all stages are regenerated (no copying/symlinking of prior artifacts).
* Parent integrity is enforced: every essay.parent_gen_id is present among draft gen_ids, and every evaluation.parent_gen_id is present among essay gen_ids (within the same cohort). Violations cause `cohort_membership` to fail with a clear error.
* Legacy, non‑cohort runs still work.

---

## 8) Migration & rollout (three small PRs)

**PR 1 — Dagster membership asset**

* Add asset `cohort_membership` that writes `membership.csv` per rules above (branches on existence of `data/2_tasks/selected_essays.txt`) and registers partitions.
* Add a short guide in `docs/cohorts.md` (format + how to trigger Path A vs Path B) and an example `data/2_tasks/selected_essays.txt` in docs (not committed under data/).
* Drop the minimal list asset idea; membership is now authoritative and wide.

**PR 2 — Task assets consume cohort**

* Refactor `draft_generation_tasks`, `essay_generation_tasks`, and `evaluation_tasks` to depend on `cohort_membership` and project their rows from `membership.csv`.
* Enforce presence of membership;
* Keep partition registration using the `gen_id`s from membership.


> After PR 2 merges, we can start using cohorts immediately. Later PRs can consider artifact reuse helpers.

---

## 9) Testing plan

**Unit tests:**

* `cohort_membership` Path A: with a fixture `selected_essays.txt`, writes `membership.csv` containing full rows per stage (including IDs and task context) and registers partitions.
* `cohort_membership` Path B: with active axes, writes full draft/essay/evaluation rows and registers partitions.
* Cohort task assets (or overrides) project membership into DataFrames matching current schemas for `draft_generation_tasks`, `essay_generation_tasks`, and `evaluation_tasks`.
* Regeneration smoke: materialize draft → essay → evaluation for one small cohort and verify gens are written under `data/gens/<stage>/<cohort_gen_id>/` with no reliance on prior artifacts.
* Parent integrity checks: unit test that a membership with an essay/evaluation row whose `parent_gen_id` is missing (in its respective stage set) causes the asset to fail with a helpful error; positive test that valid membership passes.

**Integration (smoke) test:**

1. Create a tiny “toy” prior generation (1 draft → 1 essay).
2. Write `data/2_tasks/selected_essays.txt` with that essay `gen_id`.
3. Materialize `cohort_membership` (Path A) and verify `membership.csv` contains the essay and evaluation rows.
4. Materialize an evaluation partition (partitions already registered by the asset) and assert drafts/essays were not re‑generated.
5. Remove `selected_essays.txt`; materialize `cohort_membership` (Path B) and verify Cartesian draft/essay rows.

---

## 10) Risks & mitigations

* **Where do we read “active” params?**
  Use the same loaders the current scripts/assets use (raw CSVs and task tables). No new APIs needed.

* **Duplicated gen\_ids across cohorts:**
  Already avoided by cohort‑scoped `reserve_gen_id(..., run_id=cohort_id)`.

* **Evaluation expansion correctness:**
  Deterministic: evaluation `gen_id = reserve_gen_id("evaluation", f"{essay_gen_id}__{tpl}__{model}", run_id=cohort_id)`.

* **Script retirement:**
  `scripts/register_partitions_for_generations.py` will be retired. Use Dagster UI/CLI and the `cohort_membership` asset to register dynamic partitions and materialize assets. Any remaining references should be removed as the new asset lands.

* **Mixed/manual cohorts leading to dangling parents:**
  Prevented by strict parent integrity validation in `cohort_membership`. If operators hand-edit membership, invalid lineage will fail fast with a clear diffable report (first N offending rows), keeping the cohort graph consistent.

---

## 11) What we’re deferring (Phase 2+)

* **Symlink**/**manifest** artifact strategies.
* **`membership.parquet`** + indices for very large cohorts.
* Cohort sensors that auto‑sync partitions.
* Cohort status summary asset & UI.
* Artifact reuse helpers (copy/symlink).
* Rich selection filters inside the asset (e.g., include/exclude models/templates).

---

## 12) Quickstart (operator cookbook)

**Re‑evaluate a curated set of essays (evaluate‑only):**

```bash
# selected_essays.txt: one essay gen_id per line
printf "%s\n" 1hx4p8q9z2k1v0sj > data/2_tasks/selected_essays.txt

# Build cohort membership (Path A or B) and register partitions automatically
uv run dagster asset materialize --select "cohort_id,cohort_membership" -f daydreaming_dagster/definitions.py

# Materialize task tables from cohort (they project from membership)
uv run dagster asset materialize --select "group:task_definitions" -f daydreaming_dagster/definitions.py

# Materialize generation then evaluation (fresh computations under the cohort)
# 1) Materialize draft_response for a few partitions or all
# 2) Materialize essay_response for matching partitions
# 3) Materialize evaluation_response for desired partitions
# Example: materialize a specific evaluation partition by gen_id
# uv run dagster asset materialize --select "evaluation_response" --partition <evaluation_gen_id> -f daydreaming_dagster/definitions.py
```

**Freeze a full active grid today:**

```bash
# No selected_essays.txt present → Path B (Cartesian encoded in membership)
uv run dagster asset materialize --select "cohort_id,cohort_membership,group:task_definitions" -f daydreaming_dagster/definitions.py

# Materialize desired partitions in Dagster UI/CLI as needed (drafts → essays → evaluations)
```

---

### TL;DR

* Add cohorts with **just** `cohort.yaml` + `membership.csv` (wide rows per stage).
* Build cohorts via Dagster: curated (if `selected_essays.txt` exists) or full Cartesian; cohort_membership registers partitions.
* Task assets consume membership to produce task tables; runtime generation/evaluation assets remain unchanged.
* No artifact copying in Phase 1; rely on existing gens store and recompute under cohort gen_ids.

This gets you a working end‑to‑end path with minimal code churn. When we’re happy with it, we can layer on symlinks/manifests, sensors, and richer tooling.

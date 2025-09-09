Below is the execution plan to migrate DayDreamingDayDreaming to partitions keyed by `doc_id` across all stages (draft, essay, evaluation). Task IDs remain present in task CSVs for provenance, but partitions, IO, and checks will use `doc_id` as the canonical key.

Status: ready
Owner: daydreaming_dagster

Decision summary
- Migrate dynamic partitions to doc‑id keys: `draft_docs`, `essay_docs`, `evaluation_docs`.
- Partition key semantics: `context.partition_key == doc_id` for all stage assets and checks.
- Assets that need metadata from tasks will look up rows by `doc_id` (not `*_task_id`).
- IO is already `data/docs/<stage>/<doc_id>/*`; no change to file layout.
- Transitional aids (optional, not required): `scripts/link_tasks_to_latest_docs.py` can populate/repair `doc_id` and `parent_doc_id` in task CSVs and help cut over safely.

---

## What relies on `task_id` today (audit)

> **TL;DR**: partitions, IO managers, asset code, asset checks, and the partition‑registration code all lean on `*_task_id`. We’ll flip those to `doc_id`.

1. **Partitions are defined per task**
   `daydreaming_dagster/assets/partitions.py` declares dynamic partitions named `draft_tasks`, `essay_tasks`, `evaluation_tasks`. ([GitHub][1])

2. **Generation (draft) assets**

   * Use `draft_tasks_partitions` (task‑keyed).
   * Read the task row via `get_task_row(..., "draft_task_id", ...)`.
   * Treat `context.partition_key` as a `task_id`.
   * Write documents to `data/docs/draft/<doc_id>`.
     ([GitHub][2])

3. **Generation (essay) assets**

   * Use `essay_tasks_partitions`.
   * Read via `get_task_row(..., "essay_task_id", ...)`.
   * Treat `context.partition_key` as a `task_id`.
   * Resolve Phase‑1 via `parent_doc_id` (already doc‑id‑first).
     ([GitHub][3])

4. **Evaluation assets**

   * Use `evaluation_tasks_partitions`.
   * Read via `get_task_row(..., "evaluation_task_id", ...)`.
   * Treat `context.partition_key` as a `task_id`.
     ([GitHub][4])

5. **Prompt IO manager wiring**
   In `definitions.py`, each `DocsPromptIOManager` is configured with an `id_col` pointing at a `*_task_id` column, which it uses to look up `doc_id` in the corresponding tasks CSV (doc‑id value is required; failures are explicit). ([GitHub][5])

6. **Task definition assets also register task‑key partitions**
   In `group_task_definitions.py`, the three task assets build CSVs and **register dynamic partitions using the task ids** (e.g., `df["draft_task_id"]`). Note: `doc_id` is already computed and persisted; `evaluation_tasks` even uses `run_id` to salt `doc_id`. ([GitHub][6])

7. **Asset checks**
   Checks map (partition key → `*_task_id`) through CSVs to find a `doc_id` before verifying files exist; i.e., they assume partition key is a task id. ([GitHub][7])

8. **README expects task‑key partitions**
   The README describes dynamic partitions keyed by `draft_task_id`/`essay_task_id`. ([GitHub][8])

9. **Doc‑ID helper**
   `reserve_doc_id` produces deterministic ids from (`stage`,`task_id`); salting with `run_id` is optional. Given multi‑attempt needs, we keep ids stable and rely on the linker for mapping tasks → existing docs. ([GitHub][9])

10. **Parsed scores & analysis**
    These consume **doc‑id‑first** outputs (they read from `data/docs/*/<doc_id>` and enrich off metadata), so they **do not** depend on task ids. ([GitHub][10])

---

## Target design (doc‑id partitions everywhere)

- Partitions by stage are doc‑id keyed (one partition per document):
  - `draft_docs`, `essay_docs`, `evaluation_docs`.
- Partition key semantics: `context.partition_key` is the document id (`doc_id`).
- Task CSVs still include `*_task_id` for lineage and join convenience, but runtime reads join on `doc_id`.
- Evaluations refer to their target essays via `parent_doc_id` (doc‑id‑first lineage remains unchanged).
- Aggregation and analysis remain parsed.txt‑first and continue to read directly from `data/docs/*/<doc_id>`.

---

## Concrete change set (execute now)

### A) Partitions: switch names and semantics

**`daydreaming_dagster/assets/partitions.py`**

```diff
-from dagster import DynamicPartitionsDefinition
-# Create dynamic partition definitions
-generation_tasks_partitions = DynamicPartitionsDefinition(name="generation_tasks")
-evaluation_tasks_partitions = DynamicPartitionsDefinition(name="evaluation_tasks")
-# Two-phase architecture
-essay_tasks_partitions = DynamicPartitionsDefinition(name="essay_tasks")
-draft_tasks_partitions = DynamicPartitionsDefinition(name="draft_tasks")
+from dagster import DynamicPartitionsDefinition
+# Partitions are doc-id keyed (one partition per document)
+draft_docs_partitions = DynamicPartitionsDefinition(name="draft_docs")
+essay_docs_partitions = DynamicPartitionsDefinition(name="essay_docs")
+evaluation_docs_partitions = DynamicPartitionsDefinition(name="evaluation_docs")
```

(Replace imports at call sites accordingly.) ([GitHub][1])

### B) Generation (draft) assets: use `doc_id` partitions

**`daydreaming_dagster/assets/group_generation_draft.py`**

```diff
-from .partitions import draft_tasks_partitions
+from .partitions import draft_docs_partitions
@@
-@asset(
-    partitions_def=draft_tasks_partitions,
+@asset(
+    partitions_def=draft_docs_partitions,
@@
-def draft_prompt(context, draft_generation_tasks, content_combinations) -> str:
-    """Generate Phase 1 prompts for draft generation."""
-    task_id = context.partition_key
-    task_row = get_task_row(
-        draft_generation_tasks, "draft_task_id", task_id, context, "draft_generation_tasks"
-    )
+def draft_prompt(context, draft_generation_tasks, content_combinations) -> str:
+    """Generate Phase 1 prompts for draft generation (doc-id keyed)."""
+    doc_id = context.partition_key
+    task_row = get_task_row(
+        draft_generation_tasks, "doc_id", doc_id, context, "draft_generation_tasks"
+    )
@@
-    context.log.info(f"Generated draft prompt for task {task_id} using template {template_name}")
+    context.log.info(f"Generated draft prompt for doc {doc_id} using template {template_name}")
@@
-def draft_response(context, draft_prompt, draft_generation_tasks) -> str:
+def draft_response(context, draft_prompt, draft_generation_tasks) -> str:
@@
-    task_id = context.partition_key
-    task_row = get_task_row(draft_generation_tasks, "draft_task_id", task_id, context, "draft_generation_tasks")
+    doc_id = context.partition_key
+    task_row = get_task_row(draft_generation_tasks, "doc_id", doc_id, context, "draft_generation_tasks")
@@
-    doc_id = task_row.get("doc_id")
+    # `doc_id` == partition key; assert/guard
+    doc_id = str(doc_id)
```

(Keep the `Document` write unchanged; it already writes under `data/docs/draft/<doc_id>`) ([GitHub][2])

### C) Generation (essay) assets: use `doc_id` partitions

**`daydreaming_dagster/assets/group_generation_essays.py`**

```diff
-from .partitions import essay_tasks_partitions
+from .partitions import essay_docs_partitions
@@
-@asset(
-    partitions_def=essay_tasks_partitions,
+@asset(
+    partitions_def=essay_docs_partitions,
@@
-    task_id = context.partition_key
-    task_row = get_task_row(essay_generation_tasks, "essay_task_id", task_id, context, "essay_generation_tasks")
+    doc_id = context.partition_key
+    task_row = get_task_row(essay_generation_tasks, "doc_id", doc_id, context, "essay_generation_tasks")
@@
-    context.add_output_metadata({"function": MetadataValue.text("essay_prompt"), "essay_task_id": MetadataValue.text(task_id), ...})
+    context.add_output_metadata({"function": MetadataValue.text("essay_prompt"), "doc_id": MetadataValue.text(doc_id), ...})
@@
-@asset(partitions_def=essay_tasks_partitions, ...
+@asset(partitions_def=essay_docs_partitions, ...
@@
-    task_id = context.partition_key
-    task_row = get_task_row(essay_generation_tasks, "essay_task_id", task_id, context, "essay_generation_tasks")
+    doc_id = context.partition_key
+    task_row = get_task_row(essay_generation_tasks, "doc_id", doc_id, context, "essay_generation_tasks")
```

(Phase‑1 loading via `parent_doc_id` stays as is.) ([GitHub][3])

### D) Evaluation assets: use `doc_id` partitions

**`daydreaming_dagster/assets/group_evaluation.py`**

```diff
-from .partitions import evaluation_tasks_partitions
+from .partitions import evaluation_docs_partitions
@@
-@asset(partitions_def=evaluation_tasks_partitions, ...
+@asset(partitions_def=evaluation_docs_partitions, ...
@@
-    task_id = context.partition_key
-    task_row = get_task_row(evaluation_tasks, "evaluation_task_id", task_id, context, "evaluation_tasks")
+    doc_id = context.partition_key
+    task_row = get_task_row(evaluation_tasks, "doc_id", doc_id, context, "evaluation_tasks")
@@
-@asset(partitions_def=evaluation_tasks_partitions, ...
+@asset(partitions_def=evaluation_docs_partitions, ...
```

([GitHub][4])

### E) Task definition assets: register **doc‑id** partitions

**`daydreaming_dagster/assets/group_task_definitions.py`**

```diff
-from .partitions import (
-    draft_tasks_partitions, essay_tasks_partitions, evaluation_tasks_partitions,
-)
+from .partitions import (
+    draft_docs_partitions, essay_docs_partitions, evaluation_docs_partitions,
+)
@@  # in draft_generation_tasks(...)
-# refresh dynamic partitions
-existing = context.instance.get_dynamic_partitions(draft_tasks_partitions.name)
+existing = context.instance.get_dynamic_partitions(draft_docs_partitions.name)
 if existing:
-    for p in existing: context.instance.delete_dynamic_partition(draft_tasks_partitions.name, p)
+    for p in existing: context.instance.delete_dynamic_partition(draft_docs_partitions.name, p)
 if not df.empty:
-    context.instance.add_dynamic_partitions(draft_tasks_partitions.name, df["draft_task_id"].tolist())
+    context.instance.add_dynamic_partitions(draft_docs_partitions.name, df["doc_id"].astype(str).tolist())
@@  # in essay_generation_tasks(...)
-existing = context.instance.get_dynamic_partitions(essay_tasks_partitions.name)
+existing = context.instance.get_dynamic_partitions(essay_docs_partitions.name)
 ...
-context.instance.add_dynamic_partitions(essay_tasks_partitions.name, df["essay_task_id"].tolist())
+context.instance.add_dynamic_partitions(essay_docs_partitions.name, df["doc_id"].astype(str).tolist())
@@  # in evaluation_tasks(...)
-existing = context.instance.get_dynamic_partitions(evaluation_tasks_partitions.name)
+existing = context.instance.get_dynamic_partitions(evaluation_docs_partitions.name)
 ...
-context.instance.add_dynamic_partitions(evaluation_tasks_partitions.name, tasks_df["evaluation_task_id"].tolist())
+context.instance.add_dynamic_partitions(evaluation_docs_partitions.name, tasks_df["doc_id"].astype(str).tolist())
```

Also consider allowing **replicates** here: add an `ExperimentConfig.replicates` and expand rows, assigning multiple `doc_id`s per upstream row. (Evaluation already salts `doc_id` with `run_id`; drafts/essays currently allocate one `doc_id`—we can extend similarly.) ([GitHub][6])

### F) Prompt IO manager: look up by `doc_id` (or bypass CSV)

We can do **no class change**, just re‑wire it to use `doc_id`:

**`daydreaming_dagster/definitions.py`**

```diff
- "draft_prompt_io_manager": DocsPromptIOManager(
-     docs_root=Path("data") / "docs",
-     tasks_root=Path("data") / "2_tasks",
-     stage="draft",
-     tasks_csv_name="draft_generation_tasks.csv",
-     id_col="draft_task_id",
- ),
+ "draft_prompt_io_manager": DocsPromptIOManager(
+     docs_root=Path("data") / "docs",
+     tasks_root=Path("data") / "2_tasks",
+     stage="draft",
+     tasks_csv_name="draft_generation_tasks.csv",
+     id_col="doc_id",
+ ),
@@ (essay)
-     id_col="essay_task_id",
+     id_col="doc_id",
@@ (evaluation)
-     id_col="evaluation_task_id",
+     id_col="doc_id",
```

The manager will simply find the same row by `doc_id` and return that `doc_id`. This avoids adding a new IO manager while making it doc‑id‑first. ([GitHub][5])

> **Optional improvement (later)**: teach `DocsPromptIOManager` a “doc‑id‑only” mode that **skips reading the CSV** and writes directly to `data/docs/<stage>/<doc_id>/prompt.txt` using `partition_key` as the `doc_id`. That makes prompt persistence independent of tasks CSV and simplifies backfills from historical docs.

### 7) Asset checks: treat partition key as `doc_id`

**`daydreaming_dagster/checks/documents_checks.py`**

```diff
- tasks_csv = data_root / "2_tasks" / "draft_generation_tasks.csv"
- doc_id = _resolve_doc_id(tasks_csv, "draft_task_id", pk)
- if not doc_id: return AssetCheckResult(... reason="missing doc_id ...")
- base = data_root / "docs" / "draft" / str(doc_id)
+ base = data_root / "docs" / "draft" / str(pk)
```

(Do the same for essay/evaluation checks.) ([GitHub][7])

### 8) Register‑from‑CSV script: register doc‑id partitions

**`scripts/register_partitions_for_generations.py`**
Adjust partition registration to add **`doc_id`** keys to `draft_docs`, `essay_docs`, and `evaluation_docs` partition sets (not the task ids). The script already enforces `doc_id` presence for essays and pins evaluation lineage by doc id; extend the same for draft partitions and flip the partition set names accordingly. ([GitHub][12])

### 9) README & examples

Update the README so examples use **`doc_id`** partitions instead of task ids (e.g., `uv run dagster asset materialize --select "group:generation_draft" --partition "<DRAFT_DOC_ID>" ...`). The current README explicitly states partitions are based on `draft_task_id` / `essay_task_id` — that must be revised. ([GitHub][8])

---

## One‑step migration plan (no back‑compat code paths)

> Assumption: **No experiments are currently running** (as you stated), and we *do* want to carry historical data.

1. **Create a branch**

   ```bash
   git checkout -b docid-partitions
   ```

2. **Apply the code changes above** (or use multiple commits: partitions → assets → IO → checks → scripts → README).

3. **Clean old dynamic partitions (optional but recommended)**
   Delete the `draft_tasks`, `essay_tasks`, and `evaluation_tasks` partition sets from your Dagster instance state (or just let them become unused; they won't be referenced by code anymore).

4. **Re‑materialize the tasks layer** to (re)write task CSVs with `doc_id`s and **register new doc‑id partitions**:

   ```bash
   export DAGSTER_HOME=$(pwd)/dagster_home
   uv run dagster asset materialize --select "group:task_definitions" -f daydreaming_dagster/definitions.py
   ```

   After this, Dagster will list partitions under `draft_docs`, `essay_docs`, and `evaluation_docs`, keyed by `doc_id`. (The task assets already compute `doc_id` and can be extended to generate **replicates** as multiple rows/partitions.)

5. **Historical data**

   * Because the store is already **doc‑id–keyed** (`data/docs/<stage>/<doc_id>`), nothing moves on disk; the **new partitions will directly point at existing doc dirs**. Parsing/summary assets already work doc‑id‑first. ([GitHub][11])
   * If you have historical drafts/essays that were produced with *stable* doc ids (no `run_id` salt), they simply appear as **one doc‑id partition each**. To run **replicates** going forward, add new rows with new `doc_id`s to the tasks CSVs (either by salting with run id in the producer or pre‑allocating multiple doc ids per “logical” task).

6. **Smoke test**

   * Pick one `doc_id` from `data/2_tasks/draft_generation_tasks.csv` and materialize:

     ```bash
     DRAFT_DOC=$(awk -F, 'NR==1{for(i=1;i<=NF;i++)h[$i]=i} NR==2{print $h["doc_id"]}' data/2_tasks/draft_generation_tasks.csv)
     uv run dagster asset materialize --select "group:generation_draft" --partition "$DRAFT_DOC" -f daydreaming_dagster/definitions.py
     ```
   * Do the same for an essay and for an evaluation `doc_id`.

7. **Rebuild analysis tables (optional)**
   The cross‑experiment scripts and assets are doc‑id‑first and will work unchanged:

   ```bash
   ./scripts/rebuild_results.sh
   ```

   (Parsing already scans `data/docs/evaluation/**` and enriches off metadata; no `task_id` dependency.) ([GitHub][13])

---

## Follow‑ups to minimize `task_id` surface area

These are fast, low‑risk cleanups that further reduce `task_id` reliance:

* **Stop passing task ids through metadata** in `Document` writes. Today you write `{"task_id": ...}` for provenance; keep if you want breadcrumbs, but downstream code should never rely on it for joins. (Your parsing/summary already don’t.) ([GitHub][2])
* **Docstrings & README**: update any language that suggests grouping by `essay_task_id` (the analysis asset docstring still mentions that wording). The code already groups by `combo_id`, template, and model. ([GitHub][14])
* **Optional**: add `ExperimentConfig.replicates` and expand draft/essay task rows (`N` doc ids per logical row). Evaluations already generate doc ids salted by `run_id`; consider doing the same for drafts/essays if you prefer **per‑run ids** instead of pre‑allocating multiple rows. ([GitHub][6])
* **Optional**: add a simple **`DocIDPromptIOManager`** that writes/reads by `partition_key` only (no CSV), to remove the last coupling to tasks CSVs for prompts.

---

## Risks & mitigations

* **Dagster state**: changing partition set names creates “new” assets from the scheduler’s perspective. Mitigation: we’re doing this in one cutover with no back‑compat, and you said no runs are active.
* **Replicates strategy**: decide whether to (a) pre‑allocate multiple doc ids per logical row, or (b) salt doc ids by `run_id` when generating. Both are compatible with doc‑id partitions; (b) requires planning for prompt persistence if `doc_id` isn’t known before run. (Pre‑allocating keeps prompt IO simple.)
* **Tests**: any tests that asserted task‑key partition behavior will need updates to assert **doc‑id** partitioning.

---

## Summary checklist (what to change)

* [ ] Rename partition sets to `draft_docs`, `essay_docs`, `evaluation_docs` and key them by `doc_id`. ([GitHub][1])
* [ ] Switch all generation/evaluation assets to treat `context.partition_key` as `doc_id` and look up rows via `"doc_id"`. ([GitHub][2])
* [ ] Register doc‑id partitions in `group_task_definitions.py` (use `df["doc_id"]`). ([GitHub][6])
* [ ] Rewire `DocsPromptIOManager` calls in `definitions.py` to `id_col="doc_id"`. ([GitHub][5])
* [ ] Simplify asset checks to treat the partition key as the `doc_id`. ([GitHub][7])
* [ ] Update README and any docstrings/examples. ([GitHub][8])
* [ ] (Optional) Add replicates for drafts/essays (multiple `doc_id`s per logical row) or salt with `run_id` like evaluations already do. ([GitHub][6])

If you’d like, I can draft the exact patch as a single PR (with or without the replicates feature).

[1]: daydreaming_dagster/assets/partitions.py "raw.githubusercontent.com"
[2]: daydreaming_dagster/assets/group_generation_draft.py "raw.githubusercontent.com"
[3]: daydreaming_dagster/assets/group_generation_essays.py "raw.githubusercontent.com"
[4]: daydreaming_dagster/assets/group_evaluation.py "raw.githubusercontent.com"
[5]: daydreaming_dagster/definitions.py "raw.githubusercontent.com"
[6]: daydreaming_dagster/assets/group_task_definitions.py "raw.githubusercontent.com"
[7]: daydreaming_dagster/checks/documents_checks.py "raw.githubusercontent.com"
[8]: DayDreamingDayDreaming: An experiment to find out how LLMs can invent the \"Daydreaming LLMs\" idea from gwerns essay."
[9]: daydreaming_dagster/utils/ids.py "raw.githubusercontent.com"
[10]:daydreaming_dagster/assets/results_processing.py "raw.githubusercontent.com"
[11]:daydreaming_dagster/utils/document.py "raw.githubusercontent.com"
[12]:scripts/register_partitions_for_generations.py "raw.githubusercontent.com"
[13]:scripts/aggregate_scores.py "raw.githubusercontent.com"
[14]:daydreaming_dagster/assets/results_analysis.py "raw.githubusercontent.com"
### G) Asset checks: verify docs store by `doc_id`
Update checks to use the partition key (`doc_id`) directly:

```diff
@asset_check(asset=draft_response_asset)
def draft_files_exist_check(context) -> AssetCheckResult:
    doc_id = context.partition_key
    base = data_root / "docs" / "draft" / str(doc_id)
    ok = (base / "parsed.txt").exists() or (base / "raw.txt").exists()
    return AssetCheckResult(passed=bool(ok), metadata={"doc_dir": MetadataValue.path(str(base))})
```

Repeat for essay/evaluation checks.

### H) Schedule and docs
- Update README and guides to reflect doc‑id partitions and new CLI examples (`--partition <doc_id>`).
- Adjust any schedule logic or partition registration helpers accordingly.

## Migration steps (operational)

1) Preconditions
   - Ensure task CSVs include `doc_id` and (for essays/evals) `parent_doc_id` (already enforced; linker exists if needed).
   - Ensure docs store is consistent (run the evaluation parsed backfill; checks pass).

2) Flip partitions and assets (A–F) on a feature branch; update tests and docs.

3) Validate locally
   - Materialize one draft/essay/evaluation partition using a known `doc_id`.
   - Run checks; confirm doc‑id partition keys reflect on disk as expected.

4) Roll out
   - Merge and re‑seed partitions by reading `doc_id` columns from existing task CSVs.
   - Communicate the new partition keys to the team (CLI examples), update any automation.

## Acceptance criteria
- All stage assets accept `doc_id` as the partition key and run end‑to‑end.
- Dynamic partitions lists are populated with `doc_id`s, not task ids.
- Asset checks verify existence directly by `doc_id` and stage.
- Docs/README reflect the new usage.
- No raw‑fallback in aggregation; parsed.txt‑first remains.

## Notes
- The linker (`scripts/link_tasks_to_latest_docs.py`) remains useful for continuity when tasks are regenerated mid‑iteration, but it is not required for the doc‑id partition migration itself.
- ### I) Scripts (registration and selection)
  - `scripts/register_partitions_for_generations.py`
    - Register dynamic partitions using the new names and keys:
      - `draft_docs` ← distinct doc_id from data/2_tasks/draft_generation_tasks.csv
      - `essay_docs` ← distinct doc_id from data/2_tasks/essay_generation_tasks.csv
      - `evaluation_docs` ← distinct doc_id from data/2_tasks/evaluation_tasks.csv
    - Stop registering task‑key partitions (draft_tasks/essay_tasks/evaluation_tasks).
    - Validate required columns (`doc_id` for all three; `parent_doc_id` for essays/evaluations). Fail fast with a clear file path in the error message.
    - Do not modify essay_generation_tasks.csv (treat curated inputs as source of truth); keep warning/helpful diagnostics only.
  - `scripts/select_top_prior_art.py`
    - Ensure curated essay_generation_tasks.csv includes `doc_id` (the essay doc id) and `parent_doc_id` (the draft doc id).
    - Keep emitting rows by doc‑id; no reliance on task keys for registration.
    - Minor: update any usage/help strings that mention task‑key partitions so examples show `doc_id` flow.

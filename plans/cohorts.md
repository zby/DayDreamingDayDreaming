**Short answer:** Yes—give the full Cartesian path a **cohort id** as well.
Make the default for the full cube **deterministic** (derived from the exact task set + prompt/code fingerprints), so a “same config” rerun yields the same IDs and the same partitions. Curated runs remain timestamped (or user‑named) to avoid overwrites and keep histories separate.

---

## Recommended policy

| Mode              | Where you use it                                             | Default `cohort_id`                                 | Behavior                                                                                  |
| ----------------- | ------------------------------------------------------------ | --------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| **Deterministic** | **Full Cartesian**                                           | `cube-<manifest-hash>`                              | Idempotent: same config ⇒ same IDs. Great for a canonical baseline and stable partitions. |
| **Timestamped**   | **Curated** (top‑prior‑art, regenerations), ad‑hoc full cube | `curated-<UTC>-<gitsha>` (or `cube-<UTC>-<gitsha>`) | Every run gets fresh IDs; no overwrites; clear lineage.                                   |
| **Explicit**      | Either path when you want to name a cohort                   | user‑supplied (e.g. `baseline-v3`)                  | Useful for scheduled cohorts or paper releases.                                           |

> *Manifest hash* = a stable hash over the **exact Cartesian set** plus the prompt/template versions and any code/pipeline fingerprint that would change the outputs. If any of those change, the cohort id changes.

---

## What this buys you

* **One mental model everywhere:** `gen_id = f(stage, task_id, cohort_id)`.
* **Full cube stays stable** (deterministic cohort), so you don’t explode partitions on every run.
* **Curated runs stop overwriting** because they carry a fresh cohort id by default.
* **Lineage stays explicit**: cohort id + origin\_\* columns tie curated rows back to the baseline cohort.

---

## Minimal implementation plan (fits both paths)

1. **Introduce a single helper**

```python
# utils/cohorts.py
import json, datetime, subprocess, os
from hashlib import blake2s

PIPELINE_VERSION = "2025-09-09"  # bump when ID algo changes

def short_git_sha():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True
        ).strip()
    except Exception:
        return None

def compute_cohort_id(kind: str, manifest: dict, mode: str = "deterministic", explicit: str | None = None) -> str:
    """
    kind: 'cube' or 'curated' (affects the prefix only)
    manifest: must fully describe the task set and prompt/code fingerprints
    mode: 'deterministic' | 'timestamped'
    """
    if explicit:
        return explicit

    if mode == "timestamped":
        ts = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%SZ")
        sha = short_git_sha() or "nogit"
        return f"{kind}-{ts}-{sha}"

    # deterministic
    # Only include inputs that change the *task set* or the generated outputs
    stable = {
        "combos": manifest["combos"],            # the active combo list
        "llms": manifest["llms"],                # model identifiers
        "templates": manifest["templates"],      # draft/essay template names/versions
        "prompt_versions": manifest.get("prompt_versions"),
        "pipeline_version": PIPELINE_VERSION,
    }
    s = json.dumps(stable, sort_keys=True, separators=(",", ":")).encode("utf-8")
    h = blake2s(s, digest_size=6).hexdigest()
    return f"{kind}-{h}"
```

2. **Always pass a cohort id into ID reservation**

```python
# wherever you compute gen ids
gen_id = reserve_gen_id(stage, task_id, run_id=cohort_id)
```

3. **Full Cartesian path**

* Build a `manifest` from the active combos, LLMs, templates, and prompt versions.
* Compute: `cohort_id = compute_cohort_id("cube", manifest, mode="deterministic")`
* Use that for **all** draft/essay/eval `reserve_gen_id` calls.
* Persist the manifest for reproducibility under `data/cohorts/<cohort_id>/manifest.json`.
* Add a `cohort_id` column to all task CSVs (draft/essay/evaluation). Optionally (phase 2), nest CSVs under `data/2_tasks/<cohort_id>/…` if isolated directories are preferred.

4. **Curated scripts**

* Accept `--cohort-id` (optional) and `--cohort-mode {timestamped,deterministic}`.
* Default curated to `timestamped`.
* When curating from history, store lineage columns:

  * `origin_essay_gen_id`, `origin_draft_gen_id`, and `origin_cohort_id`
* For *this* run’s tasks:

  * `draft_gen_id = reserve_gen_id("draft", draft_task_id, run_id=cohort_id)`
  * `essay_gen_id = reserve_gen_id("essay", essay_task_id, run_id=cohort_id)`
  * `parent_gen_id = draft_gen_id` (never a historical id)

5. **Asset layer**

* Read `cohort_id` from (in order) config/resources → env var `DD_COHORT` → tasks CSV column; otherwise compute deterministically (only for the full cube path).
* For evaluation assets that intentionally target prior essays without regenerating, allow an optional `eval_input_cohort_id` (so evals can point at a prior cohort’s gen_ids), while still writing new evaluation gen_ids under the current `cohort_id`.

---

## Migration (so you don’t break existing data)

* Tag legacy runs as `cohort_id="legacy"` by writing a tiny `meta.json` beside their `gen_id` folders, or maintain a mapping file `data/cohorts/legacy/index.json`.
* Leave old `gen_id` directories untouched; the new scheme only affects **future** runs.
* If you already have a canonical “baseline full cube,” you can re‑emit its tasks with a deterministic `cohort_id` (derived from the same manifest) so future curated runs can reference it via `origin_cohort_id`.

---

## Gotchas & guardrails

* **Partition explosion:** Don’t make the full cube timestamped by default—keep it deterministic so the number of partitions stays bounded.
* **Consistency check:** Validate that all rows in a tasks file share the same `cohort_id` (unless you intentionally support mixed cohorts).
* **Repro safety:** Bump `PIPELINE_VERSION` when you change the prompt/IO format or the id function itself; that guarantees the deterministic cohort changes, preventing silent overwrites of “the same” cube with different semantics.
* **Path strategy:** If you keep a single `data/2_tasks/*.csv`, add a `cohort_id` column. If you prefer isolation, nest under `data/2_tasks/<cohort_id>/…`. Either works; isolation is easier to reason about.

---

### Bottom line

* **Yes**: give the full Cartesian path a **cohort id**.
* Default it to a **deterministic manifest‑hash** so your “baseline cube” remains stable and rerunnable.
* Keep curated runs **timestamped or named** to avoid overwrites and to preserve histories.
* Unify ID creation as `reserve_gen_id(stage, task_id, run_id=cohort_id)` across the board.

---

## Added implementation details (to avoid ambiguity)

### A) Manifest: exact shape and hashing
- combos: sorted list of the exact `combo_id`s used (strings)
- llms: sorted list of generation model ids used for drafts/essays (and evaluation model ids if you want eval in the same cohort)
- templates:
  - draft_templates: sorted list of draft template ids used
  - essay_templates: sorted list of essay template ids used
- prompt_versions: an object with stable file hashes or CSV version columns (e.g., SHA256 of each template file or a version field)
- pipeline_version: constant string (bump on semantic changes)

Hashing: `blake2s(json.dumps(stable, sort_keys=True, separators=(",", ":")).encode(), digest_size=6)` ⇒ 12‑char hex, prefixed by `cube-`.

### B) Persistence: where `cohort_id` lives
- Tasks CSVs: add `cohort_id` column to
  - `data/2_tasks/draft_generation_tasks.csv`
  - `data/2_tasks/essay_generation_tasks.csv`
  - `data/2_tasks/evaluation_tasks.csv`
- Metadata files: include `"cohort_id": <id>` in `data/gens/<stage>/<gen_id>/metadata.json` for all stages
- Manifest: write `data/cohorts/<cohort_id>/manifest.json` (contains the manifest used to compute the id)

### C) Reading and defaults
- Assets read `cohort_id` with precedence: resource/config → env `DD_COHORT` → tasks CSV column → (deterministic compute only for the full cube path)
- Curated scripts default to `timestamped`, unless `--cohort-id` or `--cohort-mode deterministic` is provided

### D) Asset changes (group_task_definitions)
- Compute or accept `cohort_id` once per run
- Reserve ids with `run_id=cohort_id` for all draft/essay/eval rows
- Add `cohort_id` column to returned DataFrames (all three)
- Propagate `cohort_id` into essay/eval metadata.json when writing artifacts

### E) Scripts
- `scripts/register_partitions_for_generations.py`
  - Flags:
    - `--cohort-id <str>` (optional explicit)
    - `--cohort-mode {deterministic,timestamped}` (default: deterministic for full cube, timestamped for curated use)
  - Compute cohort_id (if not explicit) and pass into all reserve_gen_id calls
  - Write `cohort_id` column into outputs
  - Write manifest at `data/cohorts/<cohort_id>/manifest.json` if in cube mode
- `scripts/select_top_prior_art.py`
  - Add `--cohort-id` and `--cohort-mode` (default: timestamped)
  - Output curated essay_generation_tasks.csv with `cohort_id` and lineage columns: `origin_cohort_id`, `origin_gen_id` (optional)
  - Reserve new draft/essay gen_ids using the chosen `cohort_id`; do not reuse origin ids

### F) Validations and guardrails
- Enforce a single `cohort_id` per tasks CSV (error if mixed) unless explicitly allowed
- Log `cohort_id` at materialization time for traceability
- Bump `PIPELINE_VERSION` when prompts/IO/ID semantics change to force a new deterministic cohort

### G) Tests (minimum set)
- Deterministic: with same manifest/config, `cohort_id` and resulting gen_ids stay identical across runs
- Curated: timestamped mode produces different `cohort_id`/gen_ids per run
- Parent linkage unaffected: essay parent points to draft gen_id under the same cohort (for curated recompute); evaluation parent points to essay gen_id (current or prior cohort if configured)
- Tasks CSVs contain `cohort_id`; metadata.json for draft/essay/eval contains `cohort_id`

### H) Rollout & migration
- Do not backfill old gens; treat older data as `cohort_id = "legacy"` in reporting if needed
- New runs append `cohort_id` to CSVs and metadata
- Partition naming remains gen_id; cohort is metadata/CSV context

### I) Open questions (to confirm before coding)
- Should evaluation be part of the same deterministic cohort as generation, or have a separate cohort dimension? (Default: same cohort for simplicity; allow override via `eval_input_cohort_id` when targeting prior cohorts.)
- Manifest placement: always write for cube; optional for curated runs?
- Task CSV isolation: stay flat with `cohort_id` column, or move to `data/2_tasks/<cohort_id>/…` (phase 2)?

---

## Execution plan (PR breakdown)
1) Add `utils/cohorts.py` with `compute_cohort_id`, `short_git_sha`, `PIPELINE_VERSION`, and optional `write_manifest`
2) Wire `cohort_id` into `group_task_definitions` (columns + `reserve_gen_id(..., run_id=cohort_id)`), and add it to metadata.json in generation assets
3) Update `register_partitions_for_generations.py` and `select_top_prior_art.py` flags and outputs; write manifest in cube mode
4) Add/extend tests for deterministic and curated behavior
5) Optional: document `cohort_id` usage in README + operating guide

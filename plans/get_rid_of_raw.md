# Proposal: Remove Manual “raw_data” Entry Point and Add Timestamp-Driven Updates

## What We Have Today
- Entry assets in `group:raw_data`:
  - `concepts` (reads `data/1_raw/concepts/*` and builds `List[Concept]`)
  - `llm_models` (reads `data/1_raw/llm_models.csv`)
  - `link_templates` (reads `data/1_raw/link_templates.csv` + `generation_templates/links/*.txt`)
  - `essay_templates` (reads `data/1_raw/essay_templates.csv` + `generation_templates/essay/*.txt`)
  - `evaluation_templates` (reads `data/1_raw/evaluation_templates.csv` + `evaluation_templates/*.txt`)

- These feed the “task definition” assets in `group:task_definitions`:
  - `content_combinations`, `content_combinations_csv`, `link_generation_tasks`, `essay_generation_tasks`, `evaluation_tasks`

- Generation/evaluation assets are split into partitioned groups:
  - `group:generation_links` (`links_prompt`, `links_response`)
  - `group:generation_essays` (`essay_prompt`, `essay_response`)
  - `group:evaluation` (`evaluation_prompt`, `evaluation_response`)

- Results processing and cross‑experiment assets downstream of evaluation.

- Auto‑materialize is enabled globally via `dagster_home/dagster.yaml`:
  - `auto_materialize.enabled: true`
  - Some tracking assets already opt‑in with `AutoMaterializePolicy.eager()`.

Pain point: You currently run `group:raw_data` (and often `group:task_definitions`) as an explicit “setup” step, but you don’t directly inspect the raw assets’ outputs.


## Goals
- Remove the need to manually kick `group:raw_data` as an entry point.
- Make upstream changes to input files automatically mark downstream assets as stale.
- Ensure expensive LLM assets do not silently re‑run without an explicit decision.


## Recommendation (Minimal Change, Robust Behavior)
1) Keep the current raw assets for clarity and reuse, but make them auto‑materialize when their inputs change.
   - Add observable Source Assets that compute a fingerprint (mtime/hash) over the relevant files.
   - Make each raw asset depend on the corresponding observable source asset (via `deps=[AssetKey(...)]`).
   - Set `auto_materialize_policy=AutoMaterializePolicy.eager()` on raw assets (and optionally on the cheap `task_definitions` assets), so the asset daemon materializes them automatically when the source fingerprint changes.

2) Do NOT auto‑materialize the LLM‑calling assets by default.
   - Leave `links_response`, `essay_response`, and `evaluation_response` manual/partition‑driven to avoid surprise API usage/costs.
   - Continue using the existing two‑phase runs for links/essays with partitions.

This yields:
- Any change to `data/1_raw/**/*` marks raw assets stale and re‑builds them automatically.
- Task definitions re‑build automatically from the raw assets.
- You only run generation/evaluation when you intend to.


## How Dagster Detects “Outdated” via Timestamps
Use Observable Source Assets to emit a `data_version` (e.g., a checksum derived from file mtimes or file contents). With auto‑materialize enabled, Dagster’s asset reconciliation daemon marks all downstream assets as stale when the upstream source `data_version` changes and will materialize assets that carry an eager policy.

Sketch of the approach:

```python
from dagster import observable_source_asset, AssetKey, AutoMaterializePolicy
from dagster import AssetExecutionContext, ObservationResult
from hashlib import sha256
from pathlib import Path

def _fingerprint_dir(dir_path: Path, pattern: str = "**/*") -> str:
    files = sorted([p for p in Path(dir_path).glob(pattern) if p.is_file()])
    h = sha256()
    for p in files:
        # Cheap heuristic: combine path + mtime; use content hash if you prefer
        h.update(str(p.relative_to(dir_path)).encode("utf-8"))
        h.update(str(p.stat().st_mtime_ns).encode("utf-8"))
    return h.hexdigest()

@observable_source_asset(name="raw_concepts_source")
def raw_concepts_source(context: AssetExecutionContext) -> ObservationResult:
    root = Path("data/1_raw/concepts")
    fv = _fingerprint_dir(root)
    return ObservationResult(
        metadata={"path": str(root), "fingerprint": fv},
        data_version=fv,
    )

# Repeat for llm_models.csv, link/essay templates, and evaluation templates.

# Then, in the raw asset declaration:
@asset(
    group_name="raw_data",
    required_resource_keys={"data_root"},
    deps=[AssetKey("raw_concepts_source")],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def concepts(context) -> List[Concept]:
    ...  # unchanged loader code
```

Notes:
- Use one observable source per logical dataset. For CSV+folder combos, compute a single fingerprint over both the CSV and the directory tree.
- The fingerprint can be timestamp‑based (fast) or content‑hash based (robust); either works for “mark stale when inputs change”.
- Because `auto_materialize.enabled` is already true in `dagster.yaml`, adding the observable sources + eager policies is sufficient; no extra sensor wiring is needed.


## Optional: Auto‑materialize Task Definitions
If desired, add `auto_materialize_policy=AutoMaterializePolicy.eager()` to:
- `content_combinations`, `content_combinations_csv`, `link_generation_tasks`, `essay_generation_tasks`, `evaluation_tasks`

They are fast/data‑only, so auto‑reconciling them is safe and removes another manual step. Keep the generation/evaluation assets manual.


## Alternative (Heavier Change): Remove Raw Assets Entirely
You could inline the file reads directly into task definition assets (e.g., have `link_generation_tasks` load `llm_models` and `link_templates` itself). Downsides:
- Loses modularity and testability you already have (unit tests around loaders, clean separation of concerns).
- Makes cross‑asset reuse harder (e.g., `evaluation_templates` used by both evaluation prompt + other analyses).

Given your codebase and tests, the observable source + auto‑materialize tweak is the least risky change that achieves the goal.


## Run UX After This Change
- You can stop running `group:raw_data` manually.
- Edit any `data/1_raw/**/*` input → the daemon observes new versions → raw assets re‑materialize automatically → task definitions follow if opted‑in.
- Continue to launch partitions for links/essays/evaluation intentionally.

Suggested simplification in docs/README:
- Replace “Materialize setup assets” step with a note: “Raw and task definition assets auto‑update when inputs change. Only trigger generation/evaluation partitions explicitly.”


## Edge Cases and Practical Tips
- Avoid accidental LLM runs: keep auto‑materialize off for `links_response`, `essay_response`, `evaluation_response`.
- Stability of fingerprints: use content hashes if editors touch mtimes without semantic changes.
- Large directories: consider limiting the glob to expected extensions, or compute a structured hash that includes CSV contents plus a hash of only `*.txt` under the template dirs.
- Backfills: if you change experiment config that affects downstream deterministically, consider whether to auto‑materialize `task_definitions` or keep them manual to control when partitions are (re)registered.


## Conclusion
- Yes, we can remove the “manual entry point” nature of `group:raw_data` without deleting it by:
  - Adding observable source assets that compute a file‑system fingerprint.
  - Adding `AutoMaterializePolicy.eager()` to raw (and optionally task definition) assets.
- Dagster will then use observed file timestamps (or content hashes) to deduce staleness and re‑build cheap assets automatically, while keeping expensive LLM steps manual.


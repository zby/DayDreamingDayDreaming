# Dagster Dynamic Partitions: Practical Notes

Short, current guidance for our two-phase (draft → essay → evaluation) pipeline.

Current Model
- Partitions: `draft_gens`, `essay_gens`, `evaluation_gens` (DynamicPartitionsDefinition), keyed by `gen_id`.
- Registration: created by `cohort_membership` (group `cohort`) from `data/cohorts/<cohort_id>/membership.csv`.
- I/O: GensPromptIOManager persists prompts to `data/gens/<stage>/<gen_id>/prompt.txt`; assets write responses to `data/gens/<stage>/<gen_id>/{raw,parsed,metadata}.txt/json`. Cross‑phase reads use `parent_gen_id` to load parents from the gens store.

Gotchas (and fixes)
- Registration order: start Dagster with the daemon; raw + cohort assets auto-update when `data/1_raw/**/*` changes. If needed, seed `cohort_id,cohort_membership` once before generation/evaluation assets.
- Key consistency: `gen_id` is the ground truth for partitions. Use `parent_gen_id` to link stages.
- Cross‑phase reads: assets like `essay_prompt` and `evaluation_prompt` load upstream text by `parent_gen_id` directly from the filesystem (`data/gens/<stage>/<gen_id>`); no cross‑partition IO‑manager context is needed.
- Stale CSVs: if a downstream asset expects new columns in `parsed_scores` (e.g., `draft_template`) and fails, rematerialize upstream with a clear error. Example: `uv run dagster asset materialize --select parsed_scores -f src/daydreaming_dagster/definitions.py`.
- File names: files are stored under `data/gens/<stage>/<gen_id>` with fixed filenames; no `{id}_vN.txt` for prompts/responses.

Why not multi‑dimensional partitions?
- Dagster supports only 2D multi‑partitions; our model has ≥3 dimensions. Encoding them into composite keys adds more complexity than our task tables + dynamic partitions. We’ll revisit if Dagster adds richer partitioning.

Common commands
- Initialize cohort (optional): `uv run dagster asset materialize --select "group:cohort" -f src/daydreaming_dagster/definitions.py`
- Run a draft: `uv run dagster asset materialize --select "group:generation_draft" --partition "$DRAFT_GEN_ID" -f src/daydreaming_dagster/definitions.py`
- Run an essay: `uv run dagster asset materialize --select "group:generation_essays" --partition "$ESSAY_GEN_ID" -f src/daydreaming_dagster/definitions.py`
- Evaluate an essay: `uv run dagster asset materialize --select "evaluation_prompt,evaluation_raw,evaluation_parsed" --partition "$EVALUATION_GEN_ID" -f src/daydreaming_dagster/definitions.py`

When to revisit
- If Dagster adds ≥3D partitions or simpler cross‑partition addressing.
- If we change task ID semantics or I/O layout.

Appendix: Composite Keys Attempt — Why It Broke (and how to retry)
- 2D limit vs. 3D reality: We needed at least three dimensions (combo, template/phase, model). Packing two into one composite key (e.g., "combo|template") led to pervasive string parsing.
- Inconsistent file naming: Different assets wanted different orders (e.g., combo|template vs. combo_template|model), multiplying parsing branches and mistakes.
- Cross‑partition reads: Upstream/downstream needed each other’s composite parts; reconstructing the “right” key for IO managers was brittle and produced unclear errors.
- Debuggability: Key errors surfaced as generic parse failures. Mapping a filename back to its logical dims was slow and error‑prone.
- Hidden coupling: Every consumer re‑implemented split/join logic; a small delimiter change could silently break multiple places.

If we try again, do this first
- Centralize encode/decode: Provide one KeyCodec utility with encode(dim1, dim2, …) and decode(key) used everywhere (assets, scripts, tests).
- One delimiter, one order: Document key format once (e.g., combo|template|model) and never deviate per asset.
- Validate at boundaries: Assert round‑trip integrity (decode(encode(..)) == dims) in asset starts; fail with friendly messages.
- Add tests: Unit tests for KeyCodec; golden tests for filename ↔ key ↔ columns; property tests for weird chars.
- Minimize cross‑partition IO: Prefer pulling IDs from small CSVs over reconstructing composite keys when possible.
- Consider hybrid: Use MultiPartitionsDefinition for two dims (e.g., model, phase) and keep the third dim in a task table to avoid over‑packing.

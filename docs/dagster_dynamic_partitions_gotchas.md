# Dagster Dynamic Partitions: Practical Notes

Short, current guidance for our two-phase (links → essay → evaluation) pipeline.

Current Model
- Partitions: `link_tasks`, `essay_tasks`, `evaluation_tasks` (DynamicPartitionsDefinition).
- Registration: created by assets in `group:task_definitions` and written to `data/2_tasks/`.
- I/O: PartitionedTextIOManager stores prompts/responses by task IDs; cross‑phase loads use a small helper context.

Gotchas (and fixes)
- Registration order: start Dagster with the daemon; raw + task assets auto-update when `data/1_raw/**/*` changes. If needed, seed `group:task_definitions` once before generation/evaluation assets.
- Key consistency: task IDs are the ground truth. Always use `link_task_id` for links and `essay_task_id` for essays.
- Cross‑phase reads: assets like `essay_prompt` and `evaluation_prompt` read upstream responses by FK using MockLoadContext (utils/shared_context.py). This is expected.
- Stale CSVs: if a downstream asset expects new columns in `parsed_scores` (e.g., `link_template`) and fails, rematerialize upstream with a clear error. Example: `uv run dagster asset materialize --select parsed_scores -f daydreaming_dagster/definitions.py`.
- File names: links files are saved by `link_task_id`; essay files by `essay_task_id`. Scripts that pair them must derive link_task_id from essay_task_id.

Why not multi‑dimensional partitions?
- Dagster supports only 2D multi‑partitions; our model has ≥3 dimensions. Encoding them into composite keys adds more complexity than our task tables + dynamic partitions. We’ll revisit if Dagster adds richer partitioning.

Common commands
- Initialize tasks (optional): `uv run dagster asset materialize --select "group:task_definitions" -f daydreaming_dagster/definitions.py`
- Run a link: `uv run dagster asset materialize --select "group:generation_links" --partition "$LINK_TASK_ID" -f daydreaming_dagster/definitions.py`
- Run an essay: `uv run dagster asset materialize --select "group:generation_essays" --partition "$ESSAY_TASK_ID" -f daydreaming_dagster/definitions.py`
- Evaluate an essay: `uv run dagster asset materialize --select "evaluation_prompt,evaluation_response" --partition "$EVALUATION_TASK_ID" -f daydreaming_dagster/definitions.py`

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

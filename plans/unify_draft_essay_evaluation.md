Unify the three stages (draft, essay, evaluation) around gen-id partitions and a single gens store. This plan reflects the current codebase and lists the remaining work to fully standardize behavior end-to-end.

Status: ready to implement (most foundations are already in place)

Objective
- One pattern for all stages: gen-id partitions, parent_gen_id linkage, I/O to data/gens/<stage>/<gen_id>, and consistent parsing/metadata.

Current state (as of this plan)
- Partitions: dynamic partitions keyed by gen_id for draft, essay, evaluation.
- Prompts: persisted via GensPromptIOManager to data/gens/<stage>/<gen_id>/prompt.txt.
- Responses: all stage assets use Generation.write_files to write raw.txt, parsed.txt (if valid), prompt.txt (when available), and metadata.json under data/gens.
- Parent linkage: assets take parent_gen_id from tasks CSV and read parents directly from data/gens using filesystem helpers.
- Parsing: drafts use draft_templates.csv parser mapping (identity if absent). Evaluation writes numeric-only parsed.txt; does not write parsed.txt on parse failure. Aggregator reads parsed.txt only (no raw fallback).
- Tests: use gens store; PartitionedTextIOManager removed; versioned read fallback removed from active pipeline.

Gaps to close
1) Standardize metadata keys written by all three stages (ensure parity and add a tiny helper).
2) Unify asset checks via a small generic generator (optional; current checks already point to data/gens but can be simplified).
3) Clean up docs and guides that still mention task-id files, 3_generation/, or PartitionedTextIOManager.
4) Optional: add Generation.load for ergonomic parent reads (current filesystem_rows helpers are fine, but a single API improves readability).

Implementation plan
1) Metadata normalization (small helper + apply in 3 assets)
   - Add utils/metadata.py with:
     - build_generation_metadata(stage, gen_id, parent_gen_id, template_id, model_id, task_id, extra={}) -> dict
   - Update draft_response, essay_response, evaluation_response to call this and pass consistent keys:
     - stage, gen_id, parent_gen_id, template_id (draft_template or essay/evaluation template), model_id, task_id, function, run_id, usage if available.
   - Outcome: consistent metadata.json across all stages; simpler downstream enrichment.

2) Optional ergonomic read API
   - Add Generation.load(gens_root, stage, gen_id) returning an object with prompt/raw/parsed/metadata (best-effort reads).
   - Replace direct filesystem_rows calls in assets with Generation.load for clarity. Keep filesystem_rows for other scripts if needed.

3) Checks unification (optional)
   - Provide make_generation_check(stage, asset_name) that asserts presence of parsed.txt under gens/<stage>/<gen_id>.
   - Replace bespoke checks with instances from the generator or keep current checks but remove any dead code (e.g., versioned path helpers).

4) Docs alignment
   - Update docs/architecture/* and docs/dagster_dynamic_partitions_gotchas.md:
     - gen-id partitions (draft_gens, essay_gens, evaluation_gens)
     - I/O: prompts via GensPromptIOManager; responses via Generation to data/gens
     - Parent linkage via parent_gen_id
     - Remove PartitionedTextIOManager usage notes and 3_generation/ paths
   - Update README: testing commands prefer .venv/bin/pytest; gens store examples.

5) Scripts sanity checks
   - Ensure scripts/register_partitions_for_generations.py treats essay_generation_tasks.csv as read-only (already done) and registers gen_id partitions.
   - aggregator/backfill already use parsed.txt-only; keep as-is.

6) CI/library guards (lightweight)
   - Grep for lingering 3_generation/ or PartitionedTextIOManager references and remove.
   - Confirm no active code reads versioned _vN files (tests for versioned utilities remain fine).

Risks and mitigations
- Divergent metadata shapes: mitigated by central helper applied uniformly.
- Doc drift: addressed by a focused sweep of architecture and gotchas docs.

Done criteria
- All three stages write consistent metadata.json with the normalized keys.
- Asset checks verify gens store outputs uniformly.
- Docs and examples refer only to gen-id + data/gens.
- No references to PartitionedTextIOManager or 3_generation/ remain in production code/tests.

Notes
- We intentionally keep prompts persisted via the IO manager rather than adding a write_prompt_file helper to Generation; this keeps Dagster’s I/O layering clean. Generation.load is optional sugar for parent reads.

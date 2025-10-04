# Chunk 3 Walkthrough — Results & Analysis Layer

Date: 2025-10-04
Prepared by: CouCou Codex

## Modules Reviewed
- `assets/results_processing.py`
- `assets/results_analysis.py`
- `assets/results_summary.py`
- `assets/cross_experiment.py`
- `utils/evaluation_scores.py`
- Related CLI scripts (`scripts/aggregate_scores.py`, `scripts/build_pivot_tables.py`, `scripts/report_promising_essays.py`) — skimmed for entry points

## High-Level Observations
- The layer still assumes cross-experiment fallbacks (e.g., scanning entire `gens/evaluation` trees, warning and returning empty DataFrames). Chunk 3 should convert these paths into explicit failures so we detect missing inputs early.
- Large portions of the analysis assets are defensive scaffolding for historical evaluator variance work. Many branches do nothing but log warnings and hand back empty frames, which complicates downstream logic.
- The spec-first pipeline already provides allowlists via manifest; several assets re-read specs or rebuild allowlists ad hoc instead of consuming existing manifest metadata.
- `utils/evaluation_scores` implements broad fallback behaviour (missing parsed text, metadata) and pushes through `None` scores. We should decide whether to keep the permissive behaviour or fail fast when evaluation artefacts are missing.
- CLI scripts still support multi-mode selection (`--score-span`, `--max-only`, etc.) tailored for curated workflows. Confirm whether these modes remain necessary.

## Detailed Findings

### assets/results_processing.py (`cohort_aggregated_scores`)
- Pure helper `cohort_aggregated_scores_impl` depends on `scores_aggregator.parse_all_scores` and `membership_service.stage_gen_ids`. No fallback: if `membership_service` returns empty list, aggregator returns an empty frame silently.
- Dagster asset `cohort_aggregated_scores` fails if `partition_key` missing (good), but happily writes empty outputs with `enriched=False`. Consider raising when membership has no evaluation rows.
- Metadata calculation uses `utils.evaluation_processing.calculate_evaluation_metadata`; check that it fails loudly when required columns missing.

### assets/results_analysis.py
- `_require_cohort_partition` is duplicated across analysis modules; could be centralised.
- `evaluator_agreement_analysis`:
  - Heavy logging + classification logic. Relies on `filter_valid_scores`. When no valid rows, returns empty DF rather than error.
  - Agreement classification thresholds are hard-coded heuristics (1/2/3 points). Verify stakeholders still care; otherwise drop or move to reporting script.
- `comprehensive_variance_analysis`:
  - Multi-section computation (template vs model vs overall) but returns empty DataFrame when columns missing instead of flagging error.
  - Adds new columns by mutating input DataFrame (`valid_scores['eval_template'] = ...`), which may surprise callers sharing the frame.

### assets/results_summary.py
- `generation_scores_pivot` re-instantiates spec allowlists via `_build_spec_catalogs` + `cohort_spec` compile, ignoring manifest produced earlier. Potential simplification: consume manifest or the same allowlists used in `cohort_id` asset.
- When aggregated scores empty, returns empty DF rather than fail (pattern across module).
- `final_results` uses `compute_final_results` (in `results_summary/transformations.py`). Returns summary with metadata entries; still warns instead of failing if inputs empty.
- Many metadata keys reference output paths; ensure IO managers always provided.

### assets/cross_experiment.py
- `filtered_evaluation_results` scans entire `gens/evaluation` directory, collects all `gen_id`s. No filtering by cohort; meant for global dashboards. Consider whether this is still necessary now that we focus on cohort-first workflows.
- Uses `calculate_evaluation_metadata`; includes aggregator that merges metadata from parent essay/draft.
- `template_version_comparison_pivot` allows optional config but defaults to all templates. Duplicate of pivot logic elsewhere; evaluate consolidation.

### utils/evaluation_scores.py
- `_EvaluationScoreAggregator` handles lots of edge cases:
  - `_load_via_data_layer` swallows `Err.DATA_MISSING` and returns partial metadata with empty texts.
  - `_parse_score` only reads last line; returns `None` + error string if not float.
  - `_resolve_generation_fields` falls back across evaluation / essay / draft metadata; defaulting to empty strings.
- `aggregate_evaluation_scores_for_ids` (not shown above but used) likely wraps aggregator; ensure we fail loudly when evaluation metadata missing rather than emitting rows with blanks.
- Normalised column list includes `generation_response_path`, `input_mode`, `copied_from`; confirm they still matter post-spec migration.

### CLI Scripts (skim)
- `scripts/aggregate_scores.py`: still supports `--strict` vs non-strict; numerous warnings and fallback assignments. Consider making strict the default and removing lenient path.
- `scripts/build_pivot_tables.py` and `scripts/report_promising_essays.py` continue to read `aggregated_scores.csv` directly; may be redundant if Dagster assets cover same functionality.

## Potential Deletions / Simplifications
- Remove empty-return branches in analysis assets; raise `DDError` when inputs missing so pipelines fail fast.
- Align all allowlist calculations with manifest data produced by `cohort_id` to avoid spec re-read.
- Prune the cross-experiment asset unless downstream consumers rely on global data.
- Collapse CLI options that kept curated workflows alive (`--score-span`, `--max-only`) if unused.
- Harden `_EvaluationScoreAggregator` to treat missing metadata/parsed text as errors rather than silently emitting placeholders.

## Next Steps
- Discuss with stakeholders whether cross-experiment reporting is still required. If not, plan removal or delegation to offline scripts.
- Inventory consumers of `filtered_evaluation_results` and `template_version_comparison_pivot` before deleting.
- Decide on desired failure semantics (error vs warning) and roll that consistently across assets/tests.
- Update docs (`docs/guides/operating_guide.md`, `docs/architecture/architecture.md`) once simplification decisions made.
- Add items to `plans/code_simplification_actions.md` during implementation phase.

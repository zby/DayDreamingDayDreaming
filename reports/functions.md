# Functions and docstrings

## daydreaming_dagster.assets._decorators

- function: asset_with_boundary  (assets/_decorators.py:7)
  - doc: Compose @dagster.asset with a minimal error boundary.

## daydreaming_dagster.assets._error_boundary

- function: with_asset_error_boundary  (assets/_error_boundary.py:7)
  - doc: Wrap an asset so unexpected errors surface as dagster.Failure.

## daydreaming_dagster.assets._helpers

- function: get_data_root  (assets/_helpers.py:17)
  - doc: (none)

- function: get_gens_root  (assets/_helpers.py:21)
  - doc: (none)

- function: get_run_id  (assets/_helpers.py:25)
  - doc: (none)

- function: require_membership_row  (assets/_helpers.py:34)
  - doc: (none)

- function: load_generation_parsed_text  (assets/_helpers.py:80)
  - doc: (none)

- function: _parent_stage  (assets/_helpers.py:104)
  - doc: Wrapper delegating to unified.stage_policy.parent_stage_of for consistency.

- function: load_parent_parsed_text  (assets/_helpers.py:111)
  - doc: Load parsed.txt for the parent of a generation.

- function: resolve_generator_mode  (assets/_helpers.py:154)
  - doc: Parametrized resolver for generator modes across all stages.

- function: emit_standard_output_metadata  (assets/_helpers.py:238)
  - doc: (none)

- function: build_prompt_metadata  (assets/_helpers.py:347)
  - doc: Standard prompt metadata for Dagster UI.

## daydreaming_dagster.assets.asset_factory_v1

- function: make_asset_from  (assets/asset_factory_v1.py:20)
  - doc: Build a Dagster asset that delegates to an existing execute_* function.

## daydreaming_dagster.assets.cross_experiment

- function: filtered_evaluation_results  (assets/cross_experiment.py:32)
  - doc: Collect evaluation results from gens-store and enrich with generation metadata.

- function: template_version_comparison_pivot  (assets/cross_experiment.py:119)
  - doc: Create pivot table for template version comparison from filtered results.

## daydreaming_dagster.assets.documents_reporting

- function: documents_latest_report  (assets/documents_reporting.py:18)
  - doc: Export a small CSV snapshot of recent generations by scanning filesystem metadata.

- function: documents_consistency_report  (assets/documents_reporting.py:62)
  - doc: Scan the gens store and report simple consistency issues per row.

## daydreaming_dagster.assets.group_cohorts

- function: _load_selected_essays_list  (assets/group_cohorts.py:38)
  - doc: (none)

- function: _eval_axes  (assets/group_cohorts.py:53)
  - doc: Return active evaluation template IDs and evaluation model IDs.

- function: cohort_membership  (assets/group_cohorts.py:74)
  - doc: Build the authoritative cohort membership CSV with normalized columns per stage.

- function: register_cohort_partitions  (assets/group_cohorts.py:317)
  - doc: Register dynamic partitions by gen_id for draft/essay/evaluation (add-only).

- function: cohort_id  (assets/group_cohorts.py:359)
  - doc: Compute a deterministic cohort_id from the current manifest and persist it.

- function: selected_combo_mappings  (assets/group_cohorts.py:407)
  - doc: Regenerate selected combo mappings from active concepts (deterministic ID).

- function: content_combinations  (assets/group_cohorts.py:443)
  - doc: Build combinations for generation. Preferred source: selected_combo_mappings.csv.

## daydreaming_dagster.assets.group_evaluation

- function: evaluation_prompt  (assets/group_evaluation.py:22)
  - doc: Dagster asset wrapper for evaluation prompt.

- function: evaluation_response  (assets/group_evaluation.py:38)
  - doc: Dagster asset wrapper for evaluation response.

## daydreaming_dagster.assets.group_generation_draft

- function: draft_prompt  (assets/group_generation_draft.py:19)
  - doc: Dagster asset wrapper for draft prompt.

- function: draft_response  (assets/group_generation_draft.py:34)
  - doc: Dagster asset wrapper for draft response.

## daydreaming_dagster.assets.group_generation_essays

- function: essay_prompt  (assets/group_generation_essays.py:20)
  - doc: Dagster asset wrapper for essay prompt.

- function: essay_response  (assets/group_generation_essays.py:35)
  - doc: Dagster asset wrapper for essay response.

## daydreaming_dagster.assets.maintenance

- function: _desired_gen_ids  (assets/maintenance.py:15)
  - doc: (none)

- function: prune_dynamic_partitions  (assets/maintenance.py:32)
  - doc: Delete all dynamic partitions for draft/essay/evaluation.

## daydreaming_dagster.assets.results_analysis

- function: evaluator_agreement_analysis  (assets/results_analysis.py:12)
  - doc: Calculate evaluator agreement metrics for the same generation responses.

- function: comprehensive_variance_analysis  (assets/results_analysis.py:119)
  - doc: Comprehensive variance analysis across all evaluation dimensions:

## daydreaming_dagster.assets.results_processing

- function: aggregated_scores  (assets/results_processing.py:20)
  - doc: Aggregate evaluation scores for the current cohort.

## daydreaming_dagster.assets.results_summary

- function: _noop  (assets/results_summary.py:14)
  - doc: (none)

- function: generation_scores_pivot  (assets/results_summary.py:25)
  - doc: Pivot individual evaluation scores per generation.

- function: final_results  (assets/results_summary.py:123)
  - doc: Create comprehensive pivot table summaries with statistics.

- function: perfect_score_paths  (assets/results_summary.py:279)
  - doc: Generate a file with paths to all responses that received perfect scores (10.0).

- function: evaluation_model_template_pivot  (assets/results_summary.py:367)
  - doc: Create pivot table with (evaluation_llm_model, evaluation_template) combinations as columns.

## daydreaming_dagster.assets.test_assets_helpers_unit

- method: _Resources.__init__  (assets/test_assets_helpers_unit.py:19)
  - doc: (none)

- method: _Ctx.__init__  (assets/test_assets_helpers_unit.py:24)
  - doc: (none)

- method: _Ctx.add_output_metadata  (assets/test_assets_helpers_unit.py:28)
  - doc: (none)

- function: _write_membership  (assets/test_assets_helpers_unit.py:32)
  - doc: (none)

## daydreaming_dagster.assets.test_cohort_id_asset

- function: _stub_tables  (assets/test_cohort_id_asset.py:9)
  - doc: (none)

## daydreaming_dagster.assets.test_cohort_membership

- function: _write_json  (assets/test_cohort_membership.py:11)
  - doc: (none)

## daydreaming_dagster.assets.test_results_summary_unit

- function: _make_parsed_scores  (assets/test_results_summary_unit.py:18)
  - doc: (none)

## daydreaming_dagster.checks.documents_checks

- function: _get_pk  (checks/documents_checks.py:21)
  - doc: (none)

- function: _resolve_doc_id  (checks/documents_checks.py:25)
  - doc: (none)

- function: _files_exist_check_impl  (checks/documents_checks.py:42)
  - doc: (none)

- function: _make_files_exist_check  (checks/documents_checks.py:52)
  - doc: (none)

## daydreaming_dagster.models.content_combination

- method: ContentCombination.from_concepts  (models/content_combination.py:18)
  - doc: Current approach: single level with fallback for all concepts.

- method: ContentCombination.from_concepts_multi  (models/content_combination.py:47)
  - doc: Future: generate all level combinations for experimentation.

- method: ContentCombination.from_concepts_filtered  (models/content_combination.py:76)
  - doc: Future: smart filtering strategies to reduce combinatorial explosion.

- method: ContentCombination._resolve_content  (models/content_combination.py:118)
  - doc: Resolve content with fallback: requested → paragraph → sentence → name.

- method: ContentCombination._get_available_levels  (models/content_combination.py:137)
  - doc: Get available description levels for each concept.

- function: generate_combo_id  (models/content_combination.py:148)
  - doc: Generate a deterministic, versioned combo ID from combination parameters.

## daydreaming_dagster.resources.gens_prompt_io_manager

- method: GensPromptIOManager.__init__  (resources/gens_prompt_io_manager.py:25)
  - doc: (none)

- method: GensPromptIOManager._resolve_gen_id  (resources/gens_prompt_io_manager.py:32)
  - doc: (none)

- method: GensPromptIOManager.handle_output  (resources/gens_prompt_io_manager.py:36)
  - doc: (none)

- method: GensPromptIOManager.load_input  (resources/gens_prompt_io_manager.py:43)
  - doc: (none)

## daydreaming_dagster.resources.io_managers

- method: CSVIOManager.__init__  (resources/io_managers.py:14)
  - doc: (none)

- method: CSVIOManager.handle_output  (resources/io_managers.py:17)
  - doc: Save DataFrame as CSV file

- method: CSVIOManager.load_input  (resources/io_managers.py:38)
  - doc: Load DataFrame from CSV file

- method: InMemoryIOManager.__init__  (resources/io_managers.py:76)
  - doc: (none)

- method: InMemoryIOManager.handle_output  (resources/io_managers.py:79)
  - doc: (none)

- method: InMemoryIOManager.load_input  (resources/io_managers.py:83)
  - doc: (none)

## daydreaming_dagster.resources.llm_client

- method: LLMClientResource._load_model_map  (resources/llm_client.py:35)
  - doc: Load id->provider model mapping from data/1_raw/llm_models.csv (best-effort).

- method: LLMClientResource._resolve_model_name  (resources/llm_client.py:55)
  - doc: Resolve incoming model identifier to provider model string.

- method: LLMClientResource._ensure_initialized  (resources/llm_client.py:70)
  - doc: Lazy initialization of OpenAI client.

- method: LLMClientResource.generate  (resources/llm_client.py:87)
  - doc: Generate content and return only the text.

- method: LLMClientResource.generate_with_info  (resources/llm_client.py:95)
  - doc: Generate content and return (text, info dict).

- method: LLMClientResource._make_api_call_info  (resources/llm_client.py:108)
  - doc: Make the actual API call with rate limiting and retry logic and return info dict.

- method: LLMClientResource._raw_api_call_info  (resources/llm_client.py:133)
  - doc: Raw API call with mandatory delay; return dict with text and finish info.

- method: LLMClientResource._is_retryable_error  (resources/llm_client.py:206)
  - doc: Determine if an error should trigger a retry.

## daydreaming_dagster.schedules.raw_schedule

- function: _cron  (schedules/raw_schedule.py:36)
  - doc: (none)

- function: _dagster_home  (schedules/raw_schedule.py:40)
  - doc: (none)

- function: _build_state  (schedules/raw_schedule.py:44)
  - doc: (none)

- function: _changed_files  (schedules/raw_schedule.py:50)
  - doc: (none)

- function: raw_schedule_execution_fn  (schedules/raw_schedule.py:64)
  - doc: (none)

## daydreaming_dagster.unified.envelopes

- method: GenerationEnvelope.validate  (unified/envelopes.py:19)
  - doc: (none)

- method: GenerationEnvelope.to_metadata_base  (unified/envelopes.py:34)
  - doc: (none)

## daydreaming_dagster.unified.stage_core

- method: LLMClientProto.generate_with_info  (unified/stage_core.py:30)
  - doc: (none)

- function: _templates_root  (unified/stage_core.py:41)
  - doc: (none)

- function: render_template  (unified/stage_core.py:46)
  - doc: (none)

- function: generate_llm  (unified/stage_core.py:61)
  - doc: (none)

- function: _validate_min_lines  (unified/stage_core.py:73)
  - doc: (none)

- function: validate_result  (unified/stage_core.py:82)
  - doc: Pure validation wrapper for response text and call info.

- function: resolve_parser_name  (unified/stage_core.py:102)
  - doc: Resolve the effective parser name per centralized policy with pragmatic defaults.

- function: parse_text  (unified/stage_core.py:125)
  - doc: Parse raw_text using the named parser.

- function: execute_llm  (unified/stage_core.py:146)
  - doc: (none)

- function: _base_meta  (unified/stage_core.py:253)
  - doc: (none)

- function: _merge_extras  (unified/stage_core.py:278)
  - doc: (none)

- function: execute_copy  (unified/stage_core.py:286)
  - doc: (none)

## daydreaming_dagster.unified.stage_policy

- function: parent_stage_of  (unified/stage_policy.py:24)
  - doc: (none)

- function: exp_config_for  (unified/stage_policy.py:32)
  - doc: (none)

- function: read_membership_fields  (unified/stage_policy.py:48)
  - doc: (none)

- function: _build_prompt_values_draft  (unified/stage_policy.py:70)
  - doc: (none)

- function: _build_prompt_values_essay  (unified/stage_policy.py:82)
  - doc: (none)

- function: _build_prompt_values_evaluation  (unified/stage_policy.py:91)
  - doc: (none)

- function: get_stage_spec  (unified/stage_policy.py:128)
  - doc: (none)

- function: effective_parser_name  (unified/stage_policy.py:136)
  - doc: Determine the effective parser name for a stage/template with uniform rules.

## daydreaming_dagster.unified.stage_prompts

- function: prompt_asset  (unified/stage_prompts.py:12)
  - doc: (none)

## daydreaming_dagster.unified.stage_responses

- function: response_asset  (unified/stage_responses.py:10)
  - doc: (none)

- function: essay_response_asset  (unified/stage_responses.py:77)
  - doc: (none)

- function: evaluation_response_asset  (unified/stage_responses.py:81)
  - doc: (none)

- function: draft_response_asset  (unified/stage_responses.py:85)
  - doc: (none)

## daydreaming_dagster.utils.cohorts

- function: short_git_sha  (utils/cohorts.py:11)
  - doc: (none)

- function: compute_cohort_id  (utils/cohorts.py:21)
  - doc: (none)

- function: get_env_cohort_id  (utils/cohorts.py:49)
  - doc: Return a normalized DD_COHORT override when meaningful.

- function: write_manifest  (utils/cohorts.py:66)
  - doc: (none)

## daydreaming_dagster.utils.combo_ids

- method: ComboIDManager.__init__  (utils/combo_ids.py:15)
  - doc: (none)

- method: ComboIDManager.get_or_create_combo_id  (utils/combo_ids.py:18)
  - doc: Return a stable combo ID, writing to the mapping if it's new.

- method: ComboIDManager.load_mappings  (utils/combo_ids.py:83)
  - doc: (none)

## daydreaming_dagster.utils.csv_reading

- function: _extract_line_number  (utils/csv_reading.py:19)
  - doc: Best-effort extraction of a line number from pandas ParserError message.

- function: read_csv_with_context  (utils/csv_reading.py:32)
  - doc: Read a CSV with enhanced error information on parse failures.

## daydreaming_dagster.utils.draft_parsers

- function: parse_essay_idea_last  (utils/draft_parsers.py:17)
  - doc: Extract the last (or highest-stage) <essay-idea> block from links output.

- function: parse_essay_block  (utils/draft_parsers.py:62)
  - doc: Extract the first <essay>...</essay> block.

## daydreaming_dagster.utils.eval_response_parser

- function: parse_llm_response  (utils/eval_response_parser.py:9)
  - doc: Parse LLM evaluation response to extract score and reasoning.

- function: _parse_in_last_line_format  (utils/eval_response_parser.py:33)
  - doc: Parse evaluation responses where the score is in one of the last few lines.

- function: _extract_last_non_empty_lines  (utils/eval_response_parser.py:66)
  - doc: Extract the last few non-empty lines from response text.

- function: _extract_last_non_empty_line  (utils/eval_response_parser.py:74)
  - doc: Extract the last non-empty line from response text.

- function: _clean_formatting  (utils/eval_response_parser.py:82)
  - doc: Remove markdown formatting to simplify parsing.

- function: _try_parse_three_digit_score  (utils/eval_response_parser.py:87)
  - doc: Try to parse three-digit score format (e.g., '456' -> 5.0).

- function: _try_parse_exact_score_format  (utils/eval_response_parser.py:106)
  - doc: Try to parse exact 'SCORE: X' format.

- function: _try_parse_standard_score  (utils/eval_response_parser.py:113)
  - doc: Try to parse standard numeric score format (e.g., '8.5').

- function: _build_score_pattern  (utils/eval_response_parser.py:121)
  - doc: Build a regex pattern for score extraction with consistent header/separator matching.

- function: _validate_score_range  (utils/eval_response_parser.py:130)
  - doc: Validate that score is within the valid 0-10 range.

- function: _parse_complex_format  (utils/eval_response_parser.py:136)
  - doc: Parse evaluation responses using legacy template format (complex patterns).

## daydreaming_dagster.utils.evaluation_processing

- function: calculate_evaluation_metadata  (utils/evaluation_processing.py:12)
  - doc: Calculate essential metadata for evaluation DataFrame assets.

## daydreaming_dagster.utils.evaluation_scores

- function: aggregate_evaluation_scores_for_ids  (utils/evaluation_scores.py:11)
  - doc: Aggregate evaluation scores for the given evaluation gen_ids.

## daydreaming_dagster.utils.file_fingerprint

- method: FileEntry.fingerprint  (utils/file_fingerprint.py:15)
  - doc: (none)

- function: scan_files  (utils/file_fingerprint.py:23)
  - doc: (none)

- function: combined_fingerprint  (utils/file_fingerprint.py:35)
  - doc: (none)

## daydreaming_dagster.utils.generation

- function: _write_atomic  (utils/generation.py:10)
  - doc: (none)

- function: _ensure_dir  (utils/generation.py:15)
  - doc: (none)

- function: write_gen_raw  (utils/generation.py:21)
  - doc: Write raw.txt for a generation, creating the directory if needed.

- function: write_gen_parsed  (utils/generation.py:28)
  - doc: Write parsed.txt for a generation, creating the directory if needed.

- function: write_gen_prompt  (utils/generation.py:35)
  - doc: Write prompt.txt for a generation, creating the directory if needed.

- function: write_gen_metadata  (utils/generation.py:42)
  - doc: Write metadata.json for a generation, creating the directory if needed.

- function: load_generation  (utils/generation.py:49)
  - doc: Best-effort read of an existing generation from disk into a dict.

## daydreaming_dagster.utils.ids

- function: _hash_bytes  (utils/ids.py:7)
  - doc: (none)

- function: _to_base36  (utils/ids.py:20)
  - doc: (none)

- function: gen_dir  (utils/ids.py:34)
  - doc: (none)

- function: reserve_gen_id  (utils/ids.py:41)
  - doc: Reserve a deterministic 16-char base36 generation id for a task row.

## daydreaming_dagster.utils.membership_lookup

- function: _iter_membership_paths  (utils/membership_lookup.py:8)
  - doc: (none)

- function: find_membership_row_by_gen  (utils/membership_lookup.py:21)
  - doc: Locate the membership row for (stage, gen_id) across all cohorts.

- function: find_parent_membership_row  (utils/membership_lookup.py:48)
  - doc: Locate the parent membership row by parent_gen_id across all cohorts.

- function: stage_gen_ids  (utils/membership_lookup.py:58)
  - doc: Return all gen_ids for a given stage across cohort membership CSVs.

## daydreaming_dagster.utils.parser_registry

- function: register_parser  (utils/parser_registry.py:33)
  - doc: (none)

- function: get_parser  (utils/parser_registry.py:42)
  - doc: (none)

- function: list_parsers  (utils/parser_registry.py:51)
  - doc: (none)

- function: _identity  (utils/parser_registry.py:72)
  - doc: (none)

- function: _make_eval_wrapper  (utils/parser_registry.py:81)
  - doc: (none)

## daydreaming_dagster.utils.raw_readers

- function: read_concepts  (utils/raw_readers.py:11)
  - doc: (none)

- function: read_llm_models  (utils/raw_readers.py:40)
  - doc: (none)

- function: _read_templates  (utils/raw_readers.py:44)
  - doc: (none)

- function: _validate_templates_df  (utils/raw_readers.py:53)
  - doc: Validate that template CSVs share a minimal uniform schema.

- function: read_templates  (utils/raw_readers.py:66)
  - doc: Unified reader for draft/essay/evaluation templates.

## daydreaming_dagster.utils.raw_state

- function: _state_dir  (utils/raw_state.py:17)
  - doc: (none)

- function: read_last_state  (utils/raw_state.py:23)
  - doc: (none)

- function: write_last_state  (utils/raw_state.py:32)
  - doc: (none)

## daydreaming_dagster.utils.test_evaluation_processing

- function: dummy  (utils/test_evaluation_processing.py:18)
  - doc: (none)


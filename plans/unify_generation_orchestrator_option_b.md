# Unify Generation via Orchestrator + StageSpec (Option B)

Objective
- Introduce a shared, testable “generation engine” used by draft, essay, and evaluation assets to reduce duplication while preserving stage-specific behavior and I/O.

Non-Goals
- No change to IDs, partition keys, file paths, or IO manager wiring.
- No change to prompts/templates or parsers behavior (byte-for-byte outputs where applicable).

Design Overview
- Core idea: factor the common flow into an orchestrator with a StageSpec that supplies stage-specific hooks.
- Single flow: resolve inputs → load + render template → generate (LLM or copy) → validate/parse → write files → attach metadata.

Key Types
- StageSpec (dataclass)
  - stage: Literal["draft", "essay", "evaluation"]
  - template_kind: Literal["draft", "essay", "evaluation"]  # for loader
  - input_resolver(context, task_row) -> dict
    - draft: {'concepts': List[Concept]}
    - essay: {'draft_lines': List[str], 'draft_block': str}
    - evaluation: {'response': str}  # essay parsed text
  - template_loader(template_id: str) -> str  # Jinja template source
  - generator_mode(template_id: str) -> Literal['llm','copy']  # essay supports 'copy'
  - model_selector(task_row) -> str  # returns OpenRouter model name/id for LLM
  - parser_selector(template_id: str) -> Optional[Callable[[str], dict|str]]
    - draft: optional parser from draft_templates.csv 'parser' column (identity default)
    - essay: identity (parsed == raw), unless future structure
    - evaluation: returns score parser strategy (numeric-only or template-specific)
  - max_tokens_selector(context) -> Optional[int]
  - validators: pre_prompt(ctx, task_row, payload) -> None; post_llm(text, info) -> None
  - metadata_extras(context, task_row, payload) -> dict  # additional metadata fields

- Orchestrator
  - orchestrate_generation(context, task_row, spec: StageSpec, *, gen_id: str, parent_gen_id: Optional[str]) -> str
    - Resolve payload via spec.input_resolver
    - Load template (spec.template_loader)
    - Render prompt with payload (Jinja)
    - If spec.generator_mode == 'llm': call LLM
      - Use model = spec.model_selector(task_row), max_tokens_selector
      - Normalize newlines; detect truncation and raise on truncate (keep RAW persisted)
    - Else ('copy'): treat payload text (e.g., draft_block) as RAW and prompt=None
    - Parse (optional): spec.parser_selector(template_id)
    - Build metadata via build_generation_metadata (include cohort_id from task_row or env)
    - Write files via utils.document.Generation(write_files)
    - Return RAW/parsed text per asset’s expected type

Helpers (utils)
- render_prompt(template: str, vars: dict) -> str
- llm_generate(client, prompt: str, model: str, max_tokens: Optional[int]) -> (text, info)
- resolve_cohort_id(context, task_row) -> Optional[str]  # env first, then CSV column
- normalize_newlines(text) -> str
- detect_truncation(info: dict) -> bool

Planned Module
- File: daydreaming_dagster/utils/generation_engine.py
  - StageSpec, orchestrate_generation, helpers above

Asset Integration (thin wrappers)
- draft_prompt/draft_response
  - Build StageSpec for 'draft', supply input_resolver (from combinations), draft parser, validators (min lines), model selection
  - Call orchestrate_generation and return appropriate outputs
- essay_prompt/essay_response
  - StageSpec for 'essay', generator_mode reads essay_templates.csv (llm/copy)
  - input_resolver loads draft text by parent_gen_id
  - parser: identity
- evaluation_prompt/evaluation_response
  - StageSpec for 'evaluation', input_resolver loads essay text by parent_gen_id
  - parser_selector returns score parser strategy (existing require_parser_for_template logic)

Backcompat & Invariants
- gen_id, parent_gen_id unchanged
- Filesystem paths unchanged: data/gens/<stage>/<gen_id>/{prompt.txt,raw.txt,parsed.txt,metadata.json}
- Prompt persistence remains via existing GensPromptIOManager
- Metadata: keep keys (stage, gen_id, parent_gen_id, template_id, model_id, task_id, run_id, cohort_id, usage)
- Cohort propagation: continue to read from task_row['cohort_id'] if present or DD_COHORT env

Validations (unchanged behavior)
- Draft: min_draft_lines enforced after RAW persistence
- Truncation: if detected, raise Failure with metadata; RAW persisted
- Missing parent document: Fail with clear metadata in essay/evaluation

Migration Plan (Incremental, safe)
1) Introduce generation_engine.py (dead code): StageSpec + orchestrate_generation + helpers; add unit tests
2) Switch draft assets to engine
   - draft_prompt: uses template_loader('draft'), Jinja env
   - draft_response: uses LLM path, min_lines validator, optional parser
   - Validate outputs byte-for-byte on a sample partition
3) Switch essay assets to engine
   - Respect essay template generator mode (llm/copy)
   - Validate parent draft loading and failure paths
4) Switch evaluation assets to engine
   - Wire parser strategy via existing utils (require_parser_for_template)
   - Validate numeric-only and SCORE: cases
5) Remove duplicated logic and fold tests to the orchestrator where appropriate
6) Update docs minimally (internal architecture), no changes to user-facing flows

Testing Strategy
- Unit tests (new)
  - Orchestrator: happy-path llm/copy, truncation, parser application, metadata assembly
  - StageSpec hooks: input_resolver for draft/essay/eval (mock file reads)
  - Error paths: missing parent, missing template, invalid generator mode
- Regression (existing tests)
  - Ensure existing asset tests pass unchanged
- Golden outputs
  - Capture sample prompt/raw/parsed/metadata for a representative partition before change; ensure identical after migration
- Integration smoke
  - Materialize one partition per stage end-to-end; verify files created and parsers applied

Feature Flags & Rollback
- Add env flag DD_GEN_ENGINE=off to bypass orchestrator (TEMPORARY; TODO-REMOVE-BY in code)
- Keep original code paths during migration; remove after stabilization window

Risks & Mitigations
- Prompt differences: use the same Jinja render path and env
- Truncation/usage info: normalize llm_generate info mapping to preserve metadata fields
- Parser behavior: call existing helpers for draft/eval to avoid drift
- Performance: minimal overhead; shared functions reduce duplication

Acceptance Criteria
- No changes to IDs, paths, or metadata keys
- Byte-identical prompts and outputs for unchanged templates/models on test partitions
- All unit and integration tests passing

Timeline (suggested)
- Day 1–2: Implement engine + unit tests; migrate draft assets; validate
- Day 3: Migrate essay assets; validate
- Day 4: Migrate evaluation assets; validate; remove dead code

Out of Scope (v1)
- New stages or template languages
- Declarative YAML descriptors (Option C)
- Prompt/template refactors

References
- Current assets: daydreaming_dagster/assets/group_generation_draft.py, group_generation_essays.py, group_evaluation.py
- Parsers: utils/draft_parsers.py, utils/eval_response_parser.py
- Metadata: utils/metadata.py; document IO: utils/document.py
- Cohorts: utils/cohorts.py; ids: utils/ids.py


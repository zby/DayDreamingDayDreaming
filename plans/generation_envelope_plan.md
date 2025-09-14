# Plan: Introduce GenerationEnvelope and Centralize Validation

Purpose: Create a validated “envelope” for generation inputs so prompt/response code can rely on a single place for stage‑specific invariants. This allows removing scattered inline checks and most ad‑hoc try/except around orchestration paths, relying on the asset error boundary for uniform error reporting.

## Goals
- Add a GenerationEnvelope dataclass that encapsulates per‑stage fields and validates them once.
- Use StageSpec to drive envelope validation (required fields, parent presence, model required for non‑copy, copy support, parent stage).
- Replace inline validations in response paths with envelope.validate(spec).
- Keep StageCore (render/execute) pure and unchanged; keep helper Failures where metadata is valuable.

## Non‑Goals
- Do not move policy, CSV IO, or parser/generator lookup into Generation (filesystem IO). Keep Generation IO‑only.
- Do not remove helper Failures that return rich metadata (e.g., load_parent_parsed_text) — these are useful in Dagster UI.

## Design

### GenerationEnvelope

```
@dataclass
class GenerationEnvelope:
    stage: Stage
    gen_id: str
    template_id: str
    parent_gen_id: Optional[str]
    llm_model_id: Optional[str]
    mode: Literal["llm","copy"]

    def validate(self, spec: StageSpec) -> None:
        # enforce per‑stage policy, raise ValueError on violation

    def to_metadata_base(self) -> dict:
        # returns canonical base metadata (stage, gen_id, template_id, llm_model_id, parent_gen_id, mode)
```

- Construction site: response_asset builds the envelope from membership fields + mode and calls `envelope.validate(get_stage_spec(stage))`.
- Execution site: response_asset passes envelope fields to StageCore.execute_llm / execute_copy and uses `to_metadata_base()` to initialize metadata consistently.

### StageSpec integration
- StageSpec already contains prompt/response requirements, parent stage, copy support, tokens/min_lines; reuse it for validation.
- Add a small helper in StagePolicy if needed: `def validate_envelope(envelope: GenerationEnvelope, spec: StageSpec) -> None` (or keep it as a method on the envelope).

## Implementation Steps

1) Add `GenerationEnvelope` (new file `unified/envelopes.py` or inside `unified/stage_policy.py` if preferred for locality).
   - Fields: stage, gen_id, template_id, parent_gen_id, llm_model_id, mode.
   - Methods: `validate(spec)`, `to_metadata_base()`.
   - Validation rules:
     - template_id must be non‑empty.
     - if spec.parent_stage is not None → parent_gen_id must be present.
     - if mode != "copy" and `llm_model_id` required by spec.response_fields → must be present.
     - if mode == "copy" and not spec.supports_copy_response → raise.

2) Refactor response_asset to use envelope
   - Build `mf = read_membership_fields(row)` and resolve mode as before.
   - Construct `envelope = GenerationEnvelope(stage, gen_id, mf.template_id, mf.parent_gen_id, mf.llm_model_id, mode)`.
   - `envelope.validate(get_stage_spec(stage))`.
   - Copy path: use `spec.parent_stage` + `envelope.parent_gen_id` without extra branching.
   - LLM path: tokens/min_lines from `spec.tokens_and_min_lines(context)`; forward fields from envelope.

3) Leverage `to_metadata_base` (optional)
   - Initialize metadata dict with `envelope.to_metadata_base()` in execute_llm/execute_copy call sites or merge into existing base metadata to reduce duplication.

4) Optional IO preconditions in Generation.write_files
   - Assert `stage` and `gen_id` non‑empty.
   - If `write_parsed=True` then `parsed_text` must be str; if `write_prompt=True` then `prompt_text` must be str. Raise ValueError on violation.
   - Does not enforce policy; this only guards against internal misuse.

5) Remove/trim inline validations
   - In `stage_responses.py`: drop per‑stage required field checks now guaranteed by `envelope.validate`.
   - Keep helper calls that provide useful Failure metadata (e.g., parent file missing path).

6) Tests
   - No behavioral change expected; run full suite.
   - Add a small unit test for `GenerationEnvelope.validate` to ensure rules trigger clear ValueErrors.

## Example try/except blocks to remove/avoid

- Prompt rendering try blocks (already removed):
  - Previous code wrapped `render_template` with FileNotFoundError/Exception to rewrite the message. Now we rely on the error boundary to convert unexpected errors uniformly. Validation ensures `template_id` presence up‑front; missing files bubble as boundary Failures.

- Response inline Failures for missing `llm_model_id`:
  - Replace with `envelope.validate(spec)` — raises ValueError; boundary wraps. We keep rich metadata Failures only where they add file paths or CSV context.

- Copy‑mode branching checks:
  - Replace `if not pstage` and ad‑hoc messages with `spec.supports_copy_response` + `spec.parent_stage` via `envelope.validate`.

Note: We continue to keep intentional try/except inside StageCore where best‑effort behavior is intended:
- `resolve_parser_name(...)`: returns None on CSV issues (best effort) rather than raising.
- `parse_text(...)`: returns None on parser errors for robustness.
These are part of StageCore’s “tolerant” design and not the orchestration try blocks we are removing.

## Risks and Mitigations
- Risk: over‑validating in envelope leading to less helpful Dagster metadata.
  - Mitigation: keep helper‑level Failures (membership/parent text loading) which add context.
- Risk: drift between StageSpec and envelope rules.
  - Mitigation: implement envelope.validate(spec) with spec as single source of truth; add a unit test matrix per stage.

## Rollout
- Implement envelope + response refactor behind the current API, keep prompt paths unchanged.
- Run tests; adjust only if a test asserts on specific Failure messages.
- Optional: extend envelope usage to prompts later (e.g., a `PromptEnvelope`), but not necessary for responses which are more policy‑heavy.


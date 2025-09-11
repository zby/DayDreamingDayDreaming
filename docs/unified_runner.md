# Unified Stage Runner

The StageRunner provides a single, stage‑agnostic code path for generating prompts, invoking an LLM, parsing outputs, and persisting artifacts under the gens store. Assets use the runner while keeping Dagster partitions and membership‑first resolution.

Key points
- Resolves templates by `template_id` under `data/1_raw/templates/{draft,essay,evaluation}/`.
- Modes:
  - `llm`: render prompt (Jinja StrictUndefined), call LLM, write `raw.txt`, optionally write `parsed.txt` via stage parser.
  - `copy` (essay): pass through draft `parsed.txt` → essay `parsed.txt` (no prompt/LLM).
- Parsers:
  - evaluation: required via `evaluation_templates.csv` `parser` column (e.g., `in_last_line`, `complex`).
  - draft: optional; if present, applied; otherwise identity.
  - essay: not used (we currently write normalized raw as parsed for convenience).

Interface
```
@dataclass
class StageRunSpec:
  stage: Literal["draft","essay","evaluation"]
  gen_id: str
  template_id: str
  values: dict[str, Any]
  out_dir: Path
  mode: Literal["llm","copy"] = "llm"
  model: Optional[str] = None
  parser_name: Optional[str] = None
  pass_through_from: Optional[Path] = None
  max_tokens: Optional[int] = None
  prompt_text: Optional[str] = None  # if provided, template is not rendered
```

Usage in assets
- Draft: `prompt_text=draft_prompt`; `parser_name` from `draft_templates.csv` when set.
- Essay: `mode` from `essay_templates.csv` `generator` (`llm` or `copy`); for copy, set `pass_through_from` to draft parsed path.
- Evaluation: `parser_name` from `evaluation_templates.csv`; values include the essay `parsed.txt` under `response`.

Files written
- `data/gens/<stage>/<gen_id>/prompt.txt` (LLM mode)
- `data/gens/<stage>/<gen_id>/raw.txt` (LLM mode)
- `data/gens/<stage>/<gen_id>/parsed.txt` (copy mode; or when a parser produced output)
- `data/gens/<stage>/<gen_id>/metadata.json`

Strict schema
- Template CSVs are strict: `generator` present in all files (`llm` for draft/evaluation; `llm` or `copy` for essay), `parser` required for evaluation; `active` column placed last.
- Membership is the only source for `llm_model_id`, `template_id`, and parent linkage.


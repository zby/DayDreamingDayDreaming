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

Examples

- Draft (LLM path with pre-rendered prompt):
```
from daydreaming_dagster.unified.stage_runner import StageRunner, StageRunSpec

runner = StageRunner()
spec = StageRunSpec(
  stage="draft",
  gen_id=gen_id,
  template_id=draft_template_id,
  values={},                 # prompt provided
  out_dir=data_root/"gens",
  mode="llm",
  model=generation_model_id, # from membership
  prompt_text=draft_prompt,  # from draft_prompt asset
  parser_name=optional_draft_parser,
  max_tokens=cfg.draft_generation_max_tokens,
)
res = runner.run(spec, llm_client=context.resources.openrouter_client)
```

- Essay (copy mode):
```
spec = StageRunSpec(
  stage="essay",
  gen_id=gen_id,
  template_id=essay_template_id,
  values={"draft_block": draft_text, "links_block": draft_text},
  out_dir=data_root/"gens",
  mode="copy",
  pass_through_from=(data_root/"gens"/"draft"/parent_gen_id/"parsed.txt"),
)
res = runner.run(spec, llm_client=context.resources.openrouter_client)
```

- Evaluation (LLM + parse):
```
from daydreaming_dagster.utils.evaluation_parsing_config import load_parser_map, require_parser_for_template

parser_map = load_parser_map(data_root)
parser_name = require_parser_for_template(evaluation_template_id, parser_map)
spec = StageRunSpec(
  stage="evaluation",
  gen_id=gen_id,
  template_id=evaluation_template_id,
  values={"response": essay_parsed_text},
  out_dir=data_root/"gens",
  mode="llm",
  model=evaluation_model_id,
  parser_name=parser_name,
  max_tokens=cfg.evaluation_max_tokens,
)
res = runner.run(spec, llm_client=context.resources.openrouter_client)
```

Files written
- `data/gens/<stage>/<gen_id>/prompt.txt` (LLM mode)
- `data/gens/<stage>/<gen_id>/raw.txt` (LLM mode)
- `data/gens/<stage>/<gen_id>/parsed.txt` (copy mode; or when a parser produced output)
- `data/gens/<stage>/<gen_id>/metadata.json`

Strict schema
- Template CSVs are strict: `generator` present in all files (`llm` for draft/evaluation; `llm` or `copy` for essay), `parser` required for evaluation; `active` column placed last.
- Membership is the only source for `llm_model_id`, `template_id`, and parent linkage.

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional
import os
import time

from jinja2 import Environment, StrictUndefined


Stage = Literal["draft", "essay", "evaluation"]


@dataclass
class StageRunSpec:
    stage: Stage
    gen_id: str
    template_id: str
    values: dict[str, Any]
    out_dir: Path
    mode: Literal["llm", "copy"] = "llm"
    model: Optional[str] = None
    parser_name: Optional[str] = None
    pass_through_from: Optional[Path] = None
    max_tokens: Optional[int] = None


class StageRunner:
    def __init__(self, templates_root: Optional[Path] = None):
        root = templates_root or Path(os.environ.get("GEN_TEMPLATES_ROOT", "data/1_raw/templates"))
        self.templates_root = Path(root)
        # StrictUndefined to fail fast on missing vars
        self._jinja = Environment(undefined=StrictUndefined)

    def _load_template_text(self, stage: Stage, template_id: str) -> str:
        base = self.templates_root / stage
        path = base / f"{template_id}.txt"
        if not path.exists():
            raise FileNotFoundError(f"Template not found: {path}")
        return path.read_text(encoding="utf-8")

    def _render_prompt(self, template_text: str, values: dict[str, Any]) -> str:
        tpl = self._jinja.from_string(template_text)
        return tpl.render(**values)

    def _parse(self, stage: Stage, raw_text: str, parser_name: Optional[str]) -> Optional[str]:
        if stage == "evaluation" and parser_name:
            # Use existing evaluation parsing strategy mapping
            from daydreaming_dagster.utils.eval_response_parser import parse_llm_response

            normalized = str(raw_text).replace("\r\n", "\n")
            try:
                res = parse_llm_response(normalized, parser_name)
                score = res.get("score")
                if isinstance(score, (int, float)):
                    return f"{float(score)}\n"
            except Exception:
                return None
            return None
        if stage == "draft" and parser_name:
            try:
                from daydreaming_dagster.utils.draft_parsers import get_draft_parser

                parser = get_draft_parser(parser_name)
                return parser(str(raw_text))
            except Exception:
                return None
        # Essay and default: return normalized raw for convenience
        return str(raw_text).replace("\r\n", "\n") if raw_text is not None else None

    def run(self, spec: StageRunSpec, llm_client: Any) -> dict[str, Any]:
        stage_dir = Path(spec.out_dir) / spec.stage / str(spec.gen_id)
        stage_dir.mkdir(parents=True, exist_ok=True)

        t0 = time.time()
        if spec.mode == "copy":
            if not isinstance(spec.pass_through_from, (str, Path)):
                raise ValueError("pass_through_from path required for copy mode")
            src = Path(spec.pass_through_from)
            parsed = src.read_text(encoding="utf-8") if src.exists() else ""
            (stage_dir / "parsed.txt").write_text(parsed, encoding="utf-8")
            meta = {
                "stage": spec.stage,
                "gen_id": spec.gen_id,
                "template_id": spec.template_id,
                "template_path": str((self.templates_root / spec.stage / f"{spec.template_id}.txt").resolve()),
                "mode": "copy",
                "files": {
                    # No prompt in copy-mode by default
                    "parsed": str((stage_dir / "parsed.txt").resolve()),
                },
                "duration_s": round(time.time() - t0, 3),
            }
            (stage_dir / "metadata.json").write_text(__import__("json").dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            return {"parsed": parsed, "metadata": meta}

        # LLM mode: render prompt first
        template_text = self._load_template_text(spec.stage, spec.template_id)
        prompt = self._render_prompt(template_text, spec.values)
        (stage_dir / "prompt.txt").write_text(prompt, encoding="utf-8")

        # LLM path
        if not spec.model:
            raise ValueError("model is required for LLM mode")
        max_tokens = spec.max_tokens
        raw_text, info = llm_client.generate_with_info(prompt, model=spec.model, max_tokens=max_tokens)
        normalized = str(raw_text).replace("\r\n", "\n")
        (stage_dir / "raw.txt").write_text(normalized, encoding="utf-8")
        parsed = self._parse(spec.stage, normalized, spec.parser_name)
        if parsed is not None:
            (stage_dir / "parsed.txt").write_text(parsed, encoding="utf-8")

        meta = {
            "stage": spec.stage,
            "gen_id": spec.gen_id,
            "template_id": spec.template_id,
            "template_path": str((self.templates_root / spec.stage / f"{spec.template_id}.txt").resolve()),
            "model": spec.model,
            "parser_name": spec.parser_name,
            "mode": "llm",
            "files": {
                "prompt": str((stage_dir / "prompt.txt").resolve()),
                "raw": str((stage_dir / "raw.txt").resolve()),
                **({"parsed": str((stage_dir / "parsed.txt").resolve())} if parsed is not None else {}),
            },
            "finish_reason": (info or {}).get("finish_reason") if isinstance(info, dict) else None,
            "duration_s": round(time.time() - t0, 3),
        }
        (stage_dir / "metadata.json").write_text(__import__("json").dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        return {"prompt": prompt, "raw": normalized, "parsed": parsed, "metadata": meta}

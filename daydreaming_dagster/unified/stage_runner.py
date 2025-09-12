from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional, Dict
import os
import time
import json

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
    # Optional: provide pre-rendered prompt; when set, template is not loaded/rendered
    prompt_text: Optional[str] = None
    # Optional: additional metadata to be merged into metadata.json (no override of core keys)
    metadata_extra: Optional[Dict[str, Any]] = None
    # Optional: linkage to parent gen (required for essay and evaluation stages)
    parent_gen_id: Optional[str] = None
    # Optional validations/policy knobs
    min_lines: Optional[int] = None  # enforce for drafts when set
    fail_on_truncation: bool = True


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

    def render_template(self, stage: Stage, template_id: str, values: dict[str, Any]) -> str:
        """Render a template by stage + template_id with provided values.

        Raises FileNotFoundError if the template file is missing and propagates
        Jinja template errors (StrictUndefined for missing variables).
        """
        text = self._load_template_text(stage, template_id)
        return self._render_prompt(text, values)

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

    @staticmethod
    def _write_atomic(path: Path, data: str) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(data, encoding="utf-8")
        tmp.replace(path)

    def run(self, spec: StageRunSpec, llm_client: Any) -> dict[str, Any]:
        stage_dir = Path(spec.out_dir) / spec.stage / str(spec.gen_id)
        stage_dir.mkdir(parents=True, exist_ok=True)

        t0 = time.time()
        # Enforcement: parent linkage required for essay and evaluation
        if spec.stage in ("essay", "evaluation"):
            if not (isinstance(spec.parent_gen_id, str) and spec.parent_gen_id.strip()):
                raise ValueError("parent_gen_id is required for essay and evaluation stages")
        # Enforcement: evaluation requires parser_name
        if spec.stage == "evaluation" and not (isinstance(spec.parser_name, str) and spec.parser_name.strip()):
            raise ValueError("parser_name is required for evaluation stage")
        if spec.mode == "copy":
            if not isinstance(spec.pass_through_from, (str, Path)):
                raise ValueError("pass_through_from path required for copy mode")
            src = Path(spec.pass_through_from)
            parsed = src.read_text(encoding="utf-8") if src.exists() else ""
            self._write_atomic(stage_dir / "parsed.txt", parsed)
            meta = {
                "stage": spec.stage,
                "gen_id": spec.gen_id,
                "template_id": spec.template_id,
                "mode": "copy",
                "llm_model_id": spec.model,
                **({"parent_gen_id": spec.parent_gen_id} if spec.parent_gen_id else {}),
                "files": {
                    # No prompt in copy-mode by default
                    "parsed": str((stage_dir / "parsed.txt").resolve()),
                },
                "duration_s": round(time.time() - t0, 3),
            }
            # Merge extras without overriding core keys
            if spec.metadata_extra:
                for k, v in spec.metadata_extra.items():
                    if k not in meta:
                        meta[k] = v
            self._write_atomic(stage_dir / "metadata.json", json.dumps(meta, ensure_ascii=False, indent=2))
            return {"parsed": parsed, "metadata": meta}

        # LLM mode: render prompt first (or use provided prompt)
        if isinstance(spec.prompt_text, str):
            prompt = spec.prompt_text
        else:
            template_text = self._load_template_text(spec.stage, spec.template_id)
            prompt = self._render_prompt(template_text, spec.values)
        self._write_atomic(stage_dir / "prompt.txt", prompt)

        # LLM path
        if not spec.model:
            raise ValueError("model is required for LLM mode")
        max_tokens = spec.max_tokens
        raw_text, info = llm_client.generate_with_info(prompt, model=spec.model, max_tokens=max_tokens)
        normalized = str(raw_text).replace("\r\n", "\n")
        self._write_atomic(stage_dir / "raw.txt", normalized)
        # Policy validations (after RAW write for debuggability)
        if spec.stage == "draft":
            # Enforce minimum non-empty lines if configured
            if isinstance(spec.min_lines, int) and spec.min_lines > 0:
                response_lines = [ln for ln in normalized.split("\n") if ln.strip()]
                if len(response_lines) < spec.min_lines:
                    raise ValueError(
                        f"Draft validation failed: only {len(response_lines)} non-empty lines, minimum required {spec.min_lines}"
                    )
        # Enforce truncation policy
        if bool(spec.fail_on_truncation) and isinstance(info, dict) and info.get("truncated"):
            raise ValueError("LLM response appears truncated (finish_reason=length or max_tokens hit)")
        parsed = self._parse(spec.stage, normalized, spec.parser_name)
        if parsed is not None:
            self._write_atomic(stage_dir / "parsed.txt", parsed)

        meta = {
            "stage": spec.stage,
            "gen_id": spec.gen_id,
            "template_id": spec.template_id,
            "llm_model_id": spec.model,
            **({"parent_gen_id": spec.parent_gen_id} if spec.parent_gen_id else {}),
            "parser_name": spec.parser_name,
            "mode": "llm",
            "files": {
                "prompt": str((stage_dir / "prompt.txt").resolve()),
                "raw": str((stage_dir / "raw.txt").resolve()),
                **({"parsed": str((stage_dir / "parsed.txt").resolve())} if parsed is not None else {}),
            },
            "finish_reason": (info or {}).get("finish_reason") if isinstance(info, dict) else None,
            "truncated": bool((info or {}).get("truncated")) if isinstance(info, dict) else False,
            "usage": (info or {}).get("usage") if isinstance(info, dict) else None,
            "duration_s": round(time.time() - t0, 3),
        }
        # Merge extras without overriding core keys
        if spec.metadata_extra:
            for k, v in spec.metadata_extra.items():
                if k not in meta:
                    meta[k] = v
        self._write_atomic(stage_dir / "metadata.json", json.dumps(meta, ensure_ascii=False, indent=2))
        return {"prompt": prompt, "raw": normalized, "parsed": parsed, "metadata": meta, "info": info}

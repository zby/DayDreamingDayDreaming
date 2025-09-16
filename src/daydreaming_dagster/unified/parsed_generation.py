from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import json

from daydreaming_dagster.config.paths import Paths
from daydreaming_dagster.utils.generation import write_gen_parsed
from .stage_core import parse_text


@dataclass
class ParsedGenerationResult:
    stage: str
    gen_id: str
    data_root: Path
    parsed_text: str
    parsed_metadata: Dict[str, Any]
    raw_metadata: Dict[str, Any]


def _write_parsed_metadata(path: Path, metadata: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")


def perform_parsed_generation(
    *,
    stage: str,
    gen_id: str,
    data_root: Path,
    raw_text: str,
    raw_metadata: Dict[str, Any],
    parser_name: Optional[str],
    min_lines: Optional[int],
    fail_on_truncation: bool,
    metadata_extras: Optional[Dict[str, Any]] = None,
) -> ParsedGenerationResult:
    """Parse raw text for a generation stage and persist parsed artifacts."""

    paths = Paths.from_str(data_root)

    # Enforce truncation/line constraints similar to stage_core.validate_result
    if fail_on_truncation and bool(raw_metadata.get("truncated")):
        raise ValueError("Raw generation was truncated; cannot proceed with parsing")
    if isinstance(min_lines, int) and min_lines > 0:
        non_empty = sum(1 for line in str(raw_text).splitlines() if line.strip())
        if non_empty < min_lines:
            raise ValueError(f"Raw generation has only {non_empty} non-empty lines (required {min_lines})")

    if not parser_name or not str(parser_name).strip():
        raise ValueError(f"Parser name required for parsed generation (stage={stage}, gen_id={gen_id})")

    parsed = parse_text(stage, raw_text, parser_name)
    if stage == "essay" and not isinstance(parsed, str):
        parsed = raw_text
    if not isinstance(parsed, str):
        raise ValueError(f"Parser '{parser_name}' returned non-string output for stage {stage}")

    parsed_metadata: Dict[str, Any] = {
        "stage": stage,
        "gen_id": gen_id,
        "parser_name": parser_name,
        "success": True,
        "error": None,
    }
    if metadata_extras:
        for key, value in metadata_extras.items():
            if key not in parsed_metadata:
                parsed_metadata[key] = value
    parsed_path = paths.parsed_path(stage, gen_id)
    write_gen_parsed(paths.gens_root, stage, gen_id, parsed)
    parsed_metadata_path = paths.parsed_metadata_path(stage, gen_id)
    _write_parsed_metadata(parsed_metadata_path, parsed_metadata)

    return ParsedGenerationResult(
        stage=stage,
        gen_id=gen_id,
        data_root=paths.data_root,
        parsed_text=parsed,
        parsed_metadata=parsed_metadata,
        raw_metadata=raw_metadata,
    )

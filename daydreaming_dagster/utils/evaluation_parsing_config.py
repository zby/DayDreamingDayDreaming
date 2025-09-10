from __future__ import annotations

from pathlib import Path
import pandas as pd

# Supported parsing strategies for evaluation responses
ALLOWED_PARSERS = {"complex", "in_last_line"}


def load_parser_map(data_root: Path) -> dict[str, str]:
    """Load template->parsing strategy mapping from evaluation_templates.csv.

    Accepts either 'parsing_strategy' (preferred) or legacy 'parser' column.
    If neither is present, defaults to 'in_last_line' for all rows.
    Raises FileNotFoundError or ValueError when config is missing/invalid.
    """
    csv_path = Path(data_root) / "1_raw" / "evaluation_templates.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"evaluation_templates.csv not found: {csv_path}")
    df = pd.read_csv(csv_path)
    if "template_id" not in df.columns:
        raise ValueError("evaluation_templates.csv must include 'template_id' column")
    # Prefer explicit parsing_strategy; fall back to legacy 'parser'; default when neither present
    has_strategy = "parsing_strategy" in df.columns
    has_legacy = "parser" in df.columns
    mapping: dict[str, str] = {}
    for _, r in df.iterrows():
        tid = str(r["template_id"]).strip()
        if not tid:
            continue
        if has_strategy:
            val = str(r.get("parsing_strategy") or "").strip().lower()
        elif has_legacy:
            # FALLBACK(PARSER): Accept legacy 'parser' column. TODO-REMOVE-BY: remove after data/1_raw/evaluation_templates.csv is migrated.
            val = str(r.get("parser") or "").strip().lower()
        else:
            # FALLBACK(PARSER): No strategy columns; default to 'in_last_line' to stay unblocked.
            # TODO-REMOVE-BY: when schema guarantees 'parsing_strategy' present.
            val = "in_last_line"
        if not val:
            # FALLBACK(PARSER): Empty value; default to 'in_last_line'. Prefer explicit per-row value.
            val = "in_last_line"
        if val not in ALLOWED_PARSERS:
            raise ValueError(f"Invalid parsing strategy '{val}' for template '{tid}'. Allowed: {sorted(ALLOWED_PARSERS)}")
        mapping[tid] = val
    if not mapping:
        raise ValueError("No valid template->parser mappings loaded from evaluation_templates.csv")
    return mapping


def require_parser_for_template(template_id: str, parser_map: dict[str, str]) -> str:
    """Return parser for a template or raise if missing/invalid."""
    if not isinstance(template_id, str) or not template_id:
        raise ValueError("evaluation_template is required to determine parser")
    try:
        parser = parser_map[template_id]
    except KeyError as e:
        raise ValueError(f"No parser configured for evaluation_template '{template_id}'. Update evaluation_templates.csv") from e
    if parser not in ALLOWED_PARSERS:
        raise ValueError(f"Invalid parser '{parser}' for evaluation_template '{template_id}'")
    return parser

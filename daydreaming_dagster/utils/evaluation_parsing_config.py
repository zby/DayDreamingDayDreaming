from __future__ import annotations

from pathlib import Path
import pandas as pd

ALLOWED_PARSERS = {"complex", "in_last_line"}


def load_parser_map(data_root: Path) -> dict[str, str]:
    """Load strict template->parser mapping from evaluation_templates.csv.

    Requires a 'parser' column with values in ALLOWED_PARSERS.
    Raises FileNotFoundError or ValueError when config is missing/invalid.
    """
    csv_path = Path(data_root) / "1_raw" / "evaluation_templates.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"evaluation_templates.csv not found: {csv_path}")
    df = pd.read_csv(csv_path)
    if "template_id" not in df.columns or "parser" not in df.columns:
        raise ValueError("evaluation_templates.csv must include 'template_id' and 'parser' columns")
    mapping: dict[str, str] = {}
    for _, r in df.iterrows():
        tid = str(r["template_id"]).strip()
        parser = str(r["parser"]).strip().lower()
        if not tid:
            continue
        if parser not in ALLOWED_PARSERS:
            raise ValueError(f"Invalid parser '{parser}' for template '{tid}'. Allowed: {sorted(ALLOWED_PARSERS)}")
        mapping[tid] = parser
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


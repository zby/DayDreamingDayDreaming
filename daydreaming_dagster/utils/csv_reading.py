from __future__ import annotations

"""CSV reading helpers with enhanced error context for tests and assets.

Centralizes pandas CSV parsing with rich Failure messages that include:
- Original pandas error
- Problematic line number when available
- Nearby CSV lines with a '>>>' marker on the offending line
"""

from pathlib import Path
from typing import Optional

import pandas as pd
from dagster import Failure
import re


def _extract_line_number(error_msg: str) -> Optional[int]:
    """Best-effort extraction of a line number from pandas ParserError message."""
    # Common patterns: "line X", or "in row X" sometimes appear depending on pandas version
    for pattern in (r"line\s+(\d+)", r"row\s+(\d+)"):
        m = re.search(pattern, error_msg, flags=re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    return None


def read_csv_with_context(path: Path, context_lines: int = 2) -> pd.DataFrame:
    """Read a CSV with enhanced error information on parse failures.

    Args:
        path: File path to read.
        context_lines: How many lines of context before/after the error line to include.

    Raises:
        Failure: On pandas ParserError with a detailed, test-friendly message.
    """
    try:
        return pd.read_csv(path)
    except pd.errors.ParserError as e:
        # Load raw lines for context
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            lines = []

        error_msg = str(e)
        line_num = _extract_line_number(error_msg)

        parts = [
            f"CSV parsing failed for {path}",
            f"Original error: {error_msg}",
        ]

        if line_num and 1 <= line_num <= len(lines):
            start = max(1, line_num - context_lines)
            end = min(len(lines), line_num + context_lines)
            parts.append("")
            parts.append(f"Problematic content around line {line_num}:")
            for i in range(start, end + 1):
                marker = ">>>" if i == line_num else "   "
                parts.append(f"{marker} Line {i}: {lines[i-1]}")

        raise Failure("\n".join(parts)) from e


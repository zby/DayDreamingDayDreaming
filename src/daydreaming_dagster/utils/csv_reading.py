from __future__ import annotations

"""CSV reading helpers with structured error context for tests and assets.

Centralizes pandas CSV parsing with `DDError` codes that capture:
- Original pandas error message
- Problematic line number when available
- Nearby CSV lines so boundaries can format detailed diagnostics
"""

from pathlib import Path
from typing import List, Optional

import pandas as pd
import re

from .errors import DDError, Err


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
        DDError: On pandas ParserError with structured context for callers to log/format.
    """
    try:
        return pd.read_csv(path)
    except pd.errors.ParserError as exc:
        # Load raw lines for context when available
        try:
            lines: List[str] = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            lines = []

        error_msg = str(exc)
        line_num = _extract_line_number(error_msg)

        ctx: dict[str, object] = {
            "reason": "csv_parse_error",
            "path": str(path),
            "pandas_error": error_msg,
        }
        if line_num:
            ctx["line_number"] = line_num

        if line_num and 1 <= line_num <= len(lines):
            start = max(1, line_num - context_lines)
            end = min(len(lines), line_num + context_lines)
            snippet = []
            for i in range(start, end + 1):
                snippet.append(
                    {
                        "line": i,
                        "text": lines[i - 1],
                        "is_error": i == line_num,
                    }
                )
            ctx["snippet"] = snippet

        raise DDError(Err.PARSER_FAILURE, ctx=ctx, cause=exc)

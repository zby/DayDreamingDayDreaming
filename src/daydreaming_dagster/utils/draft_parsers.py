"""
Draft-phase parsers

Canonical module for draft parsers. Formerly lived under `link_parsers`.
"""

from typing import Callable, Optional, Dict
import re

from .errors import DDError, Err

# Inline idea parser implementation (used by deliberate-rolling-thread*)
ESSAY_IDEA_BLOCK_RE = re.compile(
    r"<essay-idea(?:\s+[^>]*)?>\n([\s\S]*?)\n</essay-idea>", re.MULTILINE
)


def parse_essay_idea_last(text: str) -> str:
    """Extract the last (or highest-stage) <essay-idea> block from links output.

    Contract assumed by deliberate-rolling-thread templates:
    - Tags are on their own lines.
    - One <essay-idea ...> per step; 'stage' attribute may be present.
    - Select by highest numeric stage when available; else last occurrence.
    """
    matches = list(ESSAY_IDEA_BLOCK_RE.finditer(text))
    if not matches:
        raise DDError(
            Err.PARSER_FAILURE,
            ctx={"reason": "missing_essay_idea_block"},
        )

    stage_attr_re = re.compile(r"<essay-idea[^>]*stage=\"(\d+)\"", re.MULTILINE)
    staged = []
    for m in matches:
        start = m.start()
        line_start = text.rfind("\n", 0, start) + 1
        opening_line = text[line_start : text.find("\n", start) if text.find("\n", start) != -1 else m.start()]
        stage_val = None
        m_stage = stage_attr_re.search(text[line_start : m.end()])
        if m_stage:
            stage_token = m_stage.group(1)
            if stage_token.isdigit():
                stage_val = int(stage_token)
        staged.append((stage_val, m.group(1)))

    if any(s is not None for s, _ in staged):
        best_stage = max(s for s, _ in staged if s is not None)
        candidates = [content for s, content in staged if s == best_stage]
        return candidates[-1]
    else:
        return staged[-1][1]


# Registry of available draft-phase parsers
# Keyed by parser name as referenced in data/1_raw/draft_templates.csv (column `parser`).
DRAFT_PARSERS_REGISTRY: Dict[str, Callable[[str], str]] = {
    "essay_idea_last": parse_essay_idea_last,
}

# New parser: extract the content of the first <essay>...</essay> block
ESSAY_BLOCK_RE = re.compile(r"<essay>([\s\S]*?)</essay>", re.IGNORECASE | re.MULTILINE)


def parse_essay_block(text: str) -> str:
    """Extract the first <essay>...</essay> block.

    Raises ``DDError`` when no block is present.
    """
    m = ESSAY_BLOCK_RE.search(text)
    if m:
        return m.group(1).strip()
    # FALLBACK(PARSER): if closing tag missing, capture from the first <essay> to EOF.
    # Prefer fixing upstream template/data to always emit well-formed blocks.
    lower = text.lower()
    start = lower.find("<essay>")
    if start != -1:
        return text[start + len("<essay>"):].strip()
    raise DDError(
        Err.PARSER_FAILURE,
        ctx={"reason": "missing_essay_block"},
    )


THINKING_BLOCK_RE = re.compile(r"<thinking\b[^>]*>([\s\S]*?)</thinking>", re.IGNORECASE)


def _strip_thinking_blocks(text: str) -> str:
    """Remove any <thinking>...</thinking> sections before downstream parsing."""

    if "<thinking" not in text.lower():
        return text
    return THINKING_BLOCK_RE.sub("", text)


def parse_essay_block_lenient(text: str) -> str:
    """Essay block parser that falls back to identity when tags are absent."""

    sanitized = _strip_thinking_blocks(text)
    try:
        return parse_essay_block(sanitized)
    except DDError as exc:
        ctx = exc.ctx or {}
        if ctx.get("reason") == "missing_essay_block":
            return sanitized.strip()
        raise


# Register under names referenced by draft_templates.csv
DRAFT_PARSERS_REGISTRY["essay_block"] = parse_essay_block
DRAFT_PARSERS_REGISTRY["essay_block_lenient"] = parse_essay_block_lenient

__all__ = [
    "DRAFT_PARSERS_REGISTRY",
    "parse_essay_idea_last",
    "parse_essay_block",
    "parse_essay_block_lenient",
]

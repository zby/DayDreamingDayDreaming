"""
Draft-phase parsers

Canonical module for draft parsers. Formerly lived under `link_parsers`.
"""

from typing import Callable, Optional, Dict
import re


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
        raise ValueError("No <essay-idea> blocks found in links output")

    stage_attr_re = re.compile(r"<essay-idea[^>]*stage=\"(\d+)\"", re.MULTILINE)
    staged = []
    for m in matches:
        start = m.start()
        line_start = text.rfind("\n", 0, start) + 1
        opening_line = text[line_start : text.find("\n", start) if text.find("\n", start) != -1 else m.start()]
        stage_val = None
        m_stage = stage_attr_re.search(text[line_start : m.end()])
        if m_stage:
            try:
                stage_val = int(m_stage.group(1))
            except ValueError:
                stage_val = None
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


def get_draft_parser(name: str) -> Optional[Callable[[str], str]]:
    if not isinstance(name, str):
        return None
    return DRAFT_PARSERS_REGISTRY.get(name.strip())


# New parser: extract the content of the first <essay>...</essay> block
ESSAY_BLOCK_RE = re.compile(r"<essay>([\s\S]*?)</essay>", re.MULTILINE)


def parse_essay_block(text: str) -> str:
    """Extract the first <essay>...</essay> block.

    Raises ValueError if no block found.
    """
    m = ESSAY_BLOCK_RE.search(text)
    if m:
        return m.group(1).strip()
    # Fallback: if no closing tag, capture from the first <essay> to EOF
    start = text.find("<essay>")
    if start != -1:
        return text[start + len("<essay>"):].strip()
    raise ValueError("No <essay> block found")


# Register under a simple name referenced by draft_templates.csv
DRAFT_PARSERS_REGISTRY["essay_block"] = parse_essay_block

__all__ = [
    "get_draft_parser",
    "DRAFT_PARSERS_REGISTRY",
    "parse_essay_idea_last",
    "parse_essay_block",
]

"""
Draft-phase parsers

This module is the canonical import path for draft parsers.
It currently re-exports implementations from `link_parsers` for
backward compatibility with legacy naming.
"""

from .link_parsers import (
    get_draft_parser,
    LINK_PARSERS_REGISTRY as DRAFT_PARSERS_REGISTRY,
    parse_essay_idea_last,
    parse_essay_block,
)

__all__ = [
    "get_draft_parser",
    "DRAFT_PARSERS_REGISTRY",
    "parse_essay_idea_last",
    "parse_essay_block",
]


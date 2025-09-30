from __future__ import annotations

"""
Unified parser registry for all stages.

Provides a single place to register and look up parsers by stage.
Parsers are simple callables: (text: str) -> str that may raise on failure.

Stages: "draft", "essay", "evaluation".
Includes an identity parser for the essay stage.
Evaluation parsers are thin wrappers around eval_response_parser.parse_llm_response
to normalize outputs to a single-line numeric string (e.g., "7.0\n").
"""

from typing import Any, Callable, Dict, Optional

from .errors import DDError, Err


class ParserError(DDError):
    """Raised when a required parser is missing or parsing fails decisively."""

    def __init__(self, *, reason: str, ctx: dict[str, Any] | None = None):
        base_ctx = {"reason": reason}
        if ctx:
            base_ctx.update(ctx)
        super().__init__(Err.PARSER_FAILURE, ctx=base_ctx)

StageName = str
ParserFn = Callable[[str], str]


_REGISTRY: Dict[StageName, Dict[str, ParserFn]] = {
    "draft": {},
    "essay": {},
    "evaluation": {},
}


def register_parser(stage: StageName, name: str, fn: ParserFn) -> None:
    stage = str(stage)
    if stage not in _REGISTRY:
        raise DDError(Err.INVALID_CONFIG, ctx={"stage": stage})
    if not isinstance(name, str) or not name.strip():
        raise DDError(Err.INVALID_CONFIG, ctx={"stage": stage, "reason": "empty_parser_name"})
    _REGISTRY[stage][name.strip()] = fn


def get_parser(stage: StageName, name: str) -> Optional[ParserFn]:
    stage = str(stage)
    if stage not in _REGISTRY:
        return None
    if not isinstance(name, str):
        return None
    return _REGISTRY[stage].get(name.strip())


def list_parsers(stage: StageName) -> Dict[str, ParserFn]:
    stage = str(stage)
    if stage not in _REGISTRY:
        return {}
    # Return a shallow copy to avoid accidental mutation
    return dict(_REGISTRY[stage])


# ---- Built-in registrations ----

# Draft: reuse existing registry
from .draft_parsers import DRAFT_PARSERS_REGISTRY as _DRAFTS

for _name, _fn in _DRAFTS.items():
    register_parser("draft", _name, _fn)


# Essay/Draft: identity parser
def _identity(text: str) -> str:
    return str(text)

register_parser("essay", "identity", _identity)
register_parser("draft", "identity", _identity)
register_parser("evaluation", "identity", _identity)


# Evaluation: wrappers around eval_response_parser that normalize output
def _make_eval_wrapper(strategy: str) -> ParserFn:
    def _fn(text: str) -> str:
        from .eval_response_parser import parse_llm_response

        res = parse_llm_response(str(text), strategy)  # may raise
        score = res.get("score")
        if not isinstance(score, (int, float)):
            raise ParserError(
                reason="missing_numeric_score",
                ctx={"strategy": strategy},
            )
        return f"{float(score)}\n"

    return _fn


register_parser("evaluation", "in_last_line", _make_eval_wrapper("in_last_line"))


__all__ = [
    "register_parser",
    "get_parser",
    "list_parsers",
    "ParserError",
]

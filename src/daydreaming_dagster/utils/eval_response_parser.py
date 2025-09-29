"""Utilities for parsing LLM evaluation responses."""

import re
from typing import Literal

ParseStrategy = Literal["in_last_line"]


def parse_llm_response(response_text: str, strategy: ParseStrategy) -> dict:
    """Parse LLM evaluation response to extract score and reasoning.

    Args:
        response_text: Raw LLM response text
        strategy: The parsing strategy to use.

    Returns:
        Dictionary with score (float), reasoning (str), and error (str or None)

    Raises:
        ValueError: If response cannot be parsed or is invalid
    """
    if not response_text or not response_text.strip():
        raise ValueError("Empty or whitespace-only response")

    if strategy == "in_last_line":
        return _parse_in_last_line_format(response_text)
    else:
        raise ValueError(f"Unknown parsing strategy: {strategy}")


def _parse_in_last_line_format(response_text: str) -> dict:
    """Parse evaluation responses where the score is in one of the last few lines.

    Supports two score formats:
    1. Three-digit format (e.g., "456" -> average = 5.0)
    2. Standard numeric format (e.g., "8.5" -> 8.5)
    
    Searches the last 3 non-empty lines for a score pattern.
    Prioritizes exact "SCORE: X" format over intermediate calculations.
    """
    last_lines = _extract_last_non_empty_lines(response_text, max_lines=3)
    
    # First, try to find exact "SCORE: X" pattern for single-digit scores (highest priority)
    for line in last_lines:
        cleaned_line = _clean_formatting(line)
        # Look for lines that start with just "SCORE:" followed by a single digit (not "Final Score:" etc.)
        if re.match(r'^\s*SCORE:\s*[0-9](\.[0-9]+)?\s*$', cleaned_line, re.IGNORECASE):
            score = _try_parse_exact_score_format(cleaned_line)
            if score is not None:
                _validate_score_range(score)
                return {"score": score, "error": None}
    
    # If no exact "SCORE:" found, try other patterns
    for line in last_lines:
        cleaned_line = _clean_formatting(line)
        score = _try_parse_three_digit_score(cleaned_line) or _try_parse_standard_score(cleaned_line)
        if score is not None:
            _validate_score_range(score)
            return {"score": score, "error": None}

    raise ValueError("No score found in the last 3 lines.")


def _extract_last_non_empty_lines(response_text: str, max_lines: int = 3) -> list[str]:
    """Extract the last few non-empty lines from response text."""
    lines = [line.strip() for line in response_text.strip().split('\n') if line.strip()]
    if not lines:
        raise ValueError("Empty or whitespace-only response")
    return lines[-max_lines:]


def _clean_formatting(text: str) -> str:
    """Remove markdown formatting to simplify parsing."""
    return text.replace('*', '').replace('#', '')


def _try_parse_three_digit_score(text: str) -> float | None:
    """Try to parse three-digit score format (e.g., '456' -> 5.0)."""
    pattern = _build_score_pattern(r"(\d(\,?\s?)\d(\,?\s?)\d)")
    match = re.search(pattern, text, re.IGNORECASE)

    if not match:
        return None

    raw_score = match.group(1)
    normalized_score = raw_score.replace(',', '').replace(' ', '')

    if len(normalized_score) != 3:
        raise ValueError(f"Expected 3 digits in score, found {len(normalized_score)}: {raw_score}")

    # Calculate average of the three digits
    digits = [float(char) for char in normalized_score]
    return sum(digits) / 3


def _try_parse_exact_score_format(text: str) -> float | None:
    """Try to parse exact 'SCORE: X' format."""
    pattern = r'SCORE:\s*(-?\d+(?:\.\d+)?)'
    match = re.search(pattern, text, re.IGNORECASE)
    return float(match.group(1)) if match else None


def _try_parse_standard_score(text: str) -> float | None:
    """Try to parse standard numeric score format (e.g., '8.5')."""
    pattern = _build_score_pattern(r"(-?\d+(?:\.\d+)?)(?:\s|$)")
    match = re.search(pattern, text, re.IGNORECASE)

    return float(match.group(1)) if match else None


def _build_score_pattern(value_pattern: str) -> str:
    """Build a regex pattern for score extraction with consistent header/separator matching."""
    header_pattern = r"(?:Final\s+|Total\s+)?(?:SCORE|Score)"
    separator_pattern = r"(?::\s*|\s+)"
    whitespace_pattern = r"[\n\r\s]*"

    return f"{header_pattern}{separator_pattern}{whitespace_pattern}{value_pattern}"


def _validate_score_range(score: float) -> None:
    """Validate that score is within the valid 0-9 range."""
    if not (0 <= score <= 9):
        raise ValueError(f"Score {score} is outside valid range 0-9")

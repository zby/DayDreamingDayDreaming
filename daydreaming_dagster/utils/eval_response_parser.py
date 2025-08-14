"""Utilities for parsing LLM evaluation responses."""

import re
from typing import Literal

ParseStrategy = Literal["complex", "in_last_line"]


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

    if strategy == "complex":
        return _parse_complex_format(response_text)
    elif strategy == "in_last_line":
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


def _extract_last_non_empty_line(response_text: str) -> str:
    """Extract the last non-empty line from response text."""
    lines = [line for line in response_text.strip().split('\n') if line.strip()]
    if not lines:
        raise ValueError("Empty or whitespace-only response")
    return lines[-1].strip()


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
    """Validate that score is within the valid 0-10 range."""
    if not (0 <= score <= 10):
        raise ValueError(f"Score {score} is outside valid range 0-10")


def _parse_complex_format(response_text: str) -> dict:
    """Parse evaluation responses using legacy template format (complex patterns)."""
    # Normalize the response text and limit search to last 30 lines to avoid accidental matches
    lines = response_text.strip().split('\n')
    last_lines = lines[-30:] if len(lines) > 30 else lines
    text = '\n'.join(last_lines)

    # Remove formatting to simplify regex patterns
    text = text.replace('**', '')  # Remove bold formatting
    text = text.replace('#', '')   # Remove markdown headers

    # Define pattern components
    header_pattern = r"(?:Final\s+|Total\s+)?(?:SCORE|Score)"  # Final/Total + SCORE/Score
    separator_pattern = r"(?::\s*|\s+)"  # Either colon+space OR just whitespace
    whitespace_pattern = r"[\n\r\s]*"  # Optional whitespace/newlines
    fraction_pattern = r"(-?\d+(?:\.\d+)?)/(-?\d+(?:\.\d+)?)"  # 4/10 or 4.5/10
    single_pattern = r"(-?\d+(?:\.\d+)?)(?:\s|$)"  # 4 or 4.5

    # Build systematic score patterns
    score_patterns = [
        # Header + colon/space + optional whitespace + fraction
        f"{header_pattern}{separator_pattern}{whitespace_pattern}{fraction_pattern}",
        # Header + colon/space + optional whitespace + single number
        f"{header_pattern}{separator_pattern}{whitespace_pattern}{single_pattern}",
    ]

    score = None
    last_match = None

    for pattern in score_patterns:
        # Find all matches for this pattern
        matches = list(re.finditer(pattern, text, re.IGNORECASE))
        if matches:
            # Take the last match (rightmost/latest in text)
            match = matches[-1]
            # Update our best match if this is the first one we found
            if last_match is None:
                last_match = match

    if last_match:
        if len(last_match.groups()) == 2:  # Fraction format like 8/10
            numerator, denominator = float(last_match.group(1)), float(last_match.group(2))
            if denominator == 0:
                raise ValueError("Invalid score fraction: division by zero")
            score = (numerator / denominator) * 10  # Convert to 0-10 scale
        else:
            score = float(last_match.group(1))

    if score is None:
        raise ValueError("No SCORE field found in response")

    # Validate score range
    if score < 0 or score > 10:
        raise ValueError(f"Score {score} is outside valid range 0-10")

    return {
        "score": score,
        "error": None
    }

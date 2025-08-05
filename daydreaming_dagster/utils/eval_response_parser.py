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
    """Parse evaluation responses where the score is in the last non-empty line."""
    lines = [line for line in response_text.strip().split('\n') if line.strip()]
    if not lines:
        raise ValueError("Empty or whitespace-only response")

    text = lines[-1].strip()
    # Remove formatting to simplify regex patterns
    text = text.replace('*', '')  # Remove bold and italics formatting
    text = text.replace('#', '')   # Remove markdown headers
    print(f"Parsing last line: '{text}'")  # Debugging output

    # Regex to capture various formats in the last line.
    # Handles formats like: "SCORE: 9", "SCORE = 9", "Total Score: 9", "**Score:** 5", or just "9".
    header_pattern = r"(?:Final\s+|Total\s+)?(?:SCORE|Score)"  # Final/Total + SCORE/Score
    separator_pattern = r"(?::\s*|\s+)"  # Either colon+space OR just whitespace
    whitespace_pattern = r"[\n\r\s]*"  # Optional whitespace/newlines
    single_pattern = r"(-?\d+(?:\.\d+)?)(?:\s|$)"  # 4 or 4.5
    # Build systematic score patterns
    # Header + colon/space + optional whitespace + single number
    score_pattern = f"{header_pattern}{separator_pattern}{whitespace_pattern}{single_pattern}"

    match = re.search(
        score_pattern,
        text,
        re.IGNORECASE
    )

    if match:
        score = float(match.group(1))
        if 0 <= score <= 10:
            return {"score": score, "error": None}
        else:
            raise ValueError(f"Score {score} is outside valid range 0-10")

    raise ValueError("No score found in the last line.")


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
                raise ValueError(f"Invalid score fraction: division by zero")
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

"""Utilities for parsing LLM evaluation responses."""

import re


def parse_llm_response(response_text: str) -> dict:
    """Parse LLM evaluation response to extract score and reasoning.

    Args:
        response_text: Raw LLM response text

    Returns:
        Dictionary with score (float), reasoning (str), and error (str or None)

    Raises:
        ValueError: If response cannot be parsed or is invalid
    """
    if not response_text or not response_text.strip():
        raise ValueError("Empty or whitespace-only response")

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
    matching_pattern = None
    
    for pattern in score_patterns:
        # Find all matches for this pattern
        matches = list(re.finditer(pattern, text, re.IGNORECASE))
        if matches:
            # Take the last match (rightmost/latest in text)
            match = matches[-1]
            print(f"Found {len(matches)} matches for pattern '{pattern}'")
            print(f"Using last match: '{match.group(0)}'")
            print(f"Groups: {match.groups()}")
            print(f"Match span: {match.span()}")
            
            # Update our best match if this is the first one we found
            if last_match is None:
                last_match = match
                matching_pattern = pattern
        else:
            print(f"No match for pattern: {pattern}")
    
    if last_match:
        if len(last_match.groups()) == 2:  # Fraction format like 8/10
            numerator, denominator = float(last_match.group(1)), float(last_match.group(2))
            print(f"Parsed fraction score: {numerator}/{denominator}")
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

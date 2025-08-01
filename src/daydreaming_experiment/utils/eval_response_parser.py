"""Utilities for parsing LLM evaluation responses."""

import re


def parse_llm_response(response_text: str) -> float:
    """Parse LLM evaluation response to extract score.
    
    Args:
        response_text: Raw LLM response text
        
    Returns:
        Score as float (0-10 range)
        
    Raises:
        ValueError: If response cannot be parsed or is invalid
    """
    if not response_text or not response_text.strip():
        raise ValueError("Empty or whitespace-only response")
    
    # Normalize the response text
    text = response_text.strip()
    
    # Find score - look for various formats
    score_patterns = [
        # Markdown formatted patterns
        r'\*\*SCORE\*\*:\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # **SCORE**: 8.5
        r'\*\*Score\*\*:\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # **Score**: 8.5
        r'\*\*score\*\*:\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # **score**: 8.5
        r'\*\*SCORE\*\*:\s*(-?\d+(?:\.\d+)?)/(-?\d+(?:\.\d+)?)',  # **SCORE**: 8/10 or **SCORE**: 8.5/10
        r'\*\*SCORE\*\*\s*-\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # **SCORE** - 8.5
        
        # Markdown with colon inside
        r'\*\*SCORE:\*\*\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # **SCORE:** 8.5
        r'\*\*Score:\*\*\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # **Score:** 8.5
        r'\*\*score:\*\*\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # **score:** 8.5
        r'\*\*SCORE:\*\*\s*(-?\d+(?:\.\d+)?)/(-?\d+(?:\.\d+)?)',  # **SCORE:** 8/10 or **SCORE:** 8.5/10
        
        # Markdown values
        r'SCORE:\s*\*\*(-?\d+(?:\.\d+)?)\*\*(?:\s|\(|$)',  # SCORE: **8.5**
        r'Score:\s*\*\*(-?\d+(?:\.\d+)?)\*\*(?:\s|\(|$)',  # Score: **8.5**
        r'score:\s*\*\*(-?\d+(?:\.\d+)?)\*\*(?:\s|\(|$)',  # score: **8.5**
        r'SCORE:\s*\*\*(-?\d+(?:\.\d+)?)/(-?\d+(?:\.\d+)?)\*\*',  # SCORE: **8/10** or SCORE: **8.5/10**
        r'SCORE\s*-\s*\*\*(-?\d+(?:\.\d+)?)\*\*(?:\s|\(|$)',  # SCORE - **8.5**
        
        # Both label and value in markdown
        r'\*\*SCORE\*\*:\s*\*\*(-?\d+(?:\.\d+)?)\*\*(?:\s|\(|$)',  # **SCORE**: **8.5**
        r'\*\*SCORE\*\*:\s*\*\*(-?\d+(?:\.\d+)?)/(-?\d+(?:\.\d+)?)\*\*',  # **SCORE**: **8/10**
        
        # Regular patterns
        r'SCORE:\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # SCORE: 8.5 or SCORE: 8.5 (explanation)
        r'Score:\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # Score: 8.5
        r'score:\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # score: 8.5
        r'SCORE\s*-\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # SCORE - 8.5
        r'SCORE:\s*(-?\d+(?:\.\d+)?)/(-?\d+(?:\.\d+)?)',  # SCORE: 8/10 or SCORE: 8.5/10
    ]
    
    score = None
    for pattern in score_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            if len(match.groups()) == 2:  # Fraction format like 8/10
                numerator, denominator = float(match.group(1)), float(match.group(2))
                if denominator == 0:
                    raise ValueError(f"Invalid score fraction: division by zero")
                score = (numerator / denominator) * 10  # Convert to 0-10 scale
            else:
                score = float(match.group(1))
            break
    
    if score is None:
        raise ValueError("No SCORE field found in response")
    
    # Validate score range
    if score < 0 or score > 10:
        raise ValueError(f"Score {score} is outside valid range 0-10")
    
    return score
"""Utilities for parsing LLM evaluation responses."""

import re


def extract_template_name(evaluation_task_id: str) -> str:
    """Extract template name from evaluation_task_id.
    
    Args:
        evaluation_task_id: Format like 'combo_001_..._TEMPLATE_eval_model'
        
    Returns:
        Template name (e.g., 'daydreaming_verification')
        
    Example:
        'combo_001_02_problem_solving_deepseek_deepseek-r1:free_daydreaming_verification_qwen/qwq-32b:free'
        -> 'daydreaming_verification'
    """
    # Split by underscores and find the template part
    # Pattern: combo_XXX_YYY_ZZZ_model_TEMPLATE_eval_model
    parts = evaluation_task_id.split('_')
    
    # Look for common template names (order by length, longest first to avoid substring matches)
    template_candidates = ['daydreaming_verification_v2', 'daydreaming_verification', 
                          'scientific_rigor', 'iterative_loops', 'creativity_metrics']
    
    for candidate in template_candidates:
        if candidate in evaluation_task_id:
            return candidate
    
    # Fallback: assume template is before the final eval model part
    # Find last occurrence of known eval model pattern and work backwards
    if '_' in evaluation_task_id:
        # Look for pattern like '_templatename_evalmodel'
        # This is a heuristic that may need refinement
        for i, part in enumerate(parts):
            if part in template_candidates:
                return part
                
    return None  # Template not found


def parse_llm_response(response_text: str, template_name: str = None) -> dict:
    """Parse LLM evaluation response to extract score and reasoning.

    Args:
        response_text: Raw LLM response text
        template_name: Name of evaluation template used (optional)
                      If None, will use legacy parsing method

    Returns:
        Dictionary with score (float), reasoning (str), and error (str or None)

    Raises:
        ValueError: If response cannot be parsed or is invalid
    """
    if not response_text or not response_text.strip():
        raise ValueError("Empty or whitespace-only response")

    # Use template-specific parsing if template is known
    if template_name == "daydreaming_verification_v2":
        return _parse_v2_format(response_text)
    
    # Default to legacy parsing for unknown templates or None
    return _parse_legacy_format(response_text)


def _parse_v2_format(response_text: str) -> dict:
    """Parse evaluation responses using v2 template format (SCORE: X)."""
    lines = response_text.strip().split('\n')
    
    # Check last few lines for the expected format
    for line in reversed(lines[-5:]):  # Check last 5 lines
        line = line.strip()
        match = re.match(r'^SCORE:\s*([0-9])$', line)
        if match:
            score = int(match.group(1))
            # Convert 0-9 scale to 0-10 scale for consistency
            score_10_scale = score * 10 / 9
            return {
                "score": score_10_scale,
                "error": None
            }
    
    # If v2 format not found, raise specific error
    raise ValueError("No SCORE: X format found in response (expected for daydreaming_verification_v2 template)")


def _parse_legacy_format(response_text: str) -> dict:
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

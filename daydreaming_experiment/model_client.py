import os
import re
import time
from openai import OpenAI


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
        r'SCORE:\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # SCORE: 8.5 or SCORE: 8.5 (explanation)
        r'Score:\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # Score: 8.5
        r'score:\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # score: 8.5
        r'SCORE\s*-\s*(-?\d+(?:\.\d+)?)(?:\s|\(|$)',  # SCORE - 8.5
        r'SCORE:\s*(-?\d+)/(-?\d+)',  # SCORE: 8/10
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


class SimpleModelClient:
    """Lightweight LLM client for experiment execution using OpenRouter API."""

    def __init__(
        self, api_key: str = None, base_url: str = "https://openrouter.ai/api/v1"
    ):
        """Initialize client with OpenRouter configuration."""
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OpenRouter API key required. Set OPENROUTER_API_KEY environment variable."
            )

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=base_url,
        )

    def generate(self, prompt: str, model: str) -> str:
        """Generate content using specified model."""
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=8192,
            )
            response_text = response.choices[0].message.content.strip()
            return response_text
        except Exception as e:
            time.sleep(1)
            raise e

    def evaluate(
        self, prompt: str, response: str, model: str
    ) -> str:
        """LLM-based evaluation returning the raw response text.
        
        Note: This method expects evaluation templates that output REASONING and SCORE fields,
        but parsing is now handled by the caller for better error handling.
        The prompt parameter should contain the full evaluation template with the response inserted.
        Returns: raw_response_text
        
        Raises:
            Exception: For API errors and other failures
        """
        evaluation_prompt = prompt

        try:
            eval_response = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": evaluation_prompt}],
                temperature=0.1,
                max_tokens=2048,  # Increased to allow for full REASONING + SCORE and avoid the bug with empty responses
            )

            eval_text = eval_response.choices[0].message.content
            if eval_text is None:
                eval_text = ''

            return eval_text.strip()

        except Exception as e:
            time.sleep(1)
            raise e

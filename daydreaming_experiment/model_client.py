import os
import time
from typing import Tuple
from openai import OpenAI


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

    def generate(self, prompt: str, model: str = "openai/gpt-4") -> str:
        """Generate content using specified model."""
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=8192,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            time.sleep(1)
            raise e

    def evaluate(
        self, prompt: str, response: str, model: str = "openai/gpt-4"
    ) -> Tuple[bool, float, str, str]:
        """LLM-based evaluation returning (rating, confidence, reasoning, full_response)."""
        evaluation_prompt = f"""Does this response contain ideas similar to iterative creative loops that automatically generate, evaluate, and refine concepts?

Look for elements like:
- Iterative refinement loops and cyclical processes
- Exploration-exploitation balance concepts  
- Meta-cognitive awareness and self-monitoring systems
- Combinatorial creativity and automated ideation
- Quality filtering and selection mechanisms

Response: {response}

Answer: YES/NO
Confidence: 0.0-1.0
Reasoning: Brief explanation"""

        try:
            eval_response = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": evaluation_prompt}],
                temperature=0.1,
                max_tokens=256,
            )

            eval_text = eval_response.choices[0].message.content.strip()

            # Parse structured response
            lines = eval_text.split("\n")
            rating = False
            confidence = 0.0
            reasoning = ""

            for line in lines:
                if line.startswith("Answer:"):
                    rating = "YES" in line.upper()
                elif line.startswith("Confidence:"):
                    try:
                        confidence = float(line.split(":", 1)[1].strip())
                    except (ValueError, IndexError):
                        confidence = 0.5
                elif line.startswith("Reasoning:"):
                    reasoning = line.split(":", 1)[1].strip()

            return rating, confidence, reasoning, eval_text

        except Exception as e:
            time.sleep(1)
            return False, 0.0, f"Evaluation failed: {str(e)}", ""

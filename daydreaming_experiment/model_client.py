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
    ) -> Tuple[str, str, float]:
        """LLM-based evaluation returning (reasoning, full_response, raw_score).
        
        Note: This method now expects evaluation templates that output REASONING and SCORE fields.
        The prompt parameter should contain the full evaluation template with the response inserted.
        Returns: (reasoning, full_response, raw_score)
        """
        evaluation_prompt = prompt

        try:
            eval_response = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": evaluation_prompt}],
                temperature=0.1,
                max_tokens=256,
            )

            eval_text = eval_response.choices[0].message.content
            if eval_text is None:
                eval_text = ''
           
            eval_text = eval_text.strip()

            # Parse structured response
            lines = eval_text.split("\n")
            score = 0.0
            reasoning = ""

            for line in lines:
                if line.startswith("REASONING:"):
                    reasoning = line.split(":", 1)[1].strip()
                elif line.startswith("SCORE:"):
                    try:
                        score_text = line.split(":", 1)[1].strip()
                        # Extract just the number (handle "5 (explanation)" format)
                        score = float(score_text.split()[0])
                    except (ValueError, IndexError):
                        score = 0.0

            return reasoning, eval_text, score

        except Exception as e:
            time.sleep(1)
            return f"Evaluation failed: {str(e)}", f"EVALUATION_ERROR: {str(e)}", 0.0

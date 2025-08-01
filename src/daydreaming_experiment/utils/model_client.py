import os
import time
from openai import OpenAI

# Constants
DEFAULT_MAX_TOKENS = 8192


class SimpleModelClient:
    """Lightweight LLM client for experiment execution using OpenRouter API."""

    def __init__(
        self, 
        api_key: str = None, 
        base_url: str = "https://openrouter.ai/api/v1",
        rate_limit_delay: float = 0.1
    ):
        """Initialize client with OpenRouter configuration and rate limiting.
        
        Args:
            api_key: OpenRouter API key (defaults to OPENROUTER_API_KEY env var)
            base_url: OpenRouter API base URL
            rate_limit_delay: Delay in seconds between API calls
        """
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OpenRouter API key required. Set OPENROUTER_API_KEY environment variable."
            )

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=base_url,
        )
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0

    def generate(self, prompt: str, model: str, temperature: float = 0.7) -> str:
        """Generate content using specified model with rate limiting.
        
        Args:
            prompt: Text prompt to send to the model
            model: Model identifier
            temperature: Sampling temperature (0.0-2.0). Lower values = more deterministic
            
        Returns:
            Generated response text
            
        Raises:
            Exception: Any API errors are propagated to fail the task
        """
        # Enforce rate limiting
        time_since_last = time.time() - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        
        try:
            # Make API call - let errors propagate
            response = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=DEFAULT_MAX_TOKENS,
            )
            
            self.last_request_time = time.time()
            response_text = response.choices[0].message.content
            if response_text is None:
                response_text = ''
            return response_text.strip()
            
        except Exception as e:
            self.last_request_time = time.time()  # Update timestamp even on failure
            time.sleep(1)  # Brief delay before re-raising
            raise e



import os
import time
import logging
from typing import Optional
from openai import OpenAI
from openai import RateLimitError, APIError

# Constants
DEFAULT_MAX_TOKENS = 8192
logger = logging.getLogger(__name__)


class SimpleModelClient:
    """Lightweight LLM client for experiment execution using OpenRouter API."""

    def __init__(
        self,
        api_key: str = None,
        base_url: str = "https://openrouter.ai/api/v1",
        rate_limit_delay: float = 0.1,
        max_retries: int = 3,
        retry_delay: float = 5.0,
    ):
        """Initialize client with OpenRouter configuration and retry logic.

        Args:
            api_key: OpenRouter API key (defaults to OPENROUTER_API_KEY env var)
            base_url: OpenRouter API base URL
            rate_limit_delay: Delay in seconds between API calls
            max_retries: Maximum number of retry attempts for intermittent failures
            retry_delay: Base delay in seconds between retries (exponential backoff)
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
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.last_request_time = 0

    def generate(self, prompt: str, model: str, temperature: float = 0.7) -> str:
        """Generate content using specified model with rate limiting and retry logic.

        Args:
            prompt: Text prompt to send to the model
            model: Model identifier
            temperature: Sampling temperature (0.0-2.0). Lower values = more deterministic

        Returns:
            Generated response text

        Raises:
            ValueError: For missing API key (immediate failure)
            Exception: For permanent failures after all retries exhausted
        """
        last_exception = None

        for attempt in range(self.max_retries + 1):  # +1 for initial attempt
            # Enforce rate limiting
            time_since_last = time.time() - self.last_request_time
            if time_since_last < self.rate_limit_delay:
                time.sleep(self.rate_limit_delay - time_since_last)

            try:
                # Make API call
                response = self.client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=temperature,
                    max_tokens=DEFAULT_MAX_TOKENS,
                )

                self.last_request_time = time.time()
                response_text = response.choices[0].message.content
                if response_text is None:
                    response_text = ""
                return response_text.strip()

            except Exception as e:
                self.last_request_time = time.time()  # Update timestamp even on failure
                last_exception = e

                # Check if this is a retryable error
                if self._is_retryable_error(e) and attempt < self.max_retries:
                    delay = self.retry_delay * (2**attempt)  # Exponential backoff
                    logger.warning(
                        f"API call failed (attempt {attempt + 1}/{self.max_retries + 1}): {e}. Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                    continue
                else:
                    # Non-retryable error or max retries exceeded
                    if attempt == self.max_retries:
                        logger.error(
                            f"API call failed after {self.max_retries + 1} attempts. Final error: {e}"
                        )
                    raise e

        # This should never be reached, but just in case
        if last_exception:
            raise last_exception

    def _is_retryable_error(self, error: Exception) -> bool:
        """Determine if an error should trigger a retry.

        Args:
            error: The exception that occurred

        Returns:
            True if the error is likely intermittent and worth retrying
        """
        # Rate limiting errors are always retryable
        if isinstance(error, RateLimitError):
            return True

        # Some API errors are retryable (server issues, network problems)
        if isinstance(error, APIError):
            # 5xx server errors are typically retryable
            if hasattr(error, "status_code") and 500 <= error.status_code < 600:
                return True
            # 429 rate limit (should be handled by RateLimitError, but just in case)
            if hasattr(error, "status_code") and error.status_code == 429:
                return True

        # Check error message for common retryable patterns
        error_msg = str(error).lower()
        retryable_patterns = [
            "please retry",
            "rate limit",
            "timeout",
            "connection error",
            "network error",
            "server error",
            "service unavailable",
            "too many requests",
        ]

        return any(pattern in error_msg for pattern in retryable_patterns)

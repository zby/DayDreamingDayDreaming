import logging
import os
import time
from typing import Optional

from dagster import ConfigurableResource
from openai import APIError, OpenAI, RateLimitError

# HTTP status code constants
HTTP_SERVER_ERROR_START = 500
HTTP_SERVER_ERROR_END = 600
HTTP_TOO_MANY_REQUESTS = 429

logger = logging.getLogger(__name__)

class LLMClientResource(ConfigurableResource):
    """
    Dagster-native LLM client with built-in rate limiting and retry logic.
    Replaces SimpleModelClient with cleaner Dagster ConfigurableResource pattern.
    """
    api_key: Optional[str] = None
    base_url: str = "https://openrouter.ai/api/v1"
    rate_limit_delay: float = 0.1
    max_retries: int = 3
    retry_delay: float = 5.0
    default_max_tokens: int = 8192
    def __post_init__(self):
        """Initialize OpenAI client after Dagster resource configuration."""
        # Get API key from config or environment
        effective_api_key = self.api_key or os.getenv("OPENROUTER_API_KEY")
        if not effective_api_key:
            raise ValueError(
                "OpenRouter API key required. Set OPENROUTER_API_KEY environment variable or pass api_key parameter."
            )

        self._client = OpenAI(
            api_key=effective_api_key,
            base_url=self.base_url,
        )
        self._last_request_time = 0

    def generate(self, prompt: str, model: str, temperature: float = 0.7, max_tokens: Optional[int] = None) -> str:
        """Generate content using specified model with rate limiting and retry logic.

        Args:
            prompt: Text prompt to send to the model
            model: Model identifier
            temperature: Sampling temperature (0.0-2.0). Lower values = more deterministic
            max_tokens: Maximum tokens to generate (defaults to default_max_tokens)

        Returns:
            Generated response text

        Raises:
            ValueError: For missing API key (immediate failure)
            Exception: For permanent failures after all retries exhausted
        """
        effective_max_tokens = max_tokens or self.default_max_tokens
        last_exception = None

        for attempt in range(self.max_retries + 1):  # +1 for initial attempt
            # Enforce rate limiting
            time_since_last = time.time() - self._last_request_time
            if time_since_last < self.rate_limit_delay:
                time.sleep(self.rate_limit_delay - time_since_last)

            try:
                # Make API call
                response = self._client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=temperature,
                    max_tokens=effective_max_tokens,
                )

                self._last_request_time = time.time()
                response_text = response.choices[0].message.content
                if response_text is None:
                    response_text = ""
                return response_text.strip()

            except Exception as e:
                self._last_request_time = time.time()  # Update timestamp even on failure
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
            if hasattr(error, "status_code") and HTTP_SERVER_ERROR_START <= error.status_code < HTTP_SERVER_ERROR_END:
                return True
            # 429 rate limit (should be handled by RateLimitError, but just in case)
            if hasattr(error, "status_code") and error.status_code == HTTP_TOO_MANY_REQUESTS:
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

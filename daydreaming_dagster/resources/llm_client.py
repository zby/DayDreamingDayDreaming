import logging
import os
import time
from typing import Optional

from dagster import ConfigurableResource
from openai import APIError, OpenAI, RateLimitError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from ratelimit import limits, sleep_and_retry

# HTTP status code constants
HTTP_SERVER_ERROR_START = 500
HTTP_SERVER_ERROR_END = 600
HTTP_TOO_MANY_REQUESTS = 429

logger = logging.getLogger(__name__)

class LLMClientResource(ConfigurableResource):
    """
    Dagster-native LLM client with built-in rate limiting and retry logic using tenacity and ratelimit.
    Provides robust API calling with proactive rate limiting and reactive retries.
    """
    api_key: Optional[str] = None
    base_url: str = "https://openrouter.ai/api/v1"
    max_retries: int = 5
    default_max_tokens: int = 8192
    # Rate limiting: VERY CONSERVATIVE for free tier APIs (some models allow only 1 call per minute)
    rate_limit_calls: int = 1
    rate_limit_period: int = 60  # 1 call per minute
    mandatory_delay: float = 65.0  # Mandatory delay between calls (65 seconds to be extra safe)
    def _ensure_initialized(self):
        """Lazy initialization of OpenAI client."""
        if not hasattr(self, '_client'):
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
        self._ensure_initialized()
        effective_max_tokens = max_tokens or self.default_max_tokens
        
        # Call the decorated method that handles rate limiting and retries
        info = self._make_api_call_info(prompt, model, temperature, effective_max_tokens)
        return info.get("text", "")

    def generate_with_info(self, prompt: str, model: str, temperature: float = 0.7, max_tokens: Optional[int] = None) -> tuple[str, dict]:
        """Generate content and return (text, info dict).

        Info includes keys like 'finish_reason' and 'truncated' (True if finish_reason == 'length').
        """
        self._ensure_initialized()
        effective_max_tokens = max_tokens or self.default_max_tokens
        info = self._make_api_call_info(prompt, model, temperature, effective_max_tokens)
        return info.get("text", ""), info

    def _make_api_call_info(self, prompt: str, model: str, temperature: float, max_tokens: int) -> dict:
        """Make the actual API call with rate limiting and retry logic and return info dict."""
        
        # Create dynamic decorators based on instance configuration
        def create_decorated_call():
            @sleep_and_retry
            @limits(calls=self.rate_limit_calls, period=self.rate_limit_period)
            @retry(
                wait=wait_exponential(multiplier=2, min=2, max=60),
                stop=stop_after_attempt(self.max_retries),
                retry=retry_if_exception_type((RateLimitError, APIError, ConnectionError, TimeoutError)),
                before_sleep=lambda retry_state: logger.warning(
                    f"API call failed (attempt {retry_state.attempt_number}): {retry_state.outcome.exception()}. "
                    f"Retrying in {retry_state.next_action.sleep} seconds..."
                )
            )
            def decorated_api_call():
                return self._raw_api_call_info(prompt, model, temperature, max_tokens)
            
            return decorated_api_call
        
        # Create and call the decorated function
        decorated_call = create_decorated_call()
        return decorated_call()

    def _raw_api_call_info(self, prompt: str, model: str, temperature: float, max_tokens: int) -> dict:
        """Raw API call with mandatory delay; return dict with text and finish info."""
        
        # Enforce mandatory delay between calls (ultra-conservative for free tier)
        if hasattr(self, '_last_call_time'):
            time_since_last = time.time() - self._last_call_time
            if time_since_last < self.mandatory_delay:
                delay_needed = self.mandatory_delay - time_since_last
                logger.info(f"Mandatory delay: waiting {delay_needed:.1f}s before API call (free tier protection)")
                time.sleep(delay_needed)
        
        try:
            logger.info(f"Making API call to {model} (prompt length: {len(prompt)} chars)")
            response = self._client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=max_tokens,
            )
            
            # Record the time of this successful call
            self._last_call_time = time.time()

            choice = response.choices[0]
            response_text = choice.message.content
            if response_text is None:
                response_text = ""
            finish_reason = getattr(choice, "finish_reason", None)
            truncated = str(finish_reason).lower() == "length"
            logger.info(f"API call successful, response length: {len(response_text)} chars")
            return {"text": response_text.strip(), "finish_reason": finish_reason, "truncated": truncated}

        except Exception as e:
            # Record the time even for failed calls to maintain delay
            self._last_call_time = time.time()
            
            # Enhanced error logging for JSON decode errors
            if "JSONDecodeError" in str(type(e).__name__) or "json" in str(e).lower():
                logger.error(f"JSON parsing failed - API may have returned HTML error page or malformed response: {e}")
                # Log more context for JSON errors
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    logger.error(f"Raw response content: {e.response.text[:1000]}...")
            else:
                logger.debug(f"API call error: {e}")
            
            # Only retry if it's a retryable error
            if not self._is_retryable_error(e):
                logger.error(f"Non-retryable error, failing immediately: {e}")
                raise e
            
            # Re-raise retryable errors for tenacity to handle
            raise e

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
            "json",  # JSON decode errors often indicate API returning HTML error pages
            "expecting value",  # Common JSON decode error message
        ]

        return any(pattern in error_msg for pattern in retryable_patterns)

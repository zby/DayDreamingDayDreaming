import pytest
from unittest.mock import Mock, patch, MagicMock
import os

from daydreaming_experiment.model_client import SimpleModelClient


class TestSimpleModelClient:

    def test_init_with_api_key(self):
        """Test initialization with explicit API key."""
        client = SimpleModelClient(api_key="test_key")
        assert client.api_key == "test_key"

    def test_init_from_env(self):
        """Test initialization from environment variable."""
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "env_key"}):
            client = SimpleModelClient()
            assert client.api_key == "env_key"

    def test_init_no_api_key_raises_error(self):
        """Test that missing API key raises ValueError."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="OpenRouter API key required"):
                SimpleModelClient()

    @patch("daydreaming_experiment.model_client.OpenAI")
    def test_generate_success(self, mock_openai):
        """Test successful content generation."""
        # Setup mock
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "  Generated content  "

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai.return_value = mock_client

        # Test
        client = SimpleModelClient(api_key="test_key")
        result = client.generate("test prompt", "test-model")

        assert result == "Generated content"
        mock_client.chat.completions.create.assert_called_once_with(
            model="test-model",
            messages=[{"role": "user", "content": "test prompt"}],
            temperature=0.7,
            max_tokens=1024,
        )

    @patch("daydreaming_experiment.model_client.OpenAI")
    @patch("time.sleep")
    def test_generate_exception_propagated(self, mock_sleep, mock_openai):
        """Test that exceptions in generate are propagated after sleep."""
        mock_client = Mock()
        mock_client.chat.completions.create.side_effect = Exception("API Error")
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")

        with pytest.raises(Exception, match="API Error"):
            client.generate("test prompt")

        mock_sleep.assert_called_once_with(1)

    @patch("daydreaming_experiment.model_client.OpenAI")
    def test_evaluate_success_yes(self, mock_openai):
        """Test successful evaluation with YES response."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[
            0
        ].message.content = """Answer: YES
Confidence: 0.85
Reasoning: Contains iterative refinement loops"""

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        rating, confidence, reasoning, full_response = client.evaluate(
            "prompt", "response", "test-model"
        )

        assert rating is True
        assert confidence == 0.85
        assert reasoning == "Contains iterative refinement loops"
        assert "Answer: YES" in full_response

    @patch("daydreaming_experiment.model_client.OpenAI")
    def test_evaluate_success_no(self, mock_openai):
        """Test successful evaluation with NO response."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[
            0
        ].message.content = """Answer: NO
Confidence: 0.9
Reasoning: No iterative patterns found"""

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        rating, confidence, reasoning, full_response = client.evaluate("prompt", "response")

        assert rating is False
        assert confidence == 0.9
        assert reasoning == "No iterative patterns found"
        assert "Answer: NO" in full_response

    @patch("daydreaming_experiment.model_client.OpenAI")
    def test_evaluate_malformed_response(self, mock_openai):
        """Test evaluation with malformed response."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = (
            "Malformed response without proper structure"
        )

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        rating, confidence, reasoning, full_response = client.evaluate("prompt", "response")

        assert rating is False
        assert confidence == 0.0
        assert reasoning == ""
        assert full_response == "Malformed response without proper structure"

    @patch("daydreaming_experiment.model_client.OpenAI")
    def test_evaluate_invalid_confidence(self, mock_openai):
        """Test evaluation with invalid confidence value."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[
            0
        ].message.content = """Answer: YES
Confidence: invalid
Reasoning: Test reasoning"""

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        rating, confidence, reasoning, full_response = client.evaluate("prompt", "response")

        assert rating is True
        assert confidence == 0.5  # Default fallback
        assert reasoning == "Test reasoning"
        assert "Confidence: invalid" in full_response

    @patch("daydreaming_experiment.model_client.OpenAI")
    @patch("time.sleep")
    def test_evaluate_exception_handling(self, mock_sleep, mock_openai):
        """Test that evaluate handles exceptions gracefully."""
        mock_client = Mock()
        mock_client.chat.completions.create.side_effect = Exception("API Error")
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        rating, confidence, reasoning, full_response = client.evaluate("prompt", "response")

        assert rating is False
        assert confidence == 0.0
        assert reasoning == "Evaluation failed: API Error"
        assert full_response == ""
        mock_sleep.assert_called_once_with(1)

    @patch("daydreaming_experiment.model_client.OpenAI")
    def test_evaluate_prompt_structure(self, mock_openai):
        """Test that evaluation prompt contains expected elements."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = (
            "Answer: NO\nConfidence: 0.5\nReasoning: Test"
        )

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        client.evaluate("test prompt", "test response")

        # Check that the evaluation prompt was constructed correctly
        call_args = mock_client.chat.completions.create.call_args
        evaluation_prompt = call_args[1]["messages"][0]["content"]

        assert "test response" in evaluation_prompt
        assert "iterative creative loops" in evaluation_prompt
        assert "Answer: YES/NO" in evaluation_prompt
        assert "Confidence: 0.0-1.0" in evaluation_prompt
        assert "Reasoning:" in evaluation_prompt

import pytest
from unittest.mock import Mock, patch, MagicMock
import os

from daydreaming_experiment.model_client import SimpleModelClient, parse_llm_response


class TestParseLLMResponse:
    """Test the parse_llm_response function."""
    
    def test_standard_format(self):
        """Test parsing standard REASONING/SCORE format."""
        response = "REASONING: This shows great creativity\nSCORE: 8.5"
        reasoning, score = parse_llm_response(response)
        assert reasoning == "This shows great creativity"
        assert score == 8.5
    
    def test_multiline_reasoning(self):
        """Test parsing multiline reasoning."""
        response = """REASONING: This response demonstrates creativity
because it combines multiple concepts in novel ways.
The ideas are well-structured and innovative.
SCORE: 7.2"""
        reasoning, score = parse_llm_response(response)
        expected_reasoning = "This response demonstrates creativity\nbecause it combines multiple concepts in novel ways.\nThe ideas are well-structured and innovative."
        assert reasoning == expected_reasoning
        assert score == 7.2
    
    def test_case_variations(self):
        """Test different case variations."""
        test_cases = [
            ("reasoning: Good analysis\nscore: 6.0", "Good analysis", 6.0),
            ("Reasoning: Excellent work\nScore: 9.5", "Excellent work", 9.5),
            ("REASONING: Basic response\nSCORE: 4.0", "Basic response", 4.0),
        ]
        
        for response, expected_reasoning, expected_score in test_cases:
            reasoning, score = parse_llm_response(response)
            assert reasoning == expected_reasoning
            assert score == expected_score
    
    def test_alternative_separators(self):
        """Test alternative separators like dashes."""
        response = "REASONING - Great creativity shown\nSCORE: 8.0"
        reasoning, score = parse_llm_response(response)
        assert reasoning == "Great creativity shown"
        assert score == 8.0
    
    def test_score_with_explanation(self):
        """Test score followed by explanation in parentheses."""
        response = "REASONING: Shows innovation\nSCORE: 7.5 (above average creativity)"
        reasoning, score = parse_llm_response(response)
        assert reasoning == "Shows innovation"
        assert score == 7.5
    
    def test_score_as_fraction(self):
        """Test score in fraction format like 8/10."""
        response = "REASONING: Good work\nSCORE: 8/10"
        reasoning, score = parse_llm_response(response)
        assert reasoning == "Good work"
        assert score == 8.0  # 8/10 * 10 = 8.0
        
        # Test other fractions
        response2 = "REASONING: Excellent\nSCORE: 9/10"
        reasoning2, score2 = parse_llm_response(response2)
        assert score2 == 9.0
    
    def test_integer_scores(self):
        """Test integer scores without decimals."""
        response = "REASONING: Basic response\nSCORE: 5"
        reasoning, score = parse_llm_response(response)
        assert reasoning == "Basic response"
        assert score == 5.0
    
    def test_extra_whitespace(self):
        """Test handling of extra whitespace."""
        response = "  \n  REASONING:   Creative analysis   \n\n  SCORE:  6.8  \n  "
        reasoning, score = parse_llm_response(response)
        assert reasoning == "Creative analysis"
        assert score == 6.8
    
    def test_boundary_scores(self):
        """Test boundary score values."""
        test_cases = [
            ("REASONING: Minimum\nSCORE: 0", 0.0),
            ("REASONING: Maximum\nSCORE: 10", 10.0),
            ("REASONING: Decimal boundary\nSCORE: 0.1", 0.1),
            ("REASONING: High decimal\nSCORE: 9.9", 9.9),
        ]
        
        for response, expected_score in test_cases:
            reasoning, score = parse_llm_response(response)
            assert score == expected_score
    
    # Error cases - these should raise ValueError
    
    def test_empty_response(self):
        """Test empty response raises ValueError."""
        with pytest.raises(ValueError, match="Empty or whitespace-only response"):
            parse_llm_response("")
        
        with pytest.raises(ValueError, match="Empty or whitespace-only response"):
            parse_llm_response("   \n  \t  ")
    
    def test_missing_reasoning(self):
        """Test missing REASONING field raises ValueError."""
        with pytest.raises(ValueError, match="No REASONING field found"):
            parse_llm_response("SCORE: 8.5")
        
        with pytest.raises(ValueError, match="No REASONING field found"):
            parse_llm_response("Some random text\nSCORE: 8.5")
    
    def test_missing_score(self):
        """Test missing SCORE field raises ValueError."""
        with pytest.raises(ValueError, match="No SCORE field found"):
            parse_llm_response("REASONING: Good analysis")
        
        with pytest.raises(ValueError, match="No SCORE field found"):
            parse_llm_response("REASONING: Analysis\nSome other text")
    
    def test_invalid_score_values(self):
        """Test invalid score values raise ValueError."""
        invalid_cases = [
            ("REASONING: Test\nSCORE: -1", "outside valid range"),
            ("REASONING: Test\nSCORE: 11", "outside valid range"),
            ("REASONING: Test\nSCORE: 15.5", "outside valid range"),
            ("REASONING: Test\nSCORE: -0.1", "outside valid range"),
            ("REASONING: Test\nSCORE: abc", "No SCORE field found"),  # Non-numeric
            ("REASONING: Test\nSCORE: 8.5.2", "No SCORE field found"),  # Invalid decimal
        ]
        
        for response, error_match in invalid_cases:
            with pytest.raises(ValueError, match=error_match):
                parse_llm_response(response)
    
    def test_division_by_zero_fraction(self):
        """Test fraction with zero denominator raises ValueError."""
        with pytest.raises(ValueError, match="division by zero"):
            parse_llm_response("REASONING: Test\nSCORE: 8/0")
    
    def test_malformed_structure(self):
        """Test completely malformed responses raise ValueError."""
        malformed_cases = [
            "Just some random text without any structure",
            "REASONING but no colon\nSCORE but no colon",
            "REASONING: \nSCORE: ",  # Empty values
        ]
        
        for response in malformed_cases:
            with pytest.raises(ValueError):
                parse_llm_response(response)


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
            max_tokens=8192,
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
            client.generate("test prompt", "test-model")

        mock_sleep.assert_called_once_with(1)

    @patch("daydreaming_experiment.model_client.OpenAI")
    def test_evaluate_success_yes(self, mock_openai):
        """Test successful evaluation with YES response."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[
            0
        ].message.content = """REASONING: Contains iterative refinement loops
SCORE: 8.5"""

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        reasoning, full_response, raw_score = client.evaluate(
            "prompt", "response", "test-model"
        )

        assert reasoning == "Contains iterative refinement loops"
        assert "REASONING:" in full_response
        assert raw_score == 8.5

    @patch("daydreaming_experiment.model_client.OpenAI")
    def test_evaluate_success_no(self, mock_openai):
        """Test successful evaluation with NO response."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[
            0
        ].message.content = """REASONING: No iterative patterns found
SCORE: 2.0"""

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        reasoning, full_response, raw_score = client.evaluate(
            "prompt", "response", "test-model"
        )

        assert reasoning == "No iterative patterns found"
        assert "REASONING:" in full_response
        assert raw_score == 2.0

    @patch("daydreaming_experiment.model_client.OpenAI")
    def test_evaluate_malformed_response(self, mock_openai):
        """Test evaluation with malformed response raises ValueError."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = (
            "Malformed response without proper structure"
        )

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        
        with pytest.raises(ValueError, match="No REASONING field found"):
            client.evaluate("prompt", "response", "test-model")

    @patch("daydreaming_experiment.model_client.OpenAI")
    def test_evaluate_invalid_score(self, mock_openai):
        """Test evaluation with invalid score value raises ValueError."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[
            0
        ].message.content = """REASONING: Test reasoning
SCORE: invalid"""

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        
        with pytest.raises(ValueError, match="No SCORE field found"):
            client.evaluate("prompt", "response", "test-model")

    @patch("daydreaming_experiment.model_client.OpenAI")
    @patch("time.sleep")
    def test_evaluate_exception_propagation(self, mock_sleep, mock_openai):
        """Test that evaluate propagates API exceptions after sleep."""
        mock_client = Mock()
        mock_client.chat.completions.create.side_effect = Exception("API Error")
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        
        with pytest.raises(Exception, match="API Error"):
            client.evaluate("prompt", "response", "test-model")
        
        mock_sleep.assert_called_once_with(1)

    @patch("daydreaming_experiment.model_client.OpenAI")
    def test_evaluate_empty_response_handling(self, mock_openai):
        """Test that evaluate handles empty API responses by raising ValueError."""
        mock_completion = Mock()
        mock_completion.choices = [Mock()]
        mock_completion.choices[0].message.content = None  # Simulate empty response

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_completion
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        
        with pytest.raises(ValueError, match="Empty or whitespace-only response"):
            client.evaluate("prompt", "response", "test-model")

    @patch("daydreaming_experiment.model_client.OpenAI")
    def test_evaluate_prompt_structure(self, mock_openai):
        """Test that evaluation prompt contains expected elements."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = (
            "REASONING: Test\nSCORE: 3.0"
        )

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai.return_value = mock_client

        client = SimpleModelClient(api_key="test_key")
        test_prompt = "RESPONSE TO EVALUATE:\ntest response\n\nREASONING:\nSCORE:"
        client.evaluate(test_prompt, "test response", "test-model")

        # Check that the evaluation prompt was passed through correctly
        call_args = mock_client.chat.completions.create.call_args
        evaluation_prompt = call_args[1]["messages"][0]["content"]

        # The prompt parameter should be used directly as the evaluation prompt
        assert evaluation_prompt == test_prompt

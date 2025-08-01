import pytest
from unittest.mock import Mock, patch, MagicMock
import os

from daydreaming_experiment.utils.model_client import SimpleModelClient, parse_llm_response


class TestParseLLMResponse:
    """Test the parse_llm_response function."""
    
    def test_standard_format(self):
        """Test parsing standard REASONING/SCORE format."""
        response = "REASONING: This shows great creativity\nSCORE: 8.5"
        score = parse_llm_response(response)
        assert score == 8.5
    
    def test_multiline_reasoning(self):
        """Test parsing multiline reasoning."""
        response = """REASONING: This response demonstrates creativity
because it combines multiple concepts in novel ways.
The ideas are well-structured and innovative.
SCORE: 7.2"""
        score = parse_llm_response(response)
        assert score == 7.2
    
    def test_case_variations(self):
        """Test different case variations."""
        test_cases = [
            ("reasoning: Good analysis\nscore: 6.0", 6.0),
            ("Reasoning: Excellent work\nScore: 9.5", 9.5),
            ("REASONING: Basic response\nSCORE: 4.0", 4.0),
        ]
        
        for response, expected_score in test_cases:
            score = parse_llm_response(response)
            assert score == expected_score
    
    def test_alternative_separators(self):
        """Test alternative separators like dashes."""
        response = "REASONING - Great creativity shown\nSCORE: 8.0"
        score = parse_llm_response(response)
        assert score == 8.0
    
    def test_score_with_explanation(self):
        """Test score followed by explanation in parentheses."""
        response = "REASONING: Shows innovation\nSCORE: 7.5 (above average creativity)"
        score = parse_llm_response(response)
        assert score == 7.5
    
    def test_score_as_fraction(self):
        """Test score in fraction format like 8/10."""
        response = "REASONING: Good work\nSCORE: 8/10"
        score = parse_llm_response(response)
        assert score == 8.0
    
    def test_no_score_found(self):
        """Test when no valid score is found."""
        response = "REASONING: Good analysis but no score provided"
        with pytest.raises(ValueError, match="No SCORE field found in response"):
            parse_llm_response(response)
    
    def test_invalid_score_value(self):
        """Test when score value is not a valid number."""
        response = "REASONING: Analysis done\nSCORE: not_a_number"
        with pytest.raises(ValueError, match="No SCORE field found in response"):
            parse_llm_response(response)
    
    def test_score_out_of_range(self):
        """Test handling of scores outside 0-10 range."""
        test_cases = [
            ("REASONING: Test\nSCORE: -2.5", "Score -2.5 is outside valid range 0-10"),
            ("REASONING: Test\nSCORE: 15.0", "Score 15.0 is outside valid range 0-10"),
        ]
        
        for response, expected_error in test_cases:
            with pytest.raises(ValueError, match=expected_error):
                parse_llm_response(response)
    
    def test_multiple_score_lines(self):
        """Test when multiple SCORE lines exist - should use first one."""
        response = """REASONING: Analysis
SCORE: 7.5
SCORE: 8.0
Additional text"""
        score = parse_llm_response(response)
        assert score == 7.5


class TestSimpleModelClient:
    """Test the SimpleModelClient class."""
    
    def test_init_with_api_key(self):
        """Test initialization with explicit API key."""
        client = SimpleModelClient(api_key="test_key")
        assert client.api_key == "test_key"
        assert client.rate_limit_delay == 0.1  # default
    
    def test_init_with_custom_params(self):
        """Test initialization with custom parameters."""
        client = SimpleModelClient(
            api_key="test_key", 
            base_url="https://custom.api.com/v1",
            rate_limit_delay=0.5
        )
        assert client.api_key == "test_key"
        assert client.rate_limit_delay == 0.5
    
    @patch.dict(os.environ, {"OPENROUTER_API_KEY": "env_key"})
    def test_init_from_env(self):
        """Test initialization using environment variables."""
        client = SimpleModelClient()
        assert client.api_key == "env_key"
    
    def test_init_missing_api_key(self):
        """Test initialization fails when API key is missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="OpenRouter API key required"):
                SimpleModelClient()
    
    @patch('openai.OpenAI')
    def test_generate_success(self, mock_openai_class):
        """Test successful text generation."""
        # Mock the OpenAI client and response
        mock_client = Mock()
        mock_openai_class.return_value = mock_client
        
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message = Mock()
        mock_response.choices[0].message.content = "Generated response"
        mock_client.chat.completions.create.return_value = mock_response
        
        client = SimpleModelClient(api_key="test_key")
        result = client.generate("Test prompt", "gpt-4")
        
        assert result == "Generated response"
        mock_client.chat.completions.create.assert_called_once()
    
    @patch('openai.OpenAI')
    def test_generate_with_default_params(self, mock_openai_class):
        """Test text generation uses default parameters."""
        mock_client = Mock()
        mock_openai_class.return_value = mock_client
        
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message = Mock()
        mock_response.choices[0].message.content = "Generated response"
        mock_client.chat.completions.create.return_value = mock_response
        
        client = SimpleModelClient(api_key="test_key")
        result = client.generate("Test prompt", "gpt-4")
        
        assert result == "Generated response"
        
        # Check that default parameters were used
        call_args = mock_client.chat.completions.create.call_args
        assert call_args[1]["temperature"] == 0.7
        assert call_args[1]["max_tokens"] == 8192
    
    @patch('openai.OpenAI')
    def test_generate_api_error(self, mock_openai_class):
        """Test handling of API errors during generation."""
        mock_client = Mock()
        mock_openai_class.return_value = mock_client
        mock_client.chat.completions.create.side_effect = Exception("API Error")
        
        client = SimpleModelClient(api_key="test_key")
        
        with pytest.raises(Exception, match="API Error"):
            client.generate("Test prompt", "gpt-4")
    
    @patch('openai.OpenAI')
    def test_evaluate_success(self, mock_openai_class):
        """Test successful evaluation."""
        mock_client = Mock()
        mock_openai_class.return_value = mock_client
        
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message = Mock()
        mock_response.choices[0].message.content = "REASONING: Good analysis\nSCORE: 8.5"
        mock_client.chat.completions.create.return_value = mock_response
        
        client = SimpleModelClient(api_key="test_key")
        result = client.evaluate("Evaluate this:", "Sample response", "gpt-4")
        
        assert result == "REASONING: Good analysis\nSCORE: 8.5"
        mock_client.chat.completions.create.assert_called_once()
    
    @patch('openai.OpenAI')
    def test_evaluate_with_none_content(self, mock_openai_class):
        """Test evaluation when API returns None content."""
        mock_client = Mock()
        mock_openai_class.return_value = mock_client
        
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message = Mock()
        mock_response.choices[0].message.content = None
        mock_client.chat.completions.create.return_value = mock_response
        
        client = SimpleModelClient(api_key="test_key")
        result = client.evaluate("Evaluate this:", "Sample response", "gpt-4")
        
        assert result == ""
    
    @patch('openai.OpenAI') 
    def test_rate_limiting(self, mock_openai_class):
        """Test that rate limiting delays are applied."""
        mock_client = Mock()
        mock_openai_class.return_value = mock_client
        
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message = Mock()
        mock_response.choices[0].message.content = "Response"
        mock_client.chat.completions.create.return_value = mock_response
        
        client = SimpleModelClient(api_key="test_key", rate_limit_delay=0.01)
        
        import time
        start_time = time.time()
        client.generate("Test 1", "gpt-4")
        client.generate("Test 2", "gpt-4")
        end_time = time.time()
        
        # Should have some delay between calls
        assert end_time - start_time >= 0.01
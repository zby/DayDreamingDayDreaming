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
        score = parse_llm_response(response)
        assert score is None
    
    def test_invalid_score_value(self):
        """Test when score value is not a valid number."""
        response = "REASONING: Analysis done\nSCORE: not_a_number"
        score = parse_llm_response(response)
        assert score is None
    
    def test_score_out_of_range(self):
        """Test handling of scores outside 0-10 range."""
        test_cases = [
            ("REASONING: Test\nSCORE: -2.5", -2.5),  # Should still parse negative
            ("REASONING: Test\nSCORE: 15.0", 15.0),  # Should still parse over 10
        ]
        
        for response, expected_score in test_cases:
            score = parse_llm_response(response)
            assert score == expected_score
    
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
    
    def test_init_with_openrouter(self):
        """Test initialization with OpenRouter."""
        client = SimpleModelClient(api_key="test_key", provider="openrouter")
        assert client.provider == "openrouter"
        assert client.api_key == "test_key"
    
    def test_init_with_openai(self):
        """Test initialization with OpenAI."""
        client = SimpleModelClient(api_key="test_key", provider="openai")
        assert client.provider == "openai"
        assert client.api_key == "test_key"
    
    def test_init_invalid_provider(self):
        """Test initialization with invalid provider."""
        with pytest.raises(ValueError, match="Unsupported provider"):
            SimpleModelClient(api_key="test_key", provider="invalid_provider")
    
    @patch.dict(os.environ, {"OPENROUTER_API_KEY": "env_key"})
    def test_init_from_env_openrouter(self):
        """Test initialization using environment variables for OpenRouter."""
        client = SimpleModelClient(provider="openrouter")
        assert client.api_key == "env_key"
    
    @patch.dict(os.environ, {"OPENAI_API_KEY": "env_key"})
    def test_init_from_env_openai(self):
        """Test initialization using environment variables for OpenAI."""
        client = SimpleModelClient(provider="openai")
        assert client.api_key == "env_key"
    
    def test_init_missing_api_key(self):
        """Test initialization fails when API key is missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="API key not provided"):
                SimpleModelClient(provider="openrouter")
    
    @patch('daydreaming_experiment.utils.model_client.openai.OpenAI')
    def test_generate_text_success(self, mock_openai_class):
        """Test successful text generation."""
        # Mock the OpenAI client and response
        mock_client = Mock()
        mock_openai_class.return_value = mock_client
        
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message = Mock()
        mock_response.choices[0].message.content = "Generated response"
        mock_client.chat.completions.create.return_value = mock_response
        
        client = SimpleModelClient(api_key="test_key", provider="openrouter")
        result = client.generate_text("Test prompt", "gpt-4")
        
        assert result == "Generated response"
        mock_client.chat.completions.create.assert_called_once()
    
    @patch('daydreaming_experiment.utils.model_client.openai.OpenAI')
    def test_generate_text_with_custom_params(self, mock_openai_class):
        """Test text generation with custom parameters."""
        mock_client = Mock()
        mock_openai_class.return_value = mock_client
        
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message = Mock()
        mock_response.choices[0].message.content = "Generated response"
        mock_client.chat.completions.create.return_value = mock_response
        
        client = SimpleModelClient(api_key="test_key", provider="openrouter")
        result = client.generate_text(
            "Test prompt", 
            "gpt-4",
            max_tokens=500,
            temperature=0.8
        )
        
        assert result == "Generated response"
        
        # Check that custom parameters were passed
        call_args = mock_client.chat.completions.create.call_args
        assert call_args[1]["max_tokens"] == 500
        assert call_args[1]["temperature"] == 0.8
    
    @patch('daydreaming_experiment.utils.model_client.openai.OpenAI')
    def test_generate_text_api_error(self, mock_openai_class):
        """Test handling of API errors during generation."""
        mock_client = Mock()
        mock_openai_class.return_value = mock_client
        mock_client.chat.completions.create.side_effect = Exception("API Error")
        
        client = SimpleModelClient(api_key="test_key", provider="openrouter")
        
        with pytest.raises(Exception, match="API Error"):
            client.generate_text("Test prompt", "gpt-4")
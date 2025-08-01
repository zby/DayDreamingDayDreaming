import pytest
from unittest.mock import Mock, patch, MagicMock
import os

from daydreaming_experiment.utils.model_client import SimpleModelClient


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
    
    @patch('daydreaming_experiment.utils.model_client.OpenAI')
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
    
    @patch('daydreaming_experiment.utils.model_client.OpenAI')
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
    
    @patch('daydreaming_experiment.utils.model_client.OpenAI')
    def test_generate_api_error(self, mock_openai_class):
        """Test handling of API errors during generation."""
        mock_client = Mock()
        mock_openai_class.return_value = mock_client
        mock_client.chat.completions.create.side_effect = Exception("API Error")
        
        client = SimpleModelClient(api_key="test_key")
        
        with pytest.raises(Exception, match="API Error"):
            client.generate("Test prompt", "gpt-4")
    
    @patch('daydreaming_experiment.utils.model_client.OpenAI')
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
    
    @patch('daydreaming_experiment.utils.model_client.OpenAI')
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
    
    @patch('daydreaming_experiment.utils.model_client.OpenAI') 
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
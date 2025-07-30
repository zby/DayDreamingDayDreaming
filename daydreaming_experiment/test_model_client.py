import pytest
from unittest.mock import Mock, patch, MagicMock
import os

from daydreaming_experiment.model_client import SimpleModelClient, parse_llm_response


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
        """Test score as fraction like 8/10."""
        test_cases = [
            ("REASONING: Good work\nSCORE: 8/10", 8.0),
            ("REASONING: Excellent\nSCORE: 9/10", 9.0),
            ("REASONING: Perfect\nSCORE: 10/10", 10.0),
            ("REASONING: Poor\nSCORE: 3/10", 3.0),
        ]
        
        for response, expected_score in test_cases:
            score = parse_llm_response(response)
            assert score == expected_score
    
    def test_integer_scores(self):
        """Test integer scores without decimals."""
        test_cases = [
            ("REASONING: Test\nSCORE: 8", 8.0),
            ("REASONING: Test\nSCORE: 10", 10.0),
            ("REASONING: Test\nSCORE: 0", 0.0),
        ]
        
        for response, expected_score in test_cases:
            score = parse_llm_response(response)
            assert score == expected_score
    
    def test_extra_whitespace(self):
        """Test handling of extra whitespace."""
        test_cases = [
            ("REASONING: Test\nSCORE:     8.5", 8.5),
            ("REASONING: Test\n  SCORE: 7.0  ", 7.0),
            ("REASONING: Test\nSCORE:8.0", 8.0),
        ]
        
        for response, expected_score in test_cases:
            score = parse_llm_response(response)
            assert score == expected_score
    
    def test_boundary_scores(self):
        """Test boundary values 0 and 10."""
        test_cases = [
            ("REASONING: Minimum\nSCORE: 0.0", 0.0),
            ("REASONING: Maximum\nSCORE: 10.0", 10.0),
            ("REASONING: Minimum fraction\nSCORE: 0/10", 0.0),
            ("REASONING: Maximum fraction\nSCORE: 10/10", 10.0),
        ]
        
        for response, expected_score in test_cases:
            score = parse_llm_response(response)
            assert score == expected_score
    
    # Error cases - these should raise ValueError
    
    def test_empty_response(self):
        """Test empty response raises ValueError."""
        with pytest.raises(ValueError, match="Empty or whitespace-only response"):
            parse_llm_response("")
        
        with pytest.raises(ValueError, match="Empty or whitespace-only response"):
            parse_llm_response("   \n  \t  ")
    
    def test_score_without_reasoning(self):
        """Test that responses with only SCORE work fine."""
        score = parse_llm_response("SCORE: 8.5")
        assert score == 8.5
        
        score = parse_llm_response("Some random text\nSCORE: 8.5")
        assert score == 8.5
    
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

    def test_decimal_fraction_score(self):
        """Test parsing decimal fraction scores like 9.5/10."""
        response = """REASONING: Comprehensive analysis of creative loops and mechanisms.
SCORE: 9.5/10  
(Minor room for improvement)"""
        score = parse_llm_response(response)
        assert score == 9.5

    def test_markdown_score_formatting(self):
        """Test parsing all variations of markdown-formatted scores."""
        test_cases = [
            # Markdown labels
            ("**SCORE**: 10.0", 10.0),
            ("**Score**: 8.5", 8.5),
            ("**score**: 7.0", 7.0),
            ("**SCORE**: 9/10", 9.0),
            ("**SCORE**: 8.5/10", 8.5),
            ("**SCORE** - 8.0", 8.0),
            
            # Markdown values
            ("SCORE: **10**", 10.0),
            ("Score: **8.5**", 8.5),
            ("score: **7.0**", 7.0),
            ("SCORE: **9/10**", 9.0),
            ("SCORE: **8.5/10**", 8.5),
            ("SCORE - **8.0**", 8.0),
            
            # Both label and value in markdown
            ("**SCORE**: **10**", 10.0),
            ("**SCORE**: **8.5**", 8.5),
            ("**SCORE**: **9/10**", 9.0),
            ("**SCORE**: **8.5/10**", 8.5),
        ]
        
        for score_text, expected_score in test_cases:
            response = f"REASONING: Good analysis.\n{score_text}\n(Comment)"
            score = parse_llm_response(response)
            assert score == expected_score, f"Failed for: {score_text}"


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
            with pytest.raises(ValueError, match="API key is required"):
                SimpleModelClient()

    @patch('requests.post')
    def test_evaluate_success(self, mock_post):
        """Test successful evaluation."""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "REASONING: Great creativity\nSCORE: 8.5"
                    }
                }
            ]
        }
        mock_post.return_value = mock_response

        client = SimpleModelClient(api_key="test_key")
        result = client.evaluate(
            "Evaluate this response",
            "Sample response content",
            "test-model"
        )

        assert result == "REASONING: Great creativity\nSCORE: 8.5"
        
        # Verify API call was made correctly
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        
        assert call_args[1]['headers']['Authorization'] == 'Bearer test_key'
        assert call_args[1]['json']['model'] == 'test-model'
        assert 'Evaluate this response' in call_args[1]['json']['messages'][0]['content']
        assert 'Sample response content' in call_args[1]['json']['messages'][0]['content']

    @patch('requests.post')
    def test_evaluate_api_error(self, mock_post):
        """Test API error handling."""
        # Mock API error response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_post.return_value = mock_response

        client = SimpleModelClient(api_key="invalid_key")
        
        with pytest.raises(Exception, match="API request failed with status 401"):
            client.evaluate(
                "Evaluate this response",
                "Sample response content", 
                "test-model"
            )

    @patch('requests.post')
    def test_evaluate_no_choices(self, mock_post):
        """Test handling of response without choices."""
        # Mock response without choices
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"error": "No choices available"}
        mock_post.return_value = mock_response

        client = SimpleModelClient(api_key="test_key")
        
        with pytest.raises(Exception, match="No choices in API response"):
            client.evaluate(
                "Evaluate this response",
                "Sample response content",
                "test-model"
            )

    @patch('requests.post')
    def test_evaluate_empty_choices(self, mock_post):
        """Test handling of response with empty choices array."""
        # Mock response with empty choices
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"choices": []}
        mock_post.return_value = mock_response

        client = SimpleModelClient(api_key="test_key")
        
        with pytest.raises(Exception, match="No choices in API response"):
            client.evaluate(
                "Evaluate this response",
                "Sample response content",
                "test-model"
            )

    @patch('requests.post')
    def test_evaluate_request_exception(self, mock_post):
        """Test handling of request exceptions."""
        import requests
        mock_post.side_effect = requests.ConnectionError("Connection failed")

        client = SimpleModelClient(api_key="test_key")
        
        with pytest.raises(requests.ConnectionError):
            client.evaluate(
                "Evaluate this response",
                "Sample response content",
                "test-model"
            )

    def test_evaluate_prompt_combination(self):
        """Test that evaluation prompt and response content are properly combined."""
        with patch('requests.post') as mock_post:
            # Mock successful API response
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "choices": [{"message": {"content": "SCORE: 8.0"}}]
            }
            mock_post.return_value = mock_response

            client = SimpleModelClient(api_key="test_key")
            client.evaluate(
                "Please evaluate this response for creativity:",
                "This is a creative response about innovation.",
                "test-model"
            )

            # Check that the prompt was properly constructed
            call_args = mock_post.call_args
            sent_prompt = call_args[1]['json']['messages'][0]['content']
            
            assert "Please evaluate this response for creativity:" in sent_prompt
            assert "Response to evaluate:" in sent_prompt
            assert "This is a creative response about innovation." in sent_prompt
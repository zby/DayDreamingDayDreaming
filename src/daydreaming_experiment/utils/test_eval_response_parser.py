"""Tests for evaluation response parsing utilities."""

import pytest

from daydreaming_experiment.utils.eval_response_parser import parse_llm_response


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
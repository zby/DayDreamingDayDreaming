"""Tests for evaluation response parsing utilities."""

import pytest

from daydreaming_dagster.utils.eval_response_parser import parse_llm_response

pytestmark = [pytest.mark.unit]

class TestParseLLMResponseComplex:
    """Test the parse_llm_response function with the 'complex' strategy."""

    def test_standard_format(self):
        """Test parsing standard REASONING/SCORE format."""
        response = "REASONING: This shows great creativity\nSCORE: 8.5"
        result = parse_llm_response(response, "complex")
        assert result["score"] == 8.5
        assert result["error"] is None

    def test_multiline_reasoning(self):
        """Test parsing multiline reasoning."""
        response = """REASONING: This response demonstrates creativity
because it combines multiple concepts in novel ways.
The ideas are well-structured and innovative.
SCORE: 7.2"""
        result = parse_llm_response(response, "complex")
        assert result["score"] == 7.2
        assert result["error"] is None

    def test_case_variations(self):
        """Test different case variations."""
        test_cases = [
            ("reasoning: Good analysis\nscore: 6.0", 6.0),
            ("Reasoning: Excellent work\nScore: 9.5", 9.5),
            ("REASONING: Basic response\nSCORE: 4.0", 4.0),
        ]

        for response, expected_score in test_cases:
            result = parse_llm_response(response, "complex")
            assert result["score"] == expected_score
            assert result["error"] is None

    def test_alternative_separators(self):
        """Test alternative separators like dashes."""
        response = "REASONING - Great creativity shown\nSCORE: 8.0"
        result = parse_llm_response(response, "complex")
        assert result["score"] == 8.0
        assert result["error"] is None

    def test_score_with_explanation(self):
        """Test score followed by explanation in parentheses."""
        response = "REASONING: Shows innovation\nSCORE: 7.5 (above average creativity)"
        result = parse_llm_response(response, "complex")
        assert result["score"] == 7.5
        assert result["error"] is None

    def test_score_as_fraction(self):
        """Test score in fraction format like 8/10."""
        response = "REASONING: Good work\nSCORE: 8/10"
        result = parse_llm_response(response, "complex")
        assert result["score"] == 8.0
        assert result["error"] is None

    def test_no_score_found(self):
        """Test when no valid score is found."""
        response = "REASONING: Good analysis but no score provided"
        with pytest.raises(ValueError, match="No SCORE field found in response"):
            parse_llm_response(response, "complex")

    def test_invalid_score_value(self):
        """Test when score value is not a valid number."""
        response = "REASONING: Analysis done\nSCORE: not_a_number"
        with pytest.raises(ValueError, match="No SCORE field found in response"):
            parse_llm_response(response, "complex")

    def test_score_out_of_range(self):
        """Test handling of scores outside 0-10 range."""
        test_cases = [
            ("REASONING: Test\nSCORE: -2.5", "Score -2.5 is outside valid range 0-10"),
            ("REASONING: Test\nSCORE: 15.0", "Score 15.0 is outside valid range 0-10"),
        ]

        for response, expected_error in test_cases:
            with pytest.raises(ValueError, match=expected_error):
                parse_llm_response(response, "complex")

    def test_multiple_score_lines(self):
        """Test when multiple SCORE lines exist - should use last one (most recent/final)."""
        response = """REASONING: Analysis
SCORE: 7.5
SCORE: 8.0
Additional text"""
        result = parse_llm_response(response, "complex")
        assert result["score"] == 8.0  # Uses last/final score
        assert result["error"] is None

    def test_total_score_pattern_priority(self):
        """Test that Total Score patterns take priority over intermediate scores."""
        response = """
        ### **Core Concepts Identified**  
        **1. The Problem: Static LLMs**  
        - **Score**: **1/1** (conceptual alignment exists but lacks exact phrasing).  
        
        **2. The Proposed Solution: "Daydreaming Loop" (DDL)**  
        - **Score**: **1/1** (mechanism is present but not explicitly named).  
        
        ### **SCORE:**  
        **Total Score: 7/10**
        """
        result = parse_llm_response(response, "complex")
        assert result["score"] == 7.0
        assert result["error"] is None

    def test_avoids_intermediate_1_over_1_scores(self):
        """Test parser avoids matching intermediate 1/1 style breakdown scores."""
        # This was the bug: parser was matching **Score**: **1/1** and interpreting as (1/1)*10 = 10.0
        response = """
        **1. The Problem: Static LLMs**  
        - **Score**: **1/1** (conceptual alignment exists).  
        
        **2. The Solution**  
        - **Score**: **1/1** (mechanism is present).  
        
        **3. The Mechanism**  
        - **Score**: **1/1** (generator/critic present).  
        
        **4. Economic Implications**  
        - **Score**: **0/1** (no explicit references).  
        
        **Total for Core Concepts**: **3/5**  
        
        ### **Connections Between Concepts**  
        **1. Problem → Solution**  
        - **Score**: **1/1** (logically connected).  
        
        **Total for Connections**: **1/5**  
        
        ### **SCORE:**  
        **Total Score: 4/10**
        """
        result = parse_llm_response(response, "complex")
        # Should extract 4.0 from "Total Score: 4/10", NOT 10.0 from any 1/1 patterns
        assert result["score"] == 4.0

    def test_total_score_markdown_formats(self):
        """Test various Total Score markdown formats."""
        test_cases = [
            ("Total Score: 6/10", 6.0),
            ("**Total Score:** 7/10", 7.0), 
            ("Total Score: **8/10**", 8.0),
            ("**Total Score:** **9/10**", 9.0),
            ("Total Score: 5", 5.0),
            ("**Total Score:** 3", 3.0),
        ]
        
        for response, expected_score in test_cases:
            result = parse_llm_response(response, "complex")
            assert result["score"] == expected_score, f"Failed for response: {response}"

    def test_real_evaluation_response_format(self):
        """Test parsing the actual format from our evaluation responses."""
        # This is the exact format from our actual evaluation files
        response = """
        **REASONING:**  
        The text partially reproduces core concepts from "AI Daydreaming" but lacks precise terminology.

        ### **Core Concepts Identified**  
        **1. The Problem: Static LLMs**  
        - **Score**: **1/1** (conceptual alignment exists but lacks exact phrasing).  

        **2. The Proposed Solution: "Daydreaming Loop" (DDL)**  
        - **Score**: **1/1** (mechanism is present but not explicitly named).  

        **3. The Mechanism of Daydreaming**  
        - **Score**: **2/2** (both generator/critic and feedback loop are present).  

        **4. Economic/Strategic Implications**  
        - **Score**: **0/1** (no explicit references).  

        **Total for Core Concepts**: **4/5**  

        ### **Connections Between Concepts**  
        **1. Problem → Solution**  
        - **Score**: **1/1** (logically connected but under a different name).  

        **2. Mechanism → Feedback Loop**  
        - **Score**: **1/1** (explicit feedback connection exists).  

        **3. Process → Economics**  
        - **Score**: **0/1** (no link is made).  

        **4. Coherent Narrative Arc**  
        - **Score**: **1/2** (partial coherence due to missing economic links).  

        **Total for Connections**: **3/5**  

        ### **SCORE:**  
        **Total Score: 7/10**  

        **Summary:**  
        The text captures the problem, solution, and mechanism but lacks terminology.
        """
        result = parse_llm_response(response, "complex")
        # Should extract 7.0 from final "Total Score: 7/10", not 10.0 from any 1/1 patterns
        assert result["score"] == 7.0


class TestParseLLMResponseInLastLine:
    """Test the parse_llm_response function with the 'in_last_line' strategy."""

    @pytest.mark.parametrize(
        "response, expected_score",
        [
            ("Some text here\n**SCORE: 8**", 8.0),
            ("Some text here\n*SCORE: 8*", 8.0),
            ("Some text here\n**Score:** 5", 5.0),
            ("Some text here\nTotal Score: 6", 6.0),
            ("Some text here\nScore: 456", 5.0),
            ("Some text here\nScore: 4,5,6", 5.0),
        ],
    )
    def test_score_in_last_line_variations(self, response, expected_score):
        """Test parsing various score formats in the last line."""
        result = parse_llm_response(response, "in_last_line")
        assert result["score"] == expected_score
        assert result["error"] is None

    def test_score_with_markdown_formatting_and_explanation(self):
        """Test parsing score in markdown format with explanation in parentheses."""
        response = """The text clearly addresses the conceptual framework and architectural synthesis (core to the AI Daydreaming thesis). It aligns with the strategic rationale but lacks explicit alignment with the "data wall," "moat," and "tax" terminology. However, the functional goals (self-sustaining knowledge creation, proprietary outputs) are present.  

**SCORE: 7**  
*(The text describes the core concept and architecture but falls short in explicitly framing the strategic rationale with the exact terminology of the thesis.)*"""
        result = parse_llm_response(response, "in_last_line")
        assert result["score"] == 7.0
        assert result["error"] is None

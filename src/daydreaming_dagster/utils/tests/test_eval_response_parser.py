"""Tests for evaluation response parsing utilities."""

import pytest

from daydreaming_dagster.utils.errors import DDError, Err
from daydreaming_dagster.utils.eval_response_parser import parse_llm_response

pytestmark = [pytest.mark.unit]


class TestParseLLMResponseInLastLine:
    """Verify the consolidated evaluation parser behaviour."""

    @pytest.mark.parametrize(
        "response, expected_score",
        [
            ("REASONING: solid\nSCORE: 7", 7.0),
            ("something\nSCORE: 4.5", 4.5),
            ("notes\nSCORE: 9", 9.0),
            ("bullet\nSCORE: 0", 0.0),
        ],
    )
    def test_exact_score_line(self, response, expected_score):
        result = parse_llm_response(response, "in_last_line")
        assert result["score"] == expected_score
        assert result["error"] is None

    def test_markdown_and_spacing_tolerated(self):
        response = """Reasoning paragraph.

**Final thoughts.**

**SCORE: 8**
"""
        result = parse_llm_response(response, "in_last_line")
        assert result["score"] == 8.0

    def test_three_digit_average_supported(self):
        response = "Analysis line\nScore: 456"
        result = parse_llm_response(response, "in_last_line")
        assert result["score"] == pytest.approx(5.0)

    def test_standard_numeric_parser_fallback(self):
        response = "Reasoning\nTotal Score: 6.5"
        result = parse_llm_response(response, "in_last_line")
        assert result["score"] == 6.5

    def test_missing_score_raises(self):
        with pytest.raises(DDError) as err:
            parse_llm_response("Reasoning only", "in_last_line")
        assert err.value.code is Err.PARSER_FAILURE

    def test_out_of_range_score_raises(self):
        with pytest.raises(DDError) as err:
            parse_llm_response("Thoughts\nSCORE: 12", "in_last_line")
        assert err.value.code is Err.PARSER_FAILURE

    def test_empty_response_rejected(self):
        with pytest.raises(DDError) as err:
            parse_llm_response("   \n  ", "in_last_line")
        assert err.value.code is Err.PARSER_FAILURE

    def test_unknown_strategy_rejected(self):
        with pytest.raises(DDError) as err:
            parse_llm_response("SCORE: 5", "unknown")
        assert err.value.code is Err.INVALID_CONFIG

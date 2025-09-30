"""Additional parser tests covering richer response shapes."""

from __future__ import annotations

import textwrap

import pytest

from daydreaming_dagster.utils.errors import DDError, Err
from daydreaming_dagster.utils.eval_response_parser import parse_llm_response

pytestmark = [pytest.mark.unit]


SAMPLE_RESPONSES: dict[str, tuple[str, float]] = {
    "creative": (
        textwrap.dedent(
            """
            **REASONING:** The proposal introduces a tri-agent studio where a generator explores
            cross-domain pairings while a critic grades novelty. A curator agent integrates the
            best variants into a shared library, enabling future runs to start from higher baselines.

            Highlights:
            - Explicit generator/critic separation.
            - Cross-domain synthesis between photonics, agriculture, and bio-sensing.
            - Concrete mechanism for commercialisation pathways.

            SCORE: 8
            """
        ).strip(),
        8.0,
    ),
    "scientific": (
        textwrap.dedent(
            """
            REASONING: The response frames a hypothesis, specifies intervention and control arms,
            and outlines measurement cadence. It logs uncertainty about sensor drift and proposes
            calibration runs to quantify noise before deployment.

            SCORE: 6.5
            """
        ).strip(),
        6.5,
    ),
    "iterative": (
        textwrap.dedent(
            """
            ### Notes
            - Loop tiers escalate successful ideas from sketch → prototype → pilot.
            - Failures trigger a mutation policy and are archived for later meta-analysis.
            - Resource scheduler automatically widens search when progress stalls for 3 cycles.

            SCORE: 7
            """
        ).strip(),
        7.0,
    ),
}


class TestEvalResponseParserSamples:
    @pytest.mark.parametrize("sample_id", SAMPLE_RESPONSES.keys())
    def test_known_samples(self, sample_id: str):
        text, expected = SAMPLE_RESPONSES[sample_id]
        result = parse_llm_response(text, "in_last_line")
        assert result["score"] == expected
        assert result["error"] is None

    def test_missing_score_line(self):
        text = "REASONING: thorough audit without explicit score line"
        with pytest.raises(DDError) as err:
            parse_llm_response(text, "in_last_line")
        assert err.value.code is Err.PARSER_FAILURE
        assert err.value.ctx.get("reason") == "no_score_last_lines"
